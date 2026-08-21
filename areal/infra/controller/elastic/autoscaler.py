# SPDX-License-Identifier: Apache-2.0

"""Stateful policy shared by internal and external rollout autoscalers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


class AutoscalerError(RuntimeError):
    """Raised when an elastic scaling report or status is malformed."""


@dataclass(frozen=True)
class AutoscalerDecision:
    """One side-effect-free decision produced from a report and controller status."""

    action: Literal["ignore", "discard", "defer", "apply"]
    message: str
    direction: Literal["scale_up", "scale_down"] | None = None

    @property
    def should_apply(self) -> bool:
        return self.action == "apply"


@dataclass
class ScaleDownConfirmation:
    """Require adjacent low-load windows before removing one instance."""

    required_windows: int
    streak: int = 0
    last_window_end: int | None = None

    def __post_init__(self) -> None:
        if self.required_windows <= 0:
            raise ValueError("required_windows must be positive")

    def reset(self) -> None:
        self.streak = 0
        self.last_window_end = None

    def observe(
        self,
        report: dict[str, Any],
        *,
        desired: int,
        window_start: int,
        report_version: int,
    ) -> tuple[bool, int]:
        if not (
            report.get("branch") == "scale_down"
            and report.get("recommended_instances") == desired - 1
        ):
            self.reset()
            return False, 0

        window_end = _required_int(
            report,
            "window_end_version",
            default=report_version,
            subject="Scale-down report",
        )
        self.streak = (
            self.streak + 1
            if self.last_window_end is not None
            and window_start == self.last_window_end + 1
            else 1
        )
        self.last_window_end = window_end
        observed = self.streak
        confirmed = observed >= self.required_windows
        if confirmed:
            self.reset()
        return confirmed, observed


class ElasticAutoscalerPolicy:
    """Reject observations that cannot safely drive the current topology.

    The policy deliberately has no HTTP, sleep, or Scheduler behavior. The caller owns
    observation and actuation, while this object owns restart watermarks, cooldown,
    convergence state, and conservative scale-down confirmation.
    """

    def __init__(
        self,
        *,
        cooldown_seconds: float | None = None,
        scale_up_cooldown_seconds: float = 0.0,
        scale_down_cooldown_seconds: float = 30.0,
        direction_change_cooldown_seconds: float = 30.0,
        scale_down_windows: int,
    ) -> None:
        cooldowns = {
            "cooldown_seconds": cooldown_seconds,
            "scale_up_cooldown_seconds": scale_up_cooldown_seconds,
            "scale_down_cooldown_seconds": scale_down_cooldown_seconds,
            "direction_change_cooldown_seconds": direction_change_cooldown_seconds,
        }
        if any(value is not None and value < 0 for value in cooldowns.values()):
            raise ValueError("cooldown seconds must be non-negative")
        self._cooldown_seconds = cooldown_seconds
        self._scale_up_cooldown_seconds = scale_up_cooldown_seconds
        self._scale_down_cooldown_seconds = scale_down_cooldown_seconds
        self._direction_change_cooldown_seconds = direction_change_cooldown_seconds
        self._scale_down = ScaleDownConfirmation(scale_down_windows)
        self._last_report_version: int | None = None
        self._last_action_at = 0.0
        self._last_action_direction: Literal["scale_up", "scale_down"] | None = None
        self._minimum_window_start_version = 0
        self._last_desired_instances: int | None = None
        self._capacity_was_stable = False

    def evaluate(
        self,
        report: dict[str, Any],
        status: dict[str, Any],
        *,
        capacity_stable: bool,
        stability_detail: str,
        now: float,
    ) -> AutoscalerDecision:
        """Consume a report once and decide whether the caller may apply it."""
        report_version = _required_int(
            report, "report_version", subject="Scaling report"
        )
        if (
            self._last_report_version is not None
            and report_version <= self._last_report_version
        ):
            return AutoscalerDecision("ignore", "report version already consumed")
        # Consume before checking cooldown or topology. An invalid observation must not
        # be replayed later after the condition that invalidated it has cleared.
        self._last_report_version = report_version

        window_start = _required_int(
            report, "window_start_version", subject="Scaling report"
        )
        desired = _required_int(status, "desired_instances", subject="Instance status")
        serving_version = _required_int(
            status, "serving_version", subject="Instance status"
        )

        if self._last_desired_instances is None:
            self._reset_scale_down()
            self._last_desired_instances = desired
            self._capacity_was_stable = capacity_stable
            self._minimum_window_start_version = serving_version
            return AutoscalerDecision(
                "discard",
                f"establishing startup watermark={serving_version}",
            )
        if desired != self._last_desired_instances:
            self._reset_scale_down()
            self._last_action_at = 0.0
            self._last_action_direction = None
            self._last_desired_instances = desired
            self._capacity_was_stable = capacity_stable
            self._minimum_window_start_version = serving_version
            return AutoscalerDecision(
                "discard",
                f"desired capacity changed; new watermark={serving_version}",
            )
        if not capacity_stable:
            self._reset_scale_down()
            self._capacity_was_stable = False
            self._minimum_window_start_version = max(
                self._minimum_window_start_version, serving_version
            )
            return AutoscalerDecision(
                "discard", f"capacity is not stable: {stability_detail}"
            )
        if not self._capacity_was_stable:
            self._reset_scale_down()
            self._capacity_was_stable = True
            self._minimum_window_start_version = serving_version
            return AutoscalerDecision(
                "discard",
                f"establishing post-convergence watermark={serving_version}",
            )
        if report.get("ready_instances") != desired:
            self._reset_scale_down()
            return AutoscalerDecision(
                "discard",
                "stale capacity: "
                f"report_ready={report.get('ready_instances')} current_ready={desired}",
            )
        if window_start <= self._minimum_window_start_version:
            self._reset_scale_down()
            return AutoscalerDecision(
                "discard",
                f"window started at version={window_start} before "
                f"watermark={self._minimum_window_start_version}",
            )

        direction = _action_direction(report, desired)
        if direction is not None:
            cooldown = self._cooldown_for(direction)
            if now - self._last_action_at < cooldown:
                self._reset_scale_down()
                return AutoscalerDecision(
                    "discard",
                    f"observed during {direction} cooldown: "
                    f"last_direction={self._last_action_direction} "
                    f"cooldown_seconds={cooldown:.1f}",
                    direction,
                )

        confirmed, observed = self._scale_down.observe(
            report,
            desired=desired,
            window_start=window_start,
            report_version=report_version,
        )
        if observed and not confirmed:
            return AutoscalerDecision(
                "defer",
                f"scale-down confirmation {observed}/"
                f"{self._scale_down.required_windows}",
            )
        return AutoscalerDecision(
            "apply", "report is valid for current topology", direction
        )

    def record_convergence(
        self,
        status: dict[str, Any],
        *,
        now: float,
        action_direction: Literal["scale_up", "scale_down"] | None = None,
    ) -> None:
        """Advance the watermark after a desired-state change has converged."""
        self._reset_scale_down()
        self._last_action_at = now
        self._last_action_direction = action_direction
        self._minimum_window_start_version = _required_int(
            status, "serving_version", subject="Converged instance status"
        )
        self._last_desired_instances = _required_int(
            status, "desired_instances", subject="Converged instance status"
        )
        self._capacity_was_stable = True

    def _reset_scale_down(self) -> None:
        self._scale_down.reset()

    def _cooldown_for(self, direction: Literal["scale_up", "scale_down"]) -> float:
        if self._cooldown_seconds is not None:
            return self._cooldown_seconds
        if (
            self._last_action_direction is not None
            and direction != self._last_action_direction
        ):
            return self._direction_change_cooldown_seconds
        if direction == "scale_up":
            return self._scale_up_cooldown_seconds
        return self._scale_down_cooldown_seconds


def _action_direction(
    report: dict[str, Any], desired: int
) -> Literal["scale_up", "scale_down"] | None:
    recommended = _required_int(
        report, "recommended_instances", subject="Scaling report"
    )
    if recommended > desired:
        return "scale_up"
    if recommended < desired:
        return "scale_down"
    return None


def _required_int(
    payload: dict[str, Any],
    field: str,
    *,
    subject: str,
    default: int | None = None,
) -> int:
    value = payload.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AutoscalerError(f"{subject} has no integer {field}: {payload!r}")
    return value
