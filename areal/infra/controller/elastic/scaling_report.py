# SPDX-License-Identifier: Apache-2.0

"""AstraFlow-compatible, report-only rollout scaling recommendation."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ElasticScalingWindow:
    """One training-side observation window supplied by the caller."""

    ready_instances: int
    entered: int
    consumed: int
    wait_seconds: float
    step_seconds: float


@dataclass(frozen=True)
class ElasticScalingRecommendation:
    """Report-only desired instance recommendation."""

    branch: str
    recommended_instances: int
    rollout_wait_fraction: float


@dataclass
class ElasticScalingReporter:
    """Accumulate exact per-step samples and emit version-aligned reports."""

    report_frequency_steps: int
    min_instances: int
    max_instances: int
    _window_start_version: int | None = None
    _window_iterations: int = 0
    _timing_samples: int = 0
    _entered: int = 0
    _consumed: int = 0
    _wait_seconds: float = 0.0
    _step_seconds: float = 0.0
    _last_observed_version: int = 0

    def __post_init__(self) -> None:
        if self.report_frequency_steps < 0:
            raise ValueError("report_frequency_steps must be non-negative")
        if self.min_instances < 1 or self.max_instances < self.min_instances:
            raise ValueError("invalid elastic instance bounds")

    def record(
        self,
        *,
        report_version: int,
        ready_instances: int,
        entered: int,
        consumed: int,
        wait_seconds: float,
        step_seconds: float,
    ) -> dict[str, int | float | str] | None:
        """Add one completed training version and maybe close the window."""
        if report_version <= self._last_observed_version:
            raise ValueError(
                "report_version must increase monotonically: "
                f"last={self._last_observed_version}, got={report_version}"
            )
        if entered < 0 or consumed < 0:
            raise ValueError("entered and consumed must be non-negative")
        if wait_seconds < 0 or step_seconds < 0:
            raise ValueError("wait_seconds and step_seconds must be non-negative")

        self._last_observed_version = report_version
        if self.report_frequency_steps == 0:
            return None
        if self._window_start_version is None:
            self._window_start_version = report_version
        self._window_iterations += 1
        self._entered += entered
        self._consumed += consumed
        # AstraFlow drops the first timing sample after startup/eval because it
        # has no previous batch-completion timestamp to form a paired step time.
        if step_seconds > 0:
            self._timing_samples += 1
            self._wait_seconds += wait_seconds
            self._step_seconds += step_seconds

        if report_version % self.report_frequency_steps != 0:
            return None

        window = ElasticScalingWindow(
            ready_instances=ready_instances,
            entered=self._entered,
            consumed=self._consumed,
            wait_seconds=self._wait_seconds,
            step_seconds=self._step_seconds,
        )
        recommendation = recommend_instances(
            window,
            min_instances=self.min_instances,
            max_instances=self.max_instances,
        )
        report: dict[str, int | float | str] = {
            "report_version": report_version,
            "window_start_version": self._window_start_version,
            "window_end_version": report_version,
            "window_iterations": self._window_iterations,
            "timing_samples": self._timing_samples,
            "branch": recommendation.branch,
            "recommended_instances": recommendation.recommended_instances,
            "rollout_wait_fraction": recommendation.rollout_wait_fraction,
            "entered": self._entered,
            "consumed": self._consumed,
            "ready_instances": ready_instances,
            "wait_seconds": self._wait_seconds,
            "step_seconds": self._step_seconds,
            "avg_batch_wait_seconds": (
                self._wait_seconds / self._timing_samples
                if self._timing_samples > 0
                else 0.0
            ),
            "avg_step_seconds": (
                self._step_seconds / self._timing_samples
                if self._timing_samples > 0
                else 0.0
            ),
        }
        self._reset_window()
        return report

    def _reset_window(self) -> None:
        self._window_start_version = None
        self._window_iterations = 0
        self._timing_samples = 0
        self._entered = 0
        self._consumed = 0
        self._wait_seconds = 0.0
        self._step_seconds = 0.0


def recommend_instances(
    window: ElasticScalingWindow, *, min_instances: int, max_instances: int
) -> ElasticScalingRecommendation:
    """Apply AstraFlow's 0.05/0.10 dead-band to complete instances."""
    if window.ready_instances < 1:
        raise ValueError("ready_instances must be at least 1")
    wait_fraction = (
        min(window.wait_seconds / window.step_seconds, 0.95)
        if window.step_seconds > 0
        else 0.0
    )
    if wait_fraction > 0.10:
        branch = "scale_up"
        target = math.ceil(window.ready_instances / (1.0 - wait_fraction))
    elif wait_fraction < 0.05 and window.entered > 0 and window.consumed > 0:
        branch = "scale_down"
        target = min(
            window.ready_instances,
            math.ceil(window.ready_instances * window.consumed / window.entered * 1.10),
        )
    else:
        branch = "hold"
        target = window.ready_instances
    return ElasticScalingRecommendation(
        branch=branch,
        recommended_instances=max(min_instances, min(max_instances, target)),
        rollout_wait_fraction=wait_fraction,
    )
