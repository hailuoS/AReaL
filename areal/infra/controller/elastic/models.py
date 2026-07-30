# SPDX-License-Identifier: Apache-2.0

"""Stable identity and lifecycle models for elastic rollout instances."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import InvalidStateTransitionError


class RolloutInstanceState(str, Enum):
    """Observed lifecycle state of one complete TP × PP rollout instance."""

    PENDING = "pending"
    STARTING = "starting"
    CATCHING_UP = "catching_up"
    READY = "ready"
    DRAINING = "draining"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class InstanceDesiredState(str, Enum):
    """Desired lifecycle state assigned by the reconciler."""

    RUNNING = "running"
    STOPPED = "stopped"


_ALLOWED_TRANSITIONS: dict[RolloutInstanceState, frozenset[RolloutInstanceState]] = {
    RolloutInstanceState.PENDING: frozenset(
        {
            RolloutInstanceState.STARTING,
            RolloutInstanceState.STOPPED,
            RolloutInstanceState.FAILED,
        }
    ),
    RolloutInstanceState.STARTING: frozenset(
        {
            RolloutInstanceState.CATCHING_UP,
            RolloutInstanceState.READY,
            RolloutInstanceState.STOPPING,
            RolloutInstanceState.FAILED,
        }
    ),
    RolloutInstanceState.CATCHING_UP: frozenset(
        {
            RolloutInstanceState.READY,
            RolloutInstanceState.STOPPING,
            RolloutInstanceState.FAILED,
        }
    ),
    RolloutInstanceState.READY: frozenset(
        {
            RolloutInstanceState.DRAINING,
            RolloutInstanceState.STOPPING,
            RolloutInstanceState.FAILED,
        }
    ),
    RolloutInstanceState.DRAINING: frozenset(
        {
            RolloutInstanceState.READY,
            RolloutInstanceState.STOPPING,
            RolloutInstanceState.FAILED,
        }
    ),
    RolloutInstanceState.STOPPING: frozenset(
        {
            RolloutInstanceState.STOPPED,
            RolloutInstanceState.FAILED,
        }
    ),
    RolloutInstanceState.STOPPED: frozenset(),
    RolloutInstanceState.FAILED: frozenset(
        {
            RolloutInstanceState.STOPPING,
            RolloutInstanceState.STOPPED,
        }
    ),
}


@dataclass(frozen=True)
class RolloutRPCTarget:
    """Immutable Scheduler RPC address for an instance at snapshot time."""

    instance_id: str
    worker_id: str
    engine_name: str


@dataclass
class RolloutInstance:
    """Mutable controller-owned state for one complete TP × PP instance."""

    instance_id: str
    worker_role: str
    worker_id: str
    engine_name: str
    state: RolloutInstanceState = RolloutInstanceState.PENDING
    desired_state: InstanceDesiredState = InstanceDesiredState.RUNNING
    loaded_version: int | None = None
    workflow_task_ids: set[str] = field(default_factory=set)
    direct_inflight: int = 0
    active_sessions: int = 0
    update_leases: int = 0

    def __post_init__(self) -> None:
        for field_name in ("instance_id", "worker_role", "worker_id", "engine_name"):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must not be empty")
        if self.loaded_version is not None and self.loaded_version < 0:
            raise ValueError("loaded_version must be non-negative")
        for field_name in ("direct_inflight", "active_sessions", "update_leases"):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name} must be non-negative")

    @property
    def rpc_target(self) -> RolloutRPCTarget:
        """Return an immutable RPC target detached from mutable instance state."""
        return RolloutRPCTarget(
            instance_id=self.instance_id,
            worker_id=self.worker_id,
            engine_name=self.engine_name,
        )

    @property
    def is_routable(self) -> bool:
        """Whether this instance may accept new inference work."""
        return (
            self.state is RolloutInstanceState.READY
            and self.desired_state is InstanceDesiredState.RUNNING
        )

    @property
    def is_drained(self) -> bool:
        """Whether every tracked source of in-flight work has reached zero."""
        return (
            not self.workflow_task_ids
            and self.direct_inflight == 0
            and self.active_sessions == 0
            and self.update_leases == 0
        )

    @property
    def can_stop(self) -> bool:
        """Whether a draining instance may start resource teardown."""
        return (
            self.state is RolloutInstanceState.DRAINING
            and self.desired_state is InstanceDesiredState.STOPPED
            and self.is_drained
        )

    @property
    def is_removable(self) -> bool:
        """Whether this instance may be removed from the in-memory registry."""
        return (
            self.state is RolloutInstanceState.STOPPED
            and self.desired_state is InstanceDesiredState.STOPPED
            and self.is_drained
        )

    def transition_to(self, new_state: RolloutInstanceState) -> None:
        """Move to a legal observed lifecycle state."""
        if new_state is self.state:
            return
        if new_state not in _ALLOWED_TRANSITIONS[self.state]:
            raise InvalidStateTransitionError(
                f"instance {self.instance_id} cannot transition from "
                f"{self.state.value} to {new_state.value}"
            )
        self.state = new_state

    def request_drain(self) -> None:
        """Stop accepting new work and declare the desired stopped state."""
        self.desired_state = InstanceDesiredState.STOPPED
        if self.state is RolloutInstanceState.READY:
            self.transition_to(RolloutInstanceState.DRAINING)

    def cancel_drain(self) -> None:
        """Return a draining instance to service before teardown starts."""
        if self.state is not RolloutInstanceState.DRAINING:
            raise InvalidStateTransitionError(
                f"instance {self.instance_id} is not draining"
            )
        self.desired_state = InstanceDesiredState.RUNNING
        self.transition_to(RolloutInstanceState.READY)
