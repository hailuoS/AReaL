# SPDX-License-Identifier: Apache-2.0

"""Thread-safe in-memory registry for elastic rollout instances."""

from __future__ import annotations

import threading
import uuid
from copy import deepcopy

from .errors import (
    DuplicateInstanceError,
    InstanceNotFoundError,
    InstanceNotReadyError,
    InstanceNotRemovableError,
    InvalidDesiredCountError,
    TaskBindingError,
)
from .models import (
    InstanceDesiredState,
    RolloutInstance,
    RolloutInstanceState,
    RolloutRPCTarget,
)


class RolloutInstancePool:
    """Own stable instance identities, lifecycle state, and in-flight counters."""

    def __init__(
        self,
        *,
        min_instances: int,
        initial_instances: int,
        max_instances: int,
    ) -> None:
        if min_instances < 1:
            raise InvalidDesiredCountError("min_instances must be at least 1")
        if not min_instances <= initial_instances <= max_instances:
            raise InvalidDesiredCountError(
                "instance bounds must satisfy "
                "min_instances <= initial_instances <= max_instances"
            )
        self._min_instances = min_instances
        self._max_instances = max_instances
        self._desired_count = initial_instances
        self._instances: dict[str, RolloutInstance] = {}
        self._task_to_instance: dict[str, str] = {}
        self._selection_index = 0
        self._lock = threading.RLock()

    @property
    def desired_count(self) -> int:
        """Return the current desired number of complete rollout instances."""
        with self._lock:
            return self._desired_count

    def set_desired_count(self, desired_count: int) -> None:
        """Set desired capacity within the configured instance bounds."""
        if not self._min_instances <= desired_count <= self._max_instances:
            raise InvalidDesiredCountError(
                f"desired_count must be in [{self._min_instances}, "
                f"{self._max_instances}], got {desired_count}"
            )
        with self._lock:
            self._desired_count = desired_count

    def create(
        self,
        *,
        worker_role: str,
        worker_id: str,
        engine_name: str,
        instance_id: str | None = None,
    ) -> RolloutInstance:
        """Create and register an instance with a stable non-positional identity."""
        instance = RolloutInstance(
            instance_id=instance_id or f"ri-{uuid.uuid4().hex}",
            worker_role=worker_role,
            worker_id=worker_id,
            engine_name=engine_name,
        )
        self.add(instance)
        return instance

    def add(self, instance: RolloutInstance) -> None:
        """Register a preconstructed instance."""
        with self._lock:
            if instance.instance_id in self._instances:
                raise DuplicateInstanceError(
                    f"instance {instance.instance_id} is already registered"
                )
            self._instances[instance.instance_id] = instance

    def get(self, instance_id: str) -> RolloutInstance:
        """Return a registered instance or raise a domain-specific error."""
        with self._lock:
            try:
                return self._instances[instance_id]
            except KeyError as exc:
                raise InstanceNotFoundError(
                    f"instance {instance_id} is not registered"
                ) from exc

    def remove(self, instance_id: str) -> RolloutInstance:
        """Remove a fully stopped and drained instance."""
        with self._lock:
            instance = self.get(instance_id)
            if not instance.is_removable:
                raise InstanceNotRemovableError(
                    f"instance {instance_id} is not stopped and drained"
                )
            del self._instances[instance_id]
            return instance

    def instance_ids(self) -> tuple[str, ...]:
        """Return a stable snapshot of registered instance identities."""
        with self._lock:
            return tuple(self._instances)

    def instances_snapshot(self) -> tuple[RolloutInstance, ...]:
        """Return detached instance state for status, recovery, and GC decisions."""
        with self._lock:
            return tuple(deepcopy(instance) for instance in self._instances.values())

    def ready_snapshot(self) -> tuple[RolloutRPCTarget, ...]:
        """Snapshot instances currently eligible for new inference work."""
        with self._lock:
            return tuple(
                instance.rpc_target for instance in self._routable_instances_unlocked()
            )

    def reserve_task(
        self,
        task_id: str,
        max_inflight_per_instance: int | None = None,
    ) -> RolloutRPCTarget:
        """Atomically bind a workflow task to the least-loaded READY instance."""
        if not task_id:
            raise ValueError("task_id must not be empty")
        with self._lock:
            instance = self._select_least_loaded_unlocked(
                max_inflight_per_instance=max_inflight_per_instance,
            )
            self.bind_task(task_id, instance.instance_id)
            self._selection_index += 1
            return instance.rpc_target

    def reserve_direct_request(
        self,
        max_inflight_per_instance: int | None = None,
    ) -> RolloutRPCTarget:
        """Atomically lease the least-loaded READY instance for a direct request."""
        with self._lock:
            instance = self._select_least_loaded_unlocked(
                max_inflight_per_instance=max_inflight_per_instance,
            )
            instance.direct_inflight += 1
            self._selection_index += 1
            return instance.rpc_target

    def acquire_direct_snapshot(self) -> tuple[RolloutRPCTarget, ...]:
        """Atomically lease every READY target for one collective/direct RPC."""
        with self._lock:
            instances = self._routable_instances_unlocked()
            for instance in instances:
                instance.direct_inflight += 1
            return tuple(instance.rpc_target for instance in instances)

    def acquire_weight_update_snapshot(self) -> tuple[RolloutRPCTarget, ...]:
        """Atomically snapshot and lease every READY disk-update target."""
        with self._lock:
            instances = self._routable_instances_unlocked()
            for instance in instances:
                instance.update_leases += 1
            return tuple(instance.rpc_target for instance in instances)

    def release_direct_snapshot(self, targets: tuple[RolloutRPCTarget, ...]) -> None:
        """Release leases acquired by :meth:`acquire_direct_snapshot`."""
        with self._lock:
            for target in targets:
                self._decrement(self.get(target.instance_id), "direct_inflight")

    def release_weight_update_snapshot(
        self, targets: tuple[RolloutRPCTarget, ...]
    ) -> None:
        """Release leases acquired by :meth:`acquire_weight_update_snapshot`."""
        with self._lock:
            for target in targets:
                self._decrement(self.get(target.instance_id), "update_leases")

    def mark_loaded_version(
        self, targets: tuple[RolloutRPCTarget, ...], version: int
    ) -> None:
        """Record the disk version loaded by a successful target snapshot."""
        if version < 0:
            raise ValueError("version must be non-negative")
        with self._lock:
            for target in targets:
                self.get(target.instance_id).loaded_version = version

    def request_drain(self, instance_id: str) -> RolloutInstance:
        """Atomically remove one instance from routing eligibility."""
        with self._lock:
            instance = self.get(instance_id)
            instance.request_drain()
            return instance

    def cancel_drain(self, instance_id: str) -> RolloutInstance:
        """Atomically return a draining instance to READY service."""
        with self._lock:
            instance = self.get(instance_id)
            instance.cancel_drain()
            return instance

    def fence_failed(self, instance_id: str) -> RolloutInstance | None:
        """Atomically remove a failed READY instance from routing eligibility."""
        with self._lock:
            instance = self.get(instance_id)
            if instance.state is not RolloutInstanceState.READY:
                return None
            instance.desired_state = InstanceDesiredState.STOPPED
            instance.transition_to(RolloutInstanceState.FAILED)
            return instance

    def begin_stop_if_drained(self, instance_id: str) -> RolloutInstance | None:
        """Move a drained instance to STOPPING before resource deletion."""
        with self._lock:
            instance = self.get(instance_id)
            if instance.state is RolloutInstanceState.STOPPING:
                return instance
            if (
                instance.state is RolloutInstanceState.FAILED
                and instance.desired_state is InstanceDesiredState.STOPPED
                and instance.is_drained
            ):
                instance.transition_to(RolloutInstanceState.STOPPING)
                return instance
            if not instance.can_stop:
                return None
            instance.transition_to(RolloutInstanceState.STOPPING)
            return instance

    def bind_task(self, task_id: str, instance_id: str) -> None:
        """Bind a workflow task to one instance for its entire lifetime."""
        if not task_id:
            raise ValueError("task_id must not be empty")
        with self._lock:
            instance = self.get(instance_id)
            if not instance.is_routable:
                raise TaskBindingError(
                    f"instance {instance_id} is not eligible for new tasks"
                )
            existing = self._task_to_instance.get(task_id)
            if existing is not None and existing != instance_id:
                raise TaskBindingError(
                    f"task {task_id} is already bound to instance {existing}"
                )
            self._task_to_instance[task_id] = instance_id
            instance.workflow_task_ids.add(task_id)

    def release_task(self, task_id: str) -> str | None:
        """Release a workflow task; repeated cleanup is intentionally idempotent."""
        with self._lock:
            instance_id = self._task_to_instance.pop(task_id, None)
            if instance_id is None:
                return None
            instance = self._instances.get(instance_id)
            if instance is not None:
                instance.workflow_task_ids.discard(task_id)
            return instance_id

    def acquire_result_lease(self, instance_id: str, lease_id: str) -> None:
        """Keep an instance alive while returned RTensor shards remain readable."""
        if not lease_id:
            raise ValueError("lease_id must not be empty")
        with self._lock:
            instance = self.get(instance_id)
            if lease_id in instance.result_lease_ids:
                raise TaskBindingError(
                    f"result lease {lease_id} already belongs to instance {instance_id}"
                )
            instance.result_lease_ids.add(lease_id)

    def release_result_lease(self, instance_id: str, lease_id: str) -> bool:
        """Release returned-data ownership; repeated cleanup is idempotent."""
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None or lease_id not in instance.result_lease_ids:
                return False
            instance.result_lease_ids.remove(lease_id)
            return True

    def release_direct_request(self, instance_id: str) -> None:
        """Release one tracked direct generation request."""
        with self._lock:
            instance = self.get(instance_id)
            self._decrement(instance, "direct_inflight")

    def _routable_instances_unlocked(self) -> list[RolloutInstance]:
        """Return routable instances while the caller holds ``_lock``."""
        return [
            instance for instance in self._instances.values() if instance.is_routable
        ]

    def _select_least_loaded_unlocked(
        self,
        *,
        max_inflight_per_instance: int | None,
    ) -> RolloutInstance:
        """Choose a capacity-eligible instance while the caller holds ``_lock``."""
        if max_inflight_per_instance is not None and max_inflight_per_instance <= 0:
            raise ValueError("max_inflight_per_instance must be positive")

        instances = self._routable_instances_unlocked()
        if max_inflight_per_instance is not None:
            instances = [
                instance
                for instance in instances
                if instance.inflight_requests < max_inflight_per_instance
            ]
        if not instances:
            raise InstanceNotReadyError(
                "no READY elastic rollout instance has request capacity"
            )

        minimum_load = min(instance.inflight_requests for instance in instances)
        least_loaded = [
            instance
            for instance in instances
            if instance.inflight_requests == minimum_load
        ]
        return least_loaded[self._selection_index % len(least_loaded)]

    @staticmethod
    def _decrement(instance: RolloutInstance, field_name: str) -> None:
        value = getattr(instance, field_name)
        if value <= 0:
            raise ValueError(
                f"instance {instance.instance_id} has no {field_name} to release"
            )
        setattr(instance, field_name, value - 1)
