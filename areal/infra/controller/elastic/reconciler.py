# SPDX-License-Identifier: Apache-2.0

"""Desired-state reconciliation for single-node Rollout V1 instances."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from .disk_catalog import DiskCheckpointManifest
from .instance_pool import RolloutInstancePool
from .models import InstanceDesiredState, RolloutInstance, RolloutInstanceState


class _InstanceLauncher(Protocol):
    async def launch(
        self,
        *,
        instance_id: str,
        worker_role: str,
        server_args: dict[str, Any],
        initialize_kwargs: dict[str, Any] | None = None,
    ) -> Any: ...

    async def catch_up_from_disk(
        self, instance: RolloutInstance, checkpoint: DiskCheckpointManifest
    ) -> None: ...

    def destroy(self, instance: RolloutInstance) -> None: ...


@dataclass(frozen=True)
class ReconcileResult:
    """Stable snapshot of one desired-state reconciliation pass."""

    created_instance_ids: tuple[str, ...]
    removed_instance_ids: tuple[str, ...]
    draining_instance_ids: tuple[str, ...]


class RolloutInstanceReconciler:
    """Create and drain whole single-node instances toward Pool desired state."""

    def __init__(
        self,
        *,
        pool: RolloutInstancePool,
        launcher: _InstanceLauncher,
        role_prefix: str,
        server_args: dict[str, Any],
        initialize_kwargs: dict[str, Any] | None = None,
        latest_checkpoint: Callable[[], DiskCheckpointManifest | None],
        current_version: Callable[[], int],
    ) -> None:
        self._pool = pool
        self._launcher = launcher
        self._role_prefix = role_prefix
        self._server_args = server_args
        self._initialize_kwargs = initialize_kwargs
        self._latest_checkpoint = latest_checkpoint
        self._current_version = current_version

    async def reconcile_once(self) -> ReconcileResult:
        """Move observed capacity one pass toward the requested desired count."""
        created: list[str] = []
        removed: list[str] = []
        draining: list[str] = []

        instances = [
            self._pool.get(instance_id) for instance_id in self._pool.instance_ids()
        ]
        for instance in instances:
            if instance.state is RolloutInstanceState.DRAINING and instance.can_stop:
                instance.transition_to(RolloutInstanceState.STOPPING)
                self._launcher.destroy(instance)
                self._pool.remove(instance.instance_id)
                removed.append(instance.instance_id)

        instances = [
            self._pool.get(instance_id) for instance_id in self._pool.instance_ids()
        ]
        running = [
            instance
            for instance in instances
            if instance.desired_state is InstanceDesiredState.RUNNING
            and instance.state
            not in {RolloutInstanceState.STOPPED, RolloutInstanceState.FAILED}
        ]
        while len(running) < self._pool.desired_count:
            instance_id = f"ri-{uuid.uuid4().hex}"
            result = await self._launcher.launch(
                instance_id=instance_id,
                worker_role=f"{self._role_prefix}-{instance_id}",
                server_args=dict(self._server_args),
                initialize_kwargs=self._initialize_kwargs,
            )
            instance = result.instance
            self._pool.add(instance)
            checkpoint = self._latest_checkpoint()
            if checkpoint is None:
                instance.loaded_version = self._current_version()
                instance.transition_to(RolloutInstanceState.READY)
            else:
                await self._launcher.catch_up_from_disk(instance, checkpoint)
            created.append(instance.instance_id)
            running.append(instance)

        excess = len(running) - self._pool.desired_count
        for instance in reversed(running[-excess:] if excess > 0 else []):
            instance.request_drain()
            draining.append(instance.instance_id)
            if instance.can_stop:
                instance.transition_to(RolloutInstanceState.STOPPING)
                self._launcher.destroy(instance)
                self._pool.remove(instance.instance_id)
                removed.append(instance.instance_id)

        return ReconcileResult(
            created_instance_ids=tuple(created),
            removed_instance_ids=tuple(removed),
            draining_instance_ids=tuple(draining),
        )
