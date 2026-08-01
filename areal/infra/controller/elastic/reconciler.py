# SPDX-License-Identifier: Apache-2.0

"""Desired-state reconciliation for single-node Rollout V1 instances."""

from __future__ import annotations

import time
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

    @staticmethod
    def proxy_role(instance: RolloutInstance) -> str: ...

    async def launch_proxy(self, instance: RolloutInstance) -> None: ...

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
        drain_timeout_seconds: float = 300.0,
        begin_catch_up: Callable[[], None] | None = None,
        end_catch_up: Callable[[], None] | None = None,
        record_launch_intent: Callable[[str], None] | None = None,
        clear_launch_intent: Callable[[str], None] | None = None,
        proxy_enabled: Callable[[], bool] | None = None,
    ) -> None:
        self._pool = pool
        self._launcher = launcher
        self._role_prefix = role_prefix
        self._server_args = server_args
        self._initialize_kwargs = initialize_kwargs
        self._latest_checkpoint = latest_checkpoint
        self._current_version = current_version
        self._drain_timeout_seconds = drain_timeout_seconds
        self._begin_catch_up = begin_catch_up or (lambda: None)
        self._end_catch_up = end_catch_up or (lambda: None)
        self._record_launch_intent = record_launch_intent or (lambda _role: None)
        self._clear_launch_intent = clear_launch_intent or (lambda _role: None)
        self._proxy_enabled = proxy_enabled or (lambda: False)

    async def _ensure_proxy(self, instance: RolloutInstance) -> None:
        if not self._proxy_enabled() or instance.proxy_ready:
            return
        proxy_role = self._launcher.proxy_role(instance)
        self._record_launch_intent(proxy_role)
        try:
            await self._launcher.launch_proxy(instance)
        finally:
            self._clear_launch_intent(proxy_role)

    async def reconcile_once(self) -> ReconcileResult:
        """Move observed capacity one pass toward the requested desired count."""
        created: list[str] = []
        removed: list[str] = []
        draining: list[str] = []

        instances = [
            self._pool.get(instance_id) for instance_id in self._pool.instance_ids()
        ]
        for instance in instances:
            if (
                instance.desired_state is InstanceDesiredState.RUNNING
                and instance.state is RolloutInstanceState.READY
            ):
                await self._ensure_proxy(instance)
        running = [
            instance
            for instance in instances
            if instance.desired_state is InstanceDesiredState.RUNNING
            and instance.state
            not in {RolloutInstanceState.STOPPED, RolloutInstanceState.FAILED}
        ]
        draining_instances = [
            instance
            for instance in instances
            if instance.state is RolloutInstanceState.DRAINING
            and instance.desired_state is InstanceDesiredState.STOPPED
        ]
        while len(running) < self._pool.desired_count and draining_instances:
            instance = draining_instances.pop()
            self._pool.cancel_drain(instance.instance_id)
            running.append(instance)

        for instance in instances:
            stopping = self._pool.begin_stop_if_drained(instance.instance_id)
            if stopping is not None:
                instance = stopping
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
            worker_role = f"{self._role_prefix}-{instance_id}"
            self._record_launch_intent(worker_role)
            try:
                result = await self._launcher.launch(
                    instance_id=instance_id,
                    worker_role=worker_role,
                    server_args=dict(self._server_args),
                    initialize_kwargs=self._initialize_kwargs,
                )
            except BaseException:
                self._clear_launch_intent(worker_role)
                raise
            instance = result.instance
            self._pool.add(instance)
            self._clear_launch_intent(worker_role)
            self._begin_catch_up()
            try:
                await self._ensure_proxy(instance)
                checkpoint = self._latest_checkpoint()
                if checkpoint is None:
                    instance.loaded_version = self._current_version()
                    instance.transition_to(RolloutInstanceState.READY)
                else:
                    await self._launcher.catch_up_from_disk(instance, checkpoint)
            except BaseException:
                if instance.state is not RolloutInstanceState.FAILED:
                    instance.transition_to(RolloutInstanceState.FAILED)
                instance.desired_state = InstanceDesiredState.STOPPED
                instance.transition_to(RolloutInstanceState.STOPPING)
                self._launcher.destroy(instance)
                self._pool.remove(instance.instance_id)
                raise
            finally:
                self._end_catch_up()
            created.append(instance.instance_id)
            running.append(instance)

        excess = len(running) - self._pool.desired_count
        for instance in reversed(running[-excess:] if excess > 0 else []):
            self._pool.request_drain(instance.instance_id)
            draining.append(instance.instance_id)
            stopping = self._pool.begin_stop_if_drained(instance.instance_id)
            if stopping is not None:
                self._launcher.destroy(stopping)
                self._pool.remove(instance.instance_id)
                removed.append(instance.instance_id)

        for instance_id in self._pool.instance_ids():
            instance = self._pool.get(instance_id)
            if (
                instance.state is RolloutInstanceState.DRAINING
                and instance.instance_id not in draining
                and instance.drain_started_at is not None
                and time.monotonic() - instance.drain_started_at
                > self._drain_timeout_seconds
            ):
                raise TimeoutError(
                    f"instance {instance.instance_id} did not drain within "
                    f"{self._drain_timeout_seconds:.1f}s"
                )

        return ReconcileResult(
            created_instance_ids=tuple(created),
            removed_instance_ids=tuple(removed),
            draining_instance_ids=tuple(draining),
        )
