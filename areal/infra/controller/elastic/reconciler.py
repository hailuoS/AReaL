# SPDX-License-Identifier: Apache-2.0

"""Desired-state reconciliation for single-node Rollout V1 instances."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from areal.utils import logging

from .disk_catalog import DiskCheckpointManifest
from .instance_pool import RolloutInstancePool
from .models import InstanceDesiredState, RolloutInstance, RolloutInstanceState

logger = logging.getLogger("RolloutInstanceReconciler")


def _log_timing(
    *,
    event: str,
    started_at: float,
    batch_id: str,
    instance: RolloutInstance | None = None,
    **context: Any,
) -> None:
    fields = " ".join(f"{key}={value}" for key, value in context.items())
    logger.info(
        "Elastic scale-up timing event=%s batch_id=%s instance_id=%s "
        "elapsed_seconds=%.3f%s",
        event,
        batch_id,
        instance.instance_id if instance is not None else "-",
        time.monotonic() - started_at,
        f" {fields}" if fields else "",
    )


class _InstanceLauncher(Protocol):
    def provision(self, *, instance: RolloutInstance) -> None: ...

    async def start(
        self,
        *,
        instance: RolloutInstance,
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
    failed_instance_ids: tuple[str, ...] = ()


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
        startup_timeout_seconds: float = 300.0,
        startup_concurrency: int = 2,
        catch_up_concurrency: int = 4,
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
        if startup_timeout_seconds <= 0:
            raise ValueError("startup_timeout_seconds must be positive")
        if startup_concurrency <= 0:
            raise ValueError("startup_concurrency must be positive")
        if catch_up_concurrency <= 0:
            raise ValueError("catch_up_concurrency must be positive")
        self._startup_timeout_seconds = startup_timeout_seconds
        self._startup_concurrency = startup_concurrency
        self._catch_up_concurrency = catch_up_concurrency
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

    def _remove_new_instance(
        self, instance: RolloutInstance, *, provisioned: bool
    ) -> None:
        """Remove a failed or no-longer-needed instance from the launch batch."""
        instance.desired_state = InstanceDesiredState.STOPPED
        if provisioned:
            if instance.state is not RolloutInstanceState.STOPPING:
                instance.transition_to(RolloutInstanceState.STOPPING)
            self._launcher.destroy(instance)
        elif instance.state is not RolloutInstanceState.STOPPED:
            instance.transition_to(RolloutInstanceState.STOPPED)
        self._pool.remove(instance.instance_id)

    async def _start_instance(self, instance: RolloutInstance) -> RolloutInstance:
        result = await self._launcher.start(
            instance=instance,
            server_args=dict(self._server_args),
            initialize_kwargs=self._initialize_kwargs,
        )
        if result.instance is not instance:
            raise RuntimeError("launcher replaced the registered elastic instance")
        await self._ensure_proxy(instance)
        return instance

    async def _start_instance_bounded(
        self,
        instance: RolloutInstance,
        semaphore: asyncio.Semaphore,
        batch_id: str,
    ) -> RolloutInstance:
        queued_at = time.monotonic()
        async with semaphore:
            _log_timing(
                event="startup_slot_acquired",
                started_at=queued_at,
                batch_id=batch_id,
                instance=instance,
            )
            started_at = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    self._start_instance(instance),
                    timeout=self._startup_timeout_seconds,
                )
            except BaseException as error:
                _log_timing(
                    event="startup_task_failed",
                    started_at=started_at,
                    batch_id=batch_id,
                    instance=instance,
                    error_type=type(error).__name__,
                )
                raise
            _log_timing(
                event="startup_task_completed",
                started_at=started_at,
                batch_id=batch_id,
                instance=instance,
            )
            return result

    async def _catch_up_instance(
        self,
        instance: RolloutInstance,
        checkpoint: DiskCheckpointManifest | None,
        version: int,
        semaphore: asyncio.Semaphore,
        batch_id: str,
    ) -> RolloutInstance:
        queued_at = time.monotonic()
        async with semaphore:
            _log_timing(
                event="catch_up_slot_acquired",
                started_at=queued_at,
                batch_id=batch_id,
                instance=instance,
                version=version,
            )
            started_at = time.monotonic()
            try:
                if checkpoint is None:
                    instance.loaded_version = version
                    instance.transition_to(RolloutInstanceState.READY)
                else:
                    await self._launcher.catch_up_from_disk(instance, checkpoint)
            except BaseException as error:
                _log_timing(
                    event="catch_up_task_failed",
                    started_at=started_at,
                    batch_id=batch_id,
                    instance=instance,
                    version=version,
                    error_type=type(error).__name__,
                )
                raise
            _log_timing(
                event="instance_ready",
                started_at=started_at,
                batch_id=batch_id,
                instance=instance,
                version=version,
                checkpoint_loaded=checkpoint is not None,
            )
        return instance

    async def reconcile_once(self) -> ReconcileResult:
        """Move observed capacity one pass toward the requested desired count."""
        created: list[str] = []
        removed: list[str] = []
        draining: list[str] = []
        failed: list[str] = []

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

        pending_launches: list[RolloutInstance] = []
        for _ in range(self._pool.desired_count - len(running)):
            instance_id = f"ri-{uuid.uuid4().hex}"
            worker_role = f"{self._role_prefix}-{instance_id}"
            instance = RolloutInstance(
                instance_id=instance_id,
                worker_role=worker_role,
                worker_id=None,
                engine_name=f"rollout/{instance_id}",
            )
            self._pool.add(instance)
            pending_launches.append(instance)
            running.append(instance)

        batch_id = uuid.uuid4().hex[:8]
        batch_started_at = time.monotonic()
        if pending_launches:
            logger.info(
                "Elastic scale-up timing event=batch_started batch_id=%s "
                "instance_id=- elapsed_seconds=0.000 desired_instances=%d "
                "existing_running=%d requested_instances=%d",
                batch_id,
                self._pool.desired_count,
                len(running) - len(pending_launches),
                len(pending_launches),
            )

        provisioned: list[RolloutInstance] = []
        provision_batch_started_at = time.monotonic()
        for launch_index, instance in enumerate(pending_launches):
            worker_role = instance.worker_role
            launch_intent_recorded = False
            try:
                logger.info(
                    "Elastic scale-up timing event=provision_started batch_id=%s "
                    "instance_id=%s elapsed_seconds=0.000 worker_role=%s",
                    batch_id,
                    instance.instance_id,
                    worker_role,
                )
                self._record_launch_intent(worker_role)
                launch_intent_recorded = True
                self._launcher.provision(instance=instance)
                provisioned.append(instance)
            except Exception as error:
                failed.append(instance.instance_id)
                self._remove_new_instance(instance, provisioned=False)
                logger.warning(
                    "Failed to provision elastic instance %s: %s: %s",
                    instance.instance_id,
                    type(error).__name__,
                    error,
                )
                for unstarted in pending_launches[launch_index + 1 :]:
                    self._remove_new_instance(unstarted, provisioned=False)
                break
            finally:
                if launch_intent_recorded:
                    self._clear_launch_intent(worker_role)
        if pending_launches:
            _log_timing(
                event="provision_batch_completed",
                started_at=provision_batch_started_at,
                batch_id=batch_id,
                provisioned_instances=len(provisioned),
                failed_instances=len(failed),
            )

        started: list[RolloutInstance] = []
        if provisioned:
            startup_batch_started_at = time.monotonic()
            startup_semaphore = asyncio.Semaphore(self._startup_concurrency)
            start_tasks = [
                asyncio.create_task(
                    self._start_instance_bounded(instance, startup_semaphore, batch_id)
                )
                for instance in provisioned
            ]
            outcomes = await asyncio.gather(*start_tasks, return_exceptions=True)
            _log_timing(
                event="startup_batch_completed",
                started_at=startup_batch_started_at,
                batch_id=batch_id,
                instances=len(provisioned),
                startup_concurrency=self._startup_concurrency,
            )
            for instance, outcome in zip(provisioned, outcomes, strict=True):
                if isinstance(outcome, BaseException):
                    failed.append(instance.instance_id)
                    self._remove_new_instance(instance, provisioned=True)
                    logger.warning(
                        "Failed to start elastic instance %s: %s: %s",
                        instance.instance_id,
                        type(outcome).__name__,
                        outcome,
                    )
                else:
                    started.append(instance)

        current_instances = [
            self._pool.get(instance_id) for instance_id in self._pool.instance_ids()
        ]
        started_ids = {instance.instance_id for instance in started}
        existing_running = sum(
            instance.instance_id not in started_ids
            and instance.desired_state is InstanceDesiredState.RUNNING
            and instance.state
            not in {RolloutInstanceState.STOPPED, RolloutInstanceState.FAILED}
            for instance in current_instances
        )
        keep_count = max(0, self._pool.desired_count - existing_running)
        for instance in started[keep_count:]:
            self._remove_new_instance(instance, provisioned=True)
        started = started[:keep_count]

        if started:
            guard_started_at = time.monotonic()
            logger.info(
                "Elastic scale-up timing event=catch_up_guard_wait_started "
                "batch_id=%s instance_id=- elapsed_seconds=0.000 instances=%d",
                batch_id,
                len(started),
            )
            self._begin_catch_up()
            _log_timing(
                event="catch_up_guard_acquired",
                started_at=guard_started_at,
                batch_id=batch_id,
                instances=len(started),
            )
            try:
                try:
                    checkpoint_lookup_started_at = time.monotonic()
                    checkpoint = self._latest_checkpoint()
                    version = (
                        checkpoint.version
                        if checkpoint is not None
                        else self._current_version()
                    )
                    _log_timing(
                        event="checkpoint_resolved",
                        started_at=checkpoint_lookup_started_at,
                        batch_id=batch_id,
                        version=version,
                        checkpoint_available=checkpoint is not None,
                    )
                    # Pin one target version for the entire batch before any
                    # semaphore waiter starts loading it. Checkpoint GC can then
                    # see the shared dependency for queued CATCHING_UP instances.
                    for instance in started:
                        instance.loaded_version = version
                    semaphore = asyncio.Semaphore(self._catch_up_concurrency)
                    catch_up_batch_started_at = time.monotonic()
                    outcomes = await asyncio.gather(
                        *(
                            self._catch_up_instance(
                                instance,
                                checkpoint,
                                version,
                                semaphore,
                                batch_id,
                            )
                            for instance in started
                        ),
                        return_exceptions=True,
                    )
                    _log_timing(
                        event="catch_up_batch_completed",
                        started_at=catch_up_batch_started_at,
                        batch_id=batch_id,
                        instances=len(started),
                        catch_up_concurrency=self._catch_up_concurrency,
                        version=version,
                    )
                except Exception as error:
                    outcomes = [error] * len(started)
            finally:
                self._end_catch_up()
            for instance, outcome in zip(started, outcomes, strict=True):
                if isinstance(outcome, BaseException):
                    failed.append(instance.instance_id)
                    self._remove_new_instance(instance, provisioned=True)
                    logger.warning(
                        "Failed to catch up elastic instance %s: %s: %s",
                        instance.instance_id,
                        type(outcome).__name__,
                        outcome,
                    )
                else:
                    created.append(instance.instance_id)

        if pending_launches:
            _log_timing(
                event="batch_completed",
                started_at=batch_started_at,
                batch_id=batch_id,
                requested_instances=len(pending_launches),
                ready_instances=len(created),
                failed_instances=len(failed),
            )

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
            failed_instance_ids=tuple(failed),
        )
