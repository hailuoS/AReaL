# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import json
import os
import shutil
import threading
import time
import traceback
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Literal

from flask import Flask, jsonify, request
from torchdata.stateful_dataloader import StatefulDataLoader
from werkzeug.serving import make_server

from areal.api import (
    InferenceEngine,
    Job,
    LocalInfServerInfo,
    ModelRequest,
    ModelResponse,
    ParamSpec,
    RolloutWorkflow,
    Scheduler,
    WeightUpdateMeta,
    Worker,
    WorkflowLike,
)
from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import (
    InferenceEngineConfig,
    PerfTracerConfig,
    SchedulingSpec,
)
from areal.infra.controller.elastic import (
    DiskCheckpointCatalog,
    DiskCheckpointCatalogError,
    DiskCheckpointManifest,
    ElasticAutoscalerPolicy,
    ElasticRecoveryState,
    ElasticRecoveryStore,
    ElasticScalingReporter,
    ElasticScalingWindow,
    InvalidDesiredCountError,
    RolloutInstanceLauncher,
    RolloutInstancePool,
    RolloutInstanceReconciler,
    RolloutRPCTarget,
    recommend_instances,
)
from areal.infra.rpc.rtensor import RTensor
from areal.infra.rpc.serialization import deserialize_value
from areal.infra.utils.concurrent import run_async_task
from areal.utils import logging, perf_tracer
from areal.utils.data import cycle_dataloader
from areal.utils.dynamic_import import import_from_string
from areal.utils.network import find_free_ports, format_hostport, gethostip
from areal.utils.perf_tracer import trace_perf

from ..staleness_manager import StalenessManager
from ..workflow_executor import BatchTaskDispatcher, TaskIdGenerator

logger = logging.getLogger("RolloutController")


# NOTE: remote task input has a slightly different
# type annotation, which disallows workflow object or types
@dataclass
class _RemoteRolloutTaskInput:
    task_id: int
    data: dict[str, Any]
    workflow: str | None
    workflow_kwargs: dict[str, Any]
    should_accept_fn: str | None
    is_eval: bool = False
    group_size: int = 1
    proxy_addr: str | None = None
    enqueued_at: float = field(default_factory=time.monotonic)
    enqueued_version: int | None = None


@dataclass
class _RemoteRolloutResult:
    task_id: int
    trajectory: dict[str, Any]


class RolloutController:
    def __init__(
        self,
        inf_engine: type[InferenceEngine],
        config: InferenceEngineConfig,
        scheduler: Scheduler,
    ):
        self.inf_engine = inf_engine
        self.config = config
        self.scheduler = scheduler

        # Parse allocation from config.backend
        self.rollout_alloc = ModelAllocation.from_str(config.backend)

        # Worker management
        self.workers: list[Worker] = []  # List of Worker objects from scheduler
        self.server_infos: list[LocalInfServerInfo] = []
        self._worker_role: str

        # Round-robin scheduling
        self._current_worker_idx = 0

        # State
        self._version_lock = Lock()
        self._version = 0
        self._elastic_capacity_per_instance: int | None = None
        self._elastic_total_capacity_limit: int | None = None
        self._disk_checkpoint_catalog: DiskCheckpointCatalog | None = None
        self._instance_pool: RolloutInstancePool | None = None
        self._elastic_reconciler: RolloutInstanceReconciler | None = None
        self._elastic_reconcile_stop = threading.Event()
        self._elastic_reconcile_thread: threading.Thread | None = None
        self._elastic_last_reconcile_error: str | None = None
        self._elastic_scaling_report: dict[str, Any] | None = None
        self._elastic_scaling_report_lock = threading.Lock()
        self._elastic_scaling_reporter: ElasticScalingReporter | None = None
        self._elastic_autoscaler_policy: ElasticAutoscalerPolicy | None = None
        self._elastic_autoscaler_lock = threading.Lock()
        self._elastic_autoscaler_pending_direction: (
            Literal["scale_up", "scale_down"] | None
        ) = None
        self._elastic_last_autoscaler_decision: dict[str, Any] | None = None
        self._elastic_recovery_store: ElasticRecoveryStore | None = None
        self._elastic_desired_lock = threading.RLock()
        self._elastic_reconcile_lock = threading.Lock()
        self._elastic_update_condition = threading.Condition()
        self._elastic_pending_update_version: int | None = None
        self._elastic_catchups_inflight = 0
        self._elastic_pending_worker_roles: set[str] = set()
        self._elastic_desired_change: tuple[int, float] | None = None
        self._elastic_proxy_enabled = False
        self._elastic_result_lease_lock = threading.Lock()
        self._elastic_result_lease_instance: dict[str, str] = {}
        self._elastic_result_lease_shards: dict[str, set[Any]] = {}
        self._elastic_shard_to_result_lease: dict[Any, str] = {}
        if config.elastic.enabled:
            self._instance_pool = RolloutInstancePool(
                min_instances=config.elastic.min_instances,
                initial_instances=config.elastic.initial_instances,
                max_instances=config.elastic.max_instances,
            )
            self._elastic_scaling_reporter = ElasticScalingReporter(
                report_frequency_steps=config.elastic.report_freq_steps,
                min_instances=config.elastic.min_instances,
                max_instances=config.elastic.max_instances,
            )
            if config.elastic.auto_apply_scaling_recommendations:
                self._elastic_autoscaler_policy = ElasticAutoscalerPolicy(
                    scale_up_cooldown_seconds=(
                        config.elastic.autoscaler_scale_up_cooldown_seconds
                    ),
                    scale_down_cooldown_seconds=(
                        config.elastic.autoscaler_scale_down_cooldown_seconds
                    ),
                    direction_change_cooldown_seconds=(
                        config.elastic.autoscaler_direction_change_cooldown_seconds
                    ),
                    scale_down_windows=(config.elastic.autoscaler_scale_down_windows),
                )

        self._task_id_generator = TaskIdGenerator()

        # Use provided staleness manager or create a default one
        # The manager will be properly initialized in initialize()
        self._staleness_manager: StalenessManager | None = None

        # Dispatcher will be initialized in initialize() after staleness_manager is ready
        self._dispatcher: (
            BatchTaskDispatcher[_RemoteRolloutTaskInput, _RemoteRolloutResult] | None
        ) = None

        # HTTP callback server
        self._callback_app: Flask | None = None
        self._callback_server = None
        self._callback_server_thread: threading.Thread | None = None
        self._callback_port: int | None = None
        self._callback_host: str | None = None
        self._callback_loop: asyncio.AbstractEventLoop | None = None
        self._callback_loop_ready = threading.Event()

        # Task completion futures
        self._pending_futures: dict[int, asyncio.Future] = {}
        self._futures_lock = threading.Lock()

        # Proxy worker management (for AgentWorkflow support)
        self.proxy_workers: list[Worker] = []
        self.proxy_addrs: list[str] = []
        self._proxy_started = False

        # Proxy gateway server (for online/external access)
        self._proxy_gateway_app = None
        self._proxy_gateway_server = None
        self._proxy_gateway_thread: threading.Thread | None = None
        self._proxy_gateway_port: int | None = None
        self._proxy_gateway_host: str | None = None

    @property
    def _proxy_role(self) -> str:
        """Generate a unique proxy role name based on the worker role.

        This avoids collisions when multiple controllers (e.g., rollout and
        eval-rollout) each fork proxy workers into the same scheduler.
        """
        if not hasattr(self, "_worker_role"):
            raise RuntimeError(
                "Cannot access _proxy_role before initialize() is called"
            )
        return f"proxy-{self._worker_role}"

    def _proxy_engine_name(self, rank: int) -> str:
        """Generate engine name for a proxy worker rank."""
        return f"{self._proxy_role}/{rank}"

    def _engine_name(self, rank: int) -> str:
        """Generate engine name for a worker rank.

        Engine names follow the "role/index" format (e.g., "rollout/0", "rollout/1").
        """
        return f"{self._worker_role}/{rank}"

    def initialize(
        self,
        role: str,
        server_args: dict[str, Any] | None = None,
        server_infos: list[LocalInfServerInfo] | None = None,
        *args,
        **kwargs,
    ):
        # Get scheduling config from kwargs or use defaults
        # Schedule inference engines in the granularity of instance sizes,
        # usually TP x PP.
        self._worker_role = role

        if self.config.elastic.enabled:
            if server_infos is not None:
                raise NotImplementedError(
                    "elastic rollout does not support externally supplied servers"
                )
            self._initialize_elastic(role, server_args or {}, kwargs)
        else:
            job = self._build_rollout_job(role)
            run_async_task(
                self._async_initialize, job, server_args, server_infos, *args, **kwargs
            )

        # Initialize staleness manager for global capacity control
        max_concurrent_rollouts = (
            self.config.max_concurrent_rollouts or self.config.consumer_batch_size
        )
        consumer_batch_size = self.config.consumer_batch_size
        self._staleness_manager = StalenessManager(
            version_provider=self,
            max_concurrent_rollouts=max_concurrent_rollouts,
            consumer_batch_size=consumer_batch_size,
            max_staleness=self.config.max_head_offpolicyness,
        )
        if self._instance_pool is not None:
            self._elastic_capacity_per_instance = (
                self.config.elastic.max_concurrent_rollouts_per_instance
                or max_concurrent_rollouts
            )
            self._elastic_total_capacity_limit = (
                self.config.elastic.max_total_concurrent_rollouts
                or max_concurrent_rollouts
            )
            self._refresh_elastic_capacity()

        # Create and initialize the dispatcher
        qsize = self.config.queue_size or max_concurrent_rollouts * 16
        self._dispatcher = BatchTaskDispatcher[
            _RemoteRolloutTaskInput, _RemoteRolloutResult
        ](
            max_queue_size=qsize,
            task_factory=self._create_submit_callback,
            staleness_manager=self._staleness_manager,
            enable_tracing=self.config.enable_rollout_tracing,
        )
        # Initialize the dispatcher's async task runner
        self._dispatcher.initialize(logger=logger)

        # Start callback server for weight sync coordination
        self._start_callback_server()

    def _initialize_elastic(
        self,
        role: str,
        server_args: dict[str, Any],
        initialize_kwargs: dict[str, Any],
    ) -> None:
        assert self._instance_pool is not None
        if self.config.fileroot:
            self._elastic_recovery_store = ElasticRecoveryStore(
                self._elastic_recovery_path(role)
            )
            recovered = self._elastic_recovery_store.load()
            if recovered is not None:
                self._set_elastic_desired_instances(
                    recovered.desired_instances,
                    source="recovery",
                    persist=False,
                )
                self._version = recovered.serving_version
                if recovered.checkpoint_path is not None:
                    checkpoint_path = Path(recovered.checkpoint_path)
                    if not checkpoint_path.is_dir():
                        raise RuntimeError(
                            "recovery checkpoint directory does not exist: "
                            f"{checkpoint_path}"
                        )
                    self._disk_checkpoint_catalog = DiskCheckpointCatalog(
                        checkpoint_path.parent
                    )
                    manifest = self._disk_checkpoint_catalog.get(
                        recovered.checkpoint_version
                    )
                    if Path(manifest.path) != checkpoint_path:
                        raise RuntimeError(
                            "recovery checkpoint does not match disk catalog"
                        )
                elif recovered.serving_version > 0:
                    raise RuntimeError(
                        "elastic recovery at a nonzero serving version requires "
                        "a committed disk checkpoint"
                    )
                for stale_role in recovered.worker_roles:
                    try:
                        self.scheduler.delete_workers(role=stale_role)
                        logger.info("Deleted stale recovered role %s", stale_role)
                    except Exception:
                        logger.warning(
                            "Could not delete stale recovered role %s",
                            stale_role,
                            exc_info=True,
                        )
        launcher = RolloutInstanceLauncher(
            scheduler=self.scheduler,
            inf_engine=self.inf_engine,
            config=self.config,
            rollout_alloc=self.rollout_alloc,
            resource_provision_timeout_seconds=(
                self.config.elastic.resource_provision_timeout_seconds
            ),
        )
        self._elastic_reconciler = RolloutInstanceReconciler(
            pool=self._instance_pool,
            launcher=launcher,
            role_prefix=self.config.elastic.role_prefix,
            server_args=server_args,
            initialize_kwargs=initialize_kwargs,
            latest_checkpoint=self._latest_elastic_checkpoint,
            current_version=self.get_version,
            drain_timeout_seconds=self.config.elastic.drain_timeout_seconds,
            startup_timeout_seconds=self.config.elastic.startup_timeout_seconds,
            startup_concurrency=self.config.elastic.startup_concurrency,
            catch_up_concurrency=self.config.elastic.catch_up_concurrency,
            health_check_concurrency=self.config.elastic.health_check_concurrency,
            health_check_failure_threshold=(
                self.config.elastic.health_check_failure_threshold
            ),
            begin_catch_up=self._begin_elastic_catch_up,
            end_catch_up=self._end_elastic_catch_up,
            record_launch_intent=self._record_elastic_launch_intent,
            clear_launch_intent=self._clear_elastic_launch_intent,
            proxy_enabled=lambda: self._elastic_proxy_enabled,
        )
        run_async_task(self._reconcile_elastic_once)
        self._elastic_reconcile_thread = threading.Thread(
            target=self._elastic_reconcile_loop,
            name="rollout-elastic-reconciler",
            daemon=True,
        )
        self._elastic_reconcile_thread.start()

    def _elastic_recovery_path(self, role: str) -> Path:
        if not self.config.fileroot:
            raise ValueError("elastic recovery requires rollout.fileroot")
        recovery_root = Path(self.config.fileroot)
        if self.config.experiment_name and self.config.trial_name:
            recovery_root = (
                recovery_root / self.config.experiment_name / self.config.trial_name
            )
        recovery_role = role.replace("/", "_")
        return recovery_root / f"elastic_rollout_recovery_{recovery_role}.json"

    def _latest_elastic_checkpoint(self) -> DiskCheckpointManifest | None:
        if self._disk_checkpoint_catalog is None:
            return None
        version = self.get_version()
        return self._latest_elastic_checkpoint_for_version(version)

    def _latest_elastic_checkpoint_for_version(
        self, version: int
    ) -> DiskCheckpointManifest | None:
        if self._disk_checkpoint_catalog is None:
            if version == 0:
                return None
            raise RuntimeError(
                f"serving version {version} has no committed disk checkpoint"
            )
        try:
            return self._disk_checkpoint_catalog.get(version)
        except DiskCheckpointCatalogError:
            if version == 0:
                return None
            raise RuntimeError(
                f"serving version {version} has no committed disk checkpoint"
            ) from None

    async def _reconcile_elastic_once(self) -> None:
        assert self._elastic_reconciler is not None
        reconcile_started_at = time.monotonic()
        with self._elastic_reconcile_lock:
            reconcile_lock_wait = time.monotonic() - reconcile_started_at
            desired_count = (
                self._instance_pool.desired_count
                if self._instance_pool is not None
                else 0
            )
            desired_change_started_at = None
            with self._elastic_update_condition:
                desired_change = self._elastic_desired_change
                if desired_change is not None and desired_change[0] == desired_count:
                    desired_change_started_at = desired_change[1]
                    self._elastic_desired_change = None
            if desired_change_started_at is not None:
                logger.info(
                    "Elastic scale-up timing event=reconcile_started instance_id=- "
                    "elapsed_seconds=%.3f desired_instances=%d",
                    time.monotonic() - desired_change_started_at,
                    desired_count,
                )

            reconcile_call_started_at = time.monotonic()
            result = await self._elastic_reconciler.reconcile_once()
            reconcile_elapsed = time.monotonic() - reconcile_call_started_at
            should_log_timing = desired_change_started_at is not None or any(
                (result.created_instance_ids, result.failed_instance_ids)
            )
            capacity_started_at = time.monotonic()
            self._refresh_elastic_capacity()
            capacity_elapsed = time.monotonic() - capacity_started_at
            ready_instances = (
                len(self._instance_pool.ready_snapshot())
                if self._instance_pool is not None
                else 0
            )
            effective_capacity = 0
            if (
                self._elastic_capacity_per_instance is not None
                and self._elastic_total_capacity_limit is not None
            ):
                effective_capacity = min(
                    self._elastic_total_capacity_limit,
                    ready_instances * self._elastic_capacity_per_instance,
                )
            if should_log_timing:
                logger.info(
                    "Elastic scale-up timing event=capacity_refreshed instance_id=- "
                    "elapsed_seconds=%.3f reconcile_lock_wait_seconds=%.3f "
                    "reconcile_seconds=%.3f capacity_refresh_seconds=%.3f "
                    "desired_instances=%d ready_instances=%d effective_capacity=%d",
                    time.monotonic() - reconcile_started_at,
                    reconcile_lock_wait,
                    reconcile_elapsed,
                    capacity_elapsed,
                    desired_count,
                    ready_instances,
                    effective_capacity,
                )
            recovery_started_at = time.monotonic()
            self._save_elastic_recovery_state()
            recovery_elapsed = time.monotonic() - recovery_started_at
            if should_log_timing:
                logger.info(
                    "Elastic scale-up timing event=reconcile_completed instance_id=- "
                    "elapsed_seconds=%.3f reconcile_lock_wait_seconds=%.3f "
                    "reconcile_seconds=%.3f "
                    "capacity_refresh_seconds=%.3f recovery_save_seconds=%.3f "
                    "desired_instances=%d ready_instances=%d created_instances=%d "
                    "failed_instances=%d",
                    time.monotonic() - reconcile_started_at,
                    reconcile_lock_wait,
                    reconcile_elapsed,
                    capacity_elapsed,
                    recovery_elapsed,
                    desired_count,
                    ready_instances,
                    len(result.created_instance_ids),
                    len(result.failed_instance_ids),
                )

    def _begin_elastic_catch_up(self) -> None:
        with self._elastic_update_condition:
            self._elastic_update_condition.wait_for(
                lambda: (
                    self._elastic_pending_update_version is None
                    or self._elastic_reconcile_stop.is_set()
                )
            )
            if self._elastic_reconcile_stop.is_set():
                raise RuntimeError("elastic controller is stopping")
            self._elastic_catchups_inflight += 1

    def _end_elastic_catch_up(self) -> None:
        with self._elastic_update_condition:
            if self._elastic_catchups_inflight <= 0:
                raise RuntimeError("elastic catch-up guard is not held")
            self._elastic_catchups_inflight -= 1
            self._elastic_update_condition.notify_all()

    def _begin_elastic_weight_update(self, version: int) -> None:
        with self._elastic_update_condition:
            self._elastic_update_condition.wait_for(
                lambda: (
                    (
                        self._elastic_pending_update_version is None
                        and self._elastic_catchups_inflight == 0
                    )
                    or self._elastic_reconcile_stop.is_set()
                )
            )
            if self._elastic_reconcile_stop.is_set():
                raise RuntimeError("elastic controller is stopping")
            self._elastic_pending_update_version = version

    def _abort_elastic_weight_update(self, version: int) -> None:
        with self._elastic_update_condition:
            if self._elastic_pending_update_version == version:
                self._elastic_pending_update_version = None
                self._elastic_update_condition.notify_all()

    def _finish_elastic_weight_update(self, version: int) -> None:
        with self._elastic_update_condition:
            pending = self._elastic_pending_update_version
            if pending is not None and pending != version:
                raise RuntimeError(
                    f"serving version {version} does not match pending disk "
                    f"version {pending}"
                )
            if pending == version:
                self._elastic_pending_update_version = None
                self._elastic_update_condition.notify_all()

    def _validate_elastic_serving_version(self, version: int) -> None:
        with self._elastic_update_condition:
            pending = self._elastic_pending_update_version
            if pending is not None and pending != version:
                raise RuntimeError(
                    f"serving version {version} does not match pending disk "
                    f"version {pending}"
                )

    def _record_elastic_launch_intent(self, worker_role: str) -> None:
        with self._elastic_update_condition:
            self._elastic_pending_worker_roles.add(worker_role)
        self._save_elastic_recovery_state()

    def _clear_elastic_launch_intent(self, worker_role: str) -> None:
        with self._elastic_update_condition:
            self._elastic_pending_worker_roles.discard(worker_role)
        self._save_elastic_recovery_state()

    def _refresh_elastic_capacity(self) -> None:
        if self._instance_pool is None or self._staleness_manager is None:
            return
        assert self._elastic_capacity_per_instance is not None
        assert self._elastic_total_capacity_limit is not None
        ready_instances = len(self._instance_pool.ready_snapshot())
        effective_capacity = min(
            self._elastic_total_capacity_limit,
            ready_instances * self._elastic_capacity_per_instance,
        )
        self._staleness_manager.set_max_concurrent_rollouts(effective_capacity)
        if self._dispatcher is not None:
            self._dispatcher.notify_capacity_changed()

    def _save_elastic_recovery_state(self) -> None:
        if self._elastic_recovery_store is not None and self._instance_pool is not None:
            version = self.get_version()
            checkpoint = None
            if self._disk_checkpoint_catalog is not None:
                try:
                    checkpoint = self._disk_checkpoint_catalog.get(version)
                except DiskCheckpointCatalogError:
                    checkpoint = None
            if version > 0 and checkpoint is None:
                raise RuntimeError(
                    f"cannot persist elastic serving version {version} without "
                    "a committed disk checkpoint"
                )
            with self._elastic_update_condition:
                pending_worker_roles = frozenset(self._elastic_pending_worker_roles)
            instances = self._instance_pool.instances_snapshot()
            instance_roles = {instance.worker_role for instance in instances}
            instance_roles.update(
                instance.proxy_role
                for instance in instances
                if instance.proxy_role is not None
            )
            self._elastic_recovery_store.save(
                ElasticRecoveryState(
                    schema_version=self.config.elastic.recovery_schema_version,
                    desired_instances=self._instance_pool.desired_count,
                    serving_version=version,
                    checkpoint_version=(
                        checkpoint.version if checkpoint is not None else None
                    ),
                    checkpoint_path=(
                        checkpoint.path if checkpoint is not None else None
                    ),
                    worker_roles=tuple(sorted(instance_roles | pending_worker_roles)),
                )
            )

    def _set_elastic_desired_instances(
        self,
        desired_count: int,
        *,
        source: str,
        requested_at: float | None = None,
        persist: bool = True,
    ) -> tuple[int, int]:
        """Atomically accept one desired-capacity update.

        HTTP callers and the in-process autoscaler share this mutation boundary so
        validation, recovery persistence, and scale-up timing remain identical.
        Resource creation and deletion stay asynchronous in the reconciler.

        Returns ``(previous_count, accepted_count)``.
        """
        if self._instance_pool is None:
            raise RuntimeError("elastic rollout is disabled")
        if isinstance(desired_count, bool) or not isinstance(desired_count, int):
            raise InvalidDesiredCountError("desired_instances must be an integer")
        if not source:
            raise ValueError("desired-state update source must not be empty")

        accepted_at = time.monotonic()
        request_started_at = accepted_at if requested_at is None else requested_at
        with self._elastic_desired_lock:
            previous_desired_count = self._instance_pool.desired_count
            self._instance_pool.set_desired_count(desired_count)
            is_scale_up = desired_count > previous_desired_count
            if desired_count != previous_desired_count:
                with self._elastic_update_condition:
                    self._elastic_desired_change = (
                        (desired_count, accepted_at) if is_scale_up else None
                    )

            recovery_started_at = time.monotonic()
            if persist:
                self._save_elastic_recovery_state()
        if is_scale_up:
            logger.info(
                "Elastic scale-up timing event=desired_instances_accepted "
                "instance_id=- elapsed_seconds=%.3f recovery_save_seconds=%.3f "
                "source=%s previous_desired_instances=%d desired_instances=%d",
                time.monotonic() - request_started_at,
                time.monotonic() - recovery_started_at,
                source,
                previous_desired_count,
                desired_count,
            )
        if (
            desired_count != previous_desired_count
            and source != "internal_autoscaler"
            and self._elastic_autoscaler_policy is not None
        ):
            with self._elastic_autoscaler_lock:
                self._elastic_autoscaler_pending_direction = None
        return previous_desired_count, desired_count

    def _elastic_autoscaler_status(self) -> tuple[dict[str, Any], bool, str]:
        assert self._instance_pool is not None
        instances = self._instance_pool.instances_snapshot()
        desired = self._instance_pool.desired_count
        serving_version = self.get_version()
        with self._elastic_update_condition:
            pending_update_version = self._elastic_pending_update_version
        status = {
            "desired_instances": desired,
            "ready_instances": len(self._instance_pool.ready_snapshot()),
            "serving_version": serving_version,
            "pending_update_version": pending_update_version,
        }

        if self._elastic_last_reconcile_error:
            return status, False, "last reconcile attempt failed"
        if pending_update_version is not None:
            return status, False, f"pending weight update={pending_update_version}"
        if len(instances) != desired or status["ready_instances"] != desired:
            return (
                status,
                False,
                f"desired={desired} ready={status['ready_instances']} "
                f"total={len(instances)}",
            )
        if any(instance.state.value != "ready" for instance in instances):
            return status, False, "not all instances are READY"
        if self._elastic_proxy_enabled and any(
            not instance.proxy_ready for instance in instances
        ):
            return status, False, "not all instance proxies are ready"
        mismatched = [
            instance.instance_id
            for instance in instances
            if instance.loaded_version != serving_version
        ]
        if mismatched:
            return (
                status,
                False,
                f"serving_version={serving_version} mismatched={mismatched}",
            )
        return status, True, "stable"

    def _evaluate_elastic_autoscaler(self, report: dict[str, Any]) -> None:
        policy = self._elastic_autoscaler_policy
        if policy is None:
            return
        with self._elastic_autoscaler_lock:
            status, stable, detail = self._elastic_autoscaler_status()
            try:
                decision = policy.evaluate(
                    report,
                    status,
                    capacity_stable=stable,
                    stability_detail=detail,
                    now=time.monotonic(),
                )
                self._elastic_last_autoscaler_decision = {
                    "report_version": report.get("report_version"),
                    "action": decision.action,
                    "direction": decision.direction,
                    "message": decision.message,
                    "recommended_instances": report.get("recommended_instances"),
                }
                if not decision.should_apply:
                    logger.info(
                        "Internal elastic autoscaler action=%s report_version=%s: %s",
                        decision.action,
                        report.get("report_version"),
                        decision.message,
                    )
                    return

                recommended = report.get("recommended_instances")
                if isinstance(recommended, bool) or not isinstance(recommended, int):
                    raise ValueError(
                        "scaling recommendation requires integer recommended_instances"
                    )
                previous, accepted = self._set_elastic_desired_instances(
                    recommended,
                    source="internal_autoscaler",
                )
                if accepted != previous:
                    self._elastic_autoscaler_pending_direction = decision.direction
                logger.info(
                    "Internal elastic autoscaler applied report_version=%s "
                    "previous_desired_instances=%d desired_instances=%d "
                    "direction=%s",
                    report.get("report_version"),
                    previous,
                    accepted,
                    decision.direction,
                )
            except Exception as error:
                self._elastic_last_autoscaler_decision = {
                    "report_version": report.get("report_version"),
                    "action": "error",
                    "message": f"{type(error).__name__}: {error}",
                    "recommended_instances": report.get("recommended_instances"),
                }
                logger.warning(
                    "Internal elastic autoscaler rejected report_version=%s",
                    report.get("report_version"),
                    exc_info=True,
                )

    def _record_elastic_autoscaler_convergence(self) -> None:
        policy = self._elastic_autoscaler_policy
        if policy is None:
            return
        with self._elastic_autoscaler_lock:
            direction = self._elastic_autoscaler_pending_direction
            if direction is None:
                return
            status, stable, _ = self._elastic_autoscaler_status()
            if not stable:
                return
            policy.record_convergence(
                status,
                now=time.monotonic(),
                action_direction=direction,
            )
            self._elastic_autoscaler_pending_direction = None
            self._elastic_last_autoscaler_decision = {
                "action": "converged",
                "direction": direction,
                "message": "desired rollout capacity is stable",
                "desired_instances": status["desired_instances"],
                "serving_version": status["serving_version"],
            }

    def record_elastic_scaling_window(
        self,
        *,
        report_version: int,
        entered: int,
        consumed: int,
        wait_seconds: float,
        step_seconds: float,
    ) -> dict[str, Any] | None:
        """Accumulate one step and publish only a completed report window."""
        assert self._instance_pool is not None
        assert self._elastic_scaling_reporter is not None
        report = self._elastic_scaling_reporter.record(
            report_version=report_version,
            ready_instances=len(self._instance_pool.ready_snapshot()),
            entered=entered,
            consumed=consumed,
            wait_seconds=wait_seconds,
            step_seconds=step_seconds,
        )
        if report is None:
            return None
        self._publish_elastic_scaling_report(report, persist=True)
        self._evaluate_elastic_autoscaler(report)
        return report

    def _elastic_scaling_report_path(self, report_version: int) -> Path | None:
        if not self.config.fileroot:
            return None
        report_root = Path(self.config.fileroot)
        if self.config.experiment_name and self.config.trial_name:
            report_root = (
                report_root / self.config.experiment_name / self.config.trial_name
            )
        return (
            report_root
            / "balance_reports"
            / f"rollout_balance_report_v{report_version}.json"
        )

    def _publish_elastic_scaling_report(
        self, report: dict[str, Any], *, persist: bool
    ) -> None:
        with self._elastic_scaling_report_lock:
            self._elastic_scaling_report = dict(report)
        if not persist:
            return
        report_version = report.get("report_version")
        if isinstance(report_version, bool) or not isinstance(report_version, int):
            raise ValueError("scaling report requires an integer report_version")
        path = self._elastic_scaling_report_path(report_version)
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = path.with_suffix(f"{path.suffix}.tmp")
            with temporary_path.open("w", encoding="utf-8") as file:
                json.dump(report, file, indent=2, sort_keys=True)
                file.write("\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_path, path)
            logger.info("Elastic scaling report saved: %s", path)
        except OSError:
            logger.warning(
                "Failed to save elastic scaling report %s", path, exc_info=True
            )

    def _elastic_reconcile_loop(self) -> None:
        while not self._elastic_reconcile_stop.wait(
            self.config.elastic.reconcile_interval_seconds
        ):
            try:
                run_async_task(self._reconcile_elastic_once)
                self._elastic_last_reconcile_error = None
                self._record_elastic_autoscaler_convergence()
            except Exception:
                self._elastic_last_reconcile_error = traceback.format_exc()
                logger.error("Elastic reconciliation failed", exc_info=True)

    def _build_rollout_job(self, role: str) -> Job:
        """Build the unchanged static V1 Scheduler job for rollout workers."""
        instance_size = (
            self.rollout_alloc.parallel.tp_size * self.rollout_alloc.parallel.pp_size
        )
        dp_size = self.rollout_alloc.parallel.dp_size

        # The first element of `self.config.scheduling_spec` is the resource spec
        # of workers, aka the RPC server process. Since a worker exactly matches
        # to a single engine instance in the local environment, we can directly
        # use the spec of engines as the spec of workers here. Engine scheduling
        # specs are ignored.
        sch_spec = SchedulingSpec(**asdict(self.config.scheduling_spec[0]))
        sch_spec.cpu *= instance_size
        sch_spec.mem *= instance_size
        if sch_spec.gpu > 0:
            sch_spec.gpu = instance_size

        return Job(
            replicas=dp_size,
            tasks=[sch_spec for _ in range(dp_size)],
            scheduling_strategy=self.config.scheduling_strategy,
            role=role,
        )

    async def _async_initialize(
        self,
        job: Job,
        server_args: dict[str, Any],
        server_infos: list[LocalInfServerInfo] | None = None,
        *args,
        **kwargs,
    ):
        # Create workers via scheduler
        logger.info("Creating workers via scheduler...")
        worker_ids = self.scheduler.create_workers(job=job)
        logger.info(f"Workers created: {worker_ids}")

        # Wait for workers to be ready
        logger.info("Waiting for workers to be ready...")
        self.workers = self.scheduler.get_workers(role=job.role)
        logger.info(f"Workers ready: {[w.id for w in self.workers]}")

        # Get engine class path for dynamic import on workers
        engine_class = self.inf_engine

        # Create and initialize engines on workers
        logger.info("Creating engines...")
        targets = self._rollout_rpc_targets()
        tasks = [
            self.scheduler.create_engine(
                worker_id=target.worker_id,
                engine=f"{engine_class.__module__}.{engine_class.__name__}",
                engine_name=target.engine_name,
                config=self.config,
            )
            for target in targets
        ]
        await asyncio.gather(*tasks)
        logger.info("Engine created on all workers!")

        logger.info("Calling engine initialization...")
        # Workers are controller-managed: the controller handles staleness
        # globally, so workers must NOT apply their own dp-scaled staleness
        # constraints. Force train_data_parallel_size=1 unless explicitly
        # configured; an explicit None must not survive, or workers fall back
        # to dividing capacity by dist.get_world_size().
        if kwargs.get("train_data_parallel_size") is None:
            kwargs["train_data_parallel_size"] = 1
        if server_infos is not None:
            # Connecting to existing local servers for evaluation
            self.server_infos = server_infos
            assert len(self.server_infos) == len(self.workers), (
                len(self.server_infos),
                len(self.workers),
            )
            tasks = [
                self.scheduler.async_call_engine(
                    worker_id=target.worker_id,
                    method="initialize",
                    engine_name=target.engine_name,
                    # args in `engine_api`
                    engine_id=str(rank),
                    addr=f"{info.host}:{info.port}",
                    engine_rank=rank,
                    num_engines=len(self.workers),
                    *args,
                    **kwargs,
                )
                for rank, (target, info) in enumerate(zip(targets, self.server_infos))
            ]
            await asyncio.gather(*tasks)
        else:
            self.server_infos = await self._collective_rpc_async(
                "launch_server", server_args=server_args
            )
            tasks = [
                self.scheduler.async_call_engine(
                    worker_id=target.worker_id,
                    method="initialize",
                    engine_name=target.engine_name,
                    # args in `engine_api`
                    engine_id=str(rank),
                    engine_rank=rank,
                    num_engines=len(self.workers),
                    *args,
                    **kwargs,
                )
                for rank, target in enumerate(targets)
            ]
            await asyncio.gather(*tasks)

        logger.info("All engines are initialized...")

    def destroy(self):
        # Stop background threads and shutdown the async task runner
        if self._dispatcher is not None:
            self._dispatcher.destroy()

        if self._elastic_reconcile_thread is not None:
            self._elastic_reconcile_stop.set()
            with self._elastic_update_condition:
                self._elastic_update_condition.notify_all()
            self._elastic_reconcile_thread.join(timeout=5.0)
            if self._elastic_reconcile_thread.is_alive():
                logger.warning("Elastic reconcile thread did not stop within 5s")
            self._elastic_reconcile_thread = None

        self._stop_callback_server()

        self._collective_rpc("destroy", http_timeout=60.0)

        # Delete workers via scheduler
        if self._instance_pool is not None:
            for instance_id in self._instance_pool.instance_ids():
                instance = self._instance_pool.get(instance_id)
                try:
                    if instance.proxy_role is not None:
                        self.scheduler.delete_workers(role=instance.proxy_role)
                    self.scheduler.delete_workers(role=instance.worker_role)
                except Exception:
                    logger.error(
                        "Error deleting elastic instance %s roles: %s",
                        instance.instance_id,
                        traceback.format_exc(),
                    )
        elif hasattr(self, "_worker_role"):
            try:
                self.scheduler.delete_workers(role=self._worker_role)
                self.workers.clear()
                logger.info("Workers deleted")
            except Exception:
                logger.error(f"Error deleting workers: {traceback.format_exc()}")

        # Delete proxy workers if initialized
        if self._proxy_started and self._instance_pool is None:
            try:
                self.scheduler.delete_workers(role=self._proxy_role)
                self.proxy_workers.clear()
                self.proxy_addrs.clear()
                self._proxy_started = False
                logger.info("Proxy workers deleted")
            except Exception:
                logger.error(f"Error deleting proxy workers: {traceback.format_exc()}")
        elif self._instance_pool is not None:
            self._proxy_started = False

        # Shutdown proxy gateway if initialized
        self._stop_proxy_gateway()
        with self._futures_lock:
            self._pending_futures.clear()

    def start_proxy(self) -> None:
        """Initialize proxy workers for AgentWorkflow support.

        Creates proxy workers colocated with rollout workers. Each proxy worker
        runs a ProxyRolloutServer that connects to the same inference server
        as its corresponding rollout worker.
        """
        if self._proxy_started:
            logger.warning("Proxy workers already initialized")
            return

        if self._instance_pool is not None:
            if self._elastic_reconciler is None:
                raise RuntimeError(
                    "Cannot initialize elastic proxy workers before rollout initialize()"
                )
            self._elastic_proxy_enabled = True
            try:
                run_async_task(self._reconcile_elastic_once)
            except BaseException:
                self._elastic_proxy_enabled = False
                raise
            missing_proxy = [
                instance.instance_id
                for instance in self._instance_pool.instances_snapshot()
                if instance.is_routable and not instance.proxy_ready
            ]
            if missing_proxy:
                self._elastic_proxy_enabled = False
                raise RuntimeError(
                    "Elastic proxy initialization did not cover READY instances: "
                    f"{missing_proxy}"
                )
            self._proxy_started = True
            logger.info("Elastic instance-local proxy workers initialized")
            return

        if not self.server_infos:
            raise RuntimeError(
                "Cannot initialize proxy workers: rollout not initialized. "
                "Call initialize() first."
            )

        run_async_task(self._async_start_proxy)
        self._proxy_started = True

    async def _async_start_proxy(self) -> None:
        """Async implementation of proxy worker initialization."""
        command = "areal.experimental.openai.proxy.proxy_rollout_server"
        worker_ids = self.scheduler.fork_workers(
            role=self._proxy_role,
            target_role=self._worker_role,
            command=command,
        )
        logger.info(f"Proxy workers forked: {worker_ids}")

        self.proxy_workers = self.scheduler.get_workers(role=self._proxy_role)
        logger.info(f"Proxy workers: {[w.id for w in self.proxy_workers]}")

        engine_class = f"{self.inf_engine.__module__}.{self.inf_engine.__name__}"

        create_tasks = []
        for rank, worker in enumerate(self.proxy_workers):
            create_tasks.append(
                self.scheduler.create_engine(
                    worker_id=worker.id,
                    engine=engine_class,
                    engine_name=self._proxy_engine_name(rank),
                    config=self.config,
                )
            )
        await asyncio.gather(*create_tasks)
        logger.info("Proxy engines created")

        init_tasks = []
        for rank, (worker, server_info) in enumerate(
            zip(self.proxy_workers, self.server_infos, strict=True)
        ):
            init_tasks.append(
                self.scheduler.async_call_engine(
                    worker_id=worker.id,
                    method="initialize",
                    engine_name=self._proxy_engine_name(rank),
                    addr=f"{server_info.host}:{server_info.port}",
                )
            )
            self.proxy_addrs.append(
                f"http://{format_hostport(worker.ip, int(worker.worker_ports[0]))}"
            )
        await asyncio.gather(*init_tasks)

        logger.info(f"Proxy servers initialized. Addresses: {self.proxy_addrs}")

    def get_proxy_addr(self, rank: int) -> str:
        """Get the proxy server address for a given rollout worker rank.

        Parameters
        ----------
        rank : int
            The rank of the rollout worker

        Returns
        -------
        str
            The HTTP address of the corresponding proxy server
        """
        if not self._proxy_started:
            raise RuntimeError(
                "Proxy workers not initialized. Call start_proxy() first."
            )
        if rank >= len(self.proxy_addrs):
            raise IndexError(
                f"Invalid rank {rank}, only {len(self.proxy_addrs)} proxy workers"
            )
        return self.proxy_addrs[rank]

    def start_proxy_gateway(self) -> None:
        """Start the proxy gateway for external access.

        Creates a FastAPI server that routes requests to backend proxy
        workers. Requires ``start_proxy()`` to have been called first.
        """
        if self._instance_pool is not None:
            raise NotImplementedError(
                "Elastic RolloutController V1 supports instance-local offline "
                "AgentWorkflow proxies only; Proxy Gateway online routing is deferred"
            )
        if not self._proxy_started:
            raise RuntimeError(
                "Proxy workers not initialized. Call start_proxy() first."
            )
        if self._proxy_gateway_host is not None:
            logger.warning("Proxy gateway already running")
            return

        from areal.experimental.openai.proxy.proxy_gateway import (
            create_proxy_gateway_app,
        )

        agent_cfg = self.config.agent

        app = create_proxy_gateway_app(
            proxy_addrs=self.proxy_addrs,
            admin_api_key=agent_cfg.admin_api_key
            if agent_cfg is not None
            else "areal-admin-key",
        )

        self._proxy_gateway_port = find_free_ports(1)[0]
        self._proxy_gateway_host = gethostip()
        self._proxy_gateway_app = app

        def serve():
            import uvicorn

            try:
                config = uvicorn.Config(
                    app,
                    host="0.0.0.0",
                    port=self._proxy_gateway_port,
                    log_level="warning",
                    access_log=False,
                )
                server = uvicorn.Server(config)
                self._proxy_gateway_server = server
                server.run()
            except Exception:
                logger.error("Proxy gateway thread crashed", exc_info=True)

        self._proxy_gateway_thread = threading.Thread(target=serve, daemon=True)
        self._proxy_gateway_thread.start()

        # Wait for uvicorn to bind the port before propagating the address
        # to worker engines via collective RPC.
        import time

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if (
                self._proxy_gateway_server is not None
                and self._proxy_gateway_server.started
            ):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError(
                "Proxy gateway failed to start within 10s. "
                f"Cannot propagate address "
                f"{self._proxy_gateway_host}:{self._proxy_gateway_port}"
            )

        logger.info(
            "Proxy gateway started on "
            f"{self._proxy_gateway_host}:{self._proxy_gateway_port}"
        )

        # Propagate proxy_gateway_addr to all rollout worker engines
        # so that _resolve_workflow can pick it up for online mode.
        self._collective_rpc(
            "set_proxy_gateway_addr",
            addr=self.proxy_gateway_addr,
        )

    @property
    def proxy_gateway_addr(self) -> str:
        """Single URL for external users."""
        if self._proxy_gateway_host is None:
            raise RuntimeError("Proxy gateway not started")
        return f"http://{format_hostport(self._proxy_gateway_host, self._proxy_gateway_port)}"

    def _stop_proxy_gateway(self) -> None:
        """Stop the proxy gateway server if running."""
        if self._proxy_gateway_host is None:
            return
        logger.info("Stopping proxy gateway...")
        if self._proxy_gateway_server is not None:
            self._proxy_gateway_server.should_exit = True
        if self._proxy_gateway_thread is not None:
            self._proxy_gateway_thread.join(timeout=30.0)
            if self._proxy_gateway_thread.is_alive():
                logger.warning(
                    "Proxy gateway thread did not exit within 30s; "
                    "daemon thread will be killed on process exit"
                )
        self._proxy_gateway_app = None
        self._proxy_gateway_server = None
        self._proxy_gateway_thread = None
        self._proxy_gateway_port = None
        self._proxy_gateway_host = None

    def _start_callback_server(self):
        """Start Flask HTTP server to receive callbacks from RolloutCallback."""
        if self._callback_server is not None:
            logger.warning("Callback server already running")
            return

        app = Flask(__name__)
        app.logger.disabled = True

        @app.route("/callback/init_weights_group", methods=["POST"])
        def init_weights_group():
            payload = request.get_json() or {}
            meta = deserialize_value(payload.get("meta"))
            self._callback_loop.run_until_complete(self.init_weights_update_group(meta))
            return jsonify({"status": "ok"})

        @app.route("/callback/update_weights_xccl", methods=["POST"])
        def update_weights():
            payload = request.get_json() or {}
            meta = deserialize_value(payload.get("meta"))
            param_specs = deserialize_value(payload.get("param_specs"))
            self._callback_loop.run_until_complete(
                self.update_weights_from_distributed(meta, param_specs)
            )
            return jsonify({"status": "ok"})

        @app.route("/callback/update_weights_disk", methods=["POST"])
        def update_weights_disk():
            payload = request.get_json() or {}
            meta = deserialize_value(payload.get("meta"))
            self._callback_loop.run_until_complete(self.update_weights_from_disk(meta))
            return jsonify({"status": "ok"})

        @app.route("/callback/update_weights_awex", methods=["POST"])
        def update_weights_awex():
            payload = request.get_json() or {}
            meta = deserialize_value(payload.get("meta"))
            step_id = payload.get("step_id")
            kwargs = deserialize_value(payload.get("kwargs"))
            self._callback_loop.run_until_complete(
                self.update_weights_from_awex(meta, step_id=step_id, kwargs=kwargs)
            )
            return jsonify({"status": "ok"})

        @app.route("/elastic/desired-instances", methods=["GET", "PUT"])
        def elastic_desired_instances():
            """Read or update desired complete TP x PP rollout instances.

            This endpoint is intentionally a desired-state control plane.  A
            later reconciler owns resource creation and graceful teardown, so
            an HTTP request never creates or kills Scheduler workers inline.
            """
            if self._instance_pool is None:
                return jsonify({"error": "elastic rollout is disabled"}), 409

            if request.method == "GET":
                return jsonify(
                    {
                        "desired_instances": self._instance_pool.desired_count,
                        "instance_ids": self._instance_pool.instance_ids(),
                    }
                )

            request_started_at = time.monotonic()
            payload = request.get_json(silent=True) or {}
            desired_count = payload.get("desired_instances")
            if isinstance(desired_count, bool) or not isinstance(desired_count, int):
                return jsonify({"error": "desired_instances must be an integer"}), 400
            try:
                self._set_elastic_desired_instances(
                    desired_count,
                    source="http",
                    requested_at=request_started_at,
                )
            except InvalidDesiredCountError as exc:
                return jsonify({"error": str(exc)}), 400
            return jsonify(
                {
                    "desired_instances": self._instance_pool.desired_count,
                    "instance_ids": self._instance_pool.instance_ids(),
                }
            )

        @app.route("/elastic/instances", methods=["GET"])
        def elastic_instances():
            """Return a stable JSON snapshot for external control loops."""
            if self._instance_pool is None:
                return jsonify({"error": "elastic rollout is disabled"}), 409
            with self._elastic_update_condition:
                pending_update_version = self._elastic_pending_update_version
            with self._elastic_scaling_report_lock:
                latest_report_version = (
                    self._elastic_scaling_report.get("report_version")
                    if self._elastic_scaling_report is not None
                    else None
                )
            with self._elastic_autoscaler_lock:
                last_autoscaler_decision = (
                    dict(self._elastic_last_autoscaler_decision)
                    if self._elastic_last_autoscaler_decision is not None
                    else None
                )
            instances = []
            for instance in self._instance_pool.instances_snapshot():
                instances.append(
                    {
                        "instance_id": instance.instance_id,
                        "worker_role": instance.worker_role,
                        "worker_id": instance.worker_id,
                        "engine_name": instance.engine_name,
                        "proxy_role": instance.proxy_role,
                        "proxy_worker_id": instance.proxy_worker_id,
                        "proxy_engine_name": instance.proxy_engine_name,
                        "proxy_addr": instance.proxy_addr,
                        "proxy_ready": instance.proxy_ready,
                        "state": instance.state.value,
                        "desired_state": instance.desired_state.value,
                        "loaded_version": instance.loaded_version,
                        "active_tasks": len(instance.workflow_task_ids),
                        "inflight_requests": instance.inflight_requests,
                        "request_capacity": self._elastic_capacity_per_instance,
                        "available_request_capacity": (
                            max(
                                0,
                                self._elastic_capacity_per_instance
                                - instance.inflight_requests,
                            )
                            if self._elastic_capacity_per_instance is not None
                            else None
                        ),
                        "result_leases": len(instance.result_lease_ids),
                        "direct_inflight": instance.direct_inflight,
                        "update_leases": instance.update_leases,
                    }
                )
            return jsonify(
                {
                    "desired_instances": self._instance_pool.desired_count,
                    "ready_instances": len(self._instance_pool.ready_snapshot()),
                    "serving_version": self.get_version(),
                    "pending_update_version": pending_update_version,
                    "proxy_enabled": self._elastic_proxy_enabled,
                    "max_concurrent_rollouts": (
                        self._staleness_manager.max_concurrent_rollouts
                        if self._staleness_manager is not None
                        else None
                    ),
                    "max_concurrent_rollouts_per_instance": (
                        self._elastic_capacity_per_instance
                    ),
                    "max_total_concurrent_rollouts": (
                        self._elastic_total_capacity_limit
                    ),
                    "last_reconcile_error": self._elastic_last_reconcile_error,
                    "report_freq_steps": self.config.elastic.report_freq_steps,
                    "latest_report_version": latest_report_version,
                    "auto_apply_scaling_recommendations": (
                        self.config.elastic.auto_apply_scaling_recommendations
                    ),
                    "last_autoscaler_decision": last_autoscaler_decision,
                    "instances": instances,
                }
            )

        @app.route("/elastic/scaling-recommendation", methods=["GET", "POST"])
        def elastic_scaling_recommendation():
            if self._instance_pool is None:
                return jsonify({"error": "elastic rollout is disabled"}), 409
            if request.method == "GET":
                with self._elastic_scaling_report_lock:
                    report = (
                        dict(self._elastic_scaling_report)
                        if self._elastic_scaling_report is not None
                        else {"status": "empty"}
                    )
                return jsonify(report)
            payload = request.get_json(silent=True) or {}
            try:
                window = ElasticScalingWindow(
                    ready_instances=len(self._instance_pool.ready_snapshot()),
                    entered=int(payload["entered"]),
                    consumed=int(payload["consumed"]),
                    wait_seconds=float(payload["wait_seconds"]),
                    step_seconds=float(payload["step_seconds"]),
                )
                recommendation = recommend_instances(
                    window,
                    min_instances=self.config.elastic.min_instances,
                    max_instances=self.config.elastic.max_instances,
                )
            except (KeyError, TypeError, ValueError) as exc:
                return jsonify({"error": str(exc)}), 400
            with self._elastic_scaling_report_lock:
                previous_version = (
                    self._elastic_scaling_report.get("report_version", 0)
                    if self._elastic_scaling_report is not None
                    else 0
                )
            report_version = max(self.get_version(), int(previous_version)) + 1
            report = {
                "report_version": report_version,
                "window_start_version": report_version,
                "window_end_version": report_version,
                "window_iterations": 1,
                "timing_samples": int(window.step_seconds > 0),
                "branch": recommendation.branch,
                "recommended_instances": recommendation.recommended_instances,
                "rollout_wait_fraction": recommendation.rollout_wait_fraction,
                "entered": window.entered,
                "consumed": window.consumed,
                "ready_instances": window.ready_instances,
                "wait_seconds": window.wait_seconds,
                "step_seconds": window.step_seconds,
                "avg_batch_wait_seconds": window.wait_seconds,
                "avg_step_seconds": window.step_seconds,
            }
            self._publish_elastic_scaling_report(report, persist=False)
            return jsonify(report)

        @app.route("/callback/pause_generation", methods=["POST"])
        def pause_generation():
            self._callback_loop.run_until_complete(self.pause_generation())
            return jsonify({"status": "ok"})

        @app.route("/callback/continue_generation", methods=["POST"])
        def continue_generation():
            self._callback_loop.run_until_complete(self.continue_generation())
            return jsonify({"status": "ok"})

        @app.route("/callback/rollout_complete", methods=["POST"])
        def rollout_complete():
            payload = request.get_json() or {}
            task_id = payload.get("task_id")
            try:
                self._resolve_task_future(task_id)
                return jsonify({"status": "ok"})
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        @app.errorhandler(Exception)
        def handle_error(e):
            logger.error(f"Callback handler error: {e}")
            return jsonify({"error": str(e)}), 500

        self._callback_port = find_free_ports(1)[0]
        self._callback_host = gethostip()
        self._callback_app = app
        self._callback_server = make_server(
            self._callback_host, self._callback_port, app, threaded=False
        )

        # Suppress Werkzeug access logs (e.g., "POST /callback/rollout_complete 200 -")
        # Override log_request directly on the request handler class
        self._callback_server.RequestHandlerClass.log_request = (
            lambda self, *args, **kwargs: None
        )

        # Also configure Werkzeug logger level for any other log messages
        import logging as stdlib_logging

        werkzeug_logger = stdlib_logging.getLogger("werkzeug")
        werkzeug_logger.setLevel(stdlib_logging.WARNING)

        def serve_forever():
            # Create and set event loop for this thread
            self._callback_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._callback_loop)
            # Signal that the loop is ready
            self._callback_loop_ready.set()
            logger.info(
                f"Callback server started on {format_hostport(self._callback_host, self._callback_port)}"
            )
            self._callback_server.serve_forever()

        self._callback_server_thread = threading.Thread(
            target=serve_forever, daemon=True
        )
        self._callback_server_thread.start()
        # Wait for loop to be created
        self._callback_loop_ready.wait()

    def _stop_callback_server(self):
        """Stop the callback server if running."""
        if self._callback_server is not None:
            logger.info("Stopping callback server...")
            self._callback_server.shutdown()
            if self._callback_loop is not None:
                self._callback_loop.close()
            self._callback_server = None
            self._callback_app = None
            self._callback_server_thread = None
            self._callback_port = None
            self._callback_host = None
            self._callback_loop = None
            self._callback_loop_ready.clear()

    @property
    def callback_addr(self) -> str:
        """Return callback server address as 'host:port'."""
        if self._callback_host is None or self._callback_port is None:
            raise RuntimeError("Callback server not started")
        return format_hostport(self._callback_host, self._callback_port)

    def _resolve_task_future(self, task_id: int):
        """Resolve a pending future with the task result."""
        with self._futures_lock:
            future = self._pending_futures.pop(task_id, None)
        if future:
            future.get_loop().call_soon_threadsafe(future.set_result, None)

    def _rollout_rpc_targets(self) -> tuple[RolloutRPCTarget, ...]:
        """Snapshot static rollout RPC targets without changing rank naming."""
        if self._instance_pool is not None:
            return self._instance_pool.ready_snapshot()
        return tuple(
            RolloutRPCTarget(
                instance_id=f"static-{rank}",
                worker_id=worker.id,
                engine_name=self._engine_name(rank),
            )
            for rank, worker in enumerate(self.workers)
        )

    def _proxy_rpc_targets(self) -> tuple[RolloutRPCTarget, ...]:
        """Snapshot static proxy RPC targets without changing rank naming."""
        return tuple(
            RolloutRPCTarget(
                instance_id=f"proxy-static-{rank}",
                worker_id=worker.id,
                engine_name=self._proxy_engine_name(rank),
            )
            for rank, worker in enumerate(self.proxy_workers)
        )

    def _collective_rpc(self, method: str, *args, **kwargs) -> list[Any]:
        return run_async_task(self._collective_rpc_async, method, *args, **kwargs)

    async def _collective_rpc_async(self, method: str, *args, **kwargs) -> list[Any]:
        if self._instance_pool is None:
            return await self._collective_rpc_on_targets_async(
                method, self._rollout_rpc_targets(), *args, **kwargs
            )
        targets = self._instance_pool.acquire_direct_snapshot()
        try:
            return await self._collective_rpc_on_targets_async(
                method, targets, *args, **kwargs
            )
        finally:
            self._instance_pool.release_direct_snapshot(targets)

    def _proxy_collective_rpc(self, method: str, *args, **kwargs) -> list[Any]:
        return run_async_task(self._proxy_collective_rpc_async, method, *args, **kwargs)

    async def _proxy_collective_rpc_async(
        self, method: str, *args, **kwargs
    ) -> list[Any]:
        if self._instance_pool is None:
            return await self._collective_rpc_on_targets_async(
                method, self._proxy_rpc_targets(), *args, **kwargs
            )

        rollout_targets = self._instance_pool.acquire_direct_snapshot()
        try:
            proxy_targets = []
            for target in rollout_targets:
                if target.proxy_worker_id is None or target.proxy_engine_name is None:
                    raise RuntimeError(
                        f"READY instance {target.instance_id} has no initialized proxy"
                    )
                proxy_targets.append(
                    RolloutRPCTarget(
                        instance_id=target.instance_id,
                        worker_id=target.proxy_worker_id,
                        engine_name=target.proxy_engine_name,
                        proxy_addr=target.proxy_addr,
                    )
                )
            return await self._collective_rpc_on_targets_async(
                method, tuple(proxy_targets), *args, **kwargs
            )
        finally:
            self._instance_pool.release_direct_snapshot(rollout_targets)

    async def _collective_rpc_on_targets_async(
        self,
        method: str,
        targets: tuple[RolloutRPCTarget, ...],
        *args,
        **kwargs,
    ) -> list[Any]:
        """Call an engine method on an immutable target snapshot."""
        tasks = [
            self.scheduler.async_call_engine(
                worker_id=target.worker_id,
                method=method,
                engine_name=target.engine_name,
                *args,
                **kwargs,
            )
            for target in targets
        ]
        return await asyncio.gather(*tasks)

    def _choose_worker(self) -> tuple[Worker, int]:
        """Choose a worker for the next request using round-robin scheduling.

        Returns
        -------
        tuple[Worker, int]
            The chosen worker object and its rank
        """
        if not self.workers:
            raise RuntimeError("No workers available to choose from.")
        worker = self.workers[self._current_worker_idx]
        rank = self._current_worker_idx
        self._current_worker_idx = (self._current_worker_idx + 1) % len(self.workers)
        return worker, rank

    def _choose_rollout_target(self) -> RolloutRPCTarget:
        """Choose a routable target without using a rank as elastic identity."""
        if self._instance_pool is not None:
            targets = self._instance_pool.ready_snapshot()
            if not targets:
                raise RuntimeError("No READY elastic rollout instances available")
            target = targets[self._current_worker_idx % len(targets)]
            self._current_worker_idx = (self._current_worker_idx + 1) % len(targets)
            return target

        worker, rank = self._choose_worker()
        return RolloutRPCTarget(
            instance_id=f"static-{rank}",
            worker_id=worker.id,
            engine_name=self._engine_name(rank),
        )

    def _resolve_workflow_str(self, workflow: WorkflowLike | None) -> str | None:
        """Resolve workflow to a string import path.

        Handles RolloutWorkflow, agent workflow instances/classes, string paths,
        and ``None`` (online mode).
        """
        # None workflow = online mode (config-driven)
        if workflow is None:
            return None

        # String paths - return as-is
        if isinstance(workflow, str):
            return workflow

        # RolloutWorkflow classes
        elif isinstance(workflow, type) and issubclass(workflow, RolloutWorkflow):
            return f"{workflow.__module__}.{workflow.__name__}"

        # RolloutWorkflow instances
        elif isinstance(workflow, RolloutWorkflow):
            return f"{workflow.__module__}.{workflow.__class__.__name__}"

        # Agent-like workflow classes
        elif isinstance(workflow, type):
            return f"{workflow.__module__}.{workflow.__name__}"

        # Agent-like workflow instances
        else:
            return f"{workflow.__module__}.{workflow.__class__.__name__}"

    def _resolve_should_accept_fn(
        self, should_accept_fn: Callable[[dict[str, Any]], bool] | str | None
    ):
        if callable(should_accept_fn):
            raise RuntimeError(
                "If given, `should_accept_fn` must be an importable string path, e.g., 'my_module.filter_func'."
            )
        if should_accept_fn is not None:
            try:
                import_from_string(should_accept_fn)
            except Exception:
                raise RuntimeError(
                    f"Failed to import `should_accept_fn` from string path: {should_accept_fn}"
                )
        return should_accept_fn

    def _proxy_addr_for_target(
        self, target: RolloutRPCTarget, explicit_addr: str | None
    ) -> str | None:
        """Resolve the proxy that belongs to the already-selected target."""
        if explicit_addr is not None or not self._proxy_started:
            return explicit_addr
        if self._instance_pool is not None:
            if target.proxy_addr is None:
                raise RuntimeError(
                    f"Elastic instance {target.instance_id} has no proxy"
                )
            return target.proxy_addr
        return self.get_proxy_addr(int(target.engine_name.rsplit("/", maxsplit=1)[1]))

    def _rollout_stats(self) -> str:
        stats = self._staleness_manager.get_stats()
        return (
            f"enqueued: {stats.enqueued}, "
            f"running: {stats.running}, "
            f"accepted: {stats.accepted}, "
            f"rejected: {stats.rejected}."
        )

    def _create_submit_callback(self, pending_task: _RemoteRolloutTaskInput):
        async def _submit_then_wait() -> _RemoteRolloutResult | None:
            # NOTE: No need to call `on_rollout_submitted` here.
            # This function will be passed to `BatchTaskDispather` where
            # `on_rollout_submitted` will be called upon dispatching
            task_id = pending_task.task_id
            bound_instance_id: str | None = None
            target: RolloutRPCTarget | None = None
            dispatch_started_at = time.monotonic()
            timeout_phase = "route"

            manager = self.staleness_manager

            try:
                if self._instance_pool is not None:
                    target = self._instance_pool.reserve_task(
                        str(task_id),
                        max_inflight_per_instance=(self._elastic_capacity_per_instance),
                    )
                    bound_instance_id = target.instance_id
                else:
                    target = self._choose_rollout_target()

                # Set future for this task
                future = asyncio.get_event_loop().create_future()
                with self._futures_lock:
                    self._pending_futures[task_id] = future

                proxy_addr = self._proxy_addr_for_target(
                    target, pending_task.proxy_addr
                )
                timeout_phase = "submit_rpc"
                engine_task_id = await self.scheduler.async_call_engine(
                    target.worker_id,
                    "submit",
                    engine_name=target.engine_name,
                    data=pending_task.data,
                    workflow=pending_task.workflow,
                    workflow_kwargs=pending_task.workflow_kwargs,
                    should_accept_fn=pending_task.should_accept_fn,
                    http_timeout=self.config.request_timeout,
                    is_eval=pending_task.is_eval,
                    group_size=pending_task.group_size,
                    task_id=task_id,
                    callback_addr=f"http://{self.callback_addr}/callback/rollout_complete",
                    proxy_addr=proxy_addr,
                )

                assert task_id == engine_task_id, (task_id, engine_task_id)

                # Wait for callback to resolve the future
                timeout_phase = "wait_callback"
                await asyncio.wait_for(future, timeout=self.config.request_timeout)

                # Fetch the result
                timeout_phase = "fetch_result"
                result = await self.scheduler.async_call_engine(
                    target.worker_id,
                    "wait_for_task",
                    engine_name=target.engine_name,
                    task_id=engine_task_id,
                    timeout=0.1,  # A short time to prevent blocking other requests
                    raise_timeout=False,
                    http_timeout=self.config.request_timeout,
                )

                traj = result
                if traj is not None:
                    manager.on_rollout_accepted()
                    if self.config.enable_rollout_tracing:
                        logger.info(
                            f"Finish and accept rollout. {self._rollout_stats()}"
                        )
                    result = _RemoteRolloutResult(task_id=task_id, trajectory=traj)
                    if bound_instance_id is not None:
                        self._acquire_result_lease(
                            instance_id=bound_instance_id,
                            task_id=task_id,
                            trajectory=traj,
                        )
                    return result

                manager.on_rollout_rejected()
                if self.config.enable_rollout_tracing:
                    logger.info(f"Finish but reject rollout. {self._rollout_stats()}")
                return None

            except TimeoutError as exc:
                if task_id is not None:
                    with self._futures_lock:
                        self._pending_futures.pop(task_id, None)
                manager.on_rollout_rejected()
                now = time.monotonic()
                instance_state = "static"
                loaded_version = None
                if self._instance_pool is not None:
                    instance_state = "not_registered"
                    if bound_instance_id is not None:
                        instance = next(
                            (
                                item
                                for item in self._instance_pool.instances_snapshot()
                                if item.instance_id == bound_instance_id
                            ),
                            None,
                        )
                        if instance is not None:
                            instance_state = instance.state.value
                            loaded_version = instance.loaded_version
                logger.error(
                    "Rollout timed out task_id=%s phase=%s instance_id=%s "
                    "worker_id=%s instance_state=%s loaded_version=%s "
                    "enqueued_version=%s current_version=%s elapsed_seconds=%.2f "
                    "queue_age_seconds=%.2f configured_timeout_seconds=%.1f error=%r",
                    task_id,
                    timeout_phase,
                    bound_instance_id or (target.instance_id if target else None),
                    target.worker_id if target else None,
                    instance_state,
                    loaded_version,
                    pending_task.enqueued_version,
                    self.get_version(),
                    now - dispatch_started_at,
                    now - pending_task.enqueued_at,
                    self.config.request_timeout,
                    exc,
                )
                return None
            except Exception as exc:
                if task_id is not None:
                    with self._futures_lock:
                        self._pending_futures.pop(task_id, None)
                manager.on_rollout_rejected()
                logger.error("Workflow execution failed: %s", exc, exc_info=True)
                return None
            finally:
                if bound_instance_id is not None:
                    self._instance_pool.release_task(str(task_id))

        return _submit_then_wait

    def _acquire_result_lease(
        self,
        *,
        instance_id: str,
        task_id: int,
        trajectory: dict[str, Any],
    ) -> None:
        """Keep the trajectory's storage owner alive until the batch is cleared."""
        if self._instance_pool is None:
            return
        shards_by_node = RTensor.collect_shards(trajectory)
        shard_ids = {
            shard_id for shard_ids in shards_by_node.values() for shard_id in shard_ids
        }
        if not shard_ids:
            return

        lease_id = str(task_id)
        with self._elastic_result_lease_lock:
            if lease_id in self._elastic_result_lease_instance:
                raise RuntimeError(f"duplicate elastic result lease {lease_id}")
            collisions = shard_ids.intersection(self._elastic_shard_to_result_lease)
            if collisions:
                raise RuntimeError(
                    "elastic result shards already have an owner: "
                    f"{sorted(str(shard_id) for shard_id in collisions)}"
                )
            # Acquire the pool lease before publishing the shard mapping. The
            # workflow-task lease is still held here, so the reconciler cannot
            # remove this instance between the two operations.
            self._instance_pool.acquire_result_lease(instance_id, lease_id)
            self._elastic_result_lease_instance[lease_id] = instance_id
            self._elastic_result_lease_shards[lease_id] = shard_ids
            for shard_id in shard_ids:
                self._elastic_shard_to_result_lease[shard_id] = lease_id

    def release_batch(self, *targets: Any) -> None:
        """Release elastic instances after consumers finish returned RTensors.

        Results waiting in the dispatcher retain their leases because their
        shard IDs do not appear in ``targets`` yet. Cleanup is idempotent so a
        batch may safely contain the same shard through derived structures.
        """
        if self._instance_pool is None:
            return
        shards_by_node = RTensor.collect_shards(targets)
        shard_ids = {
            shard_id for shard_ids in shards_by_node.values() for shard_id in shard_ids
        }
        if not shard_ids:
            return

        released: list[tuple[str, str]] = []
        with self._elastic_result_lease_lock:
            lease_ids = {
                lease_id
                for shard_id in shard_ids
                if (lease_id := self._elastic_shard_to_result_lease.get(shard_id))
                is not None
            }
            for lease_id in lease_ids:
                instance_id = self._elastic_result_lease_instance.pop(lease_id)
                lease_shards = self._elastic_result_lease_shards.pop(lease_id)
                for shard_id in lease_shards:
                    self._elastic_shard_to_result_lease.pop(shard_id, None)
                released.append((instance_id, lease_id))

        for instance_id, lease_id in released:
            self._instance_pool.release_result_lease(instance_id, lease_id)

    def get_capacity(self):
        return self.staleness_manager.get_capacity()

    def submit(
        self,
        data: dict[str, Any],
        workflow: WorkflowLike,
        workflow_kwargs: dict[str, Any] | None = None,
        should_accept_fn: str | None = None,
        task_id: int | None = None,
        is_eval: bool = False,
        group_size: int = 1,
        proxy_addr: str | None = None,
    ) -> int:
        workflow_str = self._resolve_workflow_str(workflow)
        should_accept_fn = self._resolve_should_accept_fn(should_accept_fn)
        if workflow_kwargs is None:
            workflow_kwargs = {}

        # NOTE: RolloutController does not support `should_accept_fn`
        # If the workflow's result should be aborted,
        # `arun_episode` should return None instead.
        if task_id is None:
            task_id = self._task_id_generator.next()
        task_input = _RemoteRolloutTaskInput(
            data=data,
            workflow=workflow_str,
            workflow_kwargs=workflow_kwargs,
            should_accept_fn=should_accept_fn,
            task_id=task_id,
            is_eval=is_eval,
            group_size=group_size,
            proxy_addr=proxy_addr,
            enqueued_version=self.get_version(),
        )

        # Delegate to dispatcher
        self.dispatcher.submit_task_input(task_input)
        return task_id

    def wait(
        self, count: int, timeout: float | None = None, raise_timeout: bool = True
    ) -> list[dict[str, Any] | None]:
        # Delegate to dispatcher and extract trajectories
        results = self.dispatcher.wait_results(count, timeout, raise_timeout)
        # Log and trace
        if self.config.enable_rollout_tracing:
            logger.info("Rollout results are ready!")

        return [r.trajectory if r is not None else None for r in results]

    @trace_perf("rollout_controller.rollout_batch", category="scheduler")
    def rollout_batch(
        self,
        data: list[dict[str, Any]],
        workflow: WorkflowLike,
        workflow_kwargs: dict[str, Any] | None = None,
        should_accept_fn: str | None = None,
        group_size: int = 1,
    ) -> list[dict[str, Any]]:
        perf_tracer.instant(
            "rollout_controller.rollout_batch",
            category="scheduler",
            args={"data": len(data)},
        )
        for item in data:
            self.submit(
                data=item,
                workflow=workflow,
                workflow_kwargs=workflow_kwargs,
                should_accept_fn=should_accept_fn,
                group_size=group_size,
            )
        results = self.wait(count=len(data))
        # Return list of trajectories
        return [r for r in results if r is not None]

    @trace_perf("rollout_controller.prepare_batch", category="scheduler")
    def prepare_batch(
        self,
        dataloader: StatefulDataLoader,
        workflow: WorkflowLike,
        workflow_kwargs: dict[str, Any] | None = None,
        should_accept_fn: str | None = None,
        group_size: int = 1,
        dynamic_bs: bool = False,
    ) -> list[dict[str, Any]]:
        """Prepare a batch with controlled staleness.

        Continuously submits from dataloader and waits for results, ensuring at least
        two batches are pending to maximize overlap.

        See :meth:`~areal.api.engine_api.InferenceEngine.prepare_batch` for parameters.
        """

        workflow_str = self._resolve_workflow_str(workflow)
        if workflow_kwargs is None:
            workflow_kwargs = {}

        def task_input_generator():
            for data in cycle_dataloader(dataloader):
                for item in data:
                    yield _RemoteRolloutTaskInput(
                        data=item,
                        workflow=workflow_str,
                        workflow_kwargs=workflow_kwargs,
                        should_accept_fn=should_accept_fn,
                        task_id=self._task_id_generator.next(),
                        group_size=group_size,
                        enqueued_version=self.get_version(),
                    )

        if not hasattr(self, "data_generator"):
            self.data_generator = task_input_generator()

        # Delegate to dispatcher
        assert dataloader.batch_size is not None
        results = self.dispatcher.active_submit_and_wait(
            self.data_generator, batch_size=dataloader.batch_size, dynamic_bs=dynamic_bs
        )

        # Return list of trajectories
        trajectories = [r.trajectory if r is not None else None for r in results]
        return [t for t in trajectories if t is not None]

    def compute_logp(self, data: list[dict[str, Any]]) -> list[Any]:
        """Compute token log-probabilities for trajectories via remote workers."""
        if len(data) == 0:
            return []

        async def _compute():
            indexed_chunks: list[list[int]] = []
            tasks = []
            if self._instance_pool is None:
                targets = self._rollout_rpc_targets()
            else:
                targets = self._instance_pool.acquire_direct_snapshot()
            n_workers = len(targets)
            if not targets:
                raise RuntimeError("No workers available for compute_logp.")

            try:
                for rank, target in enumerate(targets):
                    idxs = list(range(rank, len(data), n_workers))
                    if not idxs:
                        continue
                    chunk = [data[i] for i in idxs]
                    indexed_chunks.append(idxs)
                    tasks.append(
                        self.scheduler.async_call_engine(
                            worker_id=target.worker_id,
                            method="compute_logp",
                            engine_name=target.engine_name,
                            data=chunk,
                            http_timeout=self.config.request_timeout,
                        )
                    )
                rpc_results = await asyncio.gather(*tasks)
                merged: list[Any] = [None] * len(data)
                for idxs, chunk_result in zip(indexed_chunks, rpc_results):
                    if len(chunk_result) != len(idxs):
                        raise RuntimeError(
                            f"compute_logp result length mismatch: got "
                            f"{len(chunk_result)}, expected {len(idxs)}"
                        )
                    for out_idx, value in zip(idxs, chunk_result):
                        merged[out_idx] = value
                return merged
            finally:
                if self._instance_pool is not None:
                    self._instance_pool.release_direct_snapshot(targets)

        return run_async_task(_compute)

    async def agenerate(self, req: ModelRequest) -> ModelResponse:
        """Asynchronously generate a response for the given request.

        This method provides direct access to the inference engine's generation capabilities
        for single requests, bypassing the workflow system.

        Parameters
        ----------
        req : ModelRequest
            The model request containing input data and generation parameters

        Returns
        -------
        ModelResponse
            The generated response from the model
        """
        if self._instance_pool is not None:
            target = self._instance_pool.reserve_direct_request(
                max_inflight_per_instance=self._elastic_capacity_per_instance
            )
        else:
            target = self._choose_rollout_target()
        try:
            return await self.scheduler.async_call_engine(
                worker_id=target.worker_id,
                method="agenerate",
                engine_name=target.engine_name,
                req=req,
            )
        finally:
            if self._instance_pool is not None:
                self._instance_pool.release_direct_request(target.instance_id)

    async def init_weights_update_group(self, meta: WeightUpdateMeta) -> None:
        if self._instance_pool is not None:
            raise RuntimeError(
                "elastic RolloutController V1 supports disk weight updates only"
            )
        targets = self._rollout_rpc_targets()
        tasks = [
            self.scheduler.async_call_engine(
                worker_id=target.worker_id,
                method="init_weights_update_group",
                engine_name=target.engine_name,
                meta=meta,
                xccl_group_ranks=[rank],
            )
            for rank, target in enumerate(targets)
        ]
        await asyncio.gather(*tasks)

    async def update_weights_from_distributed(
        self, meta: WeightUpdateMeta, param_specs: list[ParamSpec]
    ):
        if self._instance_pool is not None:
            raise RuntimeError(
                "elastic RolloutController V1 supports disk weight updates only"
            )
        await self._collective_rpc_async(
            "update_weights_from_distributed", meta=meta, param_specs=param_specs
        )

    async def update_weights_from_disk(self, meta: WeightUpdateMeta):
        meta.clear_checkpoint_after_load = False
        targets: tuple[RolloutRPCTarget, ...] = ()
        if self._instance_pool is not None:
            if meta.version is None:
                raise DiskCheckpointCatalogError(
                    "elastic disk weight updates require a checkpoint version"
                )
            self._begin_elastic_weight_update(meta.version)
        else:
            targets = self._rollout_rpc_targets()
        try:
            if self._instance_pool is not None:
                targets = self._instance_pool.acquire_weight_update_snapshot()
            if self._instance_pool is not None and not targets:
                raise RuntimeError(
                    "elastic disk update requires at least one READY rollout instance"
                )
            await self._collective_rpc_on_targets_async(
                "update_weights_from_disk", targets, meta=meta
            )
            if self.config.elastic.enabled:
                self._record_elastic_disk_checkpoint(meta)
                self._instance_pool.mark_loaded_version(targets, meta.version)
            else:
                shutil.rmtree(meta.path, ignore_errors=True)
        except BaseException:
            if self._instance_pool is not None:
                assert meta.version is not None
                self._abort_elastic_weight_update(meta.version)
            raise
        finally:
            if self._instance_pool is not None:
                self._instance_pool.release_weight_update_snapshot(targets)

    def _record_elastic_disk_checkpoint(self, meta: WeightUpdateMeta) -> None:
        """Persist a successfully loaded checkpoint for a future elastic instance.

        This runs only after every current V1 rollout worker has completed the
        disk update.  The static lifecycle intentionally keeps its historical
        immediate cleanup behavior.
        """
        if meta.type != "disk":
            raise DiskCheckpointCatalogError(
                "elastic rollout checkpoint catalog requires disk updates"
            )
        if meta.version is None:
            raise DiskCheckpointCatalogError(
                "elastic disk weight updates require a checkpoint version"
            )
        if meta.path is None:
            raise DiskCheckpointCatalogError(
                "elastic disk weight updates require a checkpoint path"
            )

        checkpoint_path = Path(meta.path).resolve()
        if self._disk_checkpoint_catalog is None:
            self._disk_checkpoint_catalog = DiskCheckpointCatalog(
                checkpoint_path.parent
            )
        elif self._disk_checkpoint_catalog.path.parent != checkpoint_path.parent:
            raise DiskCheckpointCatalogError(
                "elastic disk checkpoint directory changed during one run"
            )

        self._disk_checkpoint_catalog.commit(
            DiskCheckpointManifest(version=meta.version, path=str(checkpoint_path))
        )
        protected_versions = {
            instance.loaded_version
            for instance in self._instance_pool.instances_snapshot()
            if instance.loaded_version is not None
            and (instance.state.value == "catching_up" or instance.update_leases > 0)
        }
        self._disk_checkpoint_catalog.collect_garbage(
            retention=self.config.elastic.checkpoint_retention,
            protected_versions=protected_versions,
        )

    async def update_weights_from_awex(
        self,
        meta: WeightUpdateMeta,
        step_id: int | None = None,
        kwargs: dict[str, Any] | None = None,
    ):
        if self._instance_pool is not None:
            raise RuntimeError(
                "elastic RolloutController V1 supports disk weight updates only"
            )
        await self._collective_rpc_async(
            "update_weights_from_awex", meta=meta, step_id=step_id, kwargs=kwargs
        )

    async def pause_generation(self):
        await self._collective_rpc_async("pause_generation")

    async def continue_generation(self):
        await self._collective_rpc_async("continue_generation")

    def offload(self) -> None:
        """Offload rollout model memory on all inference workers."""
        self._collective_rpc("offload")

    def onload(self, tags: list[str] | None = None) -> None:
        """Onload rollout model memory on all inference workers."""
        self._collective_rpc("onload", tags=tags)

    def set_version(self, version: int) -> None:
        if self._instance_pool is None:
            with self._version_lock:
                self._version = version
                self._collective_rpc("set_version", version=version, http_timeout=60.0)
                if self._proxy_started:
                    self._proxy_collective_rpc(
                        "set_version", version=version, http_timeout=60.0
                    )
            return

        with self._elastic_update_condition:
            self._validate_elastic_serving_version(version)
            if version > 0:
                checkpoint = self._latest_elastic_checkpoint_for_version(version)
                if checkpoint is None or checkpoint.version != version:
                    raise RuntimeError(
                        f"serving version {version} has no matching disk checkpoint"
                    )
            with self._version_lock:
                self._collective_rpc("set_version", version=version, http_timeout=60.0)
                if self._proxy_started:
                    self._proxy_collective_rpc(
                        "set_version", version=version, http_timeout=60.0
                    )
                self._version = version
            self._save_elastic_recovery_state()
            self._finish_elastic_weight_update(version)

    def get_version(self) -> int:
        with self._version_lock:
            return self._version

    def get_active_rollout_gpu_count(self) -> int:
        """Return GPUs backing the rollout instances currently serving traffic."""
        if self._instance_pool is None:
            return self.rollout_alloc.parallel.world_size
        instance_size = (
            self.rollout_alloc.parallel.tp_size * self.rollout_alloc.parallel.pp_size
        )
        return len(self._instance_pool.ready_snapshot()) * instance_size

    def pause(self):
        self.dispatcher.pause()
        self._collective_rpc("pause", http_timeout=60.0)

    def resume(self):
        self._collective_rpc("resume", http_timeout=60.0)
        self.dispatcher.resume()

    def export_stats(self) -> dict[str, float]:
        all_raw_stats = self._collective_rpc(method="export_stats", http_timeout=60.0)
        stats = defaultdict(float)
        counts = defaultdict(int)

        for raw_stats in all_raw_stats:
            for k, v in raw_stats.items():
                if k.endswith("__count"):
                    counts[k] += v
                else:
                    stats[k] += v * raw_stats.get(k + "__count", 0)

        # Average non-count stats
        final_stats = {}
        for k, v in stats.items():
            count_key = k + "__count"
            if count_key in counts and counts[count_key] > 0:
                final_stats[k] = v / counts[count_key]
        return final_stats

    def config_perf_tracer(self, config: PerfTracerConfig, role: str) -> None:
        async def _call():
            if self._instance_pool is None:
                targets = self._rollout_rpc_targets()
            else:
                targets = self._instance_pool.acquire_direct_snapshot()
            try:
                tasks = [
                    self.scheduler.async_call_engine(
                        worker_id=target.worker_id,
                        method="config_perf_tracer",
                        engine_name=target.engine_name,
                        rank=rank,
                        role=role,
                        config=config,
                    )
                    for rank, target in enumerate(targets)
                ]
                return await asyncio.gather(*tasks)
            finally:
                if self._instance_pool is not None:
                    self._instance_pool.release_direct_snapshot(targets)

        run_async_task(_call)

    def save_perf_tracer(self, step: int | None = None, force: bool = False) -> None:
        self._collective_rpc("save_perf_tracer", step=step, force=force)

    @property
    def staleness_manager(self):
        return self._staleness_manager

    @property
    def dispatcher(
        self,
    ) -> BatchTaskDispatcher[_RemoteRolloutTaskInput, _RemoteRolloutResult]:
        """Get the task dispatcher, ensuring initialization has been called."""
        if self._dispatcher is None:
            raise RuntimeError(
                "RolloutController.initialize() must be called before scheduling rollouts."
            )
        return self._dispatcher

    @property
    def runner(self):
        """For backward compatibility. The runner is now owned by the dispatcher."""
        return self.dispatcher.runner
