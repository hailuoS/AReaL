# SPDX-License-Identifier: Apache-2.0

"""Single-node Scheduler launcher for one elastic Rollout V1 instance."""

from __future__ import annotations

import time
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any

from areal.api import (
    InferenceEngine,
    Job,
    LocalInfServerInfo,
    Scheduler,
    WeightUpdateMeta,
)
from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import InferenceEngineConfig, SchedulingSpec
from areal.infra.scheduler.capacity import WorkerCapacityProvider
from areal.utils import logging
from areal.utils.network import format_hostport

from .disk_catalog import DiskCheckpointManifest
from .errors import SingleNodeInstanceError
from .models import RolloutInstance, RolloutInstanceState

logger = logging.getLogger("RolloutInstanceLauncher")


def _log_timing(
    instance: RolloutInstance,
    *,
    event: str,
    started_at: float,
    **context: Any,
) -> None:
    fields = " ".join(f"{key}={value}" for key, value in context.items())
    logger.info(
        "Elastic scale-up timing event=%s instance_id=%s worker_role=%s "
        "elapsed_seconds=%.3f%s",
        event,
        instance.instance_id,
        instance.worker_role,
        time.monotonic() - started_at,
        f" {fields}" if fields else "",
    )


@dataclass(frozen=True)
class RolloutLaunchResult:
    """Resources created for one complete, not-yet-ready rollout instance."""

    instance: RolloutInstance
    server_info: LocalInfServerInfo


class RolloutInstanceLauncher:
    """Launch and destroy exactly one single-node TP × PP rollout instance."""

    def __init__(
        self,
        *,
        scheduler: Scheduler,
        inf_engine: type[InferenceEngine],
        config: InferenceEngineConfig,
        rollout_alloc: ModelAllocation,
        resource_provision_timeout_seconds: float | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._inf_engine = inf_engine
        self._config = config
        self._rollout_alloc = rollout_alloc
        if (
            resource_provision_timeout_seconds is not None
            and resource_provision_timeout_seconds <= 0
        ):
            raise ValueError("resource_provision_timeout_seconds must be positive")
        self._resource_provision_timeout_seconds = resource_provision_timeout_seconds
        capacity_provider_factory = getattr(
            scheduler, "create_worker_capacity_provider", None
        )
        self._capacity_provider: WorkerCapacityProvider | None = (
            capacity_provider_factory() if callable(capacity_provider_factory) else None
        )

    def _instance_scheduling_spec(self) -> SchedulingSpec:
        instance_size = self._instance_size()
        spec = SchedulingSpec(**asdict(self._config.scheduling_spec[0]))
        spec.cpu *= instance_size
        spec.mem *= instance_size
        if spec.gpu > 0:
            spec.gpu = instance_size
        return spec

    def _instance_size(self) -> int:
        return (
            self._rollout_alloc.parallel.tp_size * self._rollout_alloc.parallel.pp_size
        )

    def _validate_single_node_capacity(self) -> None:
        instance_size = self._instance_size()
        if instance_size > self._scheduler.n_gpus_per_node:
            raise SingleNodeInstanceError(
                "A single-node elastic rollout instance requires TP × PP to fit "
                f"one node: {instance_size} > {self._scheduler.n_gpus_per_node}."
            )

    @staticmethod
    def proxy_role(instance: RolloutInstance) -> str:
        """Return the stable Scheduler role for an instance-local V1 proxy."""
        return f"proxy-{instance.worker_role}"

    def _job_for(self, instance: RolloutInstance) -> Job:
        self._validate_single_node_capacity()
        if instance.state is not RolloutInstanceState.PENDING:
            raise ValueError("instance must be PENDING before provisioning")
        return Job(
            role=instance.worker_role,
            replicas=1,
            tasks=[self._instance_scheduling_spec()],
            scheduling_strategy=self._config.scheduling_strategy,
        )

    @staticmethod
    def _bind_provisioned_worker(
        instance: RolloutInstance, worker_ids: Sequence[str]
    ) -> None:
        if len(worker_ids) != 1:
            raise RuntimeError(
                "Expected one provisioned Worker ID for "
                f"{instance.worker_role}, got {len(worker_ids)}"
            )
        instance.worker_id = worker_ids[0]
        instance.transition_to(RolloutInstanceState.STARTING)

    @staticmethod
    def _mark_provision_failed(instance: RolloutInstance) -> None:
        if instance.state is not RolloutInstanceState.FAILED:
            instance.transition_to(RolloutInstanceState.FAILED)

    def provision(self, *, instance: RolloutInstance) -> None:
        """Create one Scheduler role without initializing its inference engine.

        Scheduler implementations mutate shared resource and worker registries from
        synchronous methods, so callers must serialize this phase.
        """
        started_at = time.monotonic()
        job = self._job_for(instance)
        workers_created = False
        try:
            worker_ids = self._scheduler.create_workers(job=job)
            workers_created = True
            self._bind_provisioned_worker(instance, worker_ids)
            _log_timing(instance, event="provision_completed", started_at=started_at)
        except BaseException as error:
            _log_timing(
                instance,
                event="provision_failed",
                started_at=started_at,
                error_type=type(error).__name__,
            )
            self._mark_provision_failed(instance)
            if workers_created:
                self._scheduler.delete_workers(role=instance.worker_role)
            raise

    async def provision_many(
        self, instances: Sequence[RolloutInstance]
    ) -> list[Exception | None]:
        """Provision a batch, submitting all Ray demands before waiting.

        Schedulers without a batch-capacity adapter retain the legacy serialized
        create_workers behavior.
        """
        if self._capacity_provider is None:
            results: list[Exception | None] = []
            for instance in instances:
                try:
                    self.provision(instance=instance)
                except Exception as error:
                    results.append(error)
                else:
                    results.append(None)
            return results

        results: list[Exception | None] = [None] * len(instances)
        jobs: list[Job] = []
        job_instances: list[tuple[int, RolloutInstance, float]] = []
        for index, instance in enumerate(instances):
            started_at = time.monotonic()
            try:
                job = self._job_for(instance)
            except Exception as error:
                results[index] = error
                continue
            jobs.append(job)
            job_instances.append((index, instance, started_at))

        if not jobs:
            return results

        try:
            outcomes = await self._capacity_provider.provision_many(
                jobs,
                timeout=self._resource_provision_timeout_seconds,
            )
            if len(outcomes) != len(jobs):
                for outcome in outcomes:
                    if outcome.succeeded:
                        self._scheduler.delete_workers(role=outcome.role)
                raise RuntimeError(
                    "Capacity provider returned an unexpected number of outcomes: "
                    f"{len(outcomes)} != {len(jobs)}"
                )
        except Exception as error:
            for index, instance, started_at in job_instances:
                results[index] = error
                self._mark_provision_failed(instance)
                _log_timing(
                    instance,
                    event="provision_failed",
                    started_at=started_at,
                    error_type=type(error).__name__,
                )
            return results

        for (index, instance, started_at), outcome in zip(
            job_instances, outcomes, strict=True
        ):
            error = outcome.error
            if outcome.role != instance.worker_role:
                if outcome.succeeded:
                    self._scheduler.delete_workers(role=outcome.role)
                error = RuntimeError(
                    "Capacity provider returned role "
                    f"'{outcome.role}' for '{instance.worker_role}'"
                )
            if error is None:
                try:
                    self._bind_provisioned_worker(instance, outcome.worker_ids)
                except Exception as bind_error:
                    self._scheduler.delete_workers(role=instance.worker_role)
                    error = bind_error

            if error is not None:
                results[index] = error
                self._mark_provision_failed(instance)
                _log_timing(
                    instance,
                    event="provision_failed",
                    started_at=started_at,
                    error_type=type(error).__name__,
                )
            else:
                _log_timing(
                    instance,
                    event="provision_completed",
                    started_at=started_at,
                )
        return results

    async def start(
        self,
        *,
        instance: RolloutInstance,
        server_args: dict[str, Any],
        initialize_kwargs: dict[str, Any] | None = None,
    ) -> RolloutLaunchResult:
        """Initialize one provisioned role and leave it in CATCHING_UP."""
        if instance.state is not RolloutInstanceState.STARTING:
            raise ValueError("instance must be STARTING before start")

        startup_started_at = time.monotonic()
        try:
            phase_started_at = time.monotonic()
            workers = self._scheduler.get_workers(role=instance.worker_role)
            if len(workers) != 1:
                raise RuntimeError(
                    "Expected one logical Worker for "
                    f"{instance.worker_role}, got {len(workers)}"
                )

            worker = workers[0]
            if instance.worker_id != worker.id:
                raise RuntimeError(
                    f"Provisioned Worker ID {instance.worker_id} does not match "
                    f"ready Worker ID {worker.id}"
                )
            _log_timing(instance, event="worker_ready", started_at=phase_started_at)

            phase_started_at = time.monotonic()
            await self._scheduler.create_engine(
                worker_id=worker.id,
                engine=f"{self._inf_engine.__module__}.{self._inf_engine.__name__}",
                engine_name=instance.engine_name,
                config=self._config,
            )
            _log_timing(instance, event="engine_created", started_at=phase_started_at)

            phase_started_at = time.monotonic()
            server_info = await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="launch_server",
                engine_name=instance.engine_name,
                server_args=deepcopy(server_args),
            )
            _log_timing(instance, event="server_launched", started_at=phase_started_at)
            if not isinstance(server_info, LocalInfServerInfo):
                raise TypeError(
                    "launch_server must return LocalInfServerInfo, got "
                    f"{type(server_info)!r}"
                )
            instance.server_host = server_info.host
            instance.server_port = server_info.port

            init_kwargs = dict(initialize_kwargs or {})
            if init_kwargs.get("train_data_parallel_size") is None:
                init_kwargs["train_data_parallel_size"] = 1
            phase_started_at = time.monotonic()
            await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="initialize",
                engine_name=instance.engine_name,
                engine_id=instance.instance_id,
                engine_rank=0,
                num_engines=1,
                **init_kwargs,
            )
            _log_timing(
                instance, event="engine_initialized", started_at=phase_started_at
            )
            instance.transition_to(RolloutInstanceState.CATCHING_UP)
            _log_timing(
                instance,
                event="startup_completed",
                started_at=startup_started_at,
            )
            return RolloutLaunchResult(instance=instance, server_info=server_info)
        except BaseException as error:
            _log_timing(
                instance,
                event="startup_failed",
                started_at=startup_started_at,
                error_type=type(error).__name__,
            )
            if instance.state is not RolloutInstanceState.FAILED:
                instance.transition_to(RolloutInstanceState.FAILED)
            raise

    async def launch(
        self,
        *,
        instance: RolloutInstance,
        server_args: dict[str, Any],
        initialize_kwargs: dict[str, Any] | None = None,
    ) -> RolloutLaunchResult:
        """Provision and start one instance for direct launcher callers."""
        provisioned = False
        try:
            self.provision(instance=instance)
            provisioned = True
            return await self.start(
                instance=instance,
                server_args=server_args,
                initialize_kwargs=initialize_kwargs,
            )
        except BaseException:
            if provisioned:
                self._scheduler.delete_workers(role=instance.worker_role)
            raise

    async def launch_proxy(self, instance: RolloutInstance) -> None:
        """Fork and initialize the V1 proxy owned by one elastic instance."""
        if instance.proxy_ready:
            return
        if instance.server_host is None or instance.server_port is None:
            raise ValueError("instance server address is unavailable")

        proxy_role = self.proxy_role(instance)
        proxy_created = False
        proxy_started_at = time.monotonic()
        try:
            phase_started_at = time.monotonic()
            worker_ids = self._scheduler.fork_workers(
                role=proxy_role,
                target_role=instance.worker_role,
                command="areal.experimental.openai.proxy.proxy_rollout_server",
            )
            _log_timing(instance, event="proxy_forked", started_at=phase_started_at)
            proxy_created = True
            phase_started_at = time.monotonic()
            workers = self._scheduler.get_workers(role=proxy_role)
            if len(worker_ids) != 1 or len(workers) != 1:
                raise RuntimeError(
                    f"Expected one proxy Worker for {instance.instance_id}, got "
                    f"ids={len(worker_ids)} workers={len(workers)}"
                )
            _log_timing(
                instance, event="proxy_worker_ready", started_at=phase_started_at
            )

            worker = workers[0]
            if not worker.worker_ports:
                raise RuntimeError(f"Proxy worker {worker.id} has no HTTP port")
            engine_name = f"proxy/{instance.instance_id}"
            phase_started_at = time.monotonic()
            await self._scheduler.create_engine(
                worker_id=worker.id,
                engine=f"{self._inf_engine.__module__}.{self._inf_engine.__name__}",
                engine_name=engine_name,
                config=self._config,
            )
            _log_timing(
                instance,
                event="proxy_engine_created",
                started_at=phase_started_at,
            )
            phase_started_at = time.monotonic()
            await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="initialize",
                engine_name=engine_name,
                addr=format_hostport(instance.server_host, instance.server_port),
            )
            _log_timing(
                instance,
                event="proxy_initialized",
                started_at=phase_started_at,
            )
            instance.attach_proxy(
                role=proxy_role,
                worker_id=worker.id,
                engine_name=engine_name,
                addr=f"http://{format_hostport(worker.ip, int(worker.worker_ports[0]))}",
            )
            _log_timing(
                instance,
                event="proxy_completed",
                started_at=proxy_started_at,
            )
        except BaseException as error:
            _log_timing(
                instance,
                event="proxy_failed",
                started_at=proxy_started_at,
                error_type=type(error).__name__,
            )
            if proxy_created:
                self._scheduler.delete_workers(role=proxy_role)
            raise

    async def catch_up_from_disk(
        self,
        instance: RolloutInstance,
        checkpoint: DiskCheckpointManifest,
    ) -> None:
        """Load one committed disk version before making an instance routable."""
        if instance.state is not RolloutInstanceState.CATCHING_UP:
            raise ValueError("instance must be in CATCHING_UP before disk catch-up")

        meta = WeightUpdateMeta(
            type="disk",
            path=checkpoint.path,
            version=checkpoint.version,
            clear_checkpoint_after_load=False,
        )
        # Expose the version being loaded while CATCHING_UP so checkpoint GC
        # protects the directory until this RPC finishes.
        instance.loaded_version = checkpoint.version
        catch_up_started_at = time.monotonic()
        try:
            phase_started_at = time.monotonic()
            await self._scheduler.async_call_engine(
                worker_id=instance.worker_id,
                method="update_weights_from_disk",
                engine_name=instance.engine_name,
                meta=meta,
            )
            _log_timing(
                instance,
                event="disk_weights_loaded",
                started_at=phase_started_at,
                version=checkpoint.version,
            )
            phase_started_at = time.monotonic()
            await self._scheduler.async_call_engine(
                worker_id=instance.worker_id,
                method="set_version",
                engine_name=instance.engine_name,
                version=checkpoint.version,
            )
            _log_timing(
                instance,
                event="engine_version_set",
                started_at=phase_started_at,
                version=checkpoint.version,
            )
            if instance.proxy_ready:
                phase_started_at = time.monotonic()
                await self._scheduler.async_call_engine(
                    worker_id=instance.proxy_worker_id,
                    method="set_version",
                    engine_name=instance.proxy_engine_name,
                    version=checkpoint.version,
                )
                _log_timing(
                    instance,
                    event="proxy_version_set",
                    started_at=phase_started_at,
                    version=checkpoint.version,
                )
            instance.loaded_version = checkpoint.version
            instance.transition_to(RolloutInstanceState.READY)
            _log_timing(
                instance,
                event="catch_up_completed",
                started_at=catch_up_started_at,
                version=checkpoint.version,
            )
        except BaseException as error:
            _log_timing(
                instance,
                event="catch_up_failed",
                started_at=catch_up_started_at,
                version=checkpoint.version,
                error_type=type(error).__name__,
            )
            instance.transition_to(RolloutInstanceState.FAILED)
            raise

    def destroy(self, instance: RolloutInstance) -> None:
        """Destroy a stopped instance without affecting any other Scheduler role."""
        if instance.state is not RolloutInstanceState.STOPPING:
            raise ValueError(
                "instance must be in STOPPING state before launcher destruction"
            )
        if getattr(instance, "proxy_role", None) is not None:
            self._scheduler.delete_workers(role=instance.proxy_role)
            instance.detach_proxy()
        self._scheduler.delete_workers(role=instance.worker_role)
        instance.transition_to(RolloutInstanceState.STOPPED)
