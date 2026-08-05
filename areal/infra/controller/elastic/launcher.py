# SPDX-License-Identifier: Apache-2.0

"""Single-node Scheduler launcher for one elastic Rollout V1 instance."""

from __future__ import annotations

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
from areal.utils.network import format_hostport

from .disk_catalog import DiskCheckpointManifest
from .errors import SingleNodeInstanceError
from .models import RolloutInstance, RolloutInstanceState


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
    ) -> None:
        self._scheduler = scheduler
        self._inf_engine = inf_engine
        self._config = config
        self._rollout_alloc = rollout_alloc

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

    def provision(self, *, instance: RolloutInstance) -> None:
        """Create one Scheduler role without initializing its inference engine.

        Scheduler implementations mutate shared resource and worker registries from
        synchronous methods, so callers must serialize this phase.
        """
        self._validate_single_node_capacity()
        if instance.state is not RolloutInstanceState.PENDING:
            raise ValueError("instance must be PENDING before provisioning")

        job = Job(
            role=instance.worker_role,
            replicas=1,
            tasks=[self._instance_scheduling_spec()],
            scheduling_strategy=self._config.scheduling_strategy,
        )
        workers_created = False
        try:
            worker_ids = self._scheduler.create_workers(job=job)
            workers_created = True
            if len(worker_ids) != 1:
                raise RuntimeError(
                    "Expected one provisioned Worker ID for "
                    f"{instance.worker_role}, got {len(worker_ids)}"
                )
            instance.worker_id = worker_ids[0]
            instance.transition_to(RolloutInstanceState.STARTING)
        except BaseException:
            if instance.state is not RolloutInstanceState.FAILED:
                instance.transition_to(RolloutInstanceState.FAILED)
            if workers_created:
                self._scheduler.delete_workers(role=instance.worker_role)
            raise

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

        try:
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
            await self._scheduler.create_engine(
                worker_id=worker.id,
                engine=f"{self._inf_engine.__module__}.{self._inf_engine.__name__}",
                engine_name=instance.engine_name,
                config=self._config,
            )
            server_info = await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="launch_server",
                engine_name=instance.engine_name,
                server_args=deepcopy(server_args),
            )
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
            await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="initialize",
                engine_name=instance.engine_name,
                engine_id=instance.instance_id,
                engine_rank=0,
                num_engines=1,
                **init_kwargs,
            )
            instance.transition_to(RolloutInstanceState.CATCHING_UP)
            return RolloutLaunchResult(instance=instance, server_info=server_info)
        except BaseException:
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
        try:
            worker_ids = self._scheduler.fork_workers(
                role=proxy_role,
                target_role=instance.worker_role,
                command="areal.experimental.openai.proxy.proxy_rollout_server",
            )
            proxy_created = True
            workers = self._scheduler.get_workers(role=proxy_role)
            if len(worker_ids) != 1 or len(workers) != 1:
                raise RuntimeError(
                    f"Expected one proxy Worker for {instance.instance_id}, got "
                    f"ids={len(worker_ids)} workers={len(workers)}"
                )

            worker = workers[0]
            if not worker.worker_ports:
                raise RuntimeError(f"Proxy worker {worker.id} has no HTTP port")
            engine_name = f"proxy/{instance.instance_id}"
            await self._scheduler.create_engine(
                worker_id=worker.id,
                engine=f"{self._inf_engine.__module__}.{self._inf_engine.__name__}",
                engine_name=engine_name,
                config=self._config,
            )
            await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="initialize",
                engine_name=engine_name,
                addr=format_hostport(instance.server_host, instance.server_port),
            )
            instance.attach_proxy(
                role=proxy_role,
                worker_id=worker.id,
                engine_name=engine_name,
                addr=f"http://{format_hostport(worker.ip, int(worker.worker_ports[0]))}",
            )
        except BaseException:
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
        try:
            await self._scheduler.async_call_engine(
                worker_id=instance.worker_id,
                method="update_weights_from_disk",
                engine_name=instance.engine_name,
                meta=meta,
            )
            await self._scheduler.async_call_engine(
                worker_id=instance.worker_id,
                method="set_version",
                engine_name=instance.engine_name,
                version=checkpoint.version,
            )
            if instance.proxy_ready:
                await self._scheduler.async_call_engine(
                    worker_id=instance.proxy_worker_id,
                    method="set_version",
                    engine_name=instance.proxy_engine_name,
                    version=checkpoint.version,
                )
            instance.loaded_version = checkpoint.version
            instance.transition_to(RolloutInstanceState.READY)
        except BaseException:
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
