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
            self._rollout_alloc.parallel.tp_size
            * self._rollout_alloc.parallel.pp_size
        )

    def _validate_single_node_capacity(self) -> None:
        instance_size = self._instance_size()
        if instance_size > self._scheduler.n_gpus_per_node:
            raise SingleNodeInstanceError(
                "A single-node elastic rollout instance requires TP × PP to fit "
                f"one node: {instance_size} > {self._scheduler.n_gpus_per_node}."
            )

    async def launch(
        self,
        *,
        instance_id: str,
        worker_role: str,
        server_args: dict[str, Any],
        initialize_kwargs: dict[str, Any] | None = None,
    ) -> RolloutLaunchResult:
        """Launch one instance and leave it in CATCHING_UP before it can route."""
        self._validate_single_node_capacity()
        if not instance_id:
            raise ValueError("instance_id must not be empty")
        if not worker_role:
            raise ValueError("worker_role must not be empty")

        job = Job(
            role=worker_role,
            replicas=1,
            tasks=[self._instance_scheduling_spec()],
            scheduling_strategy=self._config.scheduling_strategy,
        )
        workers_created = False
        instance: RolloutInstance | None = None
        try:
            self._scheduler.create_workers(job=job)
            workers_created = True
            workers = self._scheduler.get_workers(role=worker_role)
            if len(workers) != 1:
                raise RuntimeError(
                    f"Expected one logical Worker for {worker_role}, got {len(workers)}"
                )

            worker = workers[0]
            engine_name = f"rollout/{instance_id}"
            instance = RolloutInstance(
                instance_id=instance_id,
                worker_role=worker_role,
                worker_id=worker.id,
                engine_name=engine_name,
            )
            instance.transition_to(RolloutInstanceState.STARTING)
            await self._scheduler.create_engine(
                worker_id=worker.id,
                engine=f"{self._inf_engine.__module__}.{self._inf_engine.__name__}",
                engine_name=engine_name,
                config=self._config,
            )
            server_info = await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="launch_server",
                engine_name=engine_name,
                server_args=deepcopy(server_args),
            )
            if not isinstance(server_info, LocalInfServerInfo):
                raise TypeError(
                    "launch_server must return LocalInfServerInfo, got "
                    f"{type(server_info)!r}"
                )

            init_kwargs = dict(initialize_kwargs or {})
            if init_kwargs.get("train_data_parallel_size") is None:
                init_kwargs["train_data_parallel_size"] = 1
            await self._scheduler.async_call_engine(
                worker_id=worker.id,
                method="initialize",
                engine_name=engine_name,
                engine_id=instance_id,
                engine_rank=0,
                num_engines=1,
                **init_kwargs,
            )
            instance.transition_to(RolloutInstanceState.CATCHING_UP)
            return RolloutLaunchResult(instance=instance, server_info=server_info)
        except BaseException:
            if instance is not None:
                instance.transition_to(RolloutInstanceState.FAILED)
            if workers_created:
                self._scheduler.delete_workers(role=worker_role)
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
        self._scheduler.delete_workers(role=instance.worker_role)
        instance.transition_to(RolloutInstanceState.STOPPED)
