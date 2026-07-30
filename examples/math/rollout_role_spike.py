# SPDX-License-Identifier: Apache-2.0

"""Validate role-scoped Rollout V1 instances on an NPU Ray cluster.

This is an opt-in operational spike, not a training entry point.  It creates
one logical rollout worker per role, launches the configured inference server,
checks health, and then deletes a selected role while verifying that the other
role remains healthy.

Examples
--------
Single-node isolation check (two instances)::

    AREAL_SPMD_MODE=false python examples/math/rollout_role_spike.py \\
        --mode single-node --instances 2 -- \\
        --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray

Multi-node capability check (one instance)::

    AREAL_SPMD_MODE=false python examples/math/rollout_role_spike.py \\
        --mode multi-node --instances 1 -- \\
        --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray

Pass the same config overrides used by the normal training entry point after
``--``.  The current NPU branch is expected to reject the multi-node case for
an independent role until Scheduler role-scoped rollout support is added.
"""

from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any

import requests

from areal.api import Job, LocalInfServerInfo, Worker
from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import (
    GRPOConfig,
    InferenceEngineConfig,
    SGLangConfig,
    SchedulingSpec,
    load_expr_config,
    vLLMConfig,
)
from areal.engine import RemoteSGLangEngine, RemotevLLMEngine
from areal.infra.scheduler.ray import RayScheduler
from areal.utils import logging
from areal.utils.network import format_hostport

logger = logging.getLogger("RolloutRoleSpike")


@dataclass(frozen=True)
class SpikeInstance:
    """Live resources belonging to one independently scheduled rollout role."""

    role: str
    worker: Worker
    engine_name: str
    server_info: LocalInfServerInfo


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("single-node", "multi-node"),
        default="single-node",
        help="Validate one-node isolation or the multi-node Scheduler path.",
    )
    parser.add_argument(
        "--instances",
        type=int,
        default=None,
        help="Number of independent roles to launch. Defaults to 2/1 by mode.",
    )
    parser.add_argument(
        "--role-prefix",
        default="rollout-elastic-spike",
        help="Prefix for temporary Scheduler roles.",
    )
    parser.add_argument(
        "--keep-alive-seconds",
        type=float,
        default=0.0,
        help="Keep instances alive for manual NPU inspection before cleanup.",
    )
    options, config_args = parser.parse_known_args(argv)
    if config_args and config_args[0] == "--":
        config_args = config_args[1:]
    if not config_args:
        parser.error("Pass the normal GRPO config path and overrides after '--'.")
    if options.instances is None:
        options.instances = 2 if options.mode == "single-node" else 1
    if options.instances <= 0:
        parser.error("--instances must be positive.")
    if options.keep_alive_seconds < 0:
        parser.error("--keep-alive-seconds must be non-negative.")
    if ":" in options.role_prefix:
        parser.error("--role-prefix cannot contain ':'.")
    return options, config_args


def _engine_and_server_args(
    config: GRPOConfig,
    rollout_alloc: ModelAllocation,
) -> tuple[type[RemoteSGLangEngine] | type[RemotevLLMEngine], dict[str, Any]]:
    if rollout_alloc.backend == "sglang":
        return (
            RemoteSGLangEngine,
            SGLangConfig.build_args(
                sglang_config=config.sglang,
                tp_size=rollout_alloc.parallel.tp_size,
                pp_size=rollout_alloc.parallel.pp_size,
                base_gpu_id=0,
            ),
        )
    if rollout_alloc.backend == "vllm":
        return (
            RemotevLLMEngine,
            vLLMConfig.build_args(
                vllm_config=config.vllm,
                tp_size=rollout_alloc.parallel.tp_size,
                pp_size=rollout_alloc.parallel.pp_size,
            ),
        )
    raise ValueError(
        f"Unsupported rollout backend for role spike: {rollout_alloc.backend}"
    )


def _instance_scheduling_spec(
    rollout_config: InferenceEngineConfig,
    rollout_alloc: ModelAllocation,
) -> SchedulingSpec:
    if not rollout_config.scheduling_spec:
        raise ValueError("rollout.scheduling_spec must contain at least one entry.")
    instance_size = rollout_alloc.parallel.tp_size * rollout_alloc.parallel.pp_size
    spec = SchedulingSpec(**asdict(rollout_config.scheduling_spec[0]))
    spec.cpu *= instance_size
    spec.mem *= instance_size
    if spec.gpu > 0:
        spec.gpu = instance_size
    return spec


def _validate_mode(
    mode: str,
    instance_size: int,
    n_gpus_per_node: int,
) -> None:
    if mode == "single-node" and instance_size > n_gpus_per_node:
        raise ValueError(
            "single-node mode requires TP × PP to fit one node: "
            f"{instance_size} > {n_gpus_per_node}."
        )
    if mode == "multi-node" and instance_size <= n_gpus_per_node:
        raise ValueError(
            "multi-node mode requires TP × PP to exceed one node: "
            f"{instance_size} <= {n_gpus_per_node}."
        )
    if mode == "multi-node" and instance_size % n_gpus_per_node != 0:
        raise ValueError(
            "multi-node Ray rollout requires TP × PP to be a whole number "
            f"of nodes: {instance_size} is not divisible by {n_gpus_per_node}."
        )


async def _launch_instance(
    scheduler: RayScheduler,
    role: str,
    rollout_config: InferenceEngineConfig,
    engine_cls: type[RemoteSGLangEngine] | type[RemotevLLMEngine],
    server_args: dict[str, Any],
    scheduling_spec: SchedulingSpec,
) -> SpikeInstance:
    job = Job(
        role=role,
        replicas=1,
        tasks=[deepcopy(scheduling_spec)],
        scheduling_strategy=rollout_config.scheduling_strategy,
    )
    created = False
    worker: Worker | None = None
    engine_name = f"{role}/engine"
    try:
        scheduler.create_workers(job)
        created = True
        workers = scheduler.get_workers(role)
        if len(workers) != 1:
            raise RuntimeError(
                f"Expected exactly one logical Worker for {role}, got {len(workers)}."
            )
        worker = workers[0]
        await scheduler.create_engine(
            worker_id=worker.id,
            engine=f"{engine_cls.__module__}.{engine_cls.__name__}",
            engine_name=engine_name,
            config=rollout_config,
        )
        server_info = await scheduler.async_call_engine(
            worker_id=worker.id,
            method="launch_server",
            engine_name=engine_name,
            server_args=deepcopy(server_args),
        )
        if not isinstance(server_info, LocalInfServerInfo):
            raise TypeError(
                f"Unexpected launch_server result for {role}: {type(server_info)!r}"
            )
        await scheduler.async_call_engine(
            worker_id=worker.id,
            method="initialize",
            engine_name=engine_name,
            engine_id=role,
            engine_rank=0,
            num_engines=1,
            train_data_parallel_size=1,
        )
        return SpikeInstance(
            role=role,
            worker=worker,
            engine_name=engine_name,
            server_info=server_info,
        )
    except BaseException:
        if created:
            logger.error("Launch failed for %s; deleting its role", role, exc_info=True)
            scheduler.delete_workers(role=role)
        raise


def _probe_health(instance: SpikeInstance) -> None:
    address = format_hostport(instance.server_info.host, instance.server_info.port)
    response = requests.get(f"http://{address}/health", timeout=30)
    response.raise_for_status()
    logger.info(
        "Healthy role=%s worker=%s server=%s",
        instance.role,
        instance.worker.id,
        address,
    )


async def _run(options: argparse.Namespace, config: GRPOConfig) -> None:
    if config.scheduler.type != "ray":
        raise ValueError(
            "This spike exercises Ray role isolation. Set scheduler.type=ray."
        )
    rollout_alloc = ModelAllocation.from_str(config.rollout.backend, name="rollout")
    instance_size = rollout_alloc.parallel.tp_size * rollout_alloc.parallel.pp_size
    scheduler = RayScheduler(exp_config=config)
    _validate_mode(options.mode, instance_size, scheduler.n_gpus_per_node)
    engine_cls, server_args = _engine_and_server_args(config, rollout_alloc)
    scheduling_spec = _instance_scheduling_spec(config.rollout, rollout_alloc)
    instances: list[SpikeInstance] = []
    roles = [f"{options.role_prefix}-{index:02d}" for index in range(options.instances)]

    logger.info(
        "Starting %s role spike: roles=%s TP=%d PP=%d NPU/instance=%d",
        options.mode,
        roles,
        rollout_alloc.parallel.tp_size,
        rollout_alloc.parallel.pp_size,
        instance_size,
    )
    try:
        for role in roles:
            instance = await _launch_instance(
                scheduler,
                role,
                config.rollout,
                engine_cls,
                server_args,
                scheduling_spec,
            )
            instances.append(instance)
            _probe_health(instance)

        if options.keep_alive_seconds:
            logger.info(
                "Keeping roles alive for %.1f seconds", options.keep_alive_seconds
            )
            await asyncio.sleep(options.keep_alive_seconds)

        if len(instances) > 1:
            deleted = instances[-1]
            logger.info("Deleting role %s", deleted.role)
            scheduler.delete_workers(role=deleted.role)
            instances.pop()
            for instance in instances:
                _probe_health(instance)
            logger.info(
                "Isolation verified: deleted %s without disrupting %s",
                deleted.role,
                [instance.role for instance in instances],
            )
    finally:
        for instance in reversed(instances):
            try:
                logger.info("Cleaning up role %s", instance.role)
                scheduler.delete_workers(role=instance.role)
            except Exception:
                logger.error("Failed to clean up role %s", instance.role, exc_info=True)


def main(argv: list[str] | None = None) -> None:
    options, config_args = _parse_args(argv or [])
    config, _ = load_expr_config(config_args, GRPOConfig)
    asyncio.run(_run(options, config))


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
