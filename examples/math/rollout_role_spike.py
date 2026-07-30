# SPDX-License-Identifier: Apache-2.0

"""Validate role-scoped Rollout V1 instances on an NPU Ray cluster.

This is an opt-in operational spike, not a training entry point.  It creates
one logical rollout worker per role, launches the configured inference server,
checks health, and then deletes a selected role while verifying that the other
role remains healthy. The spike exercises ``RolloutInstanceLauncher`` directly.

Examples
--------
Single-node isolation check (two instances)::

    AREAL_SPMD_MODE=false python examples/math/rollout_role_spike.py \\
        --mode single-node --instances 2 -- \\
        --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray

Pass the same config overrides used by the normal training entry point after
``--``. This spike intentionally supports only a single-node complete instance.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any
from uuid import uuid4

import requests

from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import (
    GRPOConfig,
    SGLangConfig,
    load_expr_config,
    vLLMConfig,
)
from areal.engine import RemoteSGLangEngine, RemotevLLMEngine
from areal.infra.controller.elastic import (
    RolloutInstanceLauncher,
    RolloutInstanceState,
    RolloutLaunchResult,
)
from areal.infra.scheduler.ray import RayScheduler
from areal.utils import logging
from areal.utils.network import format_hostport

logger = logging.getLogger("RolloutRoleSpike")


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("single-node",),
        default="single-node",
        help="Validate the supported one-node role-isolation path.",
    )
    parser.add_argument(
        "--instances",
        type=int,
        default=None,
        help="Number of independent roles to launch. Defaults to 2.",
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
        options.instances = 2
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


async def _launch_instance(
    launcher: RolloutInstanceLauncher,
    role: str,
    server_args: dict[str, Any],
    instance_id: str,
) -> RolloutLaunchResult:
    return await launcher.launch(
        instance_id=instance_id,
        worker_role=role,
        server_args=server_args,
    )


def _probe_health(result: RolloutLaunchResult) -> None:
    instance = result.instance
    address = format_hostport(result.server_info.host, result.server_info.port)
    response = requests.get(f"http://{address}/health", timeout=30)
    response.raise_for_status()
    logger.info(
        "Healthy role=%s worker=%s server=%s",
        instance.worker_role,
        instance.worker_id,
        address,
    )


def _destroy_instance(
    launcher: RolloutInstanceLauncher,
    result: RolloutLaunchResult,
) -> None:
    instance = result.instance
    if instance.state is not RolloutInstanceState.STOPPED:
        instance.transition_to(RolloutInstanceState.STOPPING)
        launcher.destroy(instance)


async def _run(options: argparse.Namespace, config: GRPOConfig) -> None:
    if config.scheduler.type != "ray":
        raise ValueError(
            "This spike exercises Ray role isolation. Set scheduler.type=ray."
        )
    rollout_alloc = ModelAllocation.from_str(config.rollout.backend, name="rollout")
    instance_size = rollout_alloc.parallel.tp_size * rollout_alloc.parallel.pp_size
    scheduler = RayScheduler(exp_config=config)
    engine_cls, server_args = _engine_and_server_args(config, rollout_alloc)
    launcher = RolloutInstanceLauncher(
        scheduler=scheduler,
        inf_engine=engine_cls,
        config=config.rollout,
        rollout_alloc=rollout_alloc,
    )
    instances: list[RolloutLaunchResult] = []
    instance_ids = [f"spike-{uuid4().hex[:12]}" for _ in range(options.instances)]
    roles = [f"{options.role_prefix}-{instance_id}" for instance_id in instance_ids]

    logger.info(
        "Starting %s role spike: roles=%s TP=%d PP=%d NPU/instance=%d",
        options.mode,
        roles,
        rollout_alloc.parallel.tp_size,
        rollout_alloc.parallel.pp_size,
        instance_size,
    )
    try:
        for instance_id, role in zip(instance_ids, roles, strict=True):
            instance = await _launch_instance(
                launcher,
                role,
                server_args,
                instance_id,
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
            logger.info("Deleting role %s", deleted.instance.worker_role)
            _destroy_instance(launcher, deleted)
            instances.pop()
            for instance in instances:
                _probe_health(instance)
            logger.info(
                "Isolation verified: deleted %s without disrupting %s",
                deleted.instance.worker_role,
                [instance.instance.worker_role for instance in instances],
            )
    finally:
        for instance in reversed(instances):
            try:
                logger.info("Cleaning up role %s", instance.instance.worker_role)
                _destroy_instance(launcher, instance)
            except Exception:
                logger.error(
                    "Failed to clean up role %s",
                    instance.instance.worker_role,
                    exc_info=True,
                )


def main(argv: list[str] | None = None) -> None:
    options, config_args = _parse_args(argv or [])
    config, _ = load_expr_config(config_args, GRPOConfig)
    asyncio.run(_run(options, config))


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
