# SPDX-License-Identifier: Apache-2.0

"""Exercise RolloutController V1 elastic desired state over HTTP on one node.

Example::

    AREAL_SPMD_MODE=false python examples/math/rollout_elastic_controller_spike.py \
        -- --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
"""

from __future__ import annotations

import argparse
import time

import requests
from rollout_role_spike import _engine_and_server_args

from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import GRPOConfig, load_expr_config
from areal.infra.controller.rollout_controller import RolloutController
from areal.infra.scheduler.ray import RayScheduler
from areal.utils import logging

logger = logging.getLogger("RolloutElasticControllerSpike")


def _parse_args(argv: list[str]) -> list[str]:
    parser = argparse.ArgumentParser(description=__doc__)
    _, config_args = parser.parse_known_args(argv)
    if config_args and config_args[0] == "--":
        config_args = config_args[1:]
    if not config_args:
        parser.error("Pass the normal GRPO config path and overrides after '--'.")
    return config_args


def _wait_for_instances(base_url: str, count: int, timeout: float = 600.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = requests.get(f"{base_url}/elastic/instances", timeout=10).json()
        ready = payload["ready_instances"]
        total = len(payload["instances"])
        logger.info("Elastic status desired=%s ready=%s total=%s", payload["desired_instances"], ready, total)
        if ready == count and total == count:
            return
        if payload["last_reconcile_error"]:
            raise RuntimeError(payload["last_reconcile_error"])
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {count} READY elastic instances")


def main(argv: list[str] | None = None) -> None:
    config, _ = load_expr_config(_parse_args(argv or []), GRPOConfig)
    if config.scheduler.type != "ray":
        raise ValueError("Set scheduler.type=ray for this single-node spike.")
    rollout = config.rollout
    rollout.elastic.enabled = True
    rollout.elastic.min_instances = 1
    rollout.elastic.initial_instances = 1
    rollout.elastic.max_instances = 2
    rollout.elastic.role_prefix = "rollout-elastic-http-spike"
    rollout.elastic.reconcile_interval_seconds = 2.0

    allocation = ModelAllocation.from_str(rollout.backend, name="rollout")
    engine_cls, server_args = _engine_and_server_args(config, allocation)
    controller = RolloutController(
        inf_engine=engine_cls,
        config=rollout,
        scheduler=RayScheduler(exp_config=config),
    )
    try:
        controller.initialize(role="rollout", server_args=server_args)
        base_url = f"http://{controller.callback_addr}"
        _wait_for_instances(base_url, 1)
        response = requests.put(
            f"{base_url}/elastic/desired-instances",
            json={"desired_instances": 2},
            timeout=10,
        )
        response.raise_for_status()
        _wait_for_instances(base_url, 2)
        response = requests.put(
            f"{base_url}/elastic/desired-instances",
            json={"desired_instances": 1},
            timeout=10,
        )
        response.raise_for_status()
        _wait_for_instances(base_url, 1)
        logger.info("Elastic Controller HTTP 1->2->1 spike passed")
    finally:
        controller.destroy()


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
