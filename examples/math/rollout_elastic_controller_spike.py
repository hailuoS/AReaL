# SPDX-License-Identifier: Apache-2.0

"""Exercise RolloutController V1 elasticity over HTTP on one node.

Example::

    AREAL_SPMD_MODE=false python examples/math/rollout_elastic_controller_spike.py \
        --verify-recommendation -- \
        --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
"""

from __future__ import annotations

import argparse
import time
from uuid import uuid4

import requests
from rollout_role_spike import _engine_and_server_args

from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import GRPOConfig, load_expr_config
from areal.infra.controller.rollout_controller import RolloutController
from areal.infra.scheduler.ray import RayScheduler
from areal.utils import logging

logger = logging.getLogger("RolloutElasticControllerSpike")
_CONTROLLER_ROLE = f"rollout-elastic-http-spike-controller-{uuid4().hex[:8]}"


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify-recommendation",
        action="store_true",
        help="Check AstraFlow scale-up, hold, and scale-down reports.",
    )
    parser.add_argument(
        "--verify-recovery",
        action="store_true",
        help="Restart the V1 Controller and verify desired-state recovery.",
    )
    parser.add_argument(
        "--verify-proxy",
        action="store_true",
        help="Attach one V1 ProxyRolloutServer to every elastic instance.",
    )
    options, config_args = parser.parse_known_args(argv)
    if config_args and config_args[0] == "--":
        config_args = config_args[1:]
    if not config_args:
        parser.error("Pass the normal GRPO config path and overrides after '--'.")
    return options, config_args


def _get_instances(base_url: str) -> dict:
    response = requests.get(f"{base_url}/elastic/instances", timeout=10)
    response.raise_for_status()
    return response.json()


def _wait_for_instances(
    base_url: str,
    count: int,
    timeout: float = 600.0,
    *,
    require_proxy: bool = False,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = _get_instances(base_url)
        ready = payload["ready_instances"]
        total = len(payload["instances"])
        logger.info(
            "Elastic status desired=%s ready=%s total=%s",
            payload["desired_instances"],
            ready,
            total,
        )
        if ready == count and total == count:
            instances = payload["instances"]
            if require_proxy and not payload["proxy_enabled"]:
                raise RuntimeError("Elastic V1 proxy workers are not enabled")
            if any(instance["state"] != "ready" for instance in instances):
                raise RuntimeError(f"Non-READY instance in ready snapshot: {instances}")
            if require_proxy and any(
                not instance["proxy_ready"] for instance in instances
            ):
                raise RuntimeError(
                    f"READY instance does not have a V1 proxy: {instances}"
                )
            if any(
                instance["loaded_version"] != payload["serving_version"]
                for instance in instances
            ):
                raise RuntimeError(
                    "READY instance does not match serving version: "
                    f"serving={payload['serving_version']} instances={instances}"
                )
            if payload["pending_update_version"] is not None:
                raise RuntimeError(
                    "Elastic weight transition is still pending: "
                    f"{payload['pending_update_version']}"
                )
            if len({instance["instance_id"] for instance in instances}) != count:
                raise RuntimeError("Elastic instances do not have unique stable IDs")
            if len({instance["worker_role"] for instance in instances}) != count:
                raise RuntimeError("Elastic instances do not have isolated roles")
            return
        if payload["last_reconcile_error"]:
            raise RuntimeError(payload["last_reconcile_error"])
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {count} READY elastic instances")


def _set_desired(base_url: str, desired_instances: int) -> None:
    response = requests.put(
        f"{base_url}/elastic/desired-instances",
        json={"desired_instances": desired_instances},
        timeout=10,
    )
    response.raise_for_status()


def _recommend(base_url: str, *, entered: int, consumed: int, wait: float) -> dict:
    response = requests.post(
        f"{base_url}/elastic/scaling-recommendation",
        json={
            "entered": entered,
            "consumed": consumed,
            "wait_seconds": wait,
            "step_seconds": 10.0,
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def _verify_recommendations(base_url: str, ready_instances: int) -> None:
    if ready_instances == 1:
        expected = ("scale_up", 2, 2.0)
    else:
        expected = ("scale_down", 1, 0.1)
    branch, target, wait = expected
    report = _recommend(
        base_url,
        entered=20,
        consumed=5 if ready_instances > 1 else 20,
        wait=wait,
    )
    if (report["branch"], report["recommended_instances"]) != (branch, target):
        raise RuntimeError(f"Unexpected scaling recommendation: {report}")

    hold = _recommend(base_url, entered=20, consumed=20, wait=0.7)
    if hold["branch"] != "hold":
        raise RuntimeError(f"Expected dead-band hold recommendation: {hold}")


def main(argv: list[str] | None = None) -> None:
    options, config_args = _parse_args(argv or [])
    config, _ = load_expr_config(config_args, GRPOConfig)
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
    scheduler = RayScheduler(exp_config=config)

    def create_controller() -> RolloutController:
        return RolloutController(
            inf_engine=engine_cls,
            config=rollout,
            scheduler=scheduler,
        )

    controller = create_controller()
    try:
        controller.initialize(role=_CONTROLLER_ROLE, server_args=server_args)
        base_url = f"http://{controller.callback_addr}"
        _wait_for_instances(base_url, 1)
        if options.verify_proxy:
            controller.start_proxy()
            _wait_for_instances(base_url, 1, require_proxy=True)
        initial_role = _get_instances(base_url)["instances"][0]["worker_role"]
        if options.verify_recommendation:
            _verify_recommendations(base_url, ready_instances=1)

        _set_desired(base_url, 2)
        _wait_for_instances(base_url, 2, require_proxy=options.verify_proxy)
        if options.verify_recommendation:
            _verify_recommendations(base_url, ready_instances=2)

        _set_desired(base_url, 1)
        _wait_for_instances(base_url, 1, require_proxy=options.verify_proxy)
        final_role = _get_instances(base_url)["instances"][0]["worker_role"]
        if final_role != initial_role:
            raise RuntimeError(
                f"Scale-in removed the original role: {initial_role} -> {final_role}"
            )
        logger.info("Elastic Controller HTTP 1->2->1 spike passed")
    finally:
        controller.destroy()

    if options.verify_recovery:
        if not rollout.fileroot:
            raise ValueError("--verify-recovery requires rollout.fileroot")
        recovered_controller = create_controller()
        try:
            recovered_controller.initialize(
                role=_CONTROLLER_ROLE, server_args=server_args
            )
            recovered_url = f"http://{recovered_controller.callback_addr}"
            if options.verify_proxy:
                recovered_controller.start_proxy()
            _wait_for_instances(recovered_url, 1, require_proxy=options.verify_proxy)
            status = _get_instances(recovered_url)
            if status["desired_instances"] != 1:
                raise RuntimeError(f"Desired state was not recovered: {status}")
            logger.info("Elastic Controller recovery spike passed")
        finally:
            recovered_controller.destroy()


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
