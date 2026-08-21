# SPDX-License-Identifier: Apache-2.0

"""Reserve fractional Ray NPU resources to exercise KubeRay autoscaling."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time

import ray
from ray.exceptions import GetTimeoutError


@ray.remote
class NPUReservation:
    def describe(self) -> dict[str, object]:
        context = ray.get_runtime_context()
        return {
            "hostname": socket.gethostname(),
            "node_id": context.get_node_id(),
            "accelerator_ids": context.get_accelerator_ids(),
            "assigned_resources": context.get_assigned_resources(),
            "ASCEND_RT_VISIBLE_DEVICES": os.environ.get(
                "ASCEND_RT_VISIBLE_DEVICES", "<unset>"
            ),
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicas", type=int, default=5)
    parser.add_argument("--resource-name", default="NPU")
    parser.add_argument("--resources-per-replica", type=float, default=2.0)
    parser.add_argument("--cpus-per-replica", type=float, default=1.0)
    parser.add_argument("--ready-timeout-seconds", type=float, default=600.0)
    parser.add_argument("--hold-seconds", type=float, default=300.0)
    args = parser.parse_args()
    if args.replicas <= 0:
        parser.error("--replicas must be positive")
    if args.resources_per_replica <= 0:
        parser.error("--resources-per-replica must be positive")
    if args.cpus_per_replica < 0:
        parser.error("--cpus-per-replica cannot be negative")
    if args.ready_timeout_seconds <= 0:
        parser.error("--ready-timeout-seconds must be positive")
    if args.hold_seconds < 0:
        parser.error("--hold-seconds cannot be negative")
    return args


def main() -> int:
    args = _parse_args()
    ray.init(address="auto")
    actors = [
        NPUReservation.options(
            num_cpus=args.cpus_per_replica,
            resources={args.resource_name: args.resources_per_replica},
        ).remote()
        for _ in range(args.replicas)
    ]
    descriptions = [actor.describe.remote() for actor in actors]
    exit_code = 0
    try:
        try:
            ready = ray.get(descriptions, timeout=args.ready_timeout_seconds)
        except GetTimeoutError:
            exit_code = 2
            print(
                "Timed out waiting for every reservation. "
                "Inspect `ray status` and KubeRay autoscaler logs now.",
                flush=True,
            )
        else:
            print(json.dumps(ready, indent=2, sort_keys=True), flush=True)
            if args.resources_per_replica.is_integer():
                expected = int(args.resources_per_replica)
                assigned_by_host: dict[str, set[str]] = {}
                for description in ready:
                    hostname = str(description["hostname"])
                    accelerator_ids = description["accelerator_ids"]
                    npu_ids = {
                        str(value)
                        for value in accelerator_ids.get(args.resource_name, [])
                    }
                    if len(npu_ids) != expected:
                        print(
                            f"Expected {expected} NPU IDs on {hostname}, got {npu_ids}",
                            flush=True,
                        )
                        exit_code = 3
                    overlap = assigned_by_host.setdefault(hostname, set()) & npu_ids
                    if overlap:
                        print(
                            f"NPU IDs overlap between actors on {hostname}: {overlap}",
                            flush=True,
                        )
                        exit_code = 3
                    assigned_by_host[hostname].update(npu_ids)
        print(
            json.dumps(
                {
                    "cluster_resources": ray.cluster_resources(),
                    "available_resources": ray.available_resources(),
                },
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )
        if args.hold_seconds:
            print(
                f"Holding reservations for {args.hold_seconds:.0f} seconds.",
                flush=True,
            )
            time.sleep(args.hold_seconds)
    finally:
        for actor in actors:
            try:
                ray.kill(actor, no_restart=True)
            except Exception as error:
                print(f"Failed to kill reservation actor: {error}", flush=True)
        ray.shutdown()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
