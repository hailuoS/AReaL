# SPDX-License-Identifier: Apache-2.0

import threading

import torch

from areal.infra.controller.elastic import RolloutInstancePool, RolloutInstanceState
from areal.infra.controller.rollout_controller import RolloutController
from areal.infra.rpc.rtensor import RTensor, TensorShardInfo


def _controller_with_ready_instance():
    pool = RolloutInstancePool(min_instances=1, initial_instances=1, max_instances=1)
    instance = pool.create(
        instance_id="ri-first",
        worker_role="rollout-elastic-first",
        worker_id="rollout-elastic-first/0",
        engine_name="rollout/ri-first",
    )
    instance.transition_to(RolloutInstanceState.STARTING)
    instance.transition_to(RolloutInstanceState.READY)

    controller = object.__new__(RolloutController)
    controller._instance_pool = pool
    controller._elastic_result_lease_lock = threading.Lock()
    controller._elastic_result_lease_instance = {}
    controller._elastic_result_lease_shards = {}
    controller._elastic_shard_to_result_lease = {}
    return controller, pool, instance


def _trajectory(shard_id: str):
    return {
        "input_ids": RTensor(
            shard=TensorShardInfo(shard_id=shard_id, node_addr="127.0.0.1:30000"),
            data=torch.empty(1, device="meta"),
        )
    }


def test_batch_release_keeps_instance_until_matching_rtensor_is_consumed():
    controller, pool, instance = _controller_with_ready_instance()
    first = _trajectory("shard-a")
    pending = _trajectory("shard-b")
    pool.bind_task("1", instance.instance_id)
    controller._acquire_result_lease(
        instance_id=instance.instance_id, task_id=1, trajectory=first
    )
    pool.release_task("1")
    pool.bind_task("2", instance.instance_id)
    controller._acquire_result_lease(
        instance_id=instance.instance_id, task_id=2, trajectory=pending
    )
    pool.release_task("2")
    instance.request_drain()

    controller.release_batch(first)

    assert instance.result_lease_ids == {"2"}
    assert not instance.can_stop

    controller.release_batch(pending)

    assert not instance.result_lease_ids
    assert instance.can_stop


def test_batch_release_is_idempotent():
    controller, pool, instance = _controller_with_ready_instance()
    trajectory = _trajectory("shard-a")
    controller._acquire_result_lease(
        instance_id=instance.instance_id, task_id=1, trajectory=trajectory
    )

    controller.release_batch(trajectory)
    controller.release_batch(trajectory)

    assert not instance.result_lease_ids
