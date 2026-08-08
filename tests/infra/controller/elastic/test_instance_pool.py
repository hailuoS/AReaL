# SPDX-License-Identifier: Apache-2.0

from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

from areal.infra.controller.elastic import (
    DuplicateInstanceError,
    InstanceNotReadyError,
    InstanceNotRemovableError,
    InvalidDesiredCountError,
    RolloutInstancePool,
    RolloutInstanceState,
    TaskBindingError,
)


def _pool() -> RolloutInstancePool:
    return RolloutInstancePool(
        min_instances=1,
        initial_instances=1,
        max_instances=3,
    )


def _add_ready(pool: RolloutInstancePool, instance_id: str):
    instance = pool.create(
        instance_id=instance_id,
        worker_role=f"rollout-elastic-{instance_id}",
        worker_id=f"rollout-elastic-{instance_id}/0",
        engine_name=f"rollout/{instance_id}",
    )
    instance.transition_to(RolloutInstanceState.STARTING)
    instance.transition_to(RolloutInstanceState.READY)
    return instance


def test_remove_instance_preserves_other_stable_identity():
    """Removing one stopped instance never renumbers another instance."""
    pool = _pool()
    first = _add_ready(pool, "ri-first")
    second = _add_ready(pool, "ri-second")
    second.request_drain()
    second.transition_to(RolloutInstanceState.STOPPING)
    second.transition_to(RolloutInstanceState.STOPPED)

    pool.remove(second.instance_id)

    assert pool.instance_ids() == (first.instance_id,)
    assert pool.get(first.instance_id).engine_name == "rollout/ri-first"


def test_ready_snapshot_is_immutable_after_pool_changes():
    """An RPC snapshot is unaffected by later pool and state mutations."""
    pool = _pool()
    first = _add_ready(pool, "ri-first")
    snapshot = pool.ready_snapshot()

    first.request_drain()
    _add_ready(pool, "ri-second")

    assert tuple(target.instance_id for target in snapshot) == ("ri-first",)
    current_ids = tuple(target.instance_id for target in pool.ready_snapshot())
    assert current_ids == ("ri-second",)


def test_bind_and_release_task_are_symmetric_and_cleanup_is_idempotent():
    """Task ownership remains stable and repeated callback cleanup is harmless."""
    pool = _pool()
    _add_ready(pool, "ri-first")

    pool.bind_task("task-a", "ri-first")
    first_release = pool.release_task("task-a")
    second_release = pool.release_task("task-a")

    assert first_release == "ri-first"
    assert second_release is None
    assert not pool.get("ri-first").workflow_task_ids


def test_task_cannot_be_rebound_to_another_instance():
    """A task keeps one stable instance binding until it is released."""
    pool = _pool()
    _add_ready(pool, "ri-first")
    _add_ready(pool, "ri-second")
    pool.bind_task("task-a", "ri-first")

    with pytest.raises(TaskBindingError, match="already bound"):
        pool.bind_task("task-a", "ri-second")


def test_draining_instance_is_excluded_from_ready_and_update_snapshots():
    """A drain atomically prevents selection for tasks and step updates."""
    pool = _pool()
    instance = _add_ready(pool, "ri-first")

    instance.request_drain()

    assert pool.ready_snapshot() == ()
    with pytest.raises(TaskBindingError, match="not eligible"):
        pool.bind_task("task-a", instance.instance_id)


def test_task_reservation_is_atomic_with_drain():
    pool = _pool()
    instance = _add_ready(pool, "ri-first")

    target = pool.reserve_task("task-a")
    pool.request_drain(instance.instance_id)

    assert target.instance_id == instance.instance_id
    assert not instance.can_stop
    pool.release_task("task-a")
    assert instance.can_stop


def test_returned_result_lease_blocks_stop_after_workflow_task_completes():
    """A completed task remains protected while its remote tensors are consumed."""
    pool = _pool()
    instance = _add_ready(pool, "ri-first")
    pool.bind_task("task-a", instance.instance_id)
    pool.acquire_result_lease(instance.instance_id, "task-a")

    pool.release_task("task-a")
    instance.request_drain()

    assert not instance.workflow_task_ids
    assert instance.result_lease_ids == {"task-a"}
    assert not instance.can_stop
    assert pool.release_result_lease(instance.instance_id, "task-a")
    assert instance.can_stop
    assert not pool.release_result_lease(instance.instance_id, "task-a")


def test_weight_update_snapshot_holds_drain_lease_until_release():
    pool = _pool()
    instance = _add_ready(pool, "ri-first")

    targets = pool.acquire_weight_update_snapshot()
    pool.request_drain(instance.instance_id)

    assert tuple(target.instance_id for target in targets) == (instance.instance_id,)
    assert not instance.can_stop
    pool.release_weight_update_snapshot(targets)
    assert instance.can_stop


def test_direct_request_reservation_blocks_stop_until_released():
    """A direct request keeps its selected instance alive until completion."""
    pool = _pool()
    instance = _add_ready(pool, "ri-first")
    target = pool.reserve_direct_request()
    instance.request_drain()

    assert not instance.can_stop
    pool.release_direct_request(target.instance_id)
    assert instance.can_stop


def test_task_reservation_prefers_new_least_loaded_instance():
    """A newly READY instance absorbs new work until loads converge."""
    pool = _pool()
    _add_ready(pool, "ri-existing")
    _add_ready(pool, "ri-new")
    pool.bind_task("old-a", "ri-existing")
    pool.bind_task("old-b", "ri-existing")

    first = pool.reserve_task("new-a")
    second = pool.reserve_task("new-b")
    third = pool.reserve_task("new-c")

    assert first.instance_id == "ri-new"
    assert second.instance_id == "ri-new"
    assert third.instance_id == "ri-existing"


def test_per_instance_capacity_is_a_hard_routing_limit():
    pool = _pool()
    first = _add_ready(pool, "ri-first")
    second = _add_ready(pool, "ri-second")

    targets = [
        pool.reserve_task(f"task-{idx}", max_inflight_per_instance=2)
        for idx in range(4)
    ]

    assert [target.instance_id for target in targets].count(first.instance_id) == 2
    assert [target.instance_id for target in targets].count(second.instance_id) == 2
    with pytest.raises(InstanceNotReadyError, match="request capacity"):
        pool.reserve_task("task-full", max_inflight_per_instance=2)


def test_direct_requests_participate_in_least_loaded_capacity():
    pool = _pool()
    _add_ready(pool, "ri-first")
    _add_ready(pool, "ri-second")
    pool.bind_task("task-a", "ri-first")

    target = pool.reserve_direct_request(max_inflight_per_instance=1)

    assert target.instance_id == "ri-second"
    with pytest.raises(InstanceNotReadyError, match="request capacity"):
        pool.reserve_direct_request(max_inflight_per_instance=1)


def test_concurrent_reservations_remain_balanced_and_within_capacity():
    pool = _pool()
    _add_ready(pool, "ri-first")
    _add_ready(pool, "ri-second")

    def reserve(idx: int) -> str:
        return pool.reserve_task(
            f"task-{idx}", max_inflight_per_instance=50
        ).instance_id

    with ThreadPoolExecutor(max_workers=16) as executor:
        instance_ids = list(executor.map(reserve, range(100)))

    assert Counter(instance_ids) == {"ri-first": 50, "ri-second": 50}
    with pytest.raises(InstanceNotReadyError, match="request capacity"):
        pool.reserve_task("task-over-capacity", max_inflight_per_instance=50)


def test_instance_cannot_be_removed_before_stopped_and_drained():
    """Registry removal requires both lifecycle completion and empty work."""
    pool = _pool()
    instance = _add_ready(pool, "ri-first")

    with pytest.raises(InstanceNotRemovableError, match="stopped and drained"):
        pool.remove(instance.instance_id)


def test_duplicate_instance_identity_is_rejected():
    """Stable identities cannot be reused while registered."""
    pool = _pool()
    _add_ready(pool, "ri-first")

    with pytest.raises(DuplicateInstanceError, match="already registered"):
        pool.create(
            instance_id="ri-first",
            worker_role="other-role",
            worker_id="other-role/0",
            engine_name="rollout/other",
        )


@pytest.mark.parametrize("desired_count", [0, 4])
def test_desired_count_outside_bounds_is_rejected(desired_count):
    """Desired state cannot violate configured minimum or maximum capacity."""
    pool = _pool()

    with pytest.raises(InvalidDesiredCountError, match="desired_count"):
        pool.set_desired_count(desired_count)
