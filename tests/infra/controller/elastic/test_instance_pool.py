# SPDX-License-Identifier: Apache-2.0

import pytest

from areal.infra.controller.elastic import (
    DuplicateInstanceError,
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
    assert pool.instance_for_task("task-a") is None
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
    assert pool.weight_update_snapshot() == ()
    with pytest.raises(TaskBindingError, match="not eligible"):
        pool.bind_task("task-a", instance.instance_id)


@pytest.mark.parametrize(
    "acquire,release",
    [
        ("acquire_direct_request", "release_direct_request"),
        ("acquire_session", "release_session"),
        ("acquire_update_lease", "release_update_lease"),
    ],
)
def test_inflight_counter_blocks_stop_until_released(acquire, release):
    """Direct requests, sessions, and update leases each block scale-in."""
    pool = _pool()
    instance = _add_ready(pool, "ri-first")
    getattr(pool, acquire)(instance.instance_id)
    instance.request_drain()

    assert not instance.can_stop
    getattr(pool, release)(instance.instance_id)
    assert instance.can_stop


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
