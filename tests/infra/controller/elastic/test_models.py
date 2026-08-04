# SPDX-License-Identifier: Apache-2.0

import pytest

from areal.infra.controller.elastic import (
    InstanceDesiredState,
    InvalidStateTransitionError,
    RolloutInstance,
    RolloutInstanceState,
)


def _instance() -> RolloutInstance:
    return RolloutInstance(
        instance_id="ri-a",
        worker_role="rollout-elastic-a",
        worker_id="rollout-elastic-a/0",
        engine_name="rollout/ri-a",
    )


def test_instance_transition_through_ready_and_drain_succeeds():
    """A complete instance follows the expected scale-out and scale-in states."""
    instance = _instance()

    instance.transition_to(RolloutInstanceState.STARTING)
    instance.transition_to(RolloutInstanceState.CATCHING_UP)
    instance.transition_to(RolloutInstanceState.READY)
    instance.request_drain()

    assert instance.state is RolloutInstanceState.DRAINING
    assert instance.desired_state is InstanceDesiredState.STOPPED
    assert instance.can_stop


def test_instance_invalid_transition_raises():
    """A pending instance cannot skip directly to READY."""
    instance = _instance()

    with pytest.raises(InvalidStateTransitionError, match="pending to ready"):
        instance.transition_to(RolloutInstanceState.READY)


def test_pending_instance_without_worker_cannot_create_rpc_target():
    instance = RolloutInstance(
        instance_id="ri-pending",
        worker_role="rollout-elastic-pending",
        worker_id=None,
        engine_name="rollout/ri-pending",
    )

    with pytest.raises(ValueError, match="no assigned worker"):
        _ = instance.rpc_target


def test_instance_draining_is_not_routable_until_cancelled():
    """DRAINING removes an instance from routing and cancellation restores it."""
    instance = _instance()
    instance.transition_to(RolloutInstanceState.STARTING)
    instance.transition_to(RolloutInstanceState.READY)

    instance.request_drain()
    assert not instance.is_routable

    instance.cancel_drain()
    assert instance.is_routable


@pytest.mark.parametrize(
    "field_name",
    [
        "workflow_task_ids",
        "result_lease_ids",
        "direct_inflight",
        "active_sessions",
        "update_leases",
    ],
)
def test_instance_with_inflight_work_cannot_stop(field_name):
    """Every tracked work category independently prevents resource teardown."""
    instance = _instance()
    instance.transition_to(RolloutInstanceState.STARTING)
    instance.transition_to(RolloutInstanceState.READY)
    instance.request_drain()

    if field_name in {"workflow_task_ids", "result_lease_ids"}:
        getattr(instance, field_name).add("task-a")
    else:
        setattr(instance, field_name, 1)

    assert not instance.is_drained
    assert not instance.can_stop


def test_rpc_target_does_not_change_with_instance_state():
    """An RPC target snapshot is detached from later lifecycle mutations."""
    instance = _instance()
    target = instance.rpc_target

    instance.transition_to(RolloutInstanceState.STARTING)

    assert target.instance_id == "ri-a"
    assert target.worker_id == "rollout-elastic-a/0"
    assert target.engine_name == "rollout/ri-a"


def test_rpc_target_carries_stable_instance_proxy_identity():
    instance = _instance()
    instance.attach_proxy(
        role="proxy-rollout-elastic-a",
        worker_id="proxy-rollout-elastic-a/0",
        engine_name="proxy/ri-a",
        addr="http://127.0.0.1:31000",
    )

    target = instance.rpc_target

    assert instance.proxy_ready
    assert target.proxy_addr == "http://127.0.0.1:31000"
    assert target.proxy_worker_id == "proxy-rollout-elastic-a/0"
    assert target.proxy_engine_name == "proxy/ri-a"
