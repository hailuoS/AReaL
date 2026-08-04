# SPDX-License-Identifier: Apache-2.0

import pytest

from areal.infra.controller.elastic import (
    DiskCheckpointManifest,
    RolloutInstance,
    RolloutInstancePool,
    RolloutInstanceReconciler,
    RolloutInstanceState,
)


class _FakeLaunchResult:
    def __init__(self, instance):
        self.instance = instance


class _FakeLauncher:
    def __init__(self):
        self.launched = []
        self.proxied = []
        self.caught_up = []
        self.destroyed = []

    async def launch(self, *, instance, **kwargs):
        instance.transition_to(RolloutInstanceState.STARTING)
        instance.worker_id = f"{instance.worker_role}/0"
        instance.server_host = "127.0.0.1"
        instance.server_port = 30000
        instance.transition_to(RolloutInstanceState.CATCHING_UP)
        self.launched.append(instance)
        return _FakeLaunchResult(instance)

    @staticmethod
    def proxy_role(instance):
        return f"proxy-{instance.worker_role}"

    async def launch_proxy(self, instance):
        role = self.proxy_role(instance)
        instance.attach_proxy(
            role=role,
            worker_id=f"{role}/0",
            engine_name=f"proxy/{instance.instance_id}",
            addr="http://127.0.0.1:31000",
        )
        self.proxied.append(instance.instance_id)

    async def catch_up_from_disk(self, instance, checkpoint):
        self.caught_up.append((instance.instance_id, checkpoint.version))
        instance.loaded_version = checkpoint.version
        instance.transition_to(RolloutInstanceState.READY)

    def destroy(self, instance):
        self.destroyed.append(instance.instance_id)
        instance.transition_to(RolloutInstanceState.STOPPED)


class _FailingCatchUpLauncher(_FakeLauncher):
    async def catch_up_from_disk(self, instance, checkpoint):
        raise RuntimeError("catch-up failed")


class _FailingProxyLauncher(_FakeLauncher):
    async def launch_proxy(self, instance):
        raise RuntimeError("proxy launch failed")


@pytest.mark.asyncio
async def test_reconciler_launches_each_instance_in_its_own_role(tmp_path):
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    recorded_roles = []
    cleared_roles = []
    checkpoint_dir = tmp_path / "weight_update_v4"
    checkpoint_dir.mkdir()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={"model_path": "/model"},
        latest_checkpoint=lambda: DiskCheckpointManifest(4, str(checkpoint_dir)),
        current_version=lambda: 0,
        record_launch_intent=recorded_roles.append,
        clear_launch_intent=cleared_roles.append,
    )

    result = await reconciler.reconcile_once()

    assert len(result.created_instance_ids) == 2
    assert len({instance.worker_role for instance in launcher.launched}) == 2
    assert all(
        instance.state is RolloutInstanceState.READY for instance in launcher.launched
    )
    assert launcher.caught_up == [
        (instance.instance_id, 4) for instance in launcher.launched
    ]
    assert recorded_roles == [instance.worker_role for instance in launcher.launched]
    assert cleared_roles == recorded_roles


@pytest.mark.asyncio
async def test_reconciler_registers_pending_instance_before_launch_completes():
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    observed_states = []

    def record_launch_intent(_worker_role):
        snapshot = pool.instances_snapshot()
        observed_states.append(
            [(instance.state, instance.worker_id) for instance in snapshot]
        )

    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
        record_launch_intent=record_launch_intent,
    )

    await reconciler.reconcile_once()

    assert observed_states[0] == [
        (RolloutInstanceState.PENDING, None),
        (RolloutInstanceState.PENDING, None),
    ]
    instances = [pool.get(instance_id) for instance_id in pool.instance_ids()]
    assert all(instance.state is RolloutInstanceState.READY for instance in instances)
    assert all(instance.worker_id is not None for instance in instances)


@pytest.mark.asyncio
async def test_reconciler_attaches_proxy_before_new_instance_is_ready():
    pool = RolloutInstancePool(min_instances=1, initial_instances=1, max_instances=2)
    launcher = _FakeLauncher()
    proxy_enabled = False
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
        proxy_enabled=lambda: proxy_enabled,
    )
    await reconciler.reconcile_once()
    initial = pool.get(pool.instance_ids()[0])
    assert not initial.proxy_ready

    proxy_enabled = True
    await reconciler.reconcile_once()
    assert initial.proxy_ready

    pool.set_desired_count(2)
    await reconciler.reconcile_once()
    instances = [pool.get(instance_id) for instance_id in pool.instance_ids()]
    assert len(instances) == 2
    assert all(instance.proxy_ready for instance in instances)
    assert all(instance.state is RolloutInstanceState.READY for instance in instances)


@pytest.mark.asyncio
async def test_reconciler_drains_before_destroying_an_instance():
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
    )
    await reconciler.reconcile_once()
    draining_id = pool.instance_ids()[-1]
    pool.bind_task("task-1", draining_id)
    pool.set_desired_count(1)

    first = await reconciler.reconcile_once()

    assert first.draining_instance_ids == (draining_id,)
    assert first.removed_instance_ids == ()
    assert pool.get(draining_id).state is RolloutInstanceState.DRAINING
    assert launcher.destroyed == []

    pool.release_task("task-1")
    second = await reconciler.reconcile_once()

    assert second.removed_instance_ids == (draining_id,)
    assert launcher.destroyed == [draining_id]


@pytest.mark.asyncio
async def test_reconciler_waits_for_returned_result_lease():
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
    )
    await reconciler.reconcile_once()
    draining_id = pool.instance_ids()[-1]
    pool.acquire_result_lease(draining_id, "task-1")
    pool.set_desired_count(1)

    first = await reconciler.reconcile_once()

    assert first.removed_instance_ids == ()
    assert pool.get(draining_id).state is RolloutInstanceState.DRAINING
    pool.release_result_lease(draining_id, "task-1")

    second = await reconciler.reconcile_once()

    assert second.removed_instance_ids == (draining_id,)


@pytest.mark.asyncio
async def test_reconciler_cancels_drain_before_launching_replacement():
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
    )
    await reconciler.reconcile_once()
    draining_id = pool.instance_ids()[-1]
    pool.bind_task("task-1", draining_id)
    pool.set_desired_count(1)
    await reconciler.reconcile_once()

    pool.set_desired_count(2)
    result = await reconciler.reconcile_once()

    assert result.created_instance_ids == ()
    assert pool.get(draining_id).state is RolloutInstanceState.READY
    assert len(launcher.launched) == 2


@pytest.mark.asyncio
async def test_reconciler_removes_instance_after_catch_up_failure(tmp_path):
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FailingCatchUpLauncher()
    checkpoint_dir = tmp_path / "weight_update_v4"
    checkpoint_dir.mkdir()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: DiskCheckpointManifest(4, str(checkpoint_dir)),
        current_version=lambda: 4,
    )

    with pytest.raises(RuntimeError, match="catch-up failed"):
        await reconciler.reconcile_once()

    assert pool.instance_ids() == ()
    assert len(launcher.destroyed) == 1


@pytest.mark.asyncio
async def test_reconciler_removes_new_instance_after_proxy_failure():
    pool = RolloutInstancePool(min_instances=1, initial_instances=1, max_instances=1)
    launcher = _FailingProxyLauncher()
    recorded_roles = []
    cleared_roles = []
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
        proxy_enabled=lambda: True,
        record_launch_intent=recorded_roles.append,
        clear_launch_intent=cleared_roles.append,
    )

    with pytest.raises(RuntimeError, match="proxy launch failed"):
        await reconciler.reconcile_once()

    assert pool.instance_ids() == ()
    assert len(launcher.destroyed) == 1
    assert recorded_roles[1].startswith("proxy-rollout-elastic-")
    assert cleared_roles == recorded_roles


@pytest.mark.asyncio
async def test_reconciler_reports_drain_timeout_without_forcing_removal():
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={},
        latest_checkpoint=lambda: None,
        current_version=lambda: 0,
        drain_timeout_seconds=0,
    )
    await reconciler.reconcile_once()
    draining_id = pool.instance_ids()[-1]
    pool.bind_task("task-1", draining_id)
    pool.set_desired_count(1)
    await reconciler.reconcile_once()

    with pytest.raises(TimeoutError, match="did not drain"):
        await reconciler.reconcile_once()

    assert pool.get(draining_id).state is RolloutInstanceState.DRAINING
    assert launcher.destroyed == []
