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
        self.caught_up = []
        self.destroyed = []

    async def launch(self, *, instance_id, worker_role, **kwargs):
        instance = RolloutInstance(
            instance_id=instance_id,
            worker_role=worker_role,
            worker_id=f"{worker_role}/0",
            engine_name=f"rollout/{instance_id}",
        )
        instance.transition_to(RolloutInstanceState.STARTING)
        instance.transition_to(RolloutInstanceState.CATCHING_UP)
        self.launched.append(instance)
        return _FakeLaunchResult(instance)

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
    pool = RolloutInstancePool(min_instances=1, initial_instances=1, max_instances=1)
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
