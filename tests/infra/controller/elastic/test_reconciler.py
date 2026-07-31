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


@pytest.mark.asyncio
async def test_reconciler_launches_each_instance_in_its_own_role(tmp_path):
    pool = RolloutInstancePool(min_instances=1, initial_instances=2, max_instances=2)
    launcher = _FakeLauncher()
    checkpoint_dir = tmp_path / "weight_update_v4"
    checkpoint_dir.mkdir()
    reconciler = RolloutInstanceReconciler(
        pool=pool,
        launcher=launcher,
        role_prefix="rollout-elastic",
        server_args={"model_path": "/model"},
        latest_checkpoint=lambda: DiskCheckpointManifest(4, str(checkpoint_dir)),
        current_version=lambda: 0,
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
