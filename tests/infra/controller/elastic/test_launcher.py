# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from areal.api import LocalInfServerInfo, Worker
from areal.api.cli_args import SchedulingSpec
from areal.infra.controller.elastic import (
    RolloutInstanceLauncher,
    RolloutInstanceState,
    SingleNodeInstanceError,
)


class _FakeInferenceEngine:
    pass


@dataclass
class _FakeConfig:
    scheduling_spec: tuple[SchedulingSpec, ...]
    scheduling_strategy: object = None


class _FakeScheduler:
    def __init__(self, n_gpus_per_node: int = 8) -> None:
        self.n_gpus_per_node = n_gpus_per_node
        self.worker = Worker(id="elastic-a/0", ip="127.0.0.1")
        self.created_job = None
        self.engine_calls = []
        self.deleted_roles = []
        self.server_info = LocalInfServerInfo(host="127.0.0.1", port=30000)

    def create_workers(self, job):
        self.created_job = job
        return [self.worker.id]

    def get_workers(self, role):
        return [self.worker]

    async def create_engine(self, worker_id, engine, engine_name, config):
        self.engine_calls.append(("create_engine", worker_id, engine_name, config))

    async def async_call_engine(self, worker_id, method, engine_name, **kwargs):
        self.engine_calls.append((method, worker_id, engine_name, kwargs))
        if method == "launch_server":
            return self.server_info
        return None

    def delete_workers(self, role):
        self.deleted_roles.append(role)


def _launcher(
    n_gpus_per_node: int = 8,
) -> tuple[RolloutInstanceLauncher, _FakeScheduler]:
    scheduler = _FakeScheduler(n_gpus_per_node=n_gpus_per_node)
    config = _FakeConfig(scheduling_spec=(SchedulingSpec(cpu=2, gpu=1, mem=3),))
    rollout_alloc = SimpleNamespace(
        parallel=SimpleNamespace(tp_size=2, pp_size=2),
    )
    launcher = RolloutInstanceLauncher(
        scheduler=scheduler,
        inf_engine=_FakeInferenceEngine,
        config=config,
        rollout_alloc=rollout_alloc,
    )
    return launcher, scheduler


@pytest.mark.asyncio
async def test_launch_creates_one_complete_instance_in_catching_up():
    """A role owns one logical Worker and a stable non-rank engine name."""
    launcher, scheduler = _launcher()

    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={"model_path": "/model"},
    )

    assert scheduler.created_job.replicas == 1
    assert scheduler.created_job.tasks[0].cpu == 8
    assert scheduler.created_job.tasks[0].gpu == 4
    assert scheduler.created_job.tasks[0].mem == 12
    assert result.instance.worker_role == "rollout-elastic-ri-a"
    assert result.instance.engine_name == "rollout/ri-a"
    assert result.instance.state is RolloutInstanceState.CATCHING_UP
    initialize_call = next(
        call for call in scheduler.engine_calls if call[0] == "initialize"
    )
    assert initialize_call[3]["train_data_parallel_size"] == 1


@pytest.mark.asyncio
async def test_launch_failure_deletes_only_its_role():
    """A failed launch cleans up its Scheduler role before propagating the error."""
    launcher, scheduler = _launcher()

    async def fail_launch_server(*args, **kwargs):
        if kwargs["method"] == "launch_server":
            raise RuntimeError("launch failed")
        return None

    scheduler.async_call_engine = fail_launch_server

    with pytest.raises(RuntimeError, match="launch failed"):
        await launcher.launch(
            instance_id="ri-a",
            worker_role="rollout-elastic-ri-a",
            server_args={},
        )

    assert scheduler.deleted_roles == ["rollout-elastic-ri-a"]


@pytest.mark.asyncio
async def test_launch_rejects_cross_node_instance_before_creating_workers():
    """Single-node launcher does not change existing TP/PP communication groups."""
    launcher, scheduler = _launcher(n_gpus_per_node=2)

    with pytest.raises(SingleNodeInstanceError, match="fit one node"):
        await launcher.launch(
            instance_id="ri-a",
            worker_role="rollout-elastic-ri-a",
            server_args={},
        )

    assert scheduler.created_job is None


def test_destroy_requires_drain_teardown_state():
    """The launcher cannot bypass the controller's later graceful drain gate."""
    launcher, scheduler = _launcher()
    instance = SimpleNamespace(
        state=RolloutInstanceState.CATCHING_UP,
        worker_role="rollout-elastic-ri-a",
    )

    with pytest.raises(ValueError, match="STOPPING"):
        launcher.destroy(instance)

    assert scheduler.deleted_roles == []
