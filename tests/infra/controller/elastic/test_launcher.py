# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from areal.api import LocalInfServerInfo, Worker
from areal.api.cli_args import SchedulingSpec
from areal.infra.controller.elastic import (
    DiskCheckpointManifest,
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
        self.proxy_worker = Worker(
            id="proxy-elastic-a/0", ip="127.0.0.1", worker_ports=["31000"]
        )
        self.created_job = None
        self.engine_calls = []
        self.deleted_roles = []
        self.forked_roles = []
        self.server_info = LocalInfServerInfo(host="127.0.0.1", port=30000)

    def create_workers(self, job):
        self.created_job = job
        return [self.worker.id]

    def get_workers(self, role):
        if role.startswith("proxy-"):
            return [self.proxy_worker]
        return [self.worker]

    def fork_workers(self, role, target_role, command):
        self.forked_roles.append((role, target_role, command))
        return [self.proxy_worker.id]

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
async def test_launch_proxy_is_bound_to_one_elastic_instance():
    launcher, scheduler = _launcher()
    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={},
    )

    await launcher.launch_proxy(result.instance)

    assert scheduler.forked_roles == [
        (
            "proxy-rollout-elastic-ri-a",
            "rollout-elastic-ri-a",
            "areal.experimental.openai.proxy.proxy_rollout_server",
        )
    ]
    assert result.instance.proxy_ready
    assert result.instance.proxy_worker_id == scheduler.proxy_worker.id
    assert result.instance.proxy_engine_name == "proxy/ri-a"
    assert result.instance.proxy_addr == "http://127.0.0.1:31000"
    initialize_proxy = [
        call
        for call in scheduler.engine_calls
        if call[0] == "initialize" and call[2] == "proxy/ri-a"
    ]
    assert initialize_proxy[0][3]["addr"] == "127.0.0.1:30000"


@pytest.mark.asyncio
async def test_launch_proxy_failure_removes_only_proxy_role():
    launcher, scheduler = _launcher()
    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={},
    )
    original_call = scheduler.async_call_engine

    async def fail_proxy_initialize(worker_id, method, engine_name, **kwargs):
        if method == "initialize" and engine_name == "proxy/ri-a":
            raise RuntimeError("proxy initialize failed")
        return await original_call(worker_id, method, engine_name, **kwargs)

    scheduler.async_call_engine = fail_proxy_initialize

    with pytest.raises(RuntimeError, match="proxy initialize failed"):
        await launcher.launch_proxy(result.instance)

    assert scheduler.deleted_roles == ["proxy-rollout-elastic-ri-a"]
    assert not result.instance.proxy_ready
    assert result.instance.state is RolloutInstanceState.CATCHING_UP


@pytest.mark.asyncio
async def test_catch_up_publishes_version_to_instance_proxy(tmp_path):
    launcher, scheduler = _launcher()
    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={},
    )
    await launcher.launch_proxy(result.instance)
    checkpoint = tmp_path / "weight_update_v7"
    checkpoint.mkdir()

    await launcher.catch_up_from_disk(
        result.instance,
        DiskCheckpointManifest(version=7, path=str(checkpoint)),
    )

    proxy_version_calls = [
        call
        for call in scheduler.engine_calls
        if call[0] == "set_version" and call[2] == "proxy/ri-a"
    ]
    assert proxy_version_calls[0][3]["version"] == 7


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


@pytest.mark.asyncio
async def test_catch_up_loads_committed_checkpoint_before_ready(tmp_path):
    """A launched instance cannot route before disk loading and versioning finish."""
    launcher, scheduler = _launcher()
    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={},
    )
    checkpoint = tmp_path / "weight_update_v7"
    checkpoint.mkdir()

    await launcher.catch_up_from_disk(
        result.instance,
        DiskCheckpointManifest(version=7, path=str(checkpoint)),
    )

    assert result.instance.state is RolloutInstanceState.READY
    assert result.instance.loaded_version == 7
    update_call = next(
        call for call in scheduler.engine_calls if call[0] == "update_weights_from_disk"
    )
    assert update_call[3]["meta"].path == str(checkpoint)
    assert update_call[3]["meta"].version == 7
    assert any(call[0] == "set_version" for call in scheduler.engine_calls)


@pytest.mark.asyncio
async def test_catch_up_failure_marks_instance_failed(tmp_path):
    """A failed disk load never accidentally exposes a partially caught-up server."""
    launcher, scheduler = _launcher()
    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={},
    )
    checkpoint = tmp_path / "weight_update_v7"
    checkpoint.mkdir()

    async def fail_weight_update(worker_id, method, engine_name, **kwargs):
        if method == "update_weights_from_disk":
            raise RuntimeError("disk load failed")
        return None

    scheduler.async_call_engine = fail_weight_update

    with pytest.raises(RuntimeError, match="disk load failed"):
        await launcher.catch_up_from_disk(
            result.instance,
            DiskCheckpointManifest(version=7, path=str(checkpoint)),
        )

    assert result.instance.state is RolloutInstanceState.FAILED
    assert result.instance.loaded_version == 7


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


@pytest.mark.asyncio
async def test_destroy_removes_proxy_before_rollout_role():
    launcher, scheduler = _launcher()
    result = await launcher.launch(
        instance_id="ri-a",
        worker_role="rollout-elastic-ri-a",
        server_args={},
    )
    await launcher.launch_proxy(result.instance)
    result.instance.transition_to(RolloutInstanceState.STOPPING)

    launcher.destroy(result.instance)

    assert scheduler.deleted_roles == [
        "proxy-rollout-elastic-ri-a",
        "rollout-elastic-ri-a",
    ]
    assert result.instance.state is RolloutInstanceState.STOPPED
