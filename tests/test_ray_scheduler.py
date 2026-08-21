# SPDX-License-Identifier: Apache-2.0

import asyncio
import gc
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import ray

import areal.infra.scheduler.ray as ray_scheduler
from areal.api import Job, Worker
from areal.api.cli_args import (
    NameResolveConfig,
    SchedulingSpec,
    SchedulingStrategy,
    SchedulingStrategyType,
)
from areal.infra.scheduler.exceptions import (
    WorkerCreationError,
    WorkerFailedError,
    WorkerNotFoundError,
)
from areal.infra.scheduler.ray import (
    RayScheduler,
    RayWorkerCapacityProvider,
    RayWorkerInfo,
)


def _scheduler(tmp_path, n_gpus_per_node: int = 8) -> RayScheduler:
    scheduler = object.__new__(RayScheduler)
    scheduler._n_gpus_per_node = n_gpus_per_node
    scheduler.experiment_name = "test_ray_scheduler"
    scheduler.trial_name = "trial"
    scheduler.fileroot = str(tmp_path)
    scheduler.enable_tms_offload = False
    scheduler.ray_device_resource = "GPU"
    scheduler.device_control_env_var = "CUDA_VISIBLE_DEVICES"
    scheduler.name_resolve_config = NameResolveConfig(
        type="nfs",
        nfs_record_root=str(tmp_path / "name_resolve"),
        etcd3_addr="localhost:2379",
    )
    scheduler.startup_timeout = 30.0
    scheduler.health_check_interval = 0.1
    scheduler.exp_config = None
    scheduler._workers = {}
    scheduler._launchers = {}
    scheduler._placement_groups = {}
    scheduler._pending_worker_reservations = {}
    scheduler._multi_node_rollout = None
    scheduler._colocated_roles = {}
    return scheduler


def _worker_info(worker_id: str, role: str = "actor") -> RayWorkerInfo:
    return RayWorkerInfo(
        worker=Worker(
            id=worker_id,
            ip="127.0.0.1",
            worker_ports=["10000"],
            engine_ports=[],
        ),
        role=role,
        task_index=int(worker_id.rsplit("/", 1)[1]),
        launchers=[],
        spec=SchedulingSpec(cpu=1, gpu=1, mem=1),
    )


def _sleep_cmd(seconds: int = 60) -> str:
    return f'{sys.executable} -c "import time; time.sleep({seconds})"'


@pytest.fixture(autouse=True)
def _disable_scheduler_destructor(monkeypatch):
    monkeypatch.setattr(RayScheduler, "__del__", lambda self: None)
    yield
    gc.collect()


@pytest.fixture(scope="module")
def local_ray_cluster():
    ray.shutdown()
    try:
        ray.init(
            address="local",
            num_cpus=16,
            num_gpus=8,
            include_dashboard=False,
            ignore_reinit_error=True,
        )
    except Exception as e:
        pytest.skip(f"Unable to start local Ray cluster: {e}")
    yield
    ray.shutdown()


def test_ray_scheduler_rejects_cpu_only_cluster(monkeypatch):
    """Test that RayScheduler fails clearly on CPU-only clusters."""
    monkeypatch.setattr(ray_scheduler.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray_scheduler, "ray_resource_type", lambda: "CPU")

    with pytest.raises(RuntimeError, match="does not support CPU-only clusters"):
        RayScheduler(experiment_name="test-exp", trial_name="test-trial")


def test_ray_scheduler_allows_explicit_device_resource_on_cpu_head(monkeypatch):
    monkeypatch.setattr(ray_scheduler.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray_scheduler, "ray_resource_type", lambda: "CPU")
    monkeypatch.setattr(ray_scheduler, "validate_shared_path", lambda *_args: None)
    monkeypatch.setattr(ray_scheduler.name_resolve, "reconfigure", lambda *_args: None)
    monkeypatch.setattr(
        ray_scheduler.name_resolve, "clear_subtree", lambda *_args: None
    )

    scheduler = RayScheduler(
        experiment_name="test-exp",
        trial_name="test-trial",
        ray_device_resource="GPU",
    )

    assert scheduler.ray_device_resource == "GPU"
    assert scheduler.device_control_env_var == "CUDA_VISIBLE_DEVICES"


def test_prepare_worker_specs_expands_single_spec(tmp_path):
    """Test that a single scheduling spec expands to all replicas."""
    scheduler = _scheduler(tmp_path)
    spec = SchedulingSpec(cpu=1, gpu=1, mem=1)

    schedulings = scheduler._prepare_worker_specs("actor", 2, [spec])

    assert schedulings == [spec, spec]


def test_empty_scheduling_spec_fails(tmp_path):
    """Test that job with no scheduling specs fails."""
    scheduler = _scheduler(tmp_path)

    with pytest.raises(ValueError, match="No scheduling specs"):
        scheduler.create_workers(Job(role="empty_spec", replicas=2, tasks=[]))


def test_mismatched_spec_count_fails(tmp_path):
    """Test that mismatched spec count fails."""
    scheduler = _scheduler(tmp_path)
    job = Job(
        role="mismatched",
        replicas=3,
        tasks=[
            SchedulingSpec(cpu=1, gpu=1, mem=1),
            SchedulingSpec(cpu=2, gpu=1, mem=2),
        ],
    )

    with pytest.raises(ValueError, match="must be 1 or match"):
        scheduler.create_workers(job)


def test_create_placement_group_submits_before_waiting(tmp_path, monkeypatch):
    scheduler = _scheduler(tmp_path)
    bundles = [{"CPU": 1, "GPU": 1}]
    pg = object()
    events = []
    monkeypatch.setattr(
        scheduler,
        "_request_placement_group",
        Mock(
            side_effect=lambda role, requested: events.append(("request", role)) or pg
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "_wait_placement_group_ready",
        Mock(side_effect=lambda role, *_args: events.append(("wait", role))),
    )

    result = scheduler._create_placement_group("rollout", bundles, timeout=30.0)

    assert result is pg
    assert events == [("request", "rollout"), ("wait", "rollout")]


def test_wait_placement_group_ready_keeps_successful_request(tmp_path, monkeypatch):
    scheduler = _scheduler(tmp_path)
    ready_ref = object()
    pg = Mock()
    pg.ready.return_value = ready_ref
    ray_get = Mock()
    remove_pg = Mock()
    monkeypatch.setattr(
        ray_scheduler.ray, "wait", lambda *_args, **_kwargs: ([ready_ref], [])
    )
    monkeypatch.setattr(ray_scheduler.ray, "get", ray_get)
    monkeypatch.setattr(ray_scheduler, "remove_placement_group", remove_pg)

    scheduler._wait_placement_group_ready(
        "rollout",
        pg,
        [{"CPU": 1, "GPU": 1}],
        timeout=30.0,
    )

    ray_get.assert_called_once_with(ready_ref, timeout=0)
    remove_pg.assert_not_called()


def test_worker_reservation_submission_does_not_wait(tmp_path, monkeypatch):
    scheduler = _scheduler(tmp_path)
    pg = object()
    wait_ready = Mock()
    monkeypatch.setattr(scheduler, "_request_placement_group", Mock(return_value=pg))
    monkeypatch.setattr(scheduler, "_wait_placement_group_ready", wait_ready)
    schedulings = [SchedulingSpec(cpu=1, gpu=1, mem=1)]

    reservation = scheduler.request_worker_reservation(
        role="rollout",
        replicas=1,
        schedulings=schedulings,
    )

    assert reservation.placement_group is pg
    assert scheduler._pending_worker_reservations == {"rollout": reservation}
    assert not reservation.ready
    wait_ready.assert_not_called()


def test_worker_reservation_wait_and_cancel_are_separate(tmp_path, monkeypatch):
    scheduler = _scheduler(tmp_path)
    pg = object()
    wait_ready = Mock()
    remove_pg = Mock()
    monkeypatch.setattr(scheduler, "_request_placement_group", Mock(return_value=pg))
    monkeypatch.setattr(scheduler, "_wait_placement_group_ready", wait_ready)
    monkeypatch.setattr(ray_scheduler, "remove_placement_group", remove_pg)
    reservation = scheduler.request_worker_reservation(
        role="rollout",
        replicas=1,
        schedulings=[SchedulingSpec(cpu=1, gpu=1, mem=1)],
    )

    scheduler.wait_worker_reservation(reservation, timeout=123.0)
    scheduler.cancel_worker_reservation(reservation)
    scheduler.cancel_worker_reservation(reservation)

    wait_ready.assert_called_once_with(
        "rollout",
        pg,
        reservation.bundles,
        123.0,
        cancel_event=reservation.cancel_event,
    )
    assert reservation.ready
    assert reservation.cancelled
    assert reservation.cancel_event.is_set()
    assert scheduler._pending_worker_reservations == {}
    remove_pg.assert_called_once_with(pg)


def test_legacy_create_workers_composes_reservation_lifecycle(tmp_path, monkeypatch):
    scheduler = _scheduler(tmp_path)
    events = []
    reservation = object()
    monkeypatch.setattr(
        scheduler,
        "request_worker_reservation",
        Mock(side_effect=lambda **_kwargs: events.append("request") or reservation),
    )
    monkeypatch.setattr(
        scheduler,
        "wait_worker_reservation",
        Mock(side_effect=lambda *_args: events.append("wait")),
    )
    monkeypatch.setattr(
        scheduler,
        "activate_worker_reservation",
        Mock(side_effect=lambda *_args: events.append("activate") or ["rollout/0"]),
    )

    worker_ids = scheduler.create_workers(
        Job(
            role="rollout",
            replicas=1,
            tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1)],
        )
    )

    assert worker_ids == ["rollout/0"]
    assert events == ["request", "wait", "activate"]


@pytest.mark.asyncio
async def test_capacity_provider_submits_all_demands_before_waiting():
    events = []
    scheduler = Mock()

    def request(job):
        events.append(("request", job.role))
        return SimpleNamespace(role=job.role)

    def wait(reservation, *, timeout=None):
        assert timeout == 45.0
        assert [event[0] for event in events[:3]] == ["request"] * 3
        events.append(("wait", reservation.role))

    def activate(reservation):
        events.append(("activate", reservation.role))
        return [f"{reservation.role}/0"]

    scheduler.request_worker_job.side_effect = request
    scheduler.wait_worker_reservation.side_effect = wait
    scheduler.activate_worker_reservation.side_effect = activate
    provider = RayWorkerCapacityProvider(scheduler)
    jobs = [
        Job(role=f"rollout-{index}", replicas=1, tasks=[SchedulingSpec(gpu=1)])
        for index in range(3)
    ]

    outcomes = await provider.provision_many(jobs, timeout=45.0)

    assert [outcome.role for outcome in outcomes] == [job.role for job in jobs]
    assert [outcome.worker_ids for outcome in outcomes] == [
        (f"{job.role}/0",) for job in jobs
    ]
    first_non_request = next(
        index for index, event in enumerate(events) if event[0] != "request"
    )
    assert first_non_request == len(jobs)


@pytest.mark.asyncio
async def test_capacity_provider_preserves_partial_success():
    scheduler = Mock()
    scheduler.request_worker_job.side_effect = lambda job: SimpleNamespace(
        role=job.role
    )

    def wait(reservation, *, timeout=None):
        if reservation.role == "rollout-1":
            raise RuntimeError("capacity unavailable")

    scheduler.wait_worker_reservation.side_effect = wait
    scheduler.activate_worker_reservation.side_effect = lambda reservation: [
        f"{reservation.role}/0"
    ]
    provider = RayWorkerCapacityProvider(scheduler)
    jobs = [
        Job(role=f"rollout-{index}", replicas=1, tasks=[SchedulingSpec(gpu=1)])
        for index in range(3)
    ]

    outcomes = await provider.provision_many(jobs)

    assert outcomes[0].succeeded
    assert isinstance(outcomes[1].error, RuntimeError)
    assert outcomes[2].succeeded
    assert [
        call.args[0].role
        for call in scheduler.activate_worker_reservation.call_args_list
    ] == ["rollout-0", "rollout-2"]


@pytest.mark.asyncio
async def test_capacity_provider_cancellation_removes_pending_demands():
    scheduler = Mock()
    wait_started = threading.Event()
    wait_released = threading.Event()

    def request(job):
        return SimpleNamespace(
            role=job.role,
            activated=False,
            cancelled=False,
        )

    def wait(_reservation, *, timeout=None):
        wait_started.set()
        wait_released.wait(timeout=1.0)
        raise RuntimeError("reservation cancelled")

    def cancel(reservation):
        reservation.cancelled = True
        wait_released.set()

    scheduler.request_worker_job.side_effect = request
    scheduler.wait_worker_reservation.side_effect = wait
    scheduler.cancel_worker_reservation.side_effect = cancel
    provider = RayWorkerCapacityProvider(scheduler)
    jobs = [
        Job(role=f"rollout-{index}", replicas=1, tasks=[SchedulingSpec(gpu=1)])
        for index in range(2)
    ]

    provision_task = asyncio.create_task(provider.provision_many(jobs, timeout=30.0))
    assert await asyncio.to_thread(wait_started.wait, 1.0)
    provision_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await provision_task

    assert scheduler.cancel_worker_reservation.call_count == 2
    scheduler.activate_worker_reservation.assert_not_called()


def test_check_worker_role_health_fails_on_unreachable_endpoint(tmp_path, monkeypatch):
    scheduler = _scheduler(tmp_path)
    scheduler._workers["rollout"] = [_worker_info("rollout/0", role="rollout")]
    process_check = Mock()
    monkeypatch.setattr(scheduler, "_check_worker_process_status", process_check)
    monkeypatch.setattr(scheduler, "_is_worker_ready", Mock(return_value=False))

    with pytest.raises(WorkerFailedError, match="health endpoint"):
        scheduler.check_worker_role_health("rollout")

    process_check.assert_called_once_with("rollout")


def test_zero_replicas_fails(tmp_path):
    """Test that zero replicas fails."""
    scheduler = _scheduler(tmp_path)
    job = Job(role="zero_replicas", replicas=0, tasks=[SchedulingSpec()])

    with pytest.raises(WorkerCreationError, match="replicas must be greater than 0"):
        scheduler.create_workers(job)


def test_additional_bash_cmds_fails(tmp_path):
    """Test that Ray rejects Slurm-style additional bash commands."""
    scheduler = _scheduler(tmp_path)
    job = Job(
        role="bash_cmd",
        replicas=1,
        tasks=[SchedulingSpec(additional_bash_cmds=["echo should-not-run"])],
    )

    with pytest.raises(ValueError, match="additional_bash_cmds"):
        scheduler.create_workers(job)


def test_build_node_plan_large_training_keeps_contiguous_node_groups(tmp_path):
    """Test full-node training creates contiguous node-sized bundles."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=8)
    spec = SchedulingSpec(cpu=1, gpu=1, mem=1)

    bundles, plan, nodes_per_worker = scheduler._build_node_plan(replicas=32, spec=spec)

    assert nodes_per_worker == 1
    assert [int(bundle["GPU"]) for bundle in bundles] == [8, 8, 8, 8]
    assert [item["workers"] for item in plan] == [8, 8, 8, 8]


def test_build_node_plan_partial_single_node_role(tmp_path):
    """Test partial-node Ray allocation builds one partial bundle."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=8)
    spec = SchedulingSpec(cpu=2, gpu=1, mem=3)

    bundles, plan, nodes_per_worker = scheduler._build_node_plan(replicas=6, spec=spec)

    assert nodes_per_worker == 1
    assert len(bundles) == 1
    assert int(bundles[0]["GPU"]) == 6
    assert bundles[0]["CPU"] == 12
    assert plan == [
        {"bundle_index": 0, "node_rank": 0, "workers": 6, "gpus_on_node": 6}
    ]


def test_build_node_plan_multi_node_instance_uses_head_and_worker_nodes(tmp_path):
    """Test multi-node workers reserve all nodes but start only the head worker."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=8)
    spec = SchedulingSpec(cpu=16, gpu=16, mem=32)

    bundles, plan, nodes_per_worker = scheduler._build_node_plan(replicas=2, spec=spec)

    assert nodes_per_worker == 2
    assert [int(bundle["GPU"]) for bundle in bundles] == [8, 8, 8, 8]
    assert [
        (item["worker_idx"], item["node_rank"], item["workers"]) for item in plan
    ] == [
        (0, 0, 1),
        (0, 1, 0),
        (1, 0, 1),
        (1, 1, 0),
    ]


def test_build_node_plan_multi_node_requires_even_cpu_mem_split(tmp_path):
    """Test multi-node workers require CPU and memory to split across nodes."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=8)
    spec = SchedulingSpec(cpu=15, gpu=16, mem=32)

    with pytest.raises(ValueError, match="evenly split CPU and memory"):
        scheduler._build_node_plan(replicas=1, spec=spec)


def test_launcher_worker_spec_defaults_to_rpc_server_and_auto_port():
    """Test launcher worker command defaults to rpc_server with port zero."""
    launcher_cls = (
        ray_scheduler.RayWorkerProcessLauncher.__ray_metadata__.modified_class
    )
    launcher = object.__new__(launcher_cls)
    launcher.env_vars = {"EXTRA_ENV": "1"}
    launcher.device_control_env_var = "CUDA_VISIBLE_DEVICES"

    worker = launcher._build_worker_spec(
        role="actor",
        worker_index=0,
        gpu_devices=["2", "3"],
        cmd=None,
        experiment_name="exp",
        trial_name="trial",
        name_resolve_type="nfs",
        nfs_record_root="/tmp/name_resolve",
        etcd3_addr="localhost:2379",
        fileroot=None,
    )

    assert worker["worker_id"] == "actor/0"
    assert worker["cmd"][:3] == [
        sys.executable,
        "-m",
        "areal.infra.rpc.rpc_server",
    ]
    assert "--port" in worker["cmd"]
    assert worker["cmd"][worker["cmd"].index("--port") + 1] == "0"
    assert worker["env"]["EXTRA_ENV"] == "1"
    assert worker["env"]["CUDA_VISIBLE_DEVICES"] == "2,3"


@pytest.mark.parametrize(
    "cmd",
    [
        "python -m areal.infra.rpc.rpc_server --port 1234",
        "python -m areal.infra.rpc.rpc_server --port=1234",
    ],
)
def test_launcher_worker_spec_rejects_custom_port_argument(cmd):
    """Test that custom commands cannot override scheduler-managed ports."""
    launcher_cls = (
        ray_scheduler.RayWorkerProcessLauncher.__ray_metadata__.modified_class
    )
    launcher = object.__new__(launcher_cls)
    launcher.env_vars = {}
    launcher.device_control_env_var = "CUDA_VISIBLE_DEVICES"

    with pytest.raises(RuntimeError, match="should not include --port"):
        launcher._build_worker_spec(
            role="actor",
            worker_index=0,
            gpu_devices=["0"],
            cmd=cmd,
            experiment_name="exp",
            trial_name="trial",
            name_resolve_type="nfs",
            nfs_record_root="/tmp/name_resolve",
            etcd3_addr="localhost:2379",
            fileroot=None,
        )


def test_launcher_actor_starts_and_stops_worker_processes(tmp_path, local_ray_cluster):
    """Test RayWorkerProcessLauncher as a real Ray actor."""
    launcher = ray_scheduler.RayWorkerProcessLauncher.options(
        num_cpus=1,
        num_gpus=2,
    ).remote(
        "actor",
        str(tmp_path / "actor.log"),
        str(tmp_path / "merged.log"),
        "GPU",
        "CUDA_VISIBLE_DEVICES",
        {},
    )
    node_info = ray.get(launcher.get_node_info.remote(), timeout=30)
    assert len(node_info["visible_devices"]) == 2

    ray.get(
        launcher.start_workers.remote(
            [
                dict(
                    role="actor",
                    worker_index=0,
                    gpu_devices=node_info["visible_devices"][:1],
                    cmd=_sleep_cmd(),
                    experiment_name="test_ray_scheduler",
                    trial_name="trial",
                    name_resolve_type="nfs",
                    nfs_record_root=str(tmp_path / "name_resolve"),
                    etcd3_addr="localhost:2379",
                    fileroot=str(tmp_path),
                ),
                dict(
                    role="actor",
                    worker_index=1,
                    gpu_devices=node_info["visible_devices"][1:],
                    cmd=_sleep_cmd(),
                    experiment_name="test_ray_scheduler",
                    trial_name="trial",
                    name_resolve_type="nfs",
                    nfs_record_root=str(tmp_path / "name_resolve"),
                    etcd3_addr="localhost:2379",
                    fileroot=str(tmp_path),
                ),
            ]
        ),
        timeout=30,
    )
    statuses = ray.get(
        launcher.worker_statuses.remote(["actor/0", "actor/1"]), timeout=30
    )
    assert statuses == {
        "actor/0": {"exists": True, "returncode": None},
        "actor/1": {"exists": True, "returncode": None},
    }

    ray.get(launcher.stop_all_processes.remote(), timeout=30)
    stopped = ray.get(
        launcher.worker_statuses.remote(["actor/0", "actor/1"]), timeout=30
    )
    assert stopped == {
        "actor/0": {"exists": False, "returncode": None},
        "actor/1": {"exists": False, "returncode": None},
    }
    ray.kill(launcher, no_restart=True)


def test_create_workers_uses_real_ray_launchers_and_tracks_state(
    tmp_path, local_ray_cluster
):
    """Test worker creation records real Ray launchers and worker metadata."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=8)
    scheduler._destroy_engines_on_workers = lambda workers: None

    job = Job(
        role="actor",
        replicas=2,
        tasks=[SchedulingSpec(cpu=2, gpu=1, mem=1, cmd=_sleep_cmd())],
    )

    try:
        worker_ids = scheduler.create_workers(job)
        assert worker_ids == ["actor/0", "actor/1"]
        assert len(scheduler._launchers["actor"]) == 1
        assert [
            worker_info.worker.id for worker_info in scheduler._workers["actor"]
        ] == [
            "actor/0",
            "actor/1",
        ]

        launcher = scheduler._launchers["actor"][0]
        statuses = ray.get(launcher.worker_statuses.remote(worker_ids), timeout=30)
        assert statuses == {
            "actor/0": {"exists": True, "returncode": None},
            "actor/1": {"exists": True, "returncode": None},
        }
    finally:
        if "actor" in scheduler._workers:
            scheduler.delete_workers("actor")

    assert "actor" not in scheduler._workers
    assert "actor" not in scheduler._launchers
    assert "actor" not in scheduler._placement_groups


def test_create_workers_multi_node_only_starts_head_launcher(
    tmp_path, local_ray_cluster
):
    """Test multi-node worker creation starts only the head launcher process."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=4)
    scheduler._destroy_engines_on_workers = lambda workers: None

    job = Job(
        role="rollout",
        replicas=1,
        tasks=[SchedulingSpec(cpu=8, gpu=8, mem=2, cmd=_sleep_cmd())],
    )

    try:
        worker_ids = scheduler.create_workers(job)
        assert worker_ids == ["rollout/0"]
        worker_info = scheduler._workers["rollout"][0]
        assert len(worker_info.launchers) == 2

        head_status = ray.get(
            worker_info.launchers[0].worker_statuses.remote(["rollout/0"]),
            timeout=30,
        )
        non_head_status = ray.get(
            worker_info.launchers[1].worker_statuses.remote(["rollout/0"]),
            timeout=30,
        )
        assert head_status == {"rollout/0": {"exists": True, "returncode": None}}
        assert non_head_status == {"rollout/0": {"exists": False, "returncode": None}}
    finally:
        if "rollout" in scheduler._workers:
            scheduler.delete_workers("rollout")

    assert "rollout" not in scheduler._workers
    assert "rollout" not in scheduler._launchers
    assert "rollout" not in scheduler._placement_groups


def test_delete_all_workers_removes_multiple_roles(tmp_path, local_ray_cluster):
    """Test deleting all Ray workers clears every owned role."""
    scheduler = _scheduler(tmp_path, n_gpus_per_node=8)
    scheduler._destroy_engines_on_workers = lambda workers: None

    scheduler.create_workers(
        Job(
            role="role1",
            replicas=1,
            tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1, cmd=_sleep_cmd())],
        )
    )
    scheduler.create_workers(
        Job(
            role="role2",
            replicas=1,
            tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1, cmd=_sleep_cmd())],
        )
    )

    scheduler.delete_workers()

    assert scheduler._workers == {}
    assert scheduler._launchers == {}
    assert scheduler._placement_groups == {}


def test_create_workers_with_non_fork_colocation_reuses_target_workers(tmp_path):
    """Test non-fork colocation reuses existing target workers."""
    scheduler = _scheduler(tmp_path)
    scheduler._workers["actor"] = [
        _worker_info("actor/0", role="actor"),
        _worker_info("actor/1", role="actor"),
    ]
    job = Job(
        role="ref",
        replicas=2,
        tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1)],
        scheduling_strategy=SchedulingStrategy(
            type=SchedulingStrategyType.colocation, target="actor", fork=False
        ),
    )

    worker_ids = scheduler.create_workers(job)

    assert worker_ids == ["actor/0", "actor/1"]
    assert "ref" not in scheduler._workers
    assert scheduler._colocated_roles["ref"] == "actor"


def test_create_workers_with_fork_colocation_delegates_to_fork_workers(
    tmp_path, monkeypatch
):
    """Test fork colocation delegates worker creation to fork_workers."""
    scheduler = _scheduler(tmp_path)
    scheduler._workers["actor"] = [
        _worker_info("actor/0", role="actor"),
        _worker_info("actor/1", role="actor"),
    ]
    called = []

    def fake_fork_workers(role: str, target_role: str):
        called.append((role, target_role))
        return ["ref/0", "ref/1"]

    monkeypatch.setattr(scheduler, "fork_workers", fake_fork_workers)
    job = Job(
        role="ref",
        replicas=2,
        tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1)],
        scheduling_strategy=SchedulingStrategy(
            type=SchedulingStrategyType.colocation, target="actor", fork=True
        ),
    )

    worker_ids = scheduler.create_workers(job)

    assert worker_ids == ["ref/0", "ref/1"]
    assert called == [("ref", "actor")]


def test_colocation_replica_mismatch_raises_error(tmp_path):
    """Test that colocation fails if replica count does not match target."""
    scheduler = _scheduler(tmp_path)
    scheduler._workers["actor"] = [_worker_info("actor/0", role="actor")]
    job = Job(
        role="ref",
        replicas=2,
        tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1)],
        scheduling_strategy=SchedulingStrategy(
            type=SchedulingStrategyType.colocation, target="actor"
        ),
    )

    with pytest.raises(WorkerCreationError, match="Replica count mismatch"):
        scheduler.create_workers(job)


def test_colocation_target_not_found_raises_error(tmp_path):
    """Test that colocation fails if target role does not exist."""
    scheduler = _scheduler(tmp_path)
    job = Job(
        role="ref",
        replicas=2,
        tasks=[SchedulingSpec(cpu=1, gpu=1, mem=1)],
        scheduling_strategy=SchedulingStrategy(
            type=SchedulingStrategyType.colocation, target="missing"
        ),
    )

    with pytest.raises(WorkerNotFoundError):
        scheduler.create_workers(job)
