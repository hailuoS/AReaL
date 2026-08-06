# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from areal.api.cli_args import (
    ElasticRolloutConfig,
    InferenceEngineConfig,
    SchedulingSpec,
)
from areal.infra.controller.elastic import ElasticScalingReporter, RolloutInstanceState
from areal.infra.controller.rollout_controller import RolloutController


def _reporter(frequency: int = 3) -> ElasticScalingReporter:
    return ElasticScalingReporter(
        report_frequency_steps=frequency,
        min_instances=1,
        max_instances=8,
    )


def test_reporter_emits_only_at_version_aligned_window_boundary():
    reporter = _reporter()

    assert (
        reporter.record(
            report_version=1,
            ready_instances=2,
            entered=10,
            consumed=8,
            wait_seconds=9.0,
            step_seconds=0.0,
        )
        is None
    )
    assert (
        reporter.record(
            report_version=2,
            ready_instances=2,
            entered=12,
            consumed=8,
            wait_seconds=1.0,
            step_seconds=10.0,
        )
        is None
    )

    report = reporter.record(
        report_version=3,
        ready_instances=2,
        entered=14,
        consumed=8,
        wait_seconds=2.0,
        step_seconds=10.0,
    )

    assert report is not None
    assert report["report_version"] == 3
    assert report["window_start_version"] == 1
    assert report["window_end_version"] == 3
    assert report["window_iterations"] == 3
    assert report["timing_samples"] == 2
    assert report["entered"] == 36
    assert report["consumed"] == 24
    # The unpaired first wait sample is dropped, matching AstraFlow.
    assert report["wait_seconds"] == 3.0
    assert report["step_seconds"] == 20.0
    assert report["rollout_wait_fraction"] == pytest.approx(0.15)
    assert report["branch"] == "scale_up"
    assert report["recommended_instances"] == 3


def test_reporter_resets_exact_window_counters_after_emission():
    reporter = _reporter(frequency=2)
    reporter.record(
        report_version=1,
        ready_instances=1,
        entered=100,
        consumed=10,
        wait_seconds=0.0,
        step_seconds=0.0,
    )
    first = reporter.record(
        report_version=2,
        ready_instances=1,
        entered=100,
        consumed=10,
        wait_seconds=0.0,
        step_seconds=10.0,
    )
    assert first is not None

    assert (
        reporter.record(
            report_version=3,
            ready_instances=2,
            entered=5,
            consumed=5,
            wait_seconds=0.5,
            step_seconds=10.0,
        )
        is None
    )
    second = reporter.record(
        report_version=4,
        ready_instances=2,
        entered=5,
        consumed=5,
        wait_seconds=0.5,
        step_seconds=10.0,
    )

    assert second is not None
    assert second["window_start_version"] == 3
    assert second["window_iterations"] == 2
    assert second["entered"] == 10
    assert second["consumed"] == 10
    assert second["ready_instances"] == 2
    assert second["wait_seconds"] == 1.0
    assert second["step_seconds"] == 20.0


def test_reporter_can_disable_automatic_reports():
    reporter = _reporter(frequency=0)

    report = reporter.record(
        report_version=1,
        ready_instances=1,
        entered=10,
        consumed=10,
        wait_seconds=1.0,
        step_seconds=10.0,
    )

    assert report is None


def test_low_wait_branch_recommends_one_instance_step_down():
    reporter = _reporter(frequency=1)

    report = reporter.record(
        report_version=1,
        ready_instances=2,
        entered=5,
        consumed=10,
        wait_seconds=0.0,
        step_seconds=10.0,
    )

    assert report is not None
    assert report["branch"] == "scale_down"
    assert report["recommended_instances"] == 1


def test_low_wait_branch_respects_minimum_instances():
    reporter = _reporter(frequency=1)

    report = reporter.record(
        report_version=1,
        ready_instances=1,
        entered=10,
        consumed=10,
        wait_seconds=0.0,
        step_seconds=10.0,
    )

    assert report is not None
    assert report["branch"] == "scale_down"
    assert report["recommended_instances"] == 1


def test_low_wait_without_timing_evidence_holds_capacity():
    reporter = _reporter(frequency=1)

    report = reporter.record(
        report_version=1,
        ready_instances=2,
        entered=10,
        consumed=10,
        wait_seconds=0.0,
        step_seconds=0.0,
    )

    assert report is not None
    assert report["branch"] == "hold"
    assert report["recommended_instances"] == 2


def test_wait_fraction_at_lower_dead_band_boundary_holds_capacity():
    reporter = _reporter(frequency=1)

    report = reporter.record(
        report_version=1,
        ready_instances=2,
        entered=10,
        consumed=10,
        wait_seconds=0.5,
        step_seconds=10.0,
    )

    assert report is not None
    assert report["branch"] == "hold"
    assert report["recommended_instances"] == 2


def test_reporter_rejects_duplicate_or_out_of_order_versions():
    reporter = _reporter()
    reporter.record(
        report_version=1,
        ready_instances=1,
        entered=1,
        consumed=1,
        wait_seconds=0.0,
        step_seconds=0.0,
    )

    with pytest.raises(ValueError, match="increase monotonically"):
        reporter.record(
            report_version=1,
            ready_instances=1,
            entered=1,
            consumed=1,
            wait_seconds=0.0,
            step_seconds=1.0,
        )


def test_controller_persists_completed_report_in_experiment_directory(tmp_path):
    config = InferenceEngineConfig(
        backend="vllm:d1",
        consumer_batch_size=1,
        scheduling_spec=(SchedulingSpec(cpu=1, gpu=1, mem=1),),
        fileroot=str(tmp_path),
        experiment_name="exp-a",
        trial_name="trial-b",
        elastic=ElasticRolloutConfig(
            enabled=True,
            max_instances=2,
            report_freq_steps=2,
        ),
    )
    controller = RolloutController(inf_engine=object, config=config, scheduler=object())
    instance = controller._instance_pool.create(
        instance_id="ri-first",
        worker_role="rollout-elastic-first",
        worker_id="rollout-elastic-first/0",
        engine_name="rollout/ri-first",
    )
    instance.transition_to(RolloutInstanceState.STARTING)
    instance.transition_to(RolloutInstanceState.READY)

    assert (
        controller.record_elastic_scaling_window(
            report_version=1,
            entered=10,
            consumed=10,
            wait_seconds=0.0,
            step_seconds=0.0,
        )
        is None
    )
    report = controller.record_elastic_scaling_window(
        report_version=2,
        entered=10,
        consumed=10,
        wait_seconds=1.0,
        step_seconds=10.0,
    )

    path = (
        tmp_path
        / "exp-a"
        / "trial-b"
        / "balance_reports"
        / "rollout_balance_report_v2.json"
    )
    assert report is not None
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == report


def test_active_rollout_gpu_count_includes_only_ready_elastic_instances():
    config = InferenceEngineConfig(
        backend="vllm:t2",
        consumer_batch_size=1,
        scheduling_spec=(SchedulingSpec(cpu=1, gpu=1, mem=1),),
        elastic=ElasticRolloutConfig(
            enabled=True,
            initial_instances=1,
            max_instances=2,
        ),
    )
    controller = RolloutController(inf_engine=object, config=config, scheduler=object())
    ready = controller._instance_pool.create(
        instance_id="ri-ready",
        worker_role="rollout-elastic-ready",
        worker_id="rollout-elastic-ready/0",
        engine_name="rollout/ri-ready",
    )
    ready.transition_to(RolloutInstanceState.STARTING)
    ready.transition_to(RolloutInstanceState.READY)
    controller._instance_pool.create(
        instance_id="ri-pending",
        worker_role="rollout-elastic-pending",
        worker_id="rollout-elastic-pending/0",
        engine_name="rollout/ri-pending",
    )

    assert controller.get_active_rollout_gpu_count() == 2
