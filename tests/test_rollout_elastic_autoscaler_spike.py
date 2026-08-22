# SPDX-License-Identifier: Apache-2.0

from types import SimpleNamespace

import pytest

pytest.importorskip("requests")

from examples.math import rollout_elastic_autoscaler_spike as autoscaler


def _stable_status(desired: int, serving_version: int) -> dict:
    return {
        "desired_instances": desired,
        "ready_instances": desired,
        "serving_version": serving_version,
        "pending_update_version": None,
        "proxy_enabled": False,
        "last_reconcile_error": None,
        "instances": [
            {
                "instance_id": f"ri-{index}",
                "state": "ready",
                "loaded_version": serving_version,
                "proxy_ready": False,
            }
            for index in range(desired)
        ],
    }


def test_live_autoscaler_processes_each_report_version_once(monkeypatch):
    reports = iter(
        [
            {
                "report_version": 10,
                "window_start_version": 1,
                "ready_instances": 1,
                "recommended_instances": 1,
            },
            {
                "report_version": 10,
                "window_start_version": 1,
                "ready_instances": 1,
                "recommended_instances": 1,
            },
            {
                "report_version": 20,
                "window_start_version": 11,
                "ready_instances": 1,
                "recommended_instances": 2,
            },
        ]
    )
    applied_versions = []

    def get_recommendation(*_args):
        try:
            return next(reports)
        except StopIteration:
            raise KeyboardInterrupt from None

    def apply_report(_base_url, report, **_kwargs):
        applied_versions.append(report["report_version"])
        return False

    monkeypatch.setattr(autoscaler, "_get_recommendation", get_recommendation)
    monkeypatch.setattr(autoscaler, "_get_status", lambda *_args: _stable_status(1, 10))
    monkeypatch.setattr(autoscaler, "_apply_report", apply_report)
    monkeypatch.setattr(autoscaler.time, "sleep", lambda _seconds: None)
    options = SimpleNamespace(
        base_url="http://127.0.0.1:18080",
        request_timeout=1.0,
        cooldown=0.0,
        convergence_timeout=1.0,
        poll_interval=0.1,
        dry_run=False,
        require_proxy=False,
    )

    with pytest.raises(KeyboardInterrupt):
        autoscaler._run_live_loop(options)

    # The first observed report establishes a restart-safe baseline.
    assert applied_versions == [20]


def test_live_autoscaler_discards_cooldown_report_without_replaying(monkeypatch):
    reports = iter(
        [
            {
                "report_version": 10,
                "window_start_version": 1,
                "ready_instances": 1,
                "recommended_instances": 1,
            },
            {
                "report_version": 20,
                "window_start_version": 11,
                "ready_instances": 1,
                "recommended_instances": 2,
            },
            {
                "report_version": 30,
                "window_start_version": 21,
                "ready_instances": 2,
                "recommended_instances": 3,
            },
            {
                "report_version": 30,
                "window_start_version": 21,
                "ready_instances": 2,
                "recommended_instances": 3,
            },
        ]
    )
    statuses = iter(
        [
            _stable_status(1, 10),
            _stable_status(1, 10),
            _stable_status(2, 20),
            _stable_status(2, 20),
            # The duplicate report still reads current status before policy dedupe.
            _stable_status(2, 20),
        ]
    )
    applied_versions = []

    def get_recommendation(*_args):
        try:
            return next(reports)
        except StopIteration:
            raise KeyboardInterrupt from None

    def apply_report(_base_url, report, **_kwargs):
        applied_versions.append(report["report_version"])
        return report["report_version"] == 20

    monotonic_values = iter([100.0, 100.0, 110.0, 120.0, 200.0])
    monkeypatch.setattr(autoscaler, "_get_recommendation", get_recommendation)
    monkeypatch.setattr(autoscaler, "_get_status", lambda *_args: next(statuses))
    monkeypatch.setattr(autoscaler, "_apply_report", apply_report)
    monkeypatch.setattr(autoscaler.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(autoscaler.time, "sleep", lambda _seconds: None)
    options = SimpleNamespace(
        base_url="http://127.0.0.1:18080",
        request_timeout=1.0,
        cooldown=30.0,
        convergence_timeout=1.0,
        poll_interval=0.1,
        dry_run=False,
        require_proxy=False,
    )

    with pytest.raises(KeyboardInterrupt):
        autoscaler._run_live_loop(options)

    assert applied_versions == [20]


def test_policy_allows_consecutive_scale_up_without_cooldown():
    policy = autoscaler.ElasticAutoscalerPolicy(
        cooldown_seconds=None,
        scale_up_cooldown_seconds=0.0,
        scale_down_cooldown_seconds=30.0,
        direction_change_cooldown_seconds=45.0,
        scale_down_windows=2,
    )

    baseline = {
        "report_version": 10,
        "window_start_version": 1,
        "ready_instances": 1,
        "recommended_instances": 1,
    }
    assert (
        policy.evaluate(
            baseline,
            _stable_status(1, 10),
            capacity_stable=True,
            stability_detail="stable",
            now=100.0,
        ).action
        == "discard"
    )

    first = policy.evaluate(
        {
            "report_version": 20,
            "window_start_version": 11,
            "ready_instances": 1,
            "recommended_instances": 2,
        },
        _stable_status(1, 10),
        capacity_stable=True,
        stability_detail="stable",
        now=100.0,
    )
    assert first.should_apply
    assert first.direction == "scale_up"
    policy.record_convergence(
        _stable_status(2, 20),
        now=100.0,
        action_direction=first.direction,
    )

    second = policy.evaluate(
        {
            "report_version": 30,
            "window_start_version": 21,
            "ready_instances": 2,
            "recommended_instances": 3,
        },
        _stable_status(2, 20),
        capacity_stable=True,
        stability_detail="stable",
        now=100.0,
    )
    assert second.should_apply
    assert second.direction == "scale_up"


def test_policy_applies_direction_change_cooldown_after_scale_up():
    policy = autoscaler.ElasticAutoscalerPolicy(
        cooldown_seconds=None,
        scale_up_cooldown_seconds=0.0,
        scale_down_cooldown_seconds=30.0,
        direction_change_cooldown_seconds=45.0,
        scale_down_windows=1,
    )
    policy.evaluate(
        {
            "report_version": 10,
            "window_start_version": 1,
            "ready_instances": 1,
            "recommended_instances": 1,
        },
        _stable_status(1, 10),
        capacity_stable=True,
        stability_detail="stable",
        now=100.0,
    )
    up = policy.evaluate(
        {
            "report_version": 20,
            "window_start_version": 11,
            "ready_instances": 1,
            "recommended_instances": 2,
        },
        _stable_status(1, 10),
        capacity_stable=True,
        stability_detail="stable",
        now=100.0,
    )
    policy.record_convergence(
        _stable_status(2, 20), now=100.0, action_direction=up.direction
    )

    reversal = policy.evaluate(
        {
            "report_version": 30,
            "window_start_version": 21,
            "branch": "scale_down",
            "ready_instances": 2,
            "recommended_instances": 1,
        },
        _stable_status(2, 20),
        capacity_stable=True,
        stability_detail="stable",
        now=120.0,
    )
    assert reversal.action == "discard"
    assert reversal.direction == "scale_down"
    assert "cooldown_seconds=45.0" in reversal.message


def test_live_autoscaler_discards_report_for_previous_capacity(monkeypatch):
    reports = iter(
        [
            {
                "report_version": 10,
                "window_start_version": 1,
                "ready_instances": 4,
                "recommended_instances": 4,
            },
            {
                "report_version": 20,
                "window_start_version": 11,
                "ready_instances": 2,
                "recommended_instances": 5,
            },
        ]
    )
    applied_versions = []

    def get_recommendation(*_args):
        try:
            return next(reports)
        except StopIteration:
            raise KeyboardInterrupt from None

    monkeypatch.setattr(autoscaler, "_get_recommendation", get_recommendation)
    monkeypatch.setattr(autoscaler, "_get_status", lambda *_args: _stable_status(4, 10))
    monkeypatch.setattr(
        autoscaler,
        "_apply_report",
        lambda _base_url, report, **_kwargs: applied_versions.append(
            report["report_version"]
        ),
    )
    monkeypatch.setattr(autoscaler.time, "sleep", lambda _seconds: None)
    options = SimpleNamespace(
        base_url="http://127.0.0.1:18080",
        request_timeout=1.0,
        cooldown=0.0,
        convergence_timeout=1.0,
        poll_interval=0.1,
        dry_run=False,
        require_proxy=False,
    )

    with pytest.raises(KeyboardInterrupt):
        autoscaler._run_live_loop(options)

    assert applied_versions == []


def test_live_autoscaler_requires_consecutive_scale_down_windows(monkeypatch):
    reports = iter(
        [
            {
                "report_version": 10,
                "window_start_version": 1,
                "branch": "hold",
                "ready_instances": 4,
                "recommended_instances": 4,
            },
            {
                "report_version": 20,
                "window_start_version": 11,
                "branch": "scale_down",
                "ready_instances": 4,
                "recommended_instances": 3,
            },
            {
                "report_version": 30,
                "window_start_version": 21,
                "branch": "scale_down",
                "ready_instances": 4,
                "recommended_instances": 3,
            },
        ]
    )
    applied_versions = []

    def get_recommendation(*_args):
        try:
            return next(reports)
        except StopIteration:
            raise KeyboardInterrupt from None

    monkeypatch.setattr(autoscaler, "_get_recommendation", get_recommendation)
    monkeypatch.setattr(autoscaler, "_get_status", lambda *_args: _stable_status(4, 10))
    monkeypatch.setattr(
        autoscaler,
        "_apply_report",
        lambda _base_url, report, **_kwargs: applied_versions.append(
            report["report_version"]
        ),
    )
    monkeypatch.setattr(autoscaler.time, "sleep", lambda _seconds: None)
    options = SimpleNamespace(
        base_url="http://127.0.0.1:18080",
        request_timeout=1.0,
        cooldown=0.0,
        convergence_timeout=1.0,
        poll_interval=0.1,
        dry_run=False,
        require_proxy=False,
        scale_down_windows=2,
    )

    with pytest.raises(KeyboardInterrupt):
        autoscaler._run_live_loop(options)

    assert applied_versions == [30]


def test_scale_down_confirmation_resets_on_hold_and_window_gap():
    confirmation = autoscaler._ScaleDownConfirmation(required_windows=2)
    down = {"branch": "scale_down", "recommended_instances": 3}

    assert confirmation.observe(
        down, desired=4, window_start=11, report_version=20
    ) == (False, 1)
    assert confirmation.observe(
        {"branch": "hold", "recommended_instances": 4},
        desired=4,
        window_start=21,
        report_version=30,
    ) == (False, 0)
    assert confirmation.observe(
        down, desired=4, window_start=31, report_version=40
    ) == (False, 1)
    assert confirmation.observe(
        down, desired=4, window_start=51, report_version=60
    ) == (False, 1)
    assert confirmation.observe(
        down, desired=4, window_start=61, report_version=70
    ) == (True, 2)


def test_scale_down_confirmation_requires_positive_window_count():
    with pytest.raises(ValueError, match="must be positive"):
        autoscaler._ScaleDownConfirmation(required_windows=0)


def test_live_autoscaler_rejects_report_without_version(monkeypatch):
    monkeypatch.setattr(
        autoscaler,
        "_get_recommendation",
        lambda *_args: {"recommended_instances": 2},
    )
    options = SimpleNamespace(
        base_url="http://127.0.0.1:18080",
        request_timeout=1.0,
        cooldown=0.0,
        convergence_timeout=1.0,
        poll_interval=0.1,
        dry_run=False,
        require_proxy=False,
    )

    with pytest.raises(autoscaler.AutoscalerError, match="report_version"):
        autoscaler._run_live_loop(options)
