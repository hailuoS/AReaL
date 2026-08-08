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


def test_live_autoscaler_allows_consecutive_scale_up_without_cooldown(monkeypatch):
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
        ]
    )
    statuses = iter(
        [
            _stable_status(1, 10),
            _stable_status(1, 10),
            _stable_status(2, 20),
            _stable_status(2, 20),
            _stable_status(3, 30),
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
        return True

    monotonic_values = iter([100.0, 100.0, 110.0, 115.0, 120.0])
    monkeypatch.setattr(autoscaler, "_get_recommendation", get_recommendation)
    monkeypatch.setattr(autoscaler, "_get_status", lambda *_args: next(statuses))
    monkeypatch.setattr(autoscaler, "_apply_report", apply_report)
    monkeypatch.setattr(autoscaler.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(autoscaler.time, "sleep", lambda _seconds: None)
    options = SimpleNamespace(
        base_url="http://127.0.0.1:18080",
        request_timeout=1.0,
        cooldown=None,
        scale_up_cooldown=0.0,
        scale_down_cooldown=30.0,
        direction_change_cooldown=30.0,
        convergence_timeout=1.0,
        poll_interval=0.1,
        dry_run=False,
        require_proxy=False,
    )

    with pytest.raises(KeyboardInterrupt):
        autoscaler._run_live_loop(options)

    assert applied_versions == [20, 30]


def test_direction_aware_cooldown_distinguishes_growth_and_reversal():
    options = SimpleNamespace(
        cooldown=None,
        scale_up_cooldown=0.0,
        scale_down_cooldown=30.0,
        direction_change_cooldown=45.0,
    )

    assert (
        autoscaler._cooldown_for_action(
            options,
            action_direction="scale_up",
            last_action_direction="scale_up",
        )
        == 0.0
    )
    assert (
        autoscaler._cooldown_for_action(
            options,
            action_direction="scale_down",
            last_action_direction="scale_down",
        )
        == 30.0
    )
    assert (
        autoscaler._cooldown_for_action(
            options,
            action_direction="scale_down",
            last_action_direction="scale_up",
        )
        == 45.0
    )


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
