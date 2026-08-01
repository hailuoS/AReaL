# SPDX-License-Identifier: Apache-2.0

from types import SimpleNamespace

import pytest

pytest.importorskip("requests")

from examples.math import rollout_elastic_autoscaler_spike as autoscaler


def test_live_autoscaler_processes_each_report_version_once(monkeypatch):
    reports = iter(
        [
            {"report_version": 10, "recommended_instances": 1},
            {"report_version": 10, "recommended_instances": 1},
            {"report_version": 20, "recommended_instances": 2},
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

    assert applied_versions == [10, 20]


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
