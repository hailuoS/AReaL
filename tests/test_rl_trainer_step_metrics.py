# SPDX-License-Identifier: Apache-2.0

import pytest

from areal.trainer import rl_trainer


def test_record_step_metrics_exports_elapsed_time_and_rollout_gpus(monkeypatch):
    recorded = {}
    monkeypatch.setattr(rl_trainer.time, "perf_counter", lambda: 15.5)
    monkeypatch.setattr(
        rl_trainer.stats_tracker,
        "scalar",
        lambda **metrics: recorded.update(metrics),
    )

    rl_trainer.PPOTrainer._record_step_metrics(
        step_started_at=10.0,
        rollout_gpu_count=8,
    )

    assert recorded["timeperf/step"] == pytest.approx(5.5)
    assert recorded["rollout/total_gpus"] == 8
