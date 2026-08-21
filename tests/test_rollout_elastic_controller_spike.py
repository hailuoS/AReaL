# SPDX-License-Identifier: Apache-2.0

import importlib
import sys
from pathlib import Path

import pytest


def _load_spike(monkeypatch):
    example_dir = Path(__file__).parents[1] / "examples" / "math"
    monkeypatch.syspath_prepend(str(example_dir))
    sys.modules.pop("rollout_elastic_controller_spike", None)
    return importlib.import_module("rollout_elastic_controller_spike")


def test_parse_args_accepts_configurable_scale_up_target(monkeypatch):
    spike = _load_spike(monkeypatch)

    options, config_args = spike._parse_args(
        ["--scale-up-to", "5", "--", "--config", "test.yaml"]
    )

    assert options.scale_up_to == 5
    assert config_args == ["--config", "test.yaml"]


@pytest.mark.parametrize("target", [0, 1])
def test_parse_args_rejects_non_scaling_target(monkeypatch, target):
    spike = _load_spike(monkeypatch)

    with pytest.raises(SystemExit):
        spike._parse_args(["--scale-up-to", str(target), "--", "--config", "test.yaml"])
