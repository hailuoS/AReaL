# SPDX-License-Identifier: Apache-2.0

import pytest

from areal.api.alloc_mode import ModelAllocation
from areal.api.cli_args import (
    ElasticRolloutConfig,
    InferenceEngineConfig,
    PPOConfig,
)
from areal.trainer.rl_trainer import PPOTrainer


def test_elastic_rollout_config_is_disabled_by_default():
    config = InferenceEngineConfig(backend="vllm:d1")

    assert config.elastic == ElasticRolloutConfig()
    assert not config.elastic.enabled


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"min_instances": 0}, "at least 1"),
        ({"min_instances": 2, "initial_instances": 1}, "min_instances"),
        ({"initial_instances": 2, "max_instances": 1}, "max_instances"),
        ({"role_prefix": "Rollout"}, "role_prefix"),
        ({"reconcile_interval_seconds": 0}, "positive"),
        ({"drain_timeout_seconds": -1}, "non-negative"),
        ({"startup_timeout_seconds": 0}, "positive"),
        ({"startup_concurrency": 0}, "positive"),
        ({"catch_up_concurrency": 0}, "positive"),
        ({"checkpoint_retention": 1}, "at least 2"),
        ({"recovery_schema_version": 2}, "schema_version"),
        ({"report_freq_steps": -1}, "non-negative"),
    ],
)
def test_elastic_rollout_config_rejects_invalid_contracts(kwargs, message):
    with pytest.raises(ValueError, match=message):
        ElasticRolloutConfig(**kwargs)


def test_elastic_rollout_config_accepts_scale_out_contract():
    config = ElasticRolloutConfig(
        enabled=True,
        min_instances=1,
        initial_instances=2,
        max_instances=4,
        role_prefix="rollout-elastic",
        checkpoint_retention=3,
        report_freq_steps=20,
        startup_timeout_seconds=120,
        startup_concurrency=2,
        catch_up_concurrency=2,
    )

    assert config.enabled
    assert config.max_instances == 4
    assert config.report_freq_steps == 20
    assert config.startup_timeout_seconds == 120
    assert config.startup_concurrency == 2
    assert config.catch_up_concurrency == 2


def _trainer_for_elastic_validation(weight_update_mode: str) -> PPOTrainer:
    config = PPOConfig()
    config.actor.backend = "fsdp:d1"
    config.actor.weight_update_mode = weight_update_mode
    config.rollout.backend = "vllm:d1"
    config.rollout.elastic = ElasticRolloutConfig(enabled=True, max_instances=2)
    trainer = PPOTrainer.__new__(PPOTrainer)
    trainer.config = config
    trainer.actor_alloc = ModelAllocation.from_str(config.actor.backend, name="actor")
    trainer.rollout_alloc = ModelAllocation.from_str(
        config.rollout.backend, name="rollout"
    )
    trainer._should_offload_rollout = False
    trainer._should_offload_actor = False
    trainer._should_offload_critic = False
    trainer._should_offload_ref = False
    trainer._should_offload_teacher = False
    return trainer


def test_elastic_rollout_rejects_non_disk_weight_updates(monkeypatch):
    monkeypatch.setattr("areal.trainer.rl_trainer.is_single_controller", lambda: True)
    trainer = _trainer_for_elastic_validation("awex")

    with pytest.raises(ValueError, match="weight_update_mode=disk"):
        trainer._validate_cfg()


def test_elastic_rollout_rejects_v2_controller(monkeypatch):
    monkeypatch.setattr("areal.trainer.rl_trainer.is_single_controller", lambda: True)
    trainer = _trainer_for_elastic_validation("disk")
    trainer.config.actor._version = "v2"
    trainer.config.rollout._version = "v2"

    with pytest.raises(ValueError, match="RolloutController V1"):
        trainer._validate_cfg()
