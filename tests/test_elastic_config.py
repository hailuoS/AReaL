# SPDX-License-Identifier: Apache-2.0

import pytest

from areal.api.cli_args import ElasticRolloutConfig, InferenceEngineConfig


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
        ({"checkpoint_retention": 1}, "at least 2"),
        ({"recovery_schema_version": 2}, "schema_version"),
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
    )

    assert config.enabled
    assert config.max_instances == 4
