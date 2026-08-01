# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from areal.infra.controller.elastic import (
    ElasticRecoveryState,
    ElasticRecoveryStore,
)


def test_recovery_store_round_trip_with_committed_checkpoint(tmp_path):
    checkpoint = tmp_path / "weight_update_v7"
    checkpoint.mkdir()
    store = ElasticRecoveryStore(tmp_path / "elastic_recovery.json")
    state = ElasticRecoveryState(
        schema_version=1,
        desired_instances=2,
        serving_version=7,
        checkpoint_version=7,
        checkpoint_path=str(checkpoint),
        worker_roles=("rollout-elastic-ri-a", "rollout-elastic-ri-b"),
    )

    store.save(state)

    assert store.load() == state


def test_recovery_store_overwrites_state_atomically(tmp_path):
    store = ElasticRecoveryStore(tmp_path / "elastic_recovery.json")
    store.save(
        ElasticRecoveryState(
            schema_version=1,
            desired_instances=1,
            serving_version=0,
        )
    )
    updated = ElasticRecoveryState(
        schema_version=1,
        desired_instances=2,
        serving_version=0,
    )

    store.save(updated)

    assert store.load() == updated
    assert not store.path.with_suffix(".tmp").exists()


def test_recovery_state_rejects_version_without_checkpoint(tmp_path):
    checkpoint = tmp_path / "weight_update_v6"
    checkpoint.mkdir()

    with pytest.raises(ValueError, match="must equal serving version"):
        ElasticRecoveryState(
            schema_version=1,
            desired_instances=1,
            serving_version=7,
            checkpoint_version=6,
            checkpoint_path=str(checkpoint),
        )

    with pytest.raises(ValueError, match="requires a committed checkpoint"):
        ElasticRecoveryState(
            schema_version=1,
            desired_instances=1,
            serving_version=7,
        )


def test_recovery_store_rejects_corrupt_payload(tmp_path):
    store = ElasticRecoveryStore(tmp_path / "elastic_recovery.json")
    store.path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="cannot load elastic recovery state"):
        store.load()
