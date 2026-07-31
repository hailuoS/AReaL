# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from areal.infra.controller.elastic import (
    DiskCheckpointCatalog,
    DiskCheckpointCatalogError,
    DiskCheckpointManifest,
)


def _checkpoint(tmp_path, version: int):
    path = tmp_path / f"weight_update_v{version}"
    path.mkdir()
    return DiskCheckpointManifest(version=version, path=str(path))


def test_commit_persists_versioned_checkpoint_and_latest(tmp_path):
    """A completed checkpoint is durable and discoverable after catalog reload."""
    catalog = DiskCheckpointCatalog(tmp_path)
    version_one = _checkpoint(tmp_path, 1)
    version_two = _checkpoint(tmp_path, 2)

    catalog.commit(version_one)
    catalog.commit(version_two)
    reloaded = DiskCheckpointCatalog(tmp_path)

    assert reloaded.get(1) == version_one
    assert reloaded.latest() == version_two
    assert reloaded.list() == (version_one, version_two)


def test_commit_same_manifest_is_idempotent(tmp_path):
    """A retry after an uncertain writer result cannot duplicate a version."""
    catalog = DiskCheckpointCatalog(tmp_path)
    manifest = _checkpoint(tmp_path, 1)

    catalog.commit(manifest)
    catalog.commit(manifest)

    assert catalog.list() == (manifest,)


def test_commit_conflicting_version_is_rejected(tmp_path):
    """One committed version can never silently point to two checkpoint paths."""
    catalog = DiskCheckpointCatalog(tmp_path)
    catalog.commit(_checkpoint(tmp_path, 1))
    other_path = tmp_path / "other_checkpoint"
    other_path.mkdir()

    with pytest.raises(DiskCheckpointCatalogError, match="already committed"):
        catalog.commit(DiskCheckpointManifest(version=1, path=str(other_path)))


def test_commit_missing_checkpoint_directory_is_rejected(tmp_path):
    """A manifest is never published before its checkpoint directory exists."""
    catalog = DiskCheckpointCatalog(tmp_path)
    missing = DiskCheckpointManifest(version=1, path=str(tmp_path / "missing"))

    with pytest.raises(DiskCheckpointCatalogError, match="does not exist"):
        catalog.commit(missing)

    assert not catalog.path.exists()


def test_invalid_catalog_payload_is_rejected(tmp_path):
    """A corrupt durable manifest cannot be mistaken for an empty catalog."""
    catalog = DiskCheckpointCatalog(tmp_path)
    catalog.path.write_text(json.dumps({"schema_version": 1, "checkpoints": {}}))

    with pytest.raises(DiskCheckpointCatalogError, match="catalog checkpoints"):
        catalog.list()


def test_collect_garbage_keeps_recent_and_protected_versions(tmp_path):
    catalog = DiskCheckpointCatalog(tmp_path / "catalog")
    checkpoints = []
    for version in range(3):
        checkpoint = tmp_path / f"weight_update_v{version}"
        checkpoint.mkdir()
        catalog.commit(DiskCheckpointManifest(version, str(checkpoint)))
        checkpoints.append(checkpoint)

    removed = catalog.collect_garbage(retention=1, protected_versions={0})

    assert [manifest.version for manifest in removed] == [1]
    assert checkpoints[0].is_dir()
    assert not checkpoints[1].exists()
    assert checkpoints[2].is_dir()
