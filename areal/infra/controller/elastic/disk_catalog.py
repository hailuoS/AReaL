# SPDX-License-Identifier: Apache-2.0

"""Durable disk checkpoint manifest for elastic Rollout V1 catch-up."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

from .errors import DiskCheckpointCatalogError

_CATALOG_FILENAME = "rollout_disk_checkpoint_catalog.json"
_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DiskCheckpointManifest:
    """One fully written disk checkpoint available for a new rollout instance."""

    version: int
    path: str

    def __post_init__(self) -> None:
        if self.version < 0:
            raise DiskCheckpointCatalogError("checkpoint version must be non-negative")
        if not self.path:
            raise DiskCheckpointCatalogError("checkpoint path must not be empty")
        if not Path(self.path).is_absolute():
            raise DiskCheckpointCatalogError("checkpoint path must be absolute")


class DiskCheckpointCatalog:
    """Atomically persist committed checkpoint paths without deleting them."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._path = self._root / _CATALOG_FILENAME

    @property
    def path(self) -> Path:
        """Return the durable catalog file path."""
        return self._path

    def commit(self, manifest: DiskCheckpointManifest) -> None:
        """Record a completed checkpoint after its writer has finished successfully."""
        checkpoint_path = Path(manifest.path)
        if not checkpoint_path.is_dir():
            raise DiskCheckpointCatalogError(
                f"committed checkpoint directory does not exist: {checkpoint_path}"
            )

        manifests = self._read_manifests()
        existing = manifests.get(manifest.version)
        if existing is not None and existing != manifest:
            raise DiskCheckpointCatalogError(
                f"checkpoint version {manifest.version} is already committed "
                f"at {existing.path}"
            )
        if existing == manifest:
            return

        manifests[manifest.version] = manifest
        self._write_manifests(manifests)

    def get(self, version: int) -> DiskCheckpointManifest:
        """Return one committed checkpoint manifest by version."""
        if version < 0:
            raise DiskCheckpointCatalogError("checkpoint version must be non-negative")
        try:
            return self._read_manifests()[version]
        except KeyError as exc:
            raise DiskCheckpointCatalogError(
                f"checkpoint version {version} is not committed"
            ) from exc

    def latest(self) -> DiskCheckpointManifest:
        """Return the highest committed checkpoint version."""
        manifests = self._read_manifests()
        if not manifests:
            raise DiskCheckpointCatalogError("no committed disk checkpoints")
        return manifests[max(manifests)]

    def list(self) -> tuple[DiskCheckpointManifest, ...]:
        """Return a version-sorted immutable checkpoint snapshot."""
        manifests = self._read_manifests()
        return tuple(manifests[version] for version in sorted(manifests))

    def collect_garbage(
        self, *, retention: int, protected_versions: set[int] | frozenset[int] = frozenset()
    ) -> tuple[DiskCheckpointManifest, ...]:
        """Remove old committed directories except explicitly protected versions."""
        if retention < 1:
            raise DiskCheckpointCatalogError("checkpoint retention must be at least 1")
        manifests = self._read_manifests()
        retained = set(sorted(manifests, reverse=True)[:retention]) | set(protected_versions)
        removed = []
        for version in sorted(set(manifests) - retained):
            manifest = manifests[version]
            shutil.rmtree(manifest.path)
            removed.append(manifest)
            del manifests[version]
        if removed:
            self._write_manifests(manifests)
        return tuple(removed)

    def _read_manifests(self) -> dict[int, DiskCheckpointManifest]:
        if not self._path.exists():
            return {}
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            if payload.get("schema_version") != _SCHEMA_VERSION:
                raise DiskCheckpointCatalogError(
                    "unsupported disk checkpoint catalog schema version"
                )
            entries = payload.get("checkpoints")
            if not isinstance(entries, list):
                raise DiskCheckpointCatalogError("catalog checkpoints must be a list")
            manifests = [DiskCheckpointManifest(**entry) for entry in entries]
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DiskCheckpointCatalogError(
                f"cannot read disk checkpoint catalog {self._path}"
            ) from exc

        result = {manifest.version: manifest for manifest in manifests}
        if len(result) != len(manifests):
            raise DiskCheckpointCatalogError("catalog contains duplicate versions")
        return result

    def _write_manifests(self, manifests: dict[int, DiskCheckpointManifest]) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "checkpoints": [
                asdict(manifests[version]) for version in sorted(manifests)
            ],
        }
        temporary_path = self._path.with_suffix(".tmp")
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, sort_keys=True)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, self._path)
