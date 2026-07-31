# SPDX-License-Identifier: Apache-2.0

"""Atomic desired-state recovery record for RolloutController V1."""

from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class ElasticRecoveryState:
    schema_version: int
    desired_instances: int
    serving_version: int
    checkpoint_version: int | None = None
    checkpoint_path: str | None = None
    worker_roles: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError(
                f"unsupported elastic recovery schema {self.schema_version}"
            )
        if self.desired_instances < 1:
            raise ValueError("desired_instances must be at least 1")
        if self.serving_version < 0:
            raise ValueError("serving_version must be non-negative")
        if (self.checkpoint_version is None) != (self.checkpoint_path is None):
            raise ValueError(
                "checkpoint_version and checkpoint_path must be set together"
            )
        if self.serving_version > 0 and self.checkpoint_version is None:
            raise ValueError(
                "nonzero serving version requires a committed checkpoint"
            )
        if self.checkpoint_version is not None:
            if self.checkpoint_version < 0:
                raise ValueError("checkpoint_version must be non-negative")
            if not Path(self.checkpoint_path).is_absolute():
                raise ValueError("checkpoint_path must be absolute")
            if self.checkpoint_version != self.serving_version:
                raise ValueError(
                    "recovery checkpoint version must equal serving version"
                )
        worker_roles = tuple(self.worker_roles)
        if any(not role for role in worker_roles):
            raise ValueError("worker roles must not be empty")
        if len(set(worker_roles)) != len(worker_roles):
            raise ValueError("worker roles must be unique")
        object.__setattr__(self, "worker_roles", worker_roles)


class ElasticRecoveryStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def load(self) -> ElasticRecoveryState | None:
        with self._lock:
            if not self.path.exists():
                return None
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                return ElasticRecoveryState(**payload)
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"cannot load elastic recovery state {self.path}"
                ) from exc

    def save(self, state: ElasticRecoveryState) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(asdict(state), file, sort_keys=True)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
