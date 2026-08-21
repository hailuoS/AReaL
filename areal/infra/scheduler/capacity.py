# SPDX-License-Identifier: Apache-2.0

"""Optional batch worker-capacity interface for elastic controllers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from areal.api import Job


@dataclass(frozen=True)
class WorkerProvisionOutcome:
    """Per-role result returned by a batch capacity request."""

    role: str
    worker_ids: tuple[str, ...] = ()
    error: Exception | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


class WorkerCapacityProvider(Protocol):
    """Submit worker demands as one batch without changing Scheduler's API."""

    async def provision_many(
        self,
        jobs: Sequence[Job],
        *,
        timeout: float | None = None,
    ) -> list[WorkerProvisionOutcome]: ...
