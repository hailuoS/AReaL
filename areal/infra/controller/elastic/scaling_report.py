# SPDX-License-Identifier: Apache-2.0

"""AstraFlow-compatible, report-only rollout scaling recommendation."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ElasticScalingWindow:
    """One training-side observation window supplied by the caller."""

    ready_instances: int
    entered: int
    consumed: int
    wait_seconds: float
    step_seconds: float


@dataclass(frozen=True)
class ElasticScalingRecommendation:
    """Report-only desired instance recommendation."""

    branch: str
    recommended_instances: int
    rollout_wait_fraction: float


def recommend_instances(
    window: ElasticScalingWindow, *, min_instances: int, max_instances: int
) -> ElasticScalingRecommendation:
    """Apply AstraFlow's 0.05/0.10 dead-band to complete instances."""
    if window.ready_instances < 1:
        raise ValueError("ready_instances must be at least 1")
    wait_fraction = (
        min(window.wait_seconds / window.step_seconds, 0.95)
        if window.step_seconds > 0
        else 0.0
    )
    if wait_fraction > 0.10:
        branch = "scale_up"
        target = math.ceil(window.ready_instances / (1.0 - wait_fraction))
    elif wait_fraction < 0.05 and window.entered > 0 and window.consumed > 0:
        branch = "scale_down"
        target = math.ceil(window.ready_instances * window.consumed / window.entered * 1.10)
    else:
        branch = "hold"
        target = window.ready_instances
    return ElasticScalingRecommendation(
        branch=branch,
        recommended_instances=max(min_instances, min(max_instances, target)),
        rollout_wait_fraction=wait_fraction,
    )
