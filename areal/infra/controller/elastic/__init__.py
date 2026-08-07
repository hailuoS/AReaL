# SPDX-License-Identifier: Apache-2.0

"""Lifecycle primitives for RolloutController V1 elasticity."""

from .errors import (
    DuplicateInstanceError,
    ElasticRolloutError,
    InstanceNotFoundError,
    InstanceNotReadyError,
    InstanceNotRemovableError,
    InvalidDesiredCountError,
    InvalidStateTransitionError,
    TaskBindingError,
)
from .instance_pool import RolloutInstancePool
from .models import (
    InstanceDesiredState,
    RolloutInstance,
    RolloutInstanceState,
    RolloutRPCTarget,
)

__all__ = [
    "DuplicateInstanceError",
    "ElasticRolloutError",
    "InstanceDesiredState",
    "InstanceNotFoundError",
    "InstanceNotReadyError",
    "InstanceNotRemovableError",
    "InvalidDesiredCountError",
    "InvalidStateTransitionError",
    "RolloutInstance",
    "RolloutInstancePool",
    "RolloutInstanceState",
    "RolloutRPCTarget",
    "TaskBindingError",
]
