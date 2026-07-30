# SPDX-License-Identifier: Apache-2.0

"""In-memory lifecycle primitives for RolloutController V1 elasticity."""

from .errors import (
    DuplicateInstanceError,
    ElasticRolloutError,
    InstanceNotFoundError,
    InstanceNotReadyError,
    InstanceNotRemovableError,
    InvalidDesiredCountError,
    InvalidStateTransitionError,
    SingleNodeInstanceError,
    TaskBindingError,
)
from .instance_pool import RolloutInstancePool
from .launcher import RolloutInstanceLauncher, RolloutLaunchResult
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
    "RolloutInstanceLauncher",
    "RolloutLaunchResult",
    "RolloutInstancePool",
    "RolloutInstanceState",
    "RolloutRPCTarget",
    "SingleNodeInstanceError",
    "TaskBindingError",
]
