# SPDX-License-Identifier: Apache-2.0

"""Lifecycle primitives for RolloutController V1 elasticity."""

from .disk_catalog import DiskCheckpointCatalog, DiskCheckpointManifest
from .errors import (
    DiskCheckpointCatalogError,
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
from .recovery_state import ElasticRecoveryState, ElasticRecoveryStore

__all__ = [
    "DuplicateInstanceError",
    "DiskCheckpointCatalog",
    "DiskCheckpointCatalogError",
    "DiskCheckpointManifest",
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
    "ElasticRecoveryState",
    "ElasticRecoveryStore",
    "TaskBindingError",
]
