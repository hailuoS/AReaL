# SPDX-License-Identifier: Apache-2.0

"""In-memory lifecycle primitives for RolloutController V1 elasticity."""

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
from .reconciler import ReconcileResult, RolloutInstanceReconciler
from .recovery_state import ElasticRecoveryState, ElasticRecoveryStore
from .scaling_report import (
    ElasticScalingRecommendation,
    ElasticScalingReporter,
    ElasticScalingWindow,
    recommend_instances,
)

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
    "RolloutInstanceLauncher",
    "RolloutLaunchResult",
    "RolloutInstancePool",
    "RolloutInstanceReconciler",
    "RolloutInstanceState",
    "RolloutRPCTarget",
    "ReconcileResult",
    "ElasticRecoveryState",
    "ElasticRecoveryStore",
    "ElasticScalingRecommendation",
    "ElasticScalingReporter",
    "ElasticScalingWindow",
    "recommend_instances",
    "SingleNodeInstanceError",
    "TaskBindingError",
]
