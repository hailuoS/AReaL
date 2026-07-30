# SPDX-License-Identifier: Apache-2.0

"""Errors raised by the RolloutController V1 elastic instance model."""


class ElasticRolloutError(RuntimeError):
    """Base error for rollout elasticity lifecycle operations."""


class DuplicateInstanceError(ElasticRolloutError):
    """Raised when an instance identity is already registered."""


class InstanceNotFoundError(ElasticRolloutError):
    """Raised when an instance identity is not registered."""


class InstanceNotReadyError(ElasticRolloutError):
    """Raised when an instance is not eligible to accept new work."""


class InstanceNotRemovableError(ElasticRolloutError):
    """Raised when an instance still owns resources or in-flight work."""


class InvalidDesiredCountError(ElasticRolloutError):
    """Raised when a desired instance count violates configured bounds."""


class InvalidStateTransitionError(ElasticRolloutError):
    """Raised when an instance lifecycle transition is not allowed."""


class TaskBindingError(ElasticRolloutError):
    """Raised when a workflow task is bound inconsistently."""
