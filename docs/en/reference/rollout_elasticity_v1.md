# Rollout elasticity for Controller V1

Rollout elasticity is an opt-in extension of `RolloutController` V1. It does
not use `RolloutControllerV2`, a V2 router, or a V2 data proxy.

## Configuration contract

`rollout.elastic.enabled` defaults to `false`. With that value, AReaL follows
the existing static rollout lifecycle without starting an instance pool,
reconciler, control API, or checkpoint catalog.

When elasticity is enabled, each instance is a complete `TP × PP` rollout
server. `min_instances`, `initial_instances`, and `max_instances` therefore
refer to complete instances, not ranks or devices. They must satisfy:

```text
1 <= min_instances <= initial_instances <= max_instances
```

`role_prefix` names the Scheduler resource scope. It is not a stable instance
identity; later controller state assigns immutable instance IDs separately.

## Disk-only weight synchronization

Elastic mode is disk-only. A later controller integration validates that the
training engine uses `actor.weight_update_mode=disk`; AWEX and XCCL remain
available only on the unchanged static path.

Disk checkpoints must retain at least two committed versions
(`checkpoint_retention >= 2`) so a newly launched instance can catch up before
it is eligible for requests. Checkpoint manifests, leases, and garbage
collection are introduced in later commits.

## Drain and recovery contracts

`drain_timeout_seconds` bounds graceful scale-in. A drain must eventually wait
for task, direct-request, online-session, and weight-update leases to empty.
`recovery_schema_version` reserves the on-disk controller state format; this
commit does not persist or recover state.
