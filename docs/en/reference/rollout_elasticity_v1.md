# Rollout elasticity for Controller V1

Rollout elasticity is an opt-in, single-node extension of `RolloutController` V1. It
does not use `RolloutControllerV2`, a V2 router, or a V2 data proxy.

## Scope

Each elastic instance is one complete `TP × PP` rollout server owned by an independent
Scheduler role and a stable, non-positional instance ID. A single instance must fit on
one node. Cross-node instances and dynamic Proxy online sessions are not supported.

Offline AgentWorkflow uses an instance-local V1 ProxyRolloutServer. Each proxy has a
stable Scheduler role and is created, routed, drained, and deleted with its owning
rollout instance. This does not start the Proxy Gateway or accept external online
sessions.

Elastic mode supports disk weight synchronization only. AWEX and XCCL retain their
existing static behavior.

Elastic V1 currently disables the separate shared-server evaluation rollout. Training
rollouts remain available, but validation rollout is skipped with a warning until
dynamic evaluation backends are implemented.

## Configuration

`rollout.elastic.enabled` defaults to `false`. When disabled, the original static
`Job(replicas=dp_size)` lifecycle, worker naming, routing, weight updates, and
checkpoint cleanup remain unchanged.

The instance bounds must satisfy:

```text
1 <= min_instances <= initial_instances <= max_instances
```

Elastic mode preserves AReaL's original global `max_concurrent_rollouts` semantics by
default. `max_concurrent_rollouts_per_instance` adds a hard per-instance ceiling, and
`max_total_concurrent_rollouts` can explicitly raise the elastic global ceiling. The
effective capacity is:

```text
min(
    max_total_concurrent_rollouts,
    READY instances × max_concurrent_rollouts_per_instance,
)
```

Both elastic limits fall back to `max_concurrent_rollouts` when omitted. Scale-out
therefore redistributes the original global concurrency by default instead of silently
multiplying it by the instance count. Set a larger `max_total_concurrent_rollouts`
explicitly when scale-out should increase the total number of in-flight workflows.

Under the InstancePool lock, the Controller selects the least-loaded `READY` instance
that still has a per-instance slot and accounts for the reservation immediately. A new
instance preferentially receives subsequent requests until loads converge; existing
bindings are not migrated. Round-robin breaks equal-load ties.

`startup_concurrency` defaults to `2` and limits concurrent inference-engine, server,
and proxy initialization. `startup_timeout_seconds` is applied independently after an
instance acquires a startup slot; time spent queued behind the concurrency limit does
not consume its startup timeout.

## HTTP control and status

The V1 callback server exposes:

```text
GET /elastic/desired-instances
PUT /elastic/desired-instances
GET /elastic/instances
GET /elastic/scaling-recommendation
POST /elastic/scaling-recommendation
```

`GET /elastic/instances` reports both `serving_version` and
`pending_update_version`, plus each instance's `loaded_version`, `proxy_role`,
`proxy_addr`, `proxy_ready`, `inflight_requests`, and
`available_request_capacity`. Top-level fields report the effective global limit, the
per-instance limit, and the elastic total ceiling. The `proxy_enabled` field distinguishes an
AgentWorkflow controller from a RolloutWorkflow controller that needs no proxy.

Set desired capacity with:

```json
{"desired_instances": 2}
```

The HTTP request changes desired state only. The background reconciler creates or drains
independent Scheduler roles. A new instance becomes `READY` only after server
initialization and loading the exact committed disk version.

During scale-out, the reconciler first registers stable identities for the entire
capacity deficit. An instance is `PENDING` while waiting for Scheduler worker
allocation, `STARTING` while initializing its server, and `CATCHING_UP` while loading
the latest committed weights. These non-routable states appear immediately in the
status endpoint.

Scale-in first changes an instance to `DRAINING`. It receives no new work and is deleted
only after workflow tasks, direct requests, and weight-update leases are empty. An
offline AgentWorkflow session is covered by its owning workflow-task lease; dynamic
Proxy Gateway online-session routing and drain remain deferred. Exceeding
`drain_timeout_seconds` reports a reconcile error and does not force-delete an instance
that still owns work.

## Scaling recommendation

Training records the time blocked in `prepare_batch`, step duration, accepted rollouts,
and consumed samples. The report uses an AstraFlow-inspired three-zone rule adapted
to AReaL's demand-driven rollout path:

```text
wait_fraction > 0.10:
    scale up to ceil(instances / (1 - wait_fraction))

wait_fraction < 0.05, measured step time is nonzero, and production and consumption are nonzero:
    recommend one fewer instance

otherwise:
    hold
```

The result is clamped to configured min/max instances. AReaL does not use the
`consumed / entered` ratio as the scale-down target because demand-driven
`prepare_batch` commonly keeps those counters close even when rollout capacity is
over-provisioned. The recommendation is report-only and never changes desired state
automatically.

The example autoscaler consumes reports observed during cooldown or unstable capacity
instead of replaying them later. After convergence it accepts only a report whose
complete window starts after the convergence version and whose reported capacity
matches the current stable capacity. Scale-down additionally requires two consecutive
valid low-wait windows by default (`--scale-down-windows`) and removes at most one
instance per convergence cycle.

## Disk checkpoint retention and recovery

Committed checkpoints are cataloged atomically. The newest `checkpoint_retention`
versions are retained, along with any version protected by catch-up or weight-update
leases.

Disk update and instance catch-up are mutually excluded across the version transition.
An instance cannot become `READY` in the gap between loading a new checkpoint and
publishing its serving version.

When `rollout.fileroot` is configured, the Controller atomically persists desired
capacity, serving version, committed checkpoint, and owned Scheduler roles. After
restart it validates the exact checkpoint, removes recorded stale roles, and lets the
reconciler recreate the requested capacity. A nonzero serving version without its
committed disk checkpoint fails recovery.

Recovery files are isolated under `rollout.fileroot/experiment_name/trial_name` so
independent trials do not reuse each other's desired state or checkpoint.

## Single-node validation

Run the Controller HTTP spike:

```bash
AREAL_SPMD_MODE=false python examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation --verify-recovery --verify-proxy -- \
  --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
```

Then run normal end-to-end training with disk updates. After at least one weight update,
increase desired capacity and verify from `GET /elastic/instances` that the new instance
has the current `loaded_version` before it reaches `ready`.

For the NPU GSM8K example, add these overrides to the normal training command:

```text
actor.weight_update_mode=disk
rollout.elastic.enabled=true
rollout.elastic.min_instances=1
rollout.elastic.initial_instances=1
rollout.elastic.max_instances=2
scheduler.type=ray
```

Finally run the same training configuration with `rollout.elastic.enabled=false` to
verify the unchanged static path.
