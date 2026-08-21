# Rollout 弹性扩缩容 KubeRay 验证指导

## 1. 目的与验收范围

本文用于在公司内网的独立 KubeRay 测试集群验证 AReaL Rollout 弹性扩缩容。验证目标是确认下面两个控制闭环能够正确衔接：

1. AReaL 根据指标或 HTTP 请求修改目标 Rollout 实例数，并管理实例启动、模型版本追平、流量路由、安全排空和销毁。
1. Ray/KubeRay 根据 pending placement group 的逻辑资源需求扩缩 Ray Worker Pod；底层 Kubernetes
   节点扩容由平台负责。

AReaL 不直接创建 Kubernetes Pod 或节点。一次扩容只有同时满足以下条件才算完成：

- `ready_instances == desired_instances`；
- 所有 READY 实例的 `loaded_version == serving_version`；
- `pending_update_version == null`；
- 启用实例 Proxy 时，所有 READY 实例的 `proxy_ready == true`；
- Ray 中没有属于本轮扩容的永久 PENDING placement group；
- AReaL Controller 没有未处理的 reconcile 错误。

建议按“本地回归 → KubeRay 前置检查 → HTTP 手工闭环 → 资源不足 → 内部自动决策 → 故障恢复”的顺序验证。先隔离变量，再进行完整训练负载测试。

## 2. 当前支持范围和已知限制

当前实现的验收边界如下：

- 一个弹性实例是一个完整的单节点 `TP × PP` Rollout 实例，暂不支持跨节点弹性实例；
- 仅支持 disk 权重同步，不支持弹性模式下的 XCCL、AWEX 和 V2 Controller；
- 只向 READY 实例路由新请求，缩容实例先进入 DRAINING，所有 lease 清零后才释放资源；
- READY 实例健康检查失败后会被摘出路由并由 desired state 创建替代实例；
- 节点故障上的在途任务不会迁移或自动重试，允许这些请求失败或超时；
- drain 超时只记录错误，不会强杀仍有 lease 的实例；
- 资源不足超时后会继续申请缺口，当前没有退避或熔断；
- Ray placement group 和 launcher actor 尚未命名，Scheduler 的 role/PG/actor
  注册表是进程内状态；Controller 或 Ray head/GCS 灾备不属于当前 worker 自愈验收范围。

特别注意当前批量资源申请存在一个已知 barrier：同一轮所有 placement group 的等待结束后，成功的 reservation
才会开始激活。部分资源先到达时，已获得资源的实例不会立即启动。第 8 节给出精确验证方法和预期结果。

## 3. 验收环境登记

开始前记录以下信息，便于把 AReaL、Ray 和 Kubernetes 三层日志按时间关联：

| 项目                          | 验收值               |
| ----------------------------- | -------------------- |
| AReaL commit                  | `git rev-parse HEAD` |
| AReaL 镜像                    |                      |
| Ray 版本                      |                      |
| KubeRay Operator 版本         |                      |
| Kubernetes 版本               |                      |
| Namespace                     |                      |
| RayCluster 名称               |                      |
| WorkerGroup 名称和数组下标    |                      |
| 加速卡资源键                  | `GPU` / `NPU`        |
| 每个 Ray Worker Pod 的设备数  |                      |
| Worker Pod CPU/内存           |                      |
| `minReplicas` / `maxReplicas` |                      |
| Ray `idleTimeoutSeconds`      |                      |
| checkpoint/fileroot 共享路径  |                      |
| 正常 Worker/节点启动 P95/P99  |                      |

当前仓库 lockfile 使用 Ray 2.55.1。AReaL driver、Ray head 和 Ray worker 应使用兼容的 Ray
版本，推荐使用同一镜像和完全一致的 Ray 版本。

## 4. 安全要求

下面操作只能在专用测试 namespace、测试 RayCluster 和允许故障注入的节点上执行：

- 修改 RayCluster `maxReplicas`；
- 终止 Ray actor 进程；
- 删除 Ray Worker Pod；
- `cordon` 测试节点。

禁止在共享或生产集群执行强制删除、节点 drain、停止 kubelet、删除 RayCluster 等操作。执行变更前必须先只读确认目标，并记录原始配置、Pod、节点和进程
ID。故障测试结束后必须恢复 `maxReplicas` 并 `uncordon` 节点。

## 5. 本地和镜像内回归

对于两台 8 卡 Ascend 910B、CANN 8.5.x 的独立裸机验证环境，仓库提供了固定为 Ray 2.53.0、KubeRay 1.5.2 的部署模板和逐步指导：

- [`examples/kuberay/README.md`](../../../examples/kuberay/README.md)
- [`examples/kuberay/raycluster-2x910b.yaml`](../../../examples/kuberay/raycluster-2x910b.yaml)

先完成模板中的无模型 Ray/KubeRay smoke，再运行本节回归和后续 AReaL HTTP 闭环。这样可以先区分 Kubernetes/Device Plugin
问题与 AReaL 推理服务问题。

在依赖完整的 AReaL 环境中运行：

```bash
uv run pytest -q \
  tests/test_elastic_config.py \
  tests/test_ray_scheduler.py \
  tests/test_rollout_controller.py \
  tests/test_rollout_elastic_autoscaler_spike.py \
  tests/infra/controller/elastic/
```

通过标准：测试全部通过，无因缺少 `ray`、`aiohttp`、`requests` 等运行依赖而跳过或 collection
error。GPU/NPU、多节点和真实推理服务未被单元测试覆盖，仍必须执行后续集群验证。

## 6. KubeRay 前置检查

### 6.1 配置要求

RayCluster 至少满足：

```yaml
spec:
  enableInTreeAutoscaling: true
  autoscalerOptions:
    version: v2
    idleTimeoutSeconds: 120 # 验收示例；生产值按冷启动成本设置
  headGroupSpec:
    rayStartParams:
      num-cpus: "0"
  workerGroupSpecs:
    - groupName: accelerator
      replicas: 1
      minReplicas: 1
      maxReplicas: 3
```

GPU Worker 使用 Ray 的 `num-gpus` 逻辑资源。NPU Worker 需要用 `rayStartParams.resources` 上报与 AReaL
完全一致的自定义资源键，例如 `NPU`。Kubernetes device plugin 的设备 limit 不等于 Ray 已正确上报逻辑资源，必须继续执行资源查询。

资源键示例，实际设备数要与 Worker Pod 规格一致：

```yaml
# GPU Worker
rayStartParams:
  num-gpus: "8"

# NPU Worker
rayStartParams:
  resources: '"{\"NPU\": 8}"'
```

### 6.2 建立观测变量

```bash
export NS="replace-with-namespace"
export RAYCLUSTER="replace-with-raycluster-name"
export DRIVER_POD="replace-with-areal-driver-pod"
export DRIVER_CONTAINER="replace-with-areal-driver-container"

export HEAD_POD=$(kubectl get pods -n "$NS" \
  -l "ray.io/cluster=$RAYCLUSTER,ray.io/node-type=head" \
  -o jsonpath='{.items[0].metadata.name}')
```

确认 RayCluster 和 Pod：

```bash
kubectl get raycluster "$RAYCLUSTER" -n "$NS" -o yaml
kubectl get pods -n "$NS" -l "ray.io/cluster=$RAYCLUSTER" -o wide
kubectl describe raycluster "$RAYCLUSTER" -n "$NS"
```

确认 driver 能连接已有 RayCluster。该命令失败时不要启动 AReaL 作业，避免误连或启动本地 Ray：

```bash
kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  python -c "import ray; ray.init(address='auto'); print(ray.__version__); print(ray.cluster_resources())"
```

在 CPU-only head 场景中，head 不应提供 `GPU`/`NPU`。Worker 注册后，`cluster_resources()`
必须出现预期的设备键和数量。NPU/GPU Worker 容器还应检查设备可见性：

```bash
export RAY_WORKER_POD="replace-with-ray-worker-pod"

kubectl exec -n "$NS" "$RAY_WORKER_POD" -c ray-worker -- env | \
  grep -E 'ASCEND_RT_VISIBLE_DEVICES|CUDA_VISIBLE_DEVICES'
```

### 6.3 校验单实例资源形状

设：

```text
instance_size = rollout TP × PP
```

一个弹性实例的主要 Ray 逻辑资源需求为：

```text
accelerator = instance_size
CPU         = scheduling_spec[0].cpu × instance_size
memory      = scheduling_spec[0].mem × instance_size GiB
```

任意一个 Worker 节点形状都必须能完整容纳该 bundle。资源总量足够但单节点形状放不下时，增加节点数量也无法调度。

如果验收要求“一实例对应一个 Worker Pod/物理节点”，还必须满足：

```text
TP × PP = 每个 Worker Pod 的设备数
```

并保证该 Pod 独占目标物理节点。当前 Ray placement group 使用 `PACK`；如果实例只使用 2/8 卡，多个实例被放入同一 8 卡 Worker
是合法行为，不能根据 Pod 或节点数量判断失败。

### 6.4 三层观测命令

Kubernetes 层：

```bash
kubectl get pod -n "$NS" "$HEAD_POD" \
  -o jsonpath='{.spec.containers[*].name}'
export AUTOSCALER_CONTAINER="replace-with-autoscaler-container-name"

kubectl get pods -n "$NS" -l "ray.io/cluster=$RAYCLUSTER" -o wide -w
kubectl logs -n "$NS" "$HEAD_POD" -c "$AUTOSCALER_CONTAINER" -f
kubectl get events -n "$NS" --sort-by=.lastTimestamp
kubectl get nodes -o wide
```

Ray 层，以下命令在 head 容器执行：

```bash
kubectl exec -n "$NS" "$HEAD_POD" -c ray-head -- ray status -v
kubectl exec -n "$NS" "$HEAD_POD" -c ray-head -- ray list nodes --detail
kubectl exec -n "$NS" "$HEAD_POD" -c ray-head -- \
  ray list placement-groups --detail
kubectl exec -n "$NS" "$HEAD_POD" -c ray-head -- ray list actors --detail
```

如果 `ray list` 不可用，确认 head 镜像安装了 Ray Dashboard/State CLI 依赖。`ray status` 中的 pending
demand 是确认 AReaL 已成功把资源需求提交给 Ray autoscaler 的关键证据。

## 7. 基础验证：Controller 1→N→1 和真实训练

### 7.1 最小 Controller spike

仓库的 `examples/math/rollout_elastic_controller_spike.py` 默认验证 1→2→1，也可通过
`--scale-up-to N` 验证 1→N→1。它用于 HTTP、Proxy 和 recovery 基础穿刺，不覆盖真实训练指标自动决策。

先执行最小路径：

```bash
export DEVICES_PER_RAY_WORKER=8
export SHARED_FILEROOT=/shared/areal

AREAL_SPMD_MODE=false python \
  examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation -- \
  --config examples/math/gsm8k_grpo_npu.yaml \
  scheduler.type=ray \
  cluster.ray_device_resource=NPU \
  cluster.n_gpus_per_node="$DEVICES_PER_RAY_WORKER" \
  actor.weight_update_mode=disk \
  cluster.fileroot="$SHARED_FILEROOT"
```

基础路径通过后，再执行 Proxy 和恢复穿刺：

```bash
export DEVICES_PER_RAY_WORKER=8
export SHARED_FILEROOT=/shared/areal

AREAL_SPMD_MODE=false python \
  examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation --verify-recovery --verify-proxy -- \
  --config examples/math/gsm8k_grpo_npu.yaml \
  scheduler.type=ray \
  cluster.ray_device_resource=NPU \
  cluster.n_gpus_per_node="$DEVICES_PER_RAY_WORKER" \
  actor.weight_update_mode=disk \
  cluster.fileroot="$SHARED_FILEROOT"
```

当一个 Ray Worker Pod 提供 8 卡，而一个 Rollout 实例只使用 2 卡时，至少扩到 5 才会触发第二个 Worker Pod：

```bash
AREAL_SPMD_MODE=false python \
  examples/math/rollout_elastic_controller_spike.py \
  --scale-up-to 5 --verify-recommendation --verify-recovery -- \
  --config examples/math/gsm8k_grpo_npu.yaml \
  scheduler.type=ray \
  cluster.ray_device_resource=NPU \
  cluster.n_gpus_per_node=8 \
  rollout.backend=vllm:d1p1t2 \
  actor.weight_update_mode=disk \
  cluster.fileroot=/shared/areal
```

GPU 环境将示例配置和 `cluster.ray_device_resource` 改成对应的 GPU 配置。`--verify-recovery` 要求
`cluster.fileroot` 对重启前后的 Controller 和所有 Worker 使用相同绝对路径。

通过标准：

- 日志出现 `Elastic Controller HTTP 1->N->1 spike passed`，其中 N 是传入的目标值；
- Proxy 场景中所有 READY 实例 `proxy_ready=true`；
- recovery 场景恢复 `desired_instances=1`；
- 扩容时 Ray/KubeRay 出现新 PG 和 Worker，缩容释放 PG 后 Worker 在 idle timeout 后回收；
- 没有异常活跃的 spike role、PENDING/CREATED/RESCHEDULING PG 或 ALIVE actor；State API 中允许保留
  REMOVED PG 和 DEAD actor 历史。

### 7.2 真实训练配置

第一次真实集群验收关闭内部自动决策：

```text
scheduler.type=ray
cluster.ray_device_resource=NPU
cluster.n_gpus_per_node=8

actor.weight_update_mode=disk

rollout.elastic.enabled=true
rollout.elastic.min_instances=1
rollout.elastic.initial_instances=1
rollout.elastic.max_instances=3
rollout.elastic.auto_apply_scaling_recommendations=false
rollout.elastic.resource_provision_timeout_seconds=900
rollout.elastic.startup_timeout_seconds=300
rollout.elastic.health_check_failure_threshold=2
rollout.elastic.max_concurrent_rollouts_per_instance=256
rollout.elastic.max_total_concurrent_rollouts=768
```

以上数值只是示例，应根据模型和 Worker 形状调整。`max_total_concurrent_rollouts`
未显式提高时，扩容默认只重新分摊原有全局并发，不会自动提高总在途上限。

直接在已连接 RayCluster 的 driver 容器中启动时，可使用以下完整命令骨架：

```bash
export RAY_DEVICE_RESOURCE=NPU
export DEVICES_PER_RAY_WORKER=8
export SHARED_FILEROOT=/shared/areal

AREAL_SPMD_MODE=false python examples/math/gsm8k_rl.py \
  --config examples/math/gsm8k_grpo_npu.yaml \
  scheduler.type=ray \
  cluster.ray_device_resource="$RAY_DEVICE_RESOURCE" \
  cluster.n_gpus_per_node="$DEVICES_PER_RAY_WORKER" \
  cluster.fileroot="$SHARED_FILEROOT" \
  actor.weight_update_mode=disk \
  rollout.elastic.enabled=true \
  rollout.elastic.min_instances=1 \
  rollout.elastic.initial_instances=1 \
  rollout.elastic.max_instances=3 \
  rollout.elastic.auto_apply_scaling_recommendations=false \
  rollout.elastic.resource_provision_timeout_seconds=900 \
  rollout.elastic.startup_timeout_seconds=300 \
  rollout.elastic.health_check_failure_threshold=2 \
  rollout.elastic.max_concurrent_rollouts_per_instance=256 \
  rollout.elastic.max_total_concurrent_rollouts=768
```

公司平台通过 RayJob、Kubernetes Job 或内部 launcher 提交时，把同一组参数放入容器 `command/args`，不要再由平台侧单独修改
Rollout 实例数。GPU 环境需要替换配置文件和 `RAY_DEVICE_RESOURCE`。

至少完成一次 disk 权重更新后再扩容，以验证新实例从共享 checkpoint 追平真实非零版本。完整训练还会占用 actor、reference 等固定资源，因此应比较
Worker/节点的增量，而不是把集群总 Pod 数直接等同于 Rollout 实例数。

### 7.3 发现并调用 Controller HTTP 接口

Callback 端口由 Controller 动态分配。先从 driver 日志找到：

```text
Callback server started on 10.0.0.12:31234
```

不要在脚本中假定固定端口。推荐从 driver Pod 或同一集群内可以访问该地址的 debug Pod 调用：

```bash
export CALLBACK_ADDR="replace-with-host-and-actual-port"

kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS "http://$CALLBACK_ADDR/elastic/instances"
```

如果平台已有 Service，或者确认 `kubectl port-forward` 能访问 Controller
的实际绑定地址，也可以转发日志中的实际端口后从本机调用。生产环境暴露该接口时需要平台提供鉴权、TLS 和审计。

### 7.4 正常扩容 1→3

```bash
kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS -X PUT "http://$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":3}'
```

`PUT` 成功只表示 desired state 已被接受，不表示资源或实例已经就绪。持续查询：

```bash
kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS "http://$CALLBACK_ADDR/elastic/instances"
```

预期时序和证据：

| 层级    | 预期                                                                             |
| ------- | -------------------------------------------------------------------------------- |
| AReaL   | 立即出现两个新实例 ID，状态依次为 PENDING、STARTING、CATCHING_UP、READY          |
| Ray     | 在等待第一个 PG 前已提交两个 PG；pending demand 出现，资源到位后 PG 变为 CREATED |
| KubeRay | Worker Pod 从基线增加，Pod 经 Pending、Running 到 Ready；必要时底层节点扩容      |

最终通过标准：

```text
desired_instances == 3
ready_instances == 3
pending_update_version == null
last_reconcile_error == null
```

所有 READY 实例还必须满足：

```text
state == "ready"
loaded_version == serving_version
proxy_enabled 时 proxy_ready == true
```

训练不能中断，新实例在 READY 前不能承接请求。日志中应能关联
`batch_started`、`provision_batch_completed`、`instance_ready` 和 `batch_completed` 的 batch
ID 和耗时。

### 7.5 正常缩容 3→1

```bash
kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS -X PUT "http://$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":1}'
```

AReaL 侧预期：

1. 两个实例进入 DRAINING，不再接收新请求；
1. 四类独立 blocker：`active_tasks`、`result_leases`、`direct_inflight` 和 `update_leases`
   逐步清零；派生合计 `inflight_requests` 也应归零；
1. role、actor 和 placement group 被删除；
1. 状态收敛为 `desired_instances=ready_instances=1`。

Ray/KubeRay 侧只在 PG 释放且 Worker 空闲超过 `idleTimeoutSeconds` 后回收 Pod。AReaL 收敛和 KubeRay Pod
缩容不是同一时刻，平台不得提前删除仍承载 DRAINING 实例的 Pod。

## 8. 资源不足：初始 1、申请新增 4、仅 3 个新增空位

### 8.1 场景配置

“初始 1，申请新增 4”对应：

```text
initial_instances = 1
max_instances >= 5
PUT desired_instances = 5
```

第 7 节使用的任务配置了 `max_instances=3`，不能在运行中请求 desired=5。本节必须停止上一任务，使用新的
`experiment_name/trial_name` 启动独立短缺测试任务，并在启动时设置：

```text
rollout.elastic.initial_instances=1
rollout.elastic.max_instances=5
rollout.elastic.auto_apply_scaling_recommendations=false
rollout.elastic.resource_provision_timeout_seconds=180
```

120～180 秒只用于验收，且必须高于本环境正常 Worker/节点冷启动 P99。不要用 `startup_timeout_seconds`
模拟资源短缺；它只覆盖资源就绪后的 engine/server 启动。测试结束后恢复生产默认值 900 秒。

在 rollout-only 专用 RayCluster 中，如果一实例对应一个完整 Worker Pod，将 KubeRay 总容量限制为 4，就表示初始 1 加 3
个新增空位。完整训练还包含 actor/reference 等固定加速卡消费者，不能直接使用常数 4；应先记录所有固定角色和初始 1 个 Rollout 都 READY 后的
accelerator Worker 基线，再将上限设置为“基线 Worker 数 + 3”。还必须确认基线 Pod 没有能容纳完整 Rollout bundle 的剩余资源。

修改前记录 WorkerGroup 数组下标和原始值。下面命令中的数组下标必须先通过 `kubectl get raycluster -o yaml` 确认：

```bash
export WORKER_GROUP_INDEX=0
export ORIGINAL_MAX=$(kubectl get raycluster "$RAYCLUSTER" -n "$NS" \
  -o jsonpath="{.spec.workerGroupSpecs[$WORKER_GROUP_INDEX].maxReplicas}")
export BASELINE_ACCELERATOR_WORKERS=$(kubectl get pods -n "$NS" \
  -l "ray.io/cluster=$RAYCLUSTER,ray.io/node-type=worker" \
  --field-selector=status.phase=Running -o name | wc -l)
export SHORTAGE_MAX=$((BASELINE_ACCELERATOR_WORKERS + 3))

if [[ ! "$ORIGINAL_MAX" =~ ^[0-9]+$ ]]; then
  echo "Invalid ORIGINAL_MAX=$ORIGINAL_MAX; stop before patching" >&2
  exit 1
fi
echo "baseline=$BASELINE_ACCELERATOR_WORKERS original_max=$ORIGINAL_MAX test_max=$SHORTAGE_MAX"

kubectl patch raycluster "$RAYCLUSTER" -n "$NS" --type=json \
  -p="[{\"op\":\"replace\",\"path\":\"/spec/workerGroupSpecs/$WORKER_GROUP_INDEX/maxReplicas\",\"value\":$SHORTAGE_MAX}]"
```

如果同一 RayCluster 包含多个 WorkerGroup，上面的 Pod 计数还需要加 WorkerGroup label 过滤。GitOps 管理的
RayCluster 可能自动恢复该字段，执行前应与平台确认变更方式。patch 后立即查询 YAML，确认修改的是目标 accelerator WorkerGroup。

### 8.2 触发和当前精确预期

```bash
kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS -X PUT "http://$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":5}'
```

当前代码预期如下：

1. AReaL 一次创建 4 个新实例身份并提交 4 个 placement group；初始实例保持 READY，4 个新实例保持 PENDING。
1. Ray 中 3 个新 PG 可以变为 CREATED 并占住资源，另 1 个 PG 保持 PENDING；KubeRay Worker 最多扩到
   `SHORTAGE_MAX`。
1. 在第 4 个 PG 达到 resource timeout 前，前 3 个 reservation 也不会启动 worker/engine，因此 AReaL 的
   `ready_instances` 仍为 1。
1. timeout 后，第 4 个 PG 被删除或在 State API 中保留为 REMOVED 历史、对应临时实例失败并移除；前 3 个成功 reservation
   才依次进入 STARTING、CATCHING_UP 和 READY。
1. 状态暂时变为 `desired_instances=5, ready_instances=4`；timeout 后到下一轮 reconcile 前可能短暂无
   pending demand。下一轮会重新申请缺口 1，并产生新的 PENDING PG。如果资源一直不恢复，该 PG 会循环等待和超时，当前没有退避。

单实例资源失败通常作为 per-instance outcome 被处理，`last_reconcile_error` 可能仍为 `null`。必须同时检查：

- AReaL 日志中的 `Ray placement group timeout` 和失败 role；
- timeout 前 `ray status -v` 中的 1 个 pending bundle demand；
- timeout 前 `ray list placement-groups --detail` 中 3 个 CREATED 和 1 个 PENDING，以及 timeout
  后消失或显示为 REMOVED 的失败 PG；
- KubeRay autoscaler 日志中的 `maxReplicas`、quota 或节点容量限制；
- 活跃 PENDING/CREATED/RESCHEDULING PG 数量没有随重试无限累积；State API 中允许保留 REMOVED 历史记录。

通过标准：已有 READY 实例持续服务且版本一致，timeout PG 被清理，无资源泄漏；恢复第 4 个新增空位后，下一轮申请最终收敛到
`desired_instances=ready_instances=5`。

恢复 WorkerGroup 上限：

```bash
kubectl patch raycluster "$RAYCLUSTER" -n "$NS" --type=json \
  -p="[{\"op\":\"replace\",\"path\":\"/spec/workerGroupSpecs/$WORKER_GROUP_INDEX/maxReplicas\",\"value\":$ORIGINAL_MAX}]"

kubectl get raycluster "$RAYCLUSTER" -n "$NS" \
  -o jsonpath="{.spec.workerGroupSpecs[$WORKER_GROUP_INDEX].maxReplicas}"
echo
```

无论用例通过、失败或人工中断，都必须执行上述恢复命令。`ORIGINAL_MAX` 还应记录到第 3 节的环境登记和外部验收记录中，避免终端退出后丢失。

如果验收要求“3 个资源先到就立即启动 3 个实例”，当前实现不满足，应将结果记录为已知缺口，后续把批次激活改为 ready-as-completed，而不是误判为
KubeRay 扩容失败。

## 9. 内部自动扩缩容闭环

手工链路全部通过后，用新任务启用内部策略；不要在同一阶段并发调用 HTTP `PUT`：

```text
rollout.elastic.auto_apply_scaling_recommendations=true
rollout.elastic.report_freq_steps=1
rollout.elastic.autoscaler_scale_down_windows=2
rollout.elastic.autoscaler_scale_up_cooldown_seconds=0
rollout.elastic.autoscaler_scale_down_cooldown_seconds=30
rollout.elastic.autoscaler_direction_change_cooldown_seconds=30
```

内部自动报告依赖真实训练 step，单纯空等不会产生新的完整报告窗口。建议先制造 Rollout 供给不足，使
`wait_fraction > 0.10`，再降低训练消费压力，使有效窗口的 `wait_fraction < 0.05`。

观察：

```bash
kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS "http://$CALLBACK_ADDR/elastic/scaling-recommendation"

kubectl exec -n "$NS" "$DRIVER_POD" -c "$DRIVER_CONTAINER" -- \
  curl -sS "http://$CALLBACK_ADDR/elastic/instances"
```

扩容链路通过标准：

```text
完整报告产生
  → last_autoscaler_decision.action == "apply"
  → desired_instances 增加
  → Ray 出现新 PG demand
  → KubeRay 扩 Worker
  → 新实例 READY
  → last_autoscaler_decision.action == "converged"
```

第一份报告通常用于建立 startup watermark 并被 discard。容量未收敛、报告窗口早于收敛版本或 report 的 READY 快照不匹配时，报告也会被
discard；这属于安全策略，不应视为扩容失效。缩容默认需要连续两个有效低等待窗口，连续缩容和方向反转默认还有 30 秒 cooldown。

`POST /elastic/scaling-recommendation` 只适合验证推荐值计算，不会替代真实训练报告驱动的内部自动扩缩容闭环。

## 10. 故障恢复验证

### 10.1 Ray launcher actor 故障

当前 launcher actor 未命名，只能通过 `ray list actors --detail` 中的 class、node ID 和 PID 定位，再用
`ray list nodes --detail` 将 Ray node IP 映射到 Worker Pod。多业务共享集群无法可靠按 AReaL role
定位，因此只能在独立验收 RayCluster 执行。

先用只读命令完成 `actor → Ray node → Pod` 映射：

```bash
kubectl exec -n "$NS" "$HEAD_POD" -c ray-head -- \
  ray list actors --detail
kubectl exec -n "$NS" "$HEAD_POD" -c ray-head -- \
  ray list nodes --detail
kubectl get pods -n "$NS" -l "ray.io/cluster=$RAYCLUSTER" -o wide
```

从 actor 输出中选择 class 为 `RayWorkerProcessLauncher` 的 ALIVE actor，记录其 PID 和 node ID；在 node
输出中把 node ID 映射成 node IP，再根据 Pod IP 找到 Worker Pod。最后只读确认 PID 和容器：

```bash
export TARGET_WORKER_POD="replace-with-confirmed-rollout-worker-pod"
export LAUNCHER_ACTOR_PID="replace-with-confirmed-launcher-actor-pid"

kubectl get pod -n "$NS" "$TARGET_WORKER_POD" -o wide
kubectl exec -n "$NS" "$TARGET_WORKER_POD" -c ray-worker -- \
  ps -fp "$LAUNCHER_ACTOR_PID"
```

确认该 Pod 不承载 driver、训练角色或其他业务，PID 确实属于 rollout 的 `RayWorkerProcessLauncher` 后，才终止 actor：

```bash
kubectl exec -n "$NS" "$TARGET_WORKER_POD" -c ray-worker -- \
  kill -9 "$LAUNCHER_ACTOR_PID"
```

预期：

- Ray actor 变为 DEAD 并记录 death cause；
- 大约在 `reconcile_interval_seconds × health_check_failure_threshold` 加一次探测耗时后，AReaL 将旧实例从
  READY fence 为 FAILED；
- 旧实例立即停止接收新请求，desired state 创建具有新 instance ID 和 role 的替代实例；
- 无 lease 时旧 role/PG 被清理；有 lease 时清理延后；
- KubeRay Pod 数可能保持不变，因为新 actor 可以复用原 Worker 资源；
- 最终恢复 `desired_instances == ready_instances`，且新实例版本与 serving version 一致；
- 老 actor 可以作为 DEAD 历史保留，但不应出现异常重复 ALIVE actor 或活跃 PG；
- Worker 容器中不应残留孤儿 RPC/backend 进程、监听端口或未释放的 GPU/NPU 占用。可以用 `ps -ef`、平台端口工具以及
  `nvidia-smi`/`npu-smi` 复核；发现残留应记录为失败或已知缺口。

### 10.2 Worker Pod 故障

删除 Worker Pod 只验证 Pod 故障，不等同于物理节点故障。在无在途请求的独立测试集群中，确认目标后执行普通删除：

```bash
export TARGET_WORKER_POD="replace-with-confirmed-rollout-worker-pod"

kubectl delete pod -n "$NS" "$TARGET_WORKER_POD"
```

预期旧 Ray node/actor 变为 DEAD，AReaL fence 旧实例并申请替代实例，KubeRay 补充 Worker Pod。READY
数量允许短暂下降，但其他 READY 实例和训练控制循环必须继续运行；最终版本和容量重新收敛。

### 10.3 节点不可用

只有平台授权后才能在专用测试节点执行。先 cordon，再删除其 Worker Pod，确保 replacement 不会回到原节点：

```bash
export TARGET_WORKER_POD="replace-with-confirmed-rollout-worker-pod"
export TEST_NODE=$(kubectl get pod -n "$NS" "$TARGET_WORKER_POD" \
  -o jsonpath='{.spec.nodeName}')

kubectl cordon "$TEST_NODE"
kubectl delete pod -n "$NS" "$TARGET_WORKER_POD"
```

预期 replacement Pod 调度到其他节点，AReaL 最终恢复 desired 容量。故障节点上的在途请求允许失败或超时，但其他实例必须持续服务。测试结束后恢复：

```bash
kubectl uncordon "$TEST_NODE"
```

真正停止 VM、kubelet 或模拟网络隔离需要平台单独制定操作和回滚方案，不属于本文默认命令。

### 10.4 Controller 恢复边界

`--verify-recovery` 验证的是 desired state、版本和 checkpoint 恢复后清理旧 role 并重建容量，不是 PG/actor
零中断重绑定。当前 PG/actor 未命名且 Scheduler registry 为内存状态，因此不能把 worker/actor 故障恢复通过等同于
Controller、Ray head 或 GCS 高可用通过。

## 11. 证据留存

每个 case 至少保存以下前、中、后证据，并记录时间戳：

### AReaL

- `/elastic/desired-instances` 和 `/elastic/instances` JSON；
- `/elastic/scaling-recommendation` 和 `last_autoscaler_decision`；
- Controller 的 batch ID、timing、health fence、timeout 和 drain 日志；
- 所有 READY 实例的 `loaded_version`、`serving_version` 和 Proxy 状态。

### Ray

- `ray status -v`；
- `ray list placement-groups --detail`；
- `ray list actors --detail`；
- `ray list nodes --detail`。

### Kubernetes/KubeRay

- `kubectl get raycluster -o yaml`；
- Worker Pod `wide` 输出和变化时间；
- autoscaler sidecar 日志；
- namespace events；
- Pod Pending 时的 `kubectl describe pod`；
- 节点和设备配额状态。

推荐按以下时间线关联：

```text
desired accepted
  → PG demand submitted
  → autoscaler decision
  → Worker Pod / node ready
  → PG ready
  → actor/worker active
  → STARTING
  → CATCHING_UP
  → READY
```

缩容时间线：

```text
desired decreased
  → DRAINING
  → leases == 0
  → role/PG removed
  → Ray worker idle
  → KubeRay worker Pod removed
  → Kubernetes node reclaimed
```

## 12. 常见问题定位

| 现象                                       | 优先检查                                                                            |
| ------------------------------------------ | ----------------------------------------------------------------------------------- |
| AReaL desired 增加，但 Ray 没有 PG demand  | driver 是否连接正确 RayCluster、`scheduler.type=ray`、Controller/reconciler 日志    |
| Ray 有 pending demand，但 KubeRay 不加 Pod | autoscaler 是否启用、WorkerGroup `maxReplicas`、bundle 是否能匹配任一节点类型       |
| Worker Pod 一直 Pending                    | quota、设备余量、nodeSelector、taint/toleration、PVC、镜像拉取和 Kubernetes events  |
| Pod Ready，但 PG 一直 Pending              | `GPU`/`NPU` 逻辑资源键、CPU/内存 bundle、Ray Worker 实际上报资源                    |
| PG 已 CREATED，AReaL 仍 PENDING            | 是否处于同批次部分资源 barrier，或 reservation 激活/worker 启动失败                 |
| 实例长期 STARTING                          | 推理镜像、设备可见性、server 日志、`startup_timeout_seconds`                        |
| 实例长期 CATCHING_UP                       | 共享 checkpoint 路径、版本 catalog、存储吞吐和权限                                  |
| 缩容长期 DRAINING                          | 四类 drain blocker、未返回结果、权重更新或 direct RPC                               |
| Pod 不随 AReaL 缩容立即删除                | 正常等待 Ray `idleTimeoutSeconds`；确认 PG 已释放                                   |
| `last_reconcile_error=null` 但容量不足     | 查看 per-instance timeout 日志、Ray PG 和 autoscaler 上限；单实例失败可能被内部处理 |

如果推理进程卡住，再按需打开 `TORCH_DISTRIBUTED_DEBUG=DETAIL`、`NCCL_DEBUG=INFO` 或 NPU/HCCL
对应日志。不要在正常验收全程打开最详细通信日志，以免产生大量噪声并影响时延。

## 13. 验收结论模板

| Case                             | 结果 | 关键耗时 | 证据路径 | 问题/备注 |
| -------------------------------- | ---- | -------- | -------- | --------- |
| 单元测试                         |      |          |          |           |
| KubeRay 资源键和 autoscaler 前置 |      |          |          |           |
| Controller spike 1→N→1           |      |          |          |           |
| 真实训练 1→3→1                   |      |          |          |           |
| 初始 1、新增 4、仅 3 个新增空位  |      |          |          |           |
| 内部指标自动扩容                 |      |          |          |           |
| 内部指标自动缩容                 |      |          |          |           |
| launcher actor 故障              |      |          |          |           |
| Worker Pod 故障                  |      |          |          |           |
| 节点不可用                       |      |          |          |           |
| Controller recovery 边界         |      |          |          |           |

最终结论应区分：

- **代码功能通过**：AReaL 状态机、版本门控、Ray 资源申请和清理符合预期；
- **平台联动通过**：Ray/KubeRay 能根据 PG demand 扩缩 Worker，资源不足和故障证据完整；
- **生产准入通过**：在上述基础上，平台另行完成权限、告警、SLO、容量、重试/熔断和 Controller/head 灾备评审。

相关设计和实现说明：

- [RolloutController V1 弹性扩缩容](rollout_elasticity_v1.md)
- [AReaL Rollout 弹性扩缩容与平台集成设计](rollout_elasticity_platform_integration.md)
