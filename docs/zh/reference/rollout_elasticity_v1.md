# RolloutController V1 弹性扩缩容

Rollout 弹性扩缩容是 `RolloutController` V1 的可选单节点扩展，不使用 `RolloutControllerV2`、V2 Router 或 V2
Data Proxy。

## 支持范围

每个弹性实例都是一个完整的 `TP × PP` Rollout Server，使用独立 Scheduler role 和稳定、非数组下标的 instance
ID。单个实例必须能够放入一个节点。 暂不支持跨节点实例和动态 Proxy online session。

offline AgentWorkflow 使用实例专属的 V1 ProxyRolloutServer。Proxy 使用独立、 稳定的 Scheduler role，与所属
Rollout 实例一起创建、路由、排空和删除。该能力 不启动 Proxy Gateway，也不接受外部 online session。

弹性模式仅支持 disk 权重同步。启用时会提前拒绝 AWEX、XCCL、V2 和 Proxy online 配置。独立的共享 server eval rollout
暂不支持；训练可以继续， validation rollout 会跳过并输出 warning。

## 配置

`rollout.elastic.enabled` 默认为 `false`。关闭时继续使用原有静态 `Job(replicas=dp_size)` 生命周期、worker
命名、路由、权重更新和 checkpoint 清理逻辑。

实例数量必须满足：

```text
1 <= min_instances <= initial_instances <= max_instances
```

弹性模式默认保留 AReaL 原有的全局 `max_concurrent_rollouts` 语义。可通过
`max_concurrent_rollouts_per_instance` 设置单实例硬上限，并通过 `max_total_concurrent_rollouts`
显式提高弹性模式的全局上限。有效容量为：

```text
min(
    max_total_concurrent_rollouts,
    READY 实例数 × max_concurrent_rollouts_per_instance,
)
```

两个弹性参数未配置时都回退到 `max_concurrent_rollouts`；因此扩容默认只重新分摊
原有全局并发，不会隐式把它乘以实例数。希望扩容提高总在途量时，必须显式配置更大 的 `max_total_concurrent_rollouts`。

Controller 在同一个 InstancePool 锁内选择 Controller 可见在途请求最少、且尚未达到 单实例上限的 `READY`
实例并立即记账。新实例加入后会优先承接后续请求，已绑定到老 实例的请求不会迁移。相同负载使用轮询打破平局。

`resource_provision_timeout_seconds` 默认值为 `900`，覆盖 Ray placement group 等待时间，包括 KubeRay
worker Pod 和节点的启动时间。资源就绪后，`startup_concurrency` 默认值为 `2`，用于限制同时初始化推理引擎、server 和 proxy
的实例数。实例获得启动 并发槽位后才单独开始计算 `startup_timeout_seconds`；资源申请和等待并发槽位的时间都不 计入实例启动超时。

对于 Ray role，reconciler 还会探测 Worker 进程和 HTTP 健康端点。连续失败达到 `health_check_failure_threshold`
后，实例会立即被摘出路由，并由 desired state 创建替代 实例。旧 placement group 会在 Controller 可见的
workflow、result、direct RPC 和权重更新 lease 全部排空后删除。

如果 Ray head 仅有 CPU，需要显式设置 `cluster.ray_device_resource=GPU` 或 `NPU`，确保 placement group
bundle 使用 KubeRay Worker 所上报的加速卡资源键。

## HTTP 控制和状态

V1 callback server 提供：

```text
GET /elastic/desired-instances
PUT /elastic/desired-instances
GET /elastic/instances
GET /elastic/scaling-recommendation
POST /elastic/scaling-recommendation
```

设置目标实例数：

```json
{"desired_instances": 2}
```

HTTP 请求只修改 desired state，后台 reconciler 负责创建或排空独立 Scheduler role。新实例完成 server
初始化并加载准确的已提交 disk 版本后， 才能进入 `READY`。

扩容时，reconciler 会先为全部缺口注册稳定实例身份。等待 Scheduler 分配 worker 时状态为 `PENDING`，获得 worker 并初始化
server 时为 `STARTING`，加载最新已提交 权重时为 `CATCHING_UP`。这些状态均不可路由，但会立即出现在状态接口中。

`GET /elastic/instances` 会返回 `serving_version`、 `pending_update_version` 以及每个实例的
`loaded_version`、`proxy_role`、 `proxy_addr`、`proxy_ready`、`inflight_requests` 和
`available_request_capacity`。顶层返回有效全局并发、单实例上限和弹性总上限； `proxy_enabled` 用于区分需要 Proxy 的
AgentWorkflow Controller 和不需要 Proxy 的 RolloutWorkflow Controller。

缩容时实例先进入 `DRAINING`，不再接收新请求；只有 workflow task、direct request 和 weight-update lease
全部清零后才会删除。超过 `drain_timeout_seconds` 时只记录 reconcile error，不会强制删除仍有在途操作的 实例。offline
AgentWorkflow session 被所属 workflow task lease 覆盖；动态 Proxy Gateway online session 路由和
drain 尚未接入。

## 扩缩容建议

训练侧采集 `prepare_batch` 等待时间、step 时间、进入 buffer 的 rollout 数和 消费样本数。报告使用针对 AReaL 按需生成路径调整后的
AstraFlow 风格三段式规则：

```text
wait_fraction > 0.10:
    扩容到 ceil(instances / (1 - wait_fraction))

wait_fraction < 0.05，且有效 step 时间、生产、消费均非零:
    建议减少 1 个实例

其他情况:
    保持
```

结果会限制在配置的 min/max 范围内。按需驱动的 `prepare_batch` 即使在容量过剩时 也常使 `consumed / entered` 接近 1，因此
AReaL 不再使用该比例计算缩容目标。建议默认仍只生成报告。设置 `auto_apply_scaling_recommendations=true`
后，RolloutController 会校验每个完整报告窗口， 并通过与 HTTP API 相同的入口更新 desired state。随后 reconciler 创建的
Ray placement group 会驱动集群级 Ray autoscaler；策略循环不会直接调用 Kubernetes 或 KubeRay 扩缩容 接口。

内部 autoscaler 与示例 autoscaler 复用同一套有状态策略。它们会立即消费冷却期或容量
未收敛时观察到的报告，不会在条件解除后重放。一次扩缩容收敛后，策略只接受采样窗口 完全开始于收敛版本之后、且报告容量与当前稳定容量一致的新报告。连续扩容默认不额外
冷却；连续缩容和扩缩方向反转默认冷却 30 秒。缩容还要求连续两个有效低等待窗口，并且 每次收敛周期最多减少 1 个实例。内部参数通过 elastic 下的
`autoscaler_*` 字段配置；示例 提供等价的命令行选项。

## Disk checkpoint 保留和恢复

已完成 checkpoint 通过原子 catalog 登记。系统保留最新 `checkpoint_retention` 个版本，以及被实例追平或权重更新 lease
保护的版本。

disk 更新与新实例追平在版本切换期间互斥。新实例不能在“新 checkpoint 已加载、 serving version 尚未发布”的窗口进入 `READY`。

配置 `rollout.fileroot` 后，Controller 会原子保存 desired count、serving version、已提交 checkpoint
和所属 Scheduler roles。恢复文件按 `experiment_name/trial_name` 隔离。重启时会校验准确版本、清理记录中的旧 role， 再由
reconciler 重建目标容量。非零 serving version 缺少对应已提交 checkpoint 时，恢复会明确失败。

## 单节点验证

执行 Controller HTTP 穿刺：

```bash
AREAL_SPMD_MODE=false python examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation --verify-recovery --verify-proxy -- \
  --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
```

随后使用 disk 模式运行正常端到端训练。至少完成一次权重更新后，通过 HTTP 扩容，并检查 `GET /elastic/instances`：所有 READY 实例的
`loaded_version` 必须等于 `serving_version`，且 `pending_update_version` 应恢复为 `null`。

NPU GSM8K 示例需要在原训练命令后增加：

```text
actor.weight_update_mode=disk
rollout.elastic.enabled=true
rollout.elastic.min_instances=1
rollout.elastic.initial_instances=1
rollout.elastic.max_instances=2
scheduler.type=ray
```

最后使用同一训练配置设置 `rollout.elastic.enabled=false`，验证原有静态路径 没有回归。
