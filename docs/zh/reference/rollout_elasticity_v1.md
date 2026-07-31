# RolloutController V1 弹性扩缩容

Rollout 弹性扩缩容是 `RolloutController` V1 的可选单节点扩展，不使用
`RolloutControllerV2`、V2 Router 或 V2 Data Proxy。

## 支持范围

每个弹性实例都是一个完整的 `TP × PP` Rollout Server，使用独立 Scheduler
role 和稳定、非数组下标的 instance ID。单个实例必须能够放入一个节点。
暂不支持跨节点实例和动态 Proxy online session。

offline AgentWorkflow 使用实例专属的 V1 ProxyRolloutServer。Proxy 使用独立、
稳定的 Scheduler role，与所属 Rollout 实例一起创建、路由、排空和删除。该能力
不启动 Proxy Gateway，也不接受外部 online session。

弹性模式仅支持 disk 权重同步。启用时会提前拒绝 AWEX、XCCL、V2 和 Proxy
online 配置。独立的共享 server eval rollout 暂不支持；训练可以继续，
validation rollout 会跳过并输出 warning。

## 配置

`rollout.elastic.enabled` 默认为 `false`。关闭时继续使用原有静态
`Job(replicas=dp_size)` 生命周期、worker 命名、路由、权重更新和 checkpoint
清理逻辑。

实例数量必须满足：

```text
1 <= min_instances <= initial_instances <= max_instances
```

弹性模式下，`max_concurrent_rollouts` 表示单个完整实例的容量。Controller
总容量动态计算为：

```text
READY 实例数 × max_concurrent_rollouts
```

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

HTTP 请求只修改 desired state，后台 reconciler 负责创建或排空独立
Scheduler role。新实例完成 server 初始化并加载准确的已提交 disk 版本后，
才能进入 `READY`。

`GET /elastic/instances` 会返回 `serving_version`、
`pending_update_version` 以及每个实例的 `loaded_version`、`proxy_role`、
`proxy_addr` 和 `proxy_ready`。顶层 `proxy_enabled` 用于区分需要 Proxy 的
AgentWorkflow Controller 和不需要 Proxy 的 RolloutWorkflow Controller。

缩容时实例先进入 `DRAINING`，不再接收新请求；只有 workflow task、direct
request 和 weight-update lease 全部清零后才会删除。超过
`drain_timeout_seconds` 时只记录 reconcile error，不会强制删除仍有在途操作的
实例。offline AgentWorkflow session 被所属 workflow task lease 覆盖；动态 Proxy
Gateway online session 路由和 drain 尚未接入。

## 扩缩容建议

训练侧采集 `prepare_batch` 等待时间、step 时间、进入 buffer 的 rollout 数和
消费样本数。报告复用 AstraFlow 的三段式规则：

```text
wait_fraction > 0.10:
    扩容到 ceil(instances / (1 - wait_fraction))

wait_fraction < 0.05 且生产、消费均非零:
    缩容到 ceil(instances × consumed / entered × 1.10)

其他情况:
    保持
```

结果会限制在配置的 min/max 范围内。该建议只生成报告，不会自动修改 desired
state。

## Disk checkpoint 保留和恢复

已完成 checkpoint 通过原子 catalog 登记。系统保留最新
`checkpoint_retention` 个版本，以及被实例追平或权重更新 lease 保护的版本。

disk 更新与新实例追平在版本切换期间互斥。新实例不能在“新 checkpoint 已加载、
serving version 尚未发布”的窗口进入 `READY`。

配置 `rollout.fileroot` 后，Controller 会原子保存 desired count、serving
version、已提交 checkpoint 和所属 Scheduler roles。恢复文件按
`experiment_name/trial_name` 隔离。重启时会校验准确版本、清理记录中的旧 role，
再由 reconciler 重建目标容量。非零 serving version 缺少对应已提交 checkpoint
时，恢复会明确失败。

## 单节点验证

执行 Controller HTTP 穿刺：

```bash
AREAL_SPMD_MODE=false python examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation --verify-recovery --verify-proxy -- \
  --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
```

随后使用 disk 模式运行正常端到端训练。至少完成一次权重更新后，通过 HTTP
扩容，并检查 `GET /elastic/instances`：所有 READY 实例的 `loaded_version`
必须等于 `serving_version`，且 `pending_update_version` 应恢复为 `null`。

NPU GSM8K 示例需要在原训练命令后增加：

```text
actor.weight_update_mode=disk
rollout.elastic.enabled=true
rollout.elastic.min_instances=1
rollout.elastic.initial_instances=1
rollout.elastic.max_instances=2
scheduler.type=ray
```

最后使用同一训练配置设置 `rollout.elastic.enabled=false`，验证原有静态路径
没有回归。
