# RolloutController V1 弹性扩缩容

Rollout 弹性扩缩容是 `RolloutController` V1 的可选单节点扩展，不使用 `RolloutControllerV2`、V2 Router 或 V2
Data Proxy。

## 支持范围

每个弹性实例都是一个完整的 `TP × PP` Rollout Server，使用独立 Scheduler role 和稳定、非数组下标的 instance
ID。单个实例必须能够放入一个节点。 暂不支持跨节点实例和动态 Proxy online session。

弹性模式仅支持 disk 权重同步。AWEX 和 XCCL 保持原有静态行为。

## 配置

`rollout.elastic.enabled` 默认为 `false`。关闭时继续使用原有静态 `Job(replicas=dp_size)` 生命周期、worker
命名、路由、权重更新和 checkpoint 清理逻辑。

实例数量必须满足：

```text
1 <= min_instances <= initial_instances <= max_instances
```

弹性模式下，`max_concurrent_rollouts` 表示单个完整实例的容量。Controller 总容量动态计算为：

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

HTTP 请求只修改 desired state。后台 reconciler 负责创建或排空独立 Scheduler role。新实例完成 server
初始化并加载准确的已提交 disk 版本后， 才能进入 `READY`。

缩容时实例先进入 `DRAINING`，不再接收新请求；只有 workflow task、direct request 和 weight-update lease
全部清零后才会删除。实例模型预留了 active session 统计，但动态 Proxy session 路由和 drain 尚未接入。

## 扩缩容建议

训练侧采集 `prepare_batch` 等待时间、step 时间、进入 buffer 的 rollout 数和 消费样本数。报告复用 AstraFlow 的三段式规则：

```text
wait_fraction > 0.10:
    扩容到 ceil(instances / (1 - wait_fraction))

wait_fraction < 0.05 且生产、消费均非零:
    缩容到 ceil(instances × consumed / entered × 1.10)

其他情况:
    保持
```

结果会限制在配置的 min/max 范围内。该建议只生成报告，不会自动修改 desired state。

## Disk checkpoint 保留和恢复

已完成 checkpoint 通过原子 catalog 登记。系统保留最新 `checkpoint_retention` 个版本，以及被实例追平或权重更新 lease
保护的版本。

配置 `rollout.fileroot` 后，Controller 会原子保存 desired count、serving version、已提交 checkpoint
和所属 Scheduler roles。重启时先校验准确版本， 清理记录中的旧 role，再由 reconciler 重建目标容量。非零 serving version
缺少对应已提交 checkpoint 时，恢复会明确失败。

## 单节点验证

执行 Controller HTTP 穿刺：

```bash
AREAL_SPMD_MODE=false python examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation --verify-recovery -- \
  --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
```

随后使用 disk 模式运行正常端到端训练。至少完成一次权重更新后，通过 HTTP 扩容，并检查 `GET /elastic/instances`：新实例的
`loaded_version` 等于当前 版本后才进入 `ready`。

最后使用同一训练配置设置 `rollout.elastic.enabled=false`，验证原有静态路径 没有回归。
