# RolloutController V1 弹性扩缩容

Rollout 弹性扩缩容是对 `RolloutController` V1 的可选扩展。它不使用
`RolloutControllerV2`、V2 Router 或 V2 Data Proxy。

## 配置契约

`rollout.elastic.enabled` 默认值为 `false`。该值为 `false` 时，AReaL
继续走原有静态 Rollout 生命周期：不会启动实例池、Reconciler、控制 API
或 checkpoint catalog。

启用弹性后，每个实例都是一个完整的 `TP × PP` Rollout Server。因此
`min_instances`、`initial_instances` 和 `max_instances` 的单位都是完整实例，
而不是 rank 或设备，并且必须满足：

```text
1 <= min_instances <= initial_instances <= max_instances
```

`role_prefix` 只用于命名 Scheduler 的资源作用域，不是稳定实例身份；后续
Controller 状态会另行分配不可变的 `instance_id`。

## 仅支持 disk 权重同步

elastic 模式仅支持 disk 权重同步。后续 Controller 接线时会校验训练侧为
`actor.weight_update_mode=disk`；AWEX 和 XCCL 仅继续支持既有静态路径。

为了让新启动实例在接流量前完成追平，disk checkpoint 至少需要保留两个已
提交版本（`checkpoint_retention >= 2`）。checkpoint manifest、lease 与 GC
将在后续 commit 中实现。

## Drain 与恢复契约

`drain_timeout_seconds` 定义缩容时的优雅排空上限。排空最终必须等待 task、
direct request、online session 和 weight-update lease 清零。
`recovery_schema_version` 预留 Controller 持久化状态的格式版本；本 commit
不实现状态持久化或恢复。
