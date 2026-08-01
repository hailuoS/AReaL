# Rollout 弹性扩缩容 V1 压缩提交审查指南

本文档用于审查 `ascend-v1.0.4-elastic-squashed` 分支。该分支将原开发分支上的
36 个提交压缩为 5 个按依赖顺序排列的功能提交；压缩完成后，5 个功能提交对应的
代码树与原分支 `58c1aad4` 完全一致。

## 审查范围和边界

- 基线：`65d0bcd8`（`upstream/ascend-v1.0.4`）
- 实现：`RolloutController` V1
- 权重同步：仅 disk 模式
- 弹性实例：单节点完整 `TP x PP` 实例
- AgentWorkflow：使用 V1 Proxy，不依赖 V2 Router 或 Data Proxy
- 不包含：跨节点单实例、AWEX 权重同步、中央调度器注册和资源发现
- `elastic.enabled=false` 时保持原静态路径

整体调用关系如下：

```text
HTTP desired replicas / autoscaler report
                    |
                    v
          desired-state reconciler
                    |
          +---------+----------+
          |                    |
       scale up             scale down
          |                    |
 launcher -> catch-up      drain -> leases clear
          |                    |
 READY instance routing    proxy/worker teardown
          |
 disk update broadcasts to every READY instance
```

## Commit 1：实例模型与单实例启动基础

```text
d41f31e7 feat(elastic): establish V1 rollout instance foundation
```

主要功能：

- 定义 disk-only 弹性配置契约以及实例状态模型。
- 使用稳定 `instance_id`，不把数组下标当作实例身份。
- 增加 `RolloutInstancePool`，集中维护实例、worker、server 和生命周期状态。
- 将 V1 Controller 的静态 collective RPC 目标改为显式 snapshot。
- 增加单节点完整 `TP x PP` 实例 Launcher；每个实例使用独立 Scheduler role。
- 增加 role 创建、健康检查、独立删除的穿刺脚本。

重点文件：

- `areal/api/cli_args.py`
- `areal/infra/controller/elastic/models.py`
- `areal/infra/controller/elastic/instance_pool.py`
- `areal/infra/controller/elastic/launcher.py`
- `areal/infra/controller/rollout_controller.py`
- `examples/math/rollout_role_spike.py`

审查重点：

- `instance_id`、Scheduler role 和 engine name 是否始终稳定且一一对应。
- 一个实例的 worker 数量是否严格等于完整的 `TP x PP` world size。
- `elastic.enabled=false` 时是否仍使用原静态初始化及 RPC 行为。
- 删除独立 role 是否不会影响其他实例。

原始提交范围：`fff51d7b` 到 `82416478`，共 6 个提交。

## Commit 2：desired-state 生命周期和 disk 追平

```text
3f5a5126 feat(elastic): implement desired-state rollout lifecycle
```

主要功能：

- 建立持久化 disk checkpoint catalog 和基础 retention 机制。
- 增加外部可设置的 `desired_replicas` 状态。
- 增加 desired-state Reconciler，执行单节点实例扩容和缩容。
- 新实例启动后加载目标 disk checkpoint，追平后才转换为 `READY`。
- 推理请求只选择 `READY` 实例。
- disk 权重更新期间为实例加 lease，避免缩容与权重加载并发。
- 按 READY 实例数动态调整 `StalenessManager` capacity。
- 提供实例状态 HTTP 查询及 Controller 端到端穿刺脚本。

重点文件：

- `areal/infra/controller/elastic/disk_catalog.py`
- `areal/infra/controller/elastic/reconciler.py`
- `areal/infra/controller/rollout_controller.py`
- `areal/infra/staleness_manager.py`
- `examples/math/rollout_elastic_controller_spike.py`

审查重点：

- 扩容是否遵守 `STARTING -> CATCHING_UP -> READY` 状态顺序。
- 新实例在 serving version 追平前是否绝不会接收请求。
- 缩容是否先停止选取目标实例，再等待 lease 排空。
- disk 更新 snapshot 是否覆盖当时所有 READY 实例。
- desired state 与实际实例数不一致时 Reconciler 是否可以重试收敛。

原始提交范围：`d05e0b6c` 到 `e1ed60fb`，共 11 个提交。

## Commit 3：并发安全、恢复与 checkpoint 一致性

```text
c85bcb42 feat(elastic): harden reconciliation and disk consistency
```

主要功能：

- 增加基于 AstraFlow 指标语义的扩缩容建议报告基础实现。
- 增加受保护 checkpoint lease、retention 和安全 GC。
- 持久化并恢复 desired replicas 和实例序号等 Controller 状态。
- 将实例 lease 获取改为原子操作，消除路由与 drain 竞争窗口。
- 推理请求在 callback 完成前持续持有实例所有权。
- 串行化 disk 权重版本发布，避免不同训练 step 的版本交错。
- 加固 Reconciler 失败回滚、部分启动失败和恢复逻辑。
- 增加 serving version 收敛及故障场景测试。

重点文件：

- `areal/infra/controller/elastic/recovery_state.py`
- `areal/infra/controller/elastic/scaling_report.py`
- `areal/infra/controller/elastic/instance_pool.py`
- `areal/infra/controller/elastic/disk_catalog.py`
- `areal/infra/controller/elastic/reconciler.py`
- `areal/infra/controller/rollout_controller.py`
- `areal/trainer/rl_trainer.py`

审查重点：

- request、result callback、disk update 三类 lease 的开始和释放时机。
- drain 与新请求竞争时是否可能再选中正在缩容的实例。
- checkpoint 是否只有在 catalog、catch-up 和 update lease 均不再引用后才可 GC。
- Controller 重启后是否会把过期运行时对象错误地恢复为 READY。
- disk serving version 是否单调，并且不会出现新实例追到旧版本即开放流量。

原始提交范围：`0508df21` 到 `602c1fb6`，共 11 个提交。

## Commit 4：报告消费和闭环 autoscaler

```text
a00ca3f1 feat(elastic): add closed-loop autoscaler workflow
```

主要功能：

- 补充 V1 弹性运行边界、配置和验证说明。
- 增加 autoscaler 穿刺脚本，读取扩缩容建议报告。
- 使用 `report_version` 去重，避免重复执行同一建议。
- 调用 Controller HTTP API 设置 desired replicas，形成外部闭环。
- 支持注入负载 spike，验证 `1 -> 2 -> 1` 扩缩容流程。

重点文件：

- `examples/math/rollout_elastic_autoscaler_spike.py`
- `docs/zh/reference/rollout_elasticity_v1_implementation_summary.md`
- `docs/zh/reference/rollout_elasticity_v1.md`

审查重点：

- autoscaler 是否只消费完整且版本更新的报告。
- desired replicas 是否受配置的最小值和最大值约束。
- HTTP 超时是否只影响本次请求，不会重复消费报告版本。
- autoscaler 是否保持外部决策器定位，不直接操作 Scheduler worker。

原始提交：`bbc408d9` 和 `2feae8cc`。

## Commit 5：AgentWorkflow V1 Proxy 和最终集成修复

```text
f95f941d feat(elastic): integrate AgentWorkflow with V1 proxies
```

主要功能：

- 为每个弹性 Rollout 实例创建并维护独立 V1 Proxy。
- AgentWorkflow 只路由到 READY 实例对应的 Proxy backend。
- 缩容时将 Proxy drain、在线 session 和实例 teardown 纳入同一生命周期。
- 保留 rollout result owner，直到训练侧真正取得远程结果，防止数据服务提前退出。
- 将报告改为按多个训练 step 聚合等待时间和 step 时间。
- 对齐 AstraFlow 的低等待缩容约束和 report version 窗口行为。

重点文件：

- `areal/infra/controller/rollout_controller.py`
- `areal/infra/controller/elastic/launcher.py`
- `areal/infra/controller/elastic/models.py`
- `areal/infra/controller/elastic/instance_pool.py`
- `areal/trainer/rl_trainer.py`
- `areal/infra/controller/elastic/scaling_report.py`

审查重点：

- Proxy 是否与稳定 `instance_id` 绑定，而不是与列表下标绑定。
- session 建立后是否保持实例亲和性，drain 后是否拒绝建立新 session。
- 缩容是否同时等待 active task、active session、result owner 和 disk update。
- Proxy 启动失败是否会阻止对应实例进入 READY。
- 非 AgentWorkflow 和 `elastic.enabled=false` 路径是否不创建弹性 Proxy。
- 多 step 报告是否在达到 `report_freq_steps` 后才生成并正确清空窗口。

原始提交范围：`c4d80a1e` 到 `58c1aad4`，共 6 个提交。

## 建议审查顺序

依次执行：

```bash
git show --stat d41f31e7
git show --stat 3f5a5126
git show --stat c85bcb42
git show --stat a00ca3f1
git show --stat f95f941d
```

查看某个模块的完整修改：

```bash
git diff <commit>^ <commit>
```

查看全部功能代码相对 NPU 基线的最终差异：

```bash
git diff 65d0bcd8 f95f941d
```

如果只想优先检查高风险代码，建议按以下顺序阅读：

1. `instance_pool.py` 的原子 lease 和 drain。
2. `reconciler.py` 的扩容、缩容以及失败收敛。
3. `rollout_controller.py` 的请求路由、callback 和 disk update snapshot。
4. `disk_catalog.py` 的 checkpoint 引用与 GC。
5. `rl_trainer.py` 的 Proxy 启动时机和 result owner 生命周期。
6. `scaling_report.py` 的窗口聚合和建议计算。

