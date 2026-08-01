# RolloutController V1 弹性扩缩容实施总结

## 1. 项目结论

本次开发基于 AReaL `ascend-v1.0.4` NPU 分支，在
`RolloutController` V1 上完成了单节点 Rollout 弹性扩缩容的主体功能。

当前分支（含本总结更新）相对 `upstream/ascend-v1.0.4` 共包含 34 个提交，维护在
以下个人仓分支：

```text
仓库：https://github.com/hailuoS/AReaL
分支：ascend-v1.0.4
最新功能提交：c7f90d34
```

当前实现支持：

- 通过 HTTP 设置 Rollout 目标实例数；
- Controller 根据 desired state 执行 `1 -> N -> 1` 扩缩容；
- 每个弹性实例都是完整的单节点 `TP × PP` Rollout 实例；
- 每个实例使用独立 Scheduler role、稳定 instance ID 和稳定 engine name；
- offline AgentWorkflow 为每个实例创建独立的 V1 ProxyRolloutServer；
- 新实例完成启动、健康检查及 disk 权重追平后才进入 `READY`；
- 推理任务只路由到 `READY` 实例；
- 训练 step 使用 disk 模式更新全部 `READY` 实例；
- 缩容前停止接收新任务，等待任务、直接请求和权重更新租约排空；
- StalenessManager 容量随 `READY` 实例数动态变化；
- 生成复用 AstraFlow 规则的扩缩容建议报告；
- 保存 desired state、serving version、checkpoint 和 Scheduler role 恢复状态；
- `elastic.enabled=false` 时继续走原有 V1 静态路径。

扩缩容建议当前是“报告模式”。框架不会根据建议自动修改 desired state，实际扩缩容动作由外部系统调用 HTTP 接口触发。

## 2. 实现边界

当前实现明确限制为：

- 使用 `RolloutController` V1；
- 仅支持 disk 权重同步；
- 一个完整 `TP × PP` 实例必须放在单个节点；
- Scheduler 使用 Ray；
- 主要面向 offline PPO/GRPO 训练。

当前不支持：

- `RolloutControllerV2`；
- V2 Router 和 Data Proxy；
- AWEX、XCCL 或 distributed 权重同步；
- 跨节点组成一个 `TP × PP` 弹性实例；
- Proxy online session 的动态 backend、迁移和 drain；
- 自动发现集群空闲 NPU 并主动修改 desired state；
- 弹性模式下独立的共享 validation rollout。

启用弹性模式时，训练入口会提前拒绝 V2、AWEX、online 等不受支持的组合。offline 训练可以继续执行，但独立 validation rollout 会跳过并输出 warning。

## 3. 最终架构

### 3.1 实例模型

`RolloutInstance` 表示一个完整的 `TP × PP` Rollout 实例，核心身份包括：

```text
instance_id
worker_role
worker_id
engine_name
proxy_role
proxy_worker_id
proxy_engine_name
proxy_addr
```

这些字段不依赖数组下标。缩容后创建的新实例不会复用当前数组位置作为身份。

实例状态机主要包含：

```text
CREATING
STARTING
CATCHING_UP
READY
DRAINING
STOPPING
STOPPED
FAILED
```

只有 `READY` 实例可以接收新推理请求。

### 3.2 InstancePool

`RolloutInstancePool` 负责：

- 保存实例及 desired count；
- 返回稳定的 `READY` 快照；
- 进行轮询目标选择；
- 维护 `task_id -> instance_id` 绑定；
- 维护 active task、direct request 和 weight-update lease；
- 原子完成“选择目标并占用”；
- 发起、取消和完成 drain；
- 判断实例是否已经完全排空；
- 更新实例 `loaded_version`。

所有影响缩容安全性的选择和计数都在 Pool 锁内完成，避免出现“刚选择实例，实例就被 Reconciler 删除”的窗口。

### 3.3 单实例 Launcher

`RolloutInstanceLauncher` 为每个实例创建一个独立 Scheduler role。

单实例 worker 数量为：

```text
tp_size × pp_size
```

Launcher 复用当前 NPU 分支已有的 worker 启动、server 初始化、设备映射和 HCCL 初始化路径，不修改 TP/PP 通信组大小。

启动前会检查单个完整实例能够放入一个节点。当前不会将一个实例拆到多个节点。

当训练使用 AgentWorkflow 时，Controller 会为每个实例从其独立 rollout role
fork 一个稳定的 proxy role。Proxy 初始化到该实例的 server 地址后，实例才能进入
`READY`。缩容时先删除 proxy role，再删除 rollout role。RolloutWorkflow 不会创建
这些 Proxy 资源。

### 3.4 Desired-state Reconciler

Reconciler 周期性比较：

```text
desired_instances
实际非停止实例数
```

扩容流程：

```text
记录 launch intent
  -> 创建独立 Scheduler role
  -> 启动完整 TP × PP server
  -> 健康检查
  -> AgentWorkflow 模式下创建并初始化实例专属 V1 Proxy
  -> 加载当前 serving version 对应的 disk checkpoint
  -> 设置 loaded_version
  -> READY
  -> 清除 launch intent
```

追平失败时，实例不会残留在 Pool 中。Reconciler 会销毁对应 role 并清理失败实例。

缩容流程：

```text
READY
  -> DRAINING
  -> 停止分配新请求
  -> active task/direct request/update lease 全部归零
  -> STOPPING
  -> 删除实例专属 proxy role
  -> 删除独立 rollout role
  -> 从 InstancePool 移除
```

如果 desired count 在 drain 期间重新升高，尚未停止的实例可以取消 drain，避免无意义地删除后重建。

`drain_timeout_seconds` 超时只记录错误，不会强制删除仍有在途操作的实例。

## 4. 关键运行链路

### 4.1 HTTP 扩缩容

控制接口：

```text
GET  /elastic/desired-instances
PUT  /elastic/desired-instances
GET  /elastic/instances
GET  /elastic/scaling-recommendation
POST /elastic/scaling-recommendation
```

设置目标实例数：

```json
{
  "desired_instances": 2
}
```

HTTP 请求只修改 desired state。创建和删除实例由 Controller 内部 Reconciler 异步完成。

### 4.2 推理任务路由

workflow task 的路径为：

```text
提交 rollout task
  -> BatchTaskDispatcher
  -> InstancePool.reserve_task
  -> 原子选择 READY 实例并绑定 task_id
  -> AgentWorkflow 使用同一实例的 proxy_addr
  -> scheduler.async_call_engine
  -> callback server 收到结果
  -> 完成 future
  -> 解除 task-instance 绑定
  -> active task 计数减一
```

`agenerate`、`compute_logp`、collective RPC 和 perf tracer 也会持有 direct request lease，防止执行过程中实例被缩容删除。

### 4.3 Disk 权重同步

弹性模式只允许：

```text
update_weights_from_disk
```

每次更新流程为：

```text
等待实例 catch-up 结束
  -> 标记 pending_update_version
  -> 原子获取全部 READY 实例及 update lease
  -> 全部实例加载同一个 disk checkpoint
  -> 原子登记 checkpoint catalog
  -> 更新全部实例 loaded_version
  -> Controller.set_version
  -> 校验 serving version 与 pending checkpoint 一致
  -> 向全部 READY 实例发布 serving version
  -> 保存 recovery state
  -> 清除 pending_update_version
```

新实例 catch-up 与训练权重更新互斥，避免新实例在“checkpoint 已加载但 serving version 尚未发布”的窗口进入 `READY`。

`GET /elastic/instances` 会返回：

```text
serving_version
pending_update_version
instances[].loaded_version
```

稳定状态必须满足：

```text
所有 READY 实例 loaded_version == serving_version
pending_update_version == null
```

### 4.4 Checkpoint catalog 和 GC

成功加载的 disk checkpoint 会写入持久化 catalog，供后续扩容实例追平。

GC 保留：

- 最新 `checkpoint_retention` 个版本；
- 正在被 catch-up 使用的版本；
- 正在被 update lease 使用的版本。

GC 会先发布新的保留 catalog，再删除旧目录。即使删除过程中进程退出，也只会留下未引用目录，不会留下指向已删除目录的有效 catalog 条目。

### 4.5 Controller 恢复

恢复文件位于：

```text
rollout.fileroot/
  experiment_name/
  trial_name/
  elastic_rollout_recovery_<role>.json
```

恢复状态记录：

- desired count；
- serving version；
- checkpoint version 和路径；
- 已创建及正在创建的 Scheduler roles。

Controller 重启时会校验 checkpoint，清理恢复记录中的旧 role，然后由 Reconciler 按 desired state 重建实例。

## 5. 扩缩容建议

建议逻辑复用 AstraFlow 的多 step 窗口和三段式规则。默认
`rollout.elastic.report_freq_steps=10`，在 serving version 为
`10、20、30...` 时关闭当前窗口并生成报告。窗口生成后重置精确计数，下一窗口重新
累计。

设：

```text
wait_fraction = sum(prepare_batch_wait_seconds) / sum(step_seconds)
```

规则：

```text
wait_fraction > 0.10:
    scale_up = ceil(ready_instances / (1 - wait_fraction))

wait_fraction < 0.05 且 entered > 0 且 consumed > 0:
    scale_down = min(
        ready_instances,
        ceil(ready_instances × sum(consumed) / sum(entered) × 1.10),
    )

其他情况:
    hold
```

最终建议限制在 `min_instances` 和 `max_instances` 之间。

采集信息包括：

- `prepare_batch` 等待时间；
- step 时间；
- 进入 rollout buffer 的数量；
- 训练消费的样本数量；
- 当前 `READY` 实例数。

和 AstraFlow 一致，启动后或 eval 后第一个无法与上一次 batch 完成时间配对的样本
不会进入 wait/step 时间求和。每 step 的 `entered` 和 `consumed` 仍会进入窗口累计。

最新报告保存在 Controller 内存中供 HTTP 查询，同时原子写入：

```text
${rollout.fileroot}/${experiment_name}/${trial_name}/balance_reports/
  rollout_balance_report_v10.json
  rollout_balance_report_v20.json
```

报告包含唯一的 `report_version`、窗口起止 version、窗口 step 数、有效 timing 样本
数、累计 wait/step 时间、累计 entered/consumed 以及最终建议。

建议只形成报告。外部控制系统可以读取报告后，再调用
`PUT /elastic/desired-instances`。

## 6. 主要文件

| 模块 | 文件 |
| --- | --- |
| 弹性配置 | `areal/api/cli_args.py` |
| 实例状态模型 | `areal/infra/controller/elastic/models.py` |
| InstancePool | `areal/infra/controller/elastic/instance_pool.py` |
| 单实例 Launcher | `areal/infra/controller/elastic/launcher.py` |
| Desired-state Reconciler | `areal/infra/controller/elastic/reconciler.py` |
| Disk checkpoint catalog | `areal/infra/controller/elastic/disk_catalog.py` |
| 恢复状态 | `areal/infra/controller/elastic/recovery_state.py` |
| 扩缩容建议 | `areal/infra/controller/elastic/scaling_report.py` |
| V1 Controller 集成 | `areal/infra/controller/rollout_controller.py` |
| V1 Proxy Server | `areal/experimental/openai/proxy/proxy_rollout_server.py`（复用） |
| 动态并发容量 | `areal/infra/staleness_manager.py` |
| 训练入口约束和指标接线 | `areal/trainer/rl_trainer.py` |
| 单 role 隔离穿刺 | `examples/math/rollout_role_spike.py` |
| HTTP 扩缩容穿刺 | `examples/math/rollout_elastic_controller_spike.py` |
| 外部 autoscaler 闭环穿刺 | `examples/math/rollout_elastic_autoscaler_spike.py` |

## 7. 实施提交

### 阶段一：实例隔离和静态结构

```text
fff51d7b feat(examples): add rollout role isolation spike
3711ba73 docs(config): define disk-only v1 rollout elasticity contracts
b3561aaf feat(infra): add elastic rollout instance pool
dddfdf1e refactor(rollout): snapshot static v1 RPC targets
72301ec9 feat(infra): add single-node rollout instance launcher
82416478 refactor(examples): exercise rollout instance launcher
```

### 阶段二：Disk 权重和 desired state

```text
d05e0b6c feat(infra): add durable disk checkpoint catalog
f269fb26 feat(infra): retain elastic disk checkpoints
accf83c7 feat(infra): expose elastic desired instance state
c191e1d5 feat(infra): catch up rollout instances from disk
```

### 阶段三：扩缩容编排和路由

```text
2890be38 feat(infra): reconcile single-node rollout instances
54be687a feat(infra): route work to ready elastic instances
01e21bb4 feat(infra): reconcile elastic rollout capacity in controller
00f29750 feat(infra): lease instances during disk updates
f6fce1b9 feat(infra): scale staleness capacity with ready instances
```

### 阶段四：HTTP、报告、恢复和测试

```text
5174cf03 feat(infra): expose elastic instance status
e1ed60fb feat(examples): add elastic controller HTTP spike
0508df21 feat(infra): report AstraFlow elastic scaling recommendations
ed64eb1c feat(infra): retain protected elastic checkpoints
a600165c feat(infra): recover elastic rollout desired state
8c01c0d9 test: extend elastic rollout validation coverage
```

### 阶段五：整体审查后的正确性修复

```text
a3e8890c fix(trainer): enforce elastic rollout runtime contract
af373d11 fix(infra): make elastic instance leases atomic
98394e17 fix(infra): harden elastic instance reconciliation
31cfa2bd fix(infra): lease elastic rollout request targets
1407bd98 fix(infra): serialize elastic disk weight versions
74a6ccdd fix(infra): make elastic recovery and checkpoint GC safer
602c1fb6 test(examples): verify elastic serving version convergence
bbc408d9 docs: document elastic V1 runtime boundaries
```

### 阶段六：外部闭环和 AgentWorkflow V1 Proxy

```text
2feae8cc feat(examples): add elastic autoscaler closed-loop spike
c4d80a1e feat(infra): attach V1 proxies to elastic instances
59ea006a feat(infra): route AgentWorkflow through elastic V1 proxies
c7f90d34 test(examples): verify elastic V1 proxy lifecycle
```

这一阶段增加报告到 desired state 的外部闭环，并将实例专属 V1 Proxy 纳入
AgentWorkflow task 绑定、扩容 READY、失败回滚、缩容删除和 Controller 恢复。

## 8. 已完成验证

开发期间已经在 NPU 单节点环境验证：

- 两个独立 Scheduler role 能分别启动完整 vLLM Rollout 实例；
- 删除一个 role 不影响另一个 role；
- 日志出现：

```text
Isolation verified: deleted ... without disrupting [...]
```

- HTTP 穿刺能够观察到 `1 -> 2 -> 1`；
- 扩容时启动两个独立 vLLM 实例；
- 缩容后保留实例继续正常运行；
- 当前穿刺运行未报错。

新增的 AgentWorkflow V1 Proxy 生命周期已完成本地静态检查，但尚待公司 NPU
环境执行 `--verify-proxy` 穿刺和真实 `MathAgent` 端到端训练验证。

本地完成的静态检查包括：

- Ruff lint；
- Ruff format check；
- Python `py_compile`；
- `git diff --check`。

按开发约定，本地笔记本没有运行依赖完整 AReaL、Ray、vLLM 和 NPU 环境的测试。

## 9. 公司 NPU 环境验证清单

### 9.1 同步代码

```bash
git switch ascend-v1.0.4
git pull --ff-only origin ascend-v1.0.4
```

同步后确认最近提交中包含：

```text
c7f90d34
```

### 9.2 单 role 隔离穿刺

运行：

```bash
AREAL_SPMD_MODE=false python examples/math/rollout_role_spike.py \
  --config examples/math/gsm8k_grpo_npu.yaml scheduler.type=ray
```

预期：

- 两个 role 均健康；
- 删除第二个 role 后第一个仍健康；
- 输出 `Isolation verified`；
- 最终清理两个 role。

进程退出前出现先 `SIGTERM`、随后强制清理少量未退出子进程的日志可以接受，关键是 Scheduler 确认 role 删除成功，且另一个实例未受影响。

### 9.3 HTTP 扩缩容穿刺

运行：

```bash
AREAL_SPMD_MODE=false python \
  examples/math/rollout_elastic_controller_spike.py \
  --verify-recommendation --verify-recovery --verify-proxy -- \
  --config examples/math/gsm8k_grpo_npu.yaml \
  scheduler.type=ray
```

如果使用 `--verify-recovery`，必须配置可写的 `rollout.fileroot`。

预期：

- 初始 1 个 `READY` 实例；
- HTTP 设置 desired 为 2 后出现 2 个 `READY` 实例；
- 两个实例具有不同 instance ID、worker role 和 engine name；
- 每个实例具有独立 proxy role，且 `proxy_ready=true`；
- HTTP 设置 desired 为 1 后完成 drain 并删除一个 role；
- 原实例保持健康；
- scale-up、hold 和 scale-down 建议符合预期；
- Controller 重启后恢复 desired count；
- 输出 `Elastic Controller HTTP 1->2->1 spike passed`。

### 9.4 报告驱动的自动扩缩容闭环

先保持 Controller 或端到端训练进程运行，从日志中取得 callback server 地址。

确定性验证“注入指标、生成报告、读取报告、设置 desired state、等待收敛”的完整
`1 -> 2 -> 1` 流程：

```bash
python examples/math/rollout_elastic_autoscaler_spike.py \
  --base-url http://127.0.0.1:PORT \
  --inject-spike \
  --require-proxy
```

脚本要求初始状态为 1 个稳定的 `READY` 实例。它会：

1. 注入 `wait_fraction=0.20` 的模拟窗口；
2. 读取生成的 `scale_up` 报告；
3. 将报告中的 `recommended_instances=2` 写入 desired state；
4. 等待 2 个实例全部 `READY` 且权重版本一致；
5. 注入低等待、低消费比的模拟窗口；
6. 读取生成的 `scale_down` 报告；
7. 将 `recommended_instances=1` 写入 desired state；
8. 等待 drain 和缩容完成。

预期最终输出：

```text
Closed-loop report -> desired state 1 -> 2 -> 1 spike passed
```

只观察训练真实生成的报告，不发送扩缩容请求：

```bash
python examples/math/rollout_elastic_autoscaler_spike.py \
  --base-url http://127.0.0.1:PORT \
  --dry-run \
  --require-proxy
```

持续读取训练真实报告并执行建议：

```bash
python examples/math/rollout_elastic_autoscaler_spike.py \
  --base-url http://127.0.0.1:PORT \
  --poll-interval 5 \
  --cooldown 30 \
  --require-proxy
```

真实报告模式持续运行，使用 `Ctrl-C` 停止。脚本按照 `report_version` 去重，只处理
新的完整窗口报告；仅在建议目标与当前 desired state 不同时发送 PUT，并在每次动作
后验证实例数量、状态和权重版本是否收敛。

### 9.5 端到端训练

`gsm8k_grpo_npu.yaml` 尚未显式声明 `rollout.elastic`，因此命令行新增字段需要使用
Hydra 的 `+` 语法。使用新的 experiment/trial 名称启动：

```bash
AREAL_SPMD_MODE=false python examples/math/gsm8k_rl.py \
  --config examples/math/gsm8k_grpo_npu.yaml \
  scheduler.type=ray \
  actor.weight_update_mode=disk \
  +rollout.elastic.enabled=true \
  +rollout.elastic.min_instances=1 \
  +rollout.elastic.initial_instances=1 \
  +rollout.elastic.max_instances=2 \
  +rollout.elastic.reconcile_interval_seconds=2 \
  +rollout.elastic.role_prefix=rollout-elastic-e2e \
  experiment_name=gsm8k-elastic-e2e \
  trial_name=trial-e2e-01
```

该示例继续使用 `areal.workflow.openai.math_agent.MathAgent`。Trainer 会在训练开始前
调用 `start_proxy()`，为当前和后续扩容实例创建独立 V1 Proxy。

验证步骤：

1. 启动正常端到端训练；
2. 等待至少完成一个训练 step 和一次 disk 权重更新；
3. 查询 Controller 日志中的 callback 地址；
4. 调用 HTTP 将 desired 从 1 调整为 2；
5. 等待新实例完成 V1 Proxy 初始化和 disk 追平后进入 `READY`；
6. 确认训练继续推进；
7. 将 desired 从 2 调整回 1；
8. 确认被删除实例先进入 `DRAINING`；
9. 确认在途请求完成后 role 被删除；
10. 确认剩余实例和训练均继续正常运行。

HTTP 示例：

```bash
curl "${BASE_URL}/elastic/instances"

curl -X PUT "${BASE_URL}/elastic/desired-instances" \
  -H "Content-Type: application/json" \
  -d '{"desired_instances":2}'

curl -X PUT "${BASE_URL}/elastic/desired-instances" \
  -H "Content-Type: application/json" \
  -d '{"desired_instances":1}'
```

每次权重更新后的稳定状态应满足：

```text
pending_update_version == null
每个 READY 实例的 loaded_version == serving_version
proxy_enabled == true
每个 READY 实例的 proxy_ready == true
last_reconcile_error == null
```

### 9.6 静态路径回归

使用相同训练配置设置：

```text
rollout.elastic.enabled=false
```

确认：

- 使用原有静态 `Job(replicas=dp_size)`；
- 原 worker 命名和初始化逻辑不变；
- 原 disk 权重更新和 checkpoint 删除行为不变；
- 训练和 validation 行为与修改前一致。

## 10. 验收重点

端到端验证时优先关注：

- 新实例在 checkpoint 追平前不能进入 `READY`；
- 扩容期间训练 step 的 disk 更新不能与新实例追平产生版本交叉；
- 缩容后不能再向 `DRAINING` 实例分配新任务；
- active task、direct request 或 update lease 非零时不能删除实例；
- 所有 `READY` 实例最终使用同一个 serving version；
- 单个 role 删除不能影响其他实例；
- AgentWorkflow task 使用的 proxy_addr 必须属于同一个已绑定 instance；
- 缩容必须先排空 task，再删除 proxy role，最后删除 rollout role；
- Controller 重启不能读取其他 experiment/trial 的恢复状态；
- checkpoint GC 不能删除正在使用的版本；
- `elastic.enabled=false` 不发生功能回归。

## 11. 当前风险和后续工作

### 当前风险

- 完整训练入口、权重 callback 和真实 NPU disk load 必须在公司环境验证；
- Ray 删除 worker 时部分 vLLM 子进程可能需要强制清理；
- 单实例容量校验基于单节点约束，不覆盖跨节点放置；
- drain 超时不会强删实例，持续不归零时需要通过状态接口和日志排查；
- 扩缩容建议使用当前可获得指标近似 AstraFlow 语义，需要结合真实训练负载校准。

### 后续工作

当前主体开发完成后，可按需要继续：

1. 完成公司 NPU 端到端验收和故障注入；
2. 补充真实 Ray/NPU 集成测试；
3. 根据真实 workload 校准建议阈值；
4. 增加外部 autoscaler，由其读取建议并设置 desired state；
5. 单独设计 Proxy online backend registry、session affinity 和 session drain；
6. 如未来需要，再设计跨节点完整实例的整体调度和删除协议。

Proxy online 和跨节点实例不应直接叠加到当前实现中，应作为独立设计和验证阶段推进。
