# AReaL Rollout 弹性扩缩容与平台集成设计

## 1. 文档目的

本文用于 AReaL 团队与云平台、Kubernetes 和资源调度团队讨论 Rollout 弹性扩缩容 的集成方式，重点说明：

- AReaL 当前具备什么能力；
- 框架与平台如何分工；
- 一次扩容和缩容如何完成；
- 双方需要对接哪些接口和状态；
- 第一阶段应该优先落地什么。

本文是一份架构沟通稿，不展开框架内部类、配置项和代码实现。

## 2. 核心设计

AReaL 将一个可以独立执行 Rollout 推理的服务单元称为 Rollout 实例，并根据训练侧 Rollout 的生产和消费情况动态调整实例数量。

平台提供计算资源，不直接决定一个新实例何时能够接收请求；AReaL 负责启动推理服务、 追赶模型版本并管理流量。只有这些步骤全部完成，新实例才会成为有效 Rollout 容量。

```text
资源已分配 != 推理服务已就绪 != 模型版本已就绪 != 有效 Rollout 容量
```

这套方案的重点不是简单增加或删除 Pod，而是在训练持续更新模型的过程中，确保新实例 以正确的模型版本加入服务，缩容实例在安全排空后退出服务。

## 3. 总体架构

```mermaid
flowchart LR
    subgraph AReaL[AReaL 框架]
        Trainer[训练过程]
        Metrics[供需指标与扩缩容建议]
        Policy[框架实例 Autoscaler]
        Controller[弹性控制与实例状态]
        Routing[请求路由与安全排空]
        Version[模型版本与 Checkpoint]
    end

    subgraph Platform[云平台 / Kubernetes]
        Autoscaler[Ray / KubeRay Autoscaler]
        Scheduler[Ray 调度层]
        Workload[Worker Pod / 推理工作负载]
        Resource[NPU/GPU 节点与存储]
    end

    Trainer --> Metrics
    Metrics --> Policy
    Policy -->|设置目标实例数| Controller
    Controller -->|创建/删除 Placement Group| Scheduler
    Scheduler -->|Pending Resource Demand| Autoscaler
    Autoscaler -->|扩缩 Worker Pod / 节点| Workload
    Scheduler --> Workload
    Workload --> Resource
    Controller --> Version
    Controller --> Routing
    Version --> Workload
    Workload -->|资源与运行状态| Controller
    Controller -->|收敛状态| Policy
```

架构中存在两个相互配合的控制闭环：

1. **AReaL 实例闭环**：根据训练指标判断需要多少 Rollout 实例，并管理启动、版本追赶、路由和排空。
1. **Ray/KubeRay 资源闭环**：根据 pending placement group 的资源需求扩缩 Worker Pod 和 NPU/GPU 节点。

当前运行方式下，AReaL 不调用 Kubernetes 扩容 API。框架批量创建 Ray placement group：集群有空闲资源时直接调度；资源不足时，Ray
autoscaler 从 pending resource demand 中计算缺口，KubeRay 扩充 Worker Pod，必要时由底层节点 autoscaler
继续增加 NPU/GPU 节点。

## 4. 框架内部已经实现的能力

AReaL 当前已经形成完整的 Rollout 实例控制闭环：

1. **供需感知**：采集训练等待时间、Rollout 生产量和消费量，按训练窗口生成推荐实例数。
1. **目标状态管理**：接收目标实例数，由后台控制循环持续比较目标容量和实际容量。
1. **实例生命周期**：按需创建 Ray Worker，启动推理引擎和服务，并管理启动、就绪、排空、停止和失败状态。
1. **请求路由**：只向 READY 实例分配新任务，并记录每个实例正在处理的任务和返回结果。
1. **模型版本门控**：新实例加载当前 checkpoint、确认模型版本后才进入 READY；实例追赶与训练权重更新相互协调。
1. **安全缩容**：缩容实例先停止接收新任务，已有任务和结果全部释放后才删除 Worker。
1. **容量发布**：根据 READY 实例数量刷新框架实际可用的 Rollout 并发容量。
1. **持久化恢复**：保存目标容量、模型版本和 checkpoint；Controller 重启后清理旧资源并重新收敛。
1. **可观测性**：输出资源创建、Worker、推理服务、Proxy、模型追赶和容量生效等阶段的耗时及错误。

部分 Agent 场景还会使用 AReaL 内部的实例专属 Proxy，将 OpenAI 风格请求转发到对应推理服务。这里的 Proxy 是框架组件，不是 Kubernetes
Ingress 或平台 API Gateway。

## 5. 框架向外部暴露的能力

### 5.1 HTTP 控制与状态接口

| 接口                                  | 方向         | 用途                                       |
| ------------------------------------- | ------------ | ------------------------------------------ |
| `GET /elastic/scaling-recommendation` | AReaL → 平台 | 观测训练侧推荐实例数和指标依据             |
| `PUT /elastic/desired-instances`      | 平台 → AReaL | 人工覆盖、调试或关闭内部策略时设置目标数量 |
| `GET /elastic/desired-instances`      | AReaL → 平台 | 查询当前目标实例数                         |
| `GET /elastic/instances`              | AReaL → 平台 | 查询 READY 数量、模型版本、容量和错误      |

启用 `auto_apply_scaling_recommendations` 后，正常运行不依赖平台调用 `PUT`；该接口仍保留 作为兼容和运维入口。`PUT`
是异步目标状态接口：响应成功表示目标已被接受，不表示资源已 创建或容量已生效。调用方需要继续查询状态，直到容量和模型版本收敛。

### 5.2 资源调度接口

AReaL 当前通过 Scheduler 抽象申请和释放资源。在现有 Ray 部署中，它负责：

- 一次性提交当前全部实例缺口对应的 Ray placement group，使 Ray autoscaler 能看到完整资源需求；
- 等待 Worker 注册并获得网络地址；
- 在 Worker 中启动推理引擎、vLLM 服务和可选框架 Proxy；
- 缩容完成后删除对应 Worker并释放 Ray 资源。

平台继续使用 Ray/KubeRay 作为资源接入层。AReaL 当前不直接创建 Kubernetes Pod 或 节点，也不重复实现节点级 autoscaler。

### 5.3 共享数据与观测输出

AReaL 还会向平台环境读写或输出：

- 共享存储中的模型 checkpoint、版本目录和恢复状态；
- 目标容量、READY 容量、模型版本和实际有效并发等运行状态；
- 带扩容批次、实例标识、阶段耗时和错误原因的日志。

## 6. 平台职责与共同约定

### 6.1 平台负责

- 在作业启动前创建启用 autoscaling 的 RayCluster，并配置 WorkerGroup 最小、最大副本数；
- 配置 GPU/NPU 自定义资源名、Worker Pod 规格和 placement group 可满足的节点形态；
- 让 Ray autoscaler 根据 pending resource demand 扩缩 Ray Worker Pod；
- 管理 NPU/GPU 配额、节点供给、设备分配和资源回收；
- 保证 Controller、Worker 和推理服务之间的网络连通；
- 提供所有相关实例可访问的 checkpoint 共享存储；
- 提供镜像、驱动、CANN/CUDA 和推理运行环境；
- 提供 Ray autoscaler、Pod 调度、节点扩容失败和资源上限的日志与监控；
- 提供生产环境所需的鉴权、TLS、审计、告警、高可用和失败重试；
- 清理 Controller 异常退出后可能残留的孤儿资源。

### 6.2 双方共同定义

- 一个 Rollout 实例对应的 Ray bundle、Worker Pod 和节点资源规格；
- 实例、Ray Worker、Pod 和节点之间的统一标识；
- placement group 申请、排队、就绪、失败和删除的状态协议；
- 扩容超时、重试、退避和熔断策略；
- 模型版本就绪与平台 readiness 状态如何映射；
- Controller 重启或平台资源残留时，以哪一侧状态为准。

## 7. 扩容流程

```mermaid
sequenceDiagram
    autonumber
    participant T as 训练过程
    participant A as AReaL 实例 Autoscaler
    participant C as AReaL 弹性控制器
    participant R as Ray Autoscaler / KubeRay
    participant I as 新推理实例

    T->>A: 上报训练等待与 Rollout 供需
    A->>C: 设置更大的目标实例数
    C->>R: 批量创建缺口对应的 Placement Group
    alt Ray 集群有空闲资源
        R->>I: 直接分配 Worker 资源
    else Ray 集群资源不足
        R->>R: 扩 Ray Worker Pod / NPU 节点
        R->>I: 节点就绪后分配 Worker 资源
    end
    C->>I: 加载当前指定模型版本
    I-->>C: 服务和模型版本就绪
    C->>C: 实例进入 READY 并加入请求路由
    C-->>A: 记录实例数、版本和容量已收敛
```

扩容过程需要注意：

- desired state 变更是异步操作，不代表新容量已经生效；
- 同一轮全部 placement group 先提交，再等待资源，避免 Ray autoscaler 逐个发现节点缺口；
- Pod 或 Worker 启动只代表资源和进程可用，不能直接视为模型就绪；
- 新实例加载权重期间不接收 Rollout 请求；
- 同一批新实例使用一致的目标模型版本；
- 模型版本和服务都确认后，实例才进入 READY 并计入有效容量。

## 8. 缩容流程

```mermaid
sequenceDiagram
    autonumber
    participant A as AReaL 实例 Autoscaler
    participant C as AReaL 弹性控制器
    participant R as 请求路由
    participant I as 待缩容实例
    participant P as Ray Autoscaler / KubeRay

    A->>C: 设置更小的目标实例数
    C->>R: 停止向目标实例分配新请求
    C->>I: 进入 DRAINING
    Note over I: 已接收任务继续执行，不做迁移
    I-->>C: 在途任务与结果全部释放
    C->>P: 删除 Worker 并释放 Placement Group
    P->>P: 空闲超时后缩 Worker Pod / 节点
    C-->>A: 缩容完成
```

`DRAINING` 是缩容中的安全排空阶段：实例仍然运行并占用设备，但不再接收新任务； 已有任务和结果使用结束后，AReaL 才通知平台释放资源。

因此 Ray/KubeRay 只根据 placement group 释放后的空闲资源缩 Pod/节点，不应根据框架的 缩容建议直接删除仍承载实例的
Pod，否则可能中断正在执行的 Rollout 请求。

## 9. 模型版本一致性

在线强化学习过程中，训练模型持续更新。扩容可能与一次新的权重更新同时发生，因此新 实例不能只加载任意一个可用 checkpoint 后就开始服务。

AReaL 当前通过以下规则保证一致性：

```mermaid
flowchart LR
    Start[新实例启动] --> Pin[确定目标模型版本]
    Pin --> Load[加载对应 Checkpoint]
    Load --> Verify[确认推理服务模型版本]
    Verify --> Ready[进入 READY]
    Ready --> Route[接收新的 Rollout 请求]
```

- 训练权重更新和新实例版本追赶会进行协调，避免交叉覆盖；
- 新实例在版本追赶完成前不可路由；
- 稳定状态下，所有 READY 实例使用相同 serving version；
- 正在被加载的 checkpoint 不能被提前清理。

平台需要保证 checkpoint 共享存储在实例之间路径一致、持续可见，并且平台侧存储清理 策略不会删除仍被 AReaL 使用的版本。

## 10. 框架与平台联动方式

推荐的基本联动方式是：

```text
AReaL 读取训练指标并执行实例级冷却和安全策略
  → AReaL 更新目标实例数
  → AReaL 批量创建或安全释放 Ray Placement Group
  → Ray Autoscaler 根据资源需求扩缩 Worker Pod
  → 底层节点 Autoscaler 按需扩缩 NPU/GPU 节点
  → AReaL 管理服务启动、版本追赶、路由和排空直到收敛
```

HTTP 控制方式继续保留，可用于第一阶段穿刺、人工审批环境和故障处置，但不是正常自动 扩缩容链路的必需组件。这样可以复用 Ray 已有的资源
autoscaler，同时避免平台重复实现 AReaL 的指标解释、模型版本门控和实例生命周期状态机。

生产集成时，建议以以下条件判断扩容完成：

- READY 实例数达到目标实例数；
- READY 实例的模型版本与当前服务版本一致；
- 需要框架 Proxy 的场景中，Proxy 已经就绪；
- 当前没有正在进行的权重版本切换；
- 没有未处理的扩缩容错误。

生产环境如果对外暴露 AReaL HTTP 运维接口，需要由平台补充服务发现、鉴权、TLS、审计 和人工覆盖权限控制。后续如果需要 Kubernetes
原生管理方式，可以再封装成 CRD 和 Operator，不建议第一阶段直接从 CRD 开始。

### 10.1 平台资源联动

当前资源链路可以理解为：

```mermaid
flowchart LR
    Desired[AReaL 目标实例数增加]
    RayReq[Ray 中出现新的资源请求]
    Worker[扩充 Ray Worker Pod]
    Node[扩充 NPU/GPU 节点]
    Register[Worker 注册进 Ray 集群]
    Launch[AReaL 启动推理实例]
    Ready[模型追赶完成并 READY]

    Desired --> RayReq
    RayReq --> Worker
    Worker -->|没有可调度设备时| Node
    Node --> Register
    Worker --> Register
    Register --> Launch
    Launch --> Ready
```

只扩 Ray Worker Pod 不会自动增加 AReaL 的目标实例数；只增加目标实例数而没有平台 资源时，placement group 会保持 pending，并成为
Ray autoscaler 的扩容依据。两个闭环 通过 Ray resource demand 解耦，不需要平台同时调用 AReaL HTTP 接口并操作 Worker
Pod。

不建议让 Kubernetes HPA 直接修改 Rollout Pod 副本数。HPA 只理解 Pod 数量，无法 处理 AReaL 的模型版本追赶、READY 路由和
DRAINING 排空语义。第一阶段应由 AReaL 管理实例，由 Ray/KubeRay autoscaler 管理 Pod 和节点资源。

## 11. 当前性能现状

在一次 4B 模型、Ascend、单实例扩容测试中，主要阶段耗时约为：

| 阶段                 |    示例耗时 |
| -------------------- | ----------: |
| 资源与 Worker 创建   |    约 55 秒 |
| vLLM 推理服务启动    |   约 105 秒 |
| AReaL 实例专属 Proxy |    约 28 秒 |
| 模型版本追赶         | 约 1～10 秒 |
| 单实例总体扩容       |   约 191 秒 |

当前瓶颈主要是推理服务和运行环境冷启动，而不是增量模型版本追赶。平台接入后应进一步 拆分并观测：

- 资源排队和配额准入；
- Worker Pod 调度和底层节点扩容；
- 镜像、运行环境和设备初始化；
- 推理服务启动；
- checkpoint 加载；
- READY 到有效容量生效。

如果缩容目标是将设备真正归还给其他任务，就需要删除 Worker Pod，并接受下一次完整 冷启动。vLLM sleep 可以缩短唤醒时间，但通常仍占用 Kubernetes
分配的设备，不能视为 真正释放资源。

## 12. 第一阶段集成建议

建议第一阶段保持 AReaL 现有实例和版本控制逻辑，只接入平台资源层：

```text
AReaL 指标与实例 Autoscaler
    ↓ 设置目标、管理生命周期、创建/释放 Placement Group
Ray Autoscaler on KubeRay
    ↓ 根据 Resource Demand 扩缩 Worker Pod
Kubernetes 节点 Autoscaler
    ↓
NPU/GPU 节点
```

第一阶段目标：

1. 打通 AReaL 目标实例数、Ray resource demand 与 KubeRay 资源扩缩的闭环；
1. 保证新增 Worker 能加入同一个 Ray 集群并正确上报设备资源；
1. 打通共享 checkpoint、网络和实例删除；
1. 用统一 ID 串联扩容请求、实例、Ray Worker、Pod 和节点日志；
1. 验证训练期间 `1 → 2 → 1`，包括模型版本追赶和安全排空；
1. 在资源不足、启动超时和 Controller 重启场景下完成故障验证。

待第一阶段稳定后，再考虑：

- Kubernetes CRD/Operator；
- Controller 高可用；
- 基于冷启动耗时的提前扩容；
- 镜像、模型和编译缓存感知调度；
- 保留少量设备资源的 Warm 实例池。

## 13. 首次会议需要确认的问题

建议与平台团队优先确认以下问题：

1. 平台当前如何扩充 Ray Worker Pod，它们如何注册进现有 Ray 集群？
1. Ray 资源不足时，能否自动触发 Worker Pod和底层 NPU/GPU 节点扩容？
1. 平台是否接受由 AReaL 内部策略作为目标实例数的正常单写者，HTTP 仅用于人工覆盖？
1. 平台如何向 AReaL 返回资源排队、Pod 调度、节点扩容和失败状态？
1. checkpoint 使用哪种共享存储，访问路径和清理规则如何保证一致？
1. Controller、Ray Worker、推理服务和框架 Proxy 的网络如何打通？
1. 实例、Ray Worker、Pod 和节点使用什么统一标识？
1. 缩容时，平台如何等待 AReaL 排空完成后再删除资源？
1. Controller 或平台异常退出后，如何发现和回收残留资源？
1. 首期扩容时延目标、资源等待上限、重试和告警标准是什么？

## 14. 建议验收场景

- 有空闲 Ray 资源时，AReaL 能完成扩容并加载正确模型版本；
- Ray 资源不足时，平台能扩 Worker Pod，必要时扩底层节点，再继续实例启动；
- 新实例 READY 前不会接收 Rollout 请求；
- 缩容实例停止接收新请求，并在已有任务完成后释放设备；
- Controller 重启后能够恢复目标容量并重新收敛；
- 资源长期不足或实例启动失败时不会形成无限高频创建；
- 日志能够区分平台资源等待和框架推理服务初始化耗时；
- 全流程完成一次训练不中断的 `1 → 2 → 1`。
