# 两节点 910B 弹性验证环境

本目录用于在两台 8 卡 Ascend 910B 裸机上验证以下链路：

```text
AReaL desired state
  -> Ray placement-group demand
  -> Ray Autoscaler
  -> KubeRay Worker Pod
  -> 单个 8 卡 Worker Pod 内打包四个 2 卡 rollout 实例
```

这是一套隔离验收环境，不是生产部署方案。完整功能验收标准参见
[`docs/zh/reference/rollout_elasticity_kuberay_validation.md`](../../docs/zh/reference/rollout_elasticity_kuberay_validation.md)。

## 1. 固定版本和拓扑

建议使用以下组合：

| 组件                               | 版本或要求                                  |
| ---------------------------------- | ------------------------------------------- |
| CANN                               | 8.5.2                                       |
| MindCluster / Ascend Device Plugin | 7.3.0                                       |
| Kubernetes                         | 1.30.x 最新 bugfix                          |
| Container runtime                  | containerd 1.6.x + Ascend Container Runtime |
| KubeRay                            | 1.5.2                                       |
| Ray                                | 2.53.0，head、worker、driver 完全一致       |

拓扑：

```text
node-1: Kubernetes control-plane + Ray head + one 8-NPU worker Pod
node-2: Kubernetes worker                 + one 8-NPU worker Pod
```

节点固定时，KubeRay 只扩缩 Ray Worker Pod，不创建或删除物理机。只有两个物理节点且只有一个 control-plane 时，不要把 `node-1`
故障测试当成高可用验证；它同时承载 Kubernetes control-plane 和 Ray head。

仓库的 `uv.npu.lock` 当前解析到 Ray 2.55.1。用于本环境的镜像必须显式安装 `ray[default]==2.53.0`，并确认没有被后续安装步骤升级。

```bash
python -c 'import ray; assert ray.__version__ == "2.53.0", ray.__version__'
```

## 2. Kubernetes 和 NPU 前置条件

使用 kubeadm 创建两节点 Kubernetes 集群并安装 CNI。具体命令随操作系统和内网软件源而异，不要直接在承载其他作业的机器上执行初始化或重置命令。

两台机器都必须满足：

- `npu-smi info` 显示 8 张健康 910B；
- CANN、驱动、固件和 Ascend Container Runtime 相互配套；
- containerd 能启动现有 NPU 镜像；
- 两台机器时间同步、主机名唯一且相互解析；
- Kubernetes Pod 网段不与主机网段、HCCL 网段或 Service 网段冲突；
- 已安装 NFS client，且能访问相同共享目录。

安装 MindCluster 7.3.0 中不依赖 Volcano 的 910 Device Plugin 清单：

```bash
kubectl apply -f device-plugin-910-v7.3.0.yaml
kubectl get pods -n kube-system -o wide | grep ascend-device-plugin
```

本验证不需要 Volcano、Ascend Operator 或训练任务 gang scheduling。先确认 Device Plugin 在两个节点均为
`Running`，再检查资源：

```bash
kubectl describe node node-1 | grep -A8 -E 'Capacity:|Allocatable:'
kubectl describe node node-2 | grep -A8 -E 'Capacity:|Allocatable:'
```

两个节点的 `Capacity` 和 `Allocatable` 都必须出现：

```text
huawei.com/Ascend910: 8
```

如果实际资源键不同，必须同时修改 `raycluster-2x910b.yaml` 的 requests 和 limits。不要修改 Ray 内部资源名 `NPU`，AReaL
使用该名称申请资源。

## 3. 安装 KubeRay Operator

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.5.2 \
  --namespace kuberay-system \
  --create-namespace

kubectl wait --for=condition=Available deployment/kuberay-operator \
  -n kuberay-system --timeout=180s
kubectl get crd rayclusters.ray.io
```

## 4. 准备 RayCluster

给两个节点添加实验标签。`node-1` 替换成 control-plane 节点名：

```bash
kubectl label node node-1 areal.io/ray-head=true areal.io/npu-worker=true
kubectl label node node-2 areal.io/npu-worker=true
```

编辑 [`raycluster-2x910b.yaml`](raycluster-2x910b.yaml) 和
[`shared-pv.yaml`](shared-pv.yaml)，至少替换：

- `REPLACE_ME_AREAL_NPU_IMAGE_WITH_RAY_2_53`：包含 CANN 8.5.2、AReaL、推理后端和
  `ray[default]==2.53.0` 的镜像；
- `REPLACE_ME_NFS_SERVER`；
- `/REPLACE_ME_NFS_EXPORT`。

模板假设每个 NPU 节点可供 Worker Pod 使用 64 CPU 和 384 GiB 内存；`node-1` 还需为 Ray head 额外预留 4 CPU 和 16
GiB 内存。AReaL 默认每卡申请 8 CPU、32 GiB Ray 逻辑内存，所以一个两卡实例申请 16 CPU、64 GiB，四个实例合计申请 64 CPU、256
GiB。如果节点规格更小，需要同时调低 Pod 资源和 AReaL `SchedulingSpec`，不能只修改其中一侧。

检查替换是否完成，然后部署：

```bash
grep -n REPLACE_ME examples/kuberay/*.yaml
kubectl apply --dry-run=client -f examples/kuberay/namespace.yaml
kubectl apply --dry-run=client -f examples/kuberay/shared-pv.yaml
kubectl apply --dry-run=client -f examples/kuberay/shared-pvc.yaml
kubectl apply --dry-run=client -f examples/kuberay/raycluster-2x910b.yaml

kubectl apply -f examples/kuberay/namespace.yaml
kubectl apply -f examples/kuberay/shared-pv.yaml
kubectl apply -f examples/kuberay/shared-pvc.yaml
kubectl apply -f examples/kuberay/raycluster-2x910b.yaml

kubectl get raycluster -n areal-elastic-validation
kubectl get pods -n areal-elastic-validation -o wide -w
```

初始状态应为一个 Ray head Pod 和一个占用 8 张 NPU 的 Ray worker Pod。进入 worker 容器检查：

```bash
NS=areal-elastic-validation
WORKER=$(kubectl get pod -n "$NS" -l ray.io/node-type=worker \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n "$NS" "$WORKER" -c ray-worker -- npu-smi info
kubectl exec -n "$NS" "$WORKER" -c ray-worker -- \
  env | grep -E 'ASCEND_RT_VISIBLE_DEVICES|ASCEND_VISIBLE_DEVICES'
```

## 5. 先验证 Ray/KubeRay 扩缩链路

这一步不启动模型。每个 Python actor 模拟一个 2-NPU Rollout 实例；前四个 actor 可以 `PACK` 到第一个 8-NPU worker
Pod，第五个 actor 才会触发第二个 worker Pod，行为与目标 Rollout 形态一致。

```bash
NS=areal-elastic-validation
HEAD=$(kubectl get pod -n "$NS" -l ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')

kubectl cp examples/kuberay/npu_autoscaler_smoke.py \
  "$NS/$HEAD:/tmp/npu_autoscaler_smoke.py" -c ray-head
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  python /tmp/npu_autoscaler_smoke.py \
    --replicas 5 --resources-per-replica 2 --hold-seconds 300
```

在另一个终端观察：

```bash
NS=areal-elastic-validation
HEAD=$(kubectl get pod -n "$NS" -l ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')

kubectl get pods -n "$NS" -o wide -w
kubectl logs -n "$NS" "$HEAD" -c autoscaler -f
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray status -v
```

通过标准：

1. 初始只有一个 8-NPU worker；
1. 前四个 actor 使用第一个 worker，第五个 actor 产生 2 个 `NPU` 的 pending demand；
1. KubeRay 将 worker replicas 从 1 改为 2；
1. 第二个 worker Pod 调度到另一台机器并注册 8 个 `NPU`；
1. actor 返回值中出现两个不同 hostname；
1. 每个 actor 的 `accelerator_ids.NPU` 恰好有两个 ID，同一 hostname 内不同 actor 的 ID 不重叠；
1. 脚本退出并等待 `idleTimeoutSeconds` 后，worker Pod 回到 1。

状态码 3 表示 Ray 没有把 Pod 内 8 张 NPU 正确隔离成互不重叠的两卡集合。此时不要继续启动真实推理服务，先核对 Ray 版本、Ray worker 启动参数和
`ASCEND_RT_VISIBLE_DEVICES`。

容量不足穿刺可以申请 9 个两卡 actor。集群最多容纳 8 个实例，第九个需求必须保持 pending；测试期间在另一终端保存 Ray 和 KubeRay 状态：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  python /tmp/npu_autoscaler_smoke.py \
    --replicas 9 --resources-per-replica 2 \
    --ready-timeout-seconds 180 --hold-seconds 120
```

该命令以状态码 2 退出是预期行为。退出后不应遗留 actor，worker 最终回到 `minReplicas=1`。

## 6. 验证 AReaL HTTP 1→5→1

确认模型已放在共享盘，例如 `/shared/areal/models/Qwen2.5-1.5B-Instruct`。在 Ray head 容器运行
RolloutController spike；它不启动训练 actor，只验证真实推理服务和弹性 Rollout 生命周期：

```bash
NS=areal-elastic-validation
HEAD=$(kubectl get pod -n "$NS" -l ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc '
  cd /AReaL
  export AREAL_SPMD_MODE=false
  python examples/math/rollout_elastic_controller_spike.py \
    --scale-up-to 5 --verify-recommendation --verify-recovery -- \
    --config examples/math/gsm8k_grpo_npu.yaml \
    scheduler.type=ray \
    cluster.ray_device_resource=NPU \
    cluster.n_gpus_per_node=8 \
    cluster.fileroot=/shared/areal/experiments \
    cluster.name_resolve.nfs_record_root=/shared/areal/name_resolve \
    rollout.fileroot=/shared/areal/experiments \
    rollout.backend=vllm:d1p1t2 \
    actor.path=/shared/areal/models/Qwen2.5-1.5B-Instruct \
    actor.weight_update_mode=disk \
    rollout.elastic.resource_provision_timeout_seconds=900 \
    rollout.elastic.startup_timeout_seconds=600
'
```

这里使用 `TP × PP = 2`，与目标 Rollout 实例一致。一个 8-NPU worker Pod 最多容纳四个两卡实例，因此必须把 desired 扩到 5
才会出现第二个 Pod。`1→2` 只能验证 AReaL 创建新实例，不能验证 KubeRay 扩容。

通过标准：

- spike 日志输出 `Elastic Controller HTTP 1->5->1 spike passed`；
- 扩容阶段出现五个不同的 elastic worker role；
- Ray 在等待资源前能看到四个新 placement group，每个申请 2 个 `NPU`；
- 前四个实例位于第一个 worker Pod，第五个实例在另一个 worker Pod 上进入 `READY`；
- 缩容先进入 `DRAINING`，对应 actor 和 placement group 删除；
- 经过 Ray idle timeout 后 worker Pod 从 2 回到 1；
- recovery 重启后 desired、实例数量和 serving version 一致。

## 7. 清理

先确认没有仍需保留的作业和证据，再删除实验资源：

```bash
kubectl delete -f examples/kuberay/raycluster-2x910b.yaml
kubectl delete -f examples/kuberay/shared-pvc.yaml
kubectl delete -f examples/kuberay/shared-pv.yaml
kubectl delete -f examples/kuberay/namespace.yaml
helm uninstall kuberay-operator -n kuberay-system
```

模板中的 PV 回收策略是 `Retain`。删除 PVC 不会删除 NFS 数据；是否清理共享目录由验证负责人单独决定。不要使用删除 Kubernetes
数据目录或重置节点的命令清理业务文件。
