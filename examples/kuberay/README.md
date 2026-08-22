# 两节点 Ascend 910B 弹性 Rollout 端到端验证手册

本文给出一条从两台裸机开始、可以逐项验收的参考路径，验证：

```text
真实训练指标或人工 desired
  -> AReaL RolloutController
  -> provision_many 批量 Placement Group demand
  -> Ray Autoscaler
  -> KubeRay Worker Pod 1 <-> 2
  -> rollout 实例 PENDING -> STARTING -> CATCHING_UP -> READY
  -> DRAINING -> 删除 Ray role/Placement Group -> Worker Pod 缩容
```

固定资源模型：

| 项目                  | 本手册配置                                                |
| --------------------- | --------------------------------------------------------- |
| 裸机                  | 2 台 Ascend 910B，每台 8 卡                               |
| Kubernetes Worker Pod | 每台机器最多一个，Pod 整机申请 8 卡                       |
| 训练                  | FSDP DP=4，共 4 卡                                        |
| 单个 rollout 实例     | vLLM TP=2，共 2 卡                                        |
| 初始状态              | 训练 4 卡 + rollout 1×2 卡 = 6 卡，一个 Worker Pod        |
| 扩容临界点            | desired=2 时共 8 卡；desired=3 时共 10 卡，触发第二个 Pod |
| 正常扩容目标          | desired=5，共 14 卡                                       |
| 最大可用目标          | desired=6，共 16 卡                                       |
| 资源不足目标          | desired=7，共 18 卡，第 7 个 rollout 无法调度             |

这里不需要第三台机器：两台裸机从一开始都加入 Kubernetes，但 RayCluster 初始只创建一个 8 卡 Worker Pod；第二台裸机处于可调度但没有 Ray
Worker 的状态。Ray Autoscaler 看到无法满足的 Placement Group 后，让 KubeRay 在第二台现有裸机上创建第二个 Worker
Pod。它不会创建或 删除裸机；本手册验证的是 Ray Worker Pod 层的 1↔2 弹性。

本文是隔离测试环境指导，不是生产高可用方案。完整验收矩阵和实现限制参见
[`docs/zh/reference/rollout_elasticity_kuberay_validation.md`](../../docs/zh/reference/rollout_elasticity_kuberay_validation.md)。

## 0. 使用约束和执行规则

### 0.1 环境假设

本文假设：

- 两台机器是 deb 或 rpm 兼容的 Linux，使用 systemd；
- 两台机器架构一致，均为 `x86_64` 或均为 `aarch64`；
- CANN 8.5.2、对应驱动和固件已经安装并可正常执行 `npu-smi info`；
- 已有包含 AReaL、PyTorch NPU、vLLM、CANN 8.5.2 和模型依赖的镜像；
- 两台机器能访问同一个共享盘或 NFS export；
- 两台机器之间没有 NAT，主机 IP、Kubernetes Pod CIDR、Service CIDR 和 HCCL 网络互不冲突；
- 以下操作只在专用测试机器执行。

如果操作系统的软件源、Ascend Docker Runtime 安装路径、NPU 资源键或网卡与本文不同，先替换对应值，不要机械执行。尤其不要用本文命令覆盖已有
`/etc/containerd/config.toml` 中的 Ascend runtime 配置。

### 0.2 危险操作边界

以下操作会修改宿主机，执行前应由机器负责人确认：

- 关闭 swap；
- 加载内核模块和修改 sysctl；
- 安装或固定 Kubernetes、containerd 软件包；
- `kubeadm init` 和 `kubeadm join`；
- 修改防火墙和主机名。

本文不提供自动执行的 `kubeadm reset`、删除 containerd 数据目录或清理 NFS 业务目录命令。需要重建集群时先备份并单独制定回滚方案。

### 0.3 每一步的停止规则

每节末尾都有“通过标准”。任何一条未满足时停止，不要继续下一节。建议按以下顺序保存证据：

```text
宿主机/CANN
  -> containerd/Ascend runtime
  -> Kubernetes/CNI
  -> Ascend Device Plugin
  -> Ray/KubeRay 无模型 smoke
  -> 真实 rollout Controller
  -> 训练4卡 + rollout2卡
  -> 自动策略、资源不足和故障
```

## 1. 固定版本

| 组件                               | 版本或要求                                    |
| ---------------------------------- | --------------------------------------------- |
| CANN                               | 8.5.2                                         |
| MindCluster / Ascend Device Plugin | 7.3.0，与 CANN 8.5.x 配套                     |
| Kubernetes                         | 1.34.10，本文固定的 1.34 bugfix 版本          |
| containerd                         | 优先 1.6.x；MindCluster 7.3 支持 1.4.x～2.1.4 |
| CNI                                | Calico 3.32.1，Pod CIDR `192.168.0.0/16`      |
| Helm                               | 3.x                                           |
| KubeRay                            | 1.5.2                                         |
| Ray                                | 2.53.0，head、worker、driver 完全一致         |

MindCluster 7.3 支持 Kubernetes 1.17～1.34，并建议使用最新 bugfix；Kubernetes 1.30 已经 EOL，因此新建集群不再推荐
1.30。若内网已有固定的 Kubernetes 1.30 集群，可以继续做兼容性验证，但要在报告中记录其 EOL 风险，不要将其作为新的生产基线。

仓库的 `uv.npu.lock` 当前解析到 Ray 2.55.1，而你的环境使用 Ray 2.53。下面命令只在已有 Python/pip
的镜像构建环境执行，不是在两台空白 裸机上安装 Python。制作验证镜像时必须在最后显式固定：

```bash
python -m pip install 'ray[default]==2.53.0'
python -c 'import ray; assert ray.__version__ == "2.53.0", ray.__version__'
```

参考资料：

- [Kubernetes kubeadm 安装](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/)
- [Kubernetes containerd 和 systemd cgroup 配置](https://kubernetes.io/docs/setup/production-environment/container-runtimes/)
- [MindCluster 7.3 软件依赖](https://www.hiascend.com/document/detail/zh/mindcluster/730/clustersched/dlug/dlug_installation_004.html)
- [Ascend Device Plugin 手工安装](https://github.com/Ascend/mind-cluster/blob/master/docs/en/scheduling/developer_guide/installation_deployment/manual_installation/04_ascend_device_plugin.md)
- [KubeRay 安装](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html)
- [KubeRay autoscaling](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html)

## 2. 规划节点、网络和离线制品

### 2.1 节点角色

本文用下面的占位符：

```text
node-1 / <CONTROL_PLANE_IP>:
  Kubernetes control-plane
  Ray head Pod
  第一个 8-NPU Ray Worker Pod

node-2 / <WORKER_IP>:
  Kubernetes worker
  第二个 8-NPU Ray Worker Pod（有 demand 时创建）
```

在两台机器分别记录真实值：

```bash
hostname
hostname -I
ip route show
uname -m
cat /etc/os-release
npu-smi info
```

确保主机名唯一，并在内部 DNS 或两台机器的 `/etc/hosts` 中建立相互解析。不要让 Kubernetes 自动选择管理口以外的默认路由
IP；多默认路由环境需要先由网络管理员固定主路由。

### 2.2 网络规划

本文默认：

```text
Pod CIDR:     192.168.0.0/16
Service CIDR: 10.96.0.0/12
Calico VXLAN: UDP 4789（如果最终配置使用 VXLAN）
```

执行前检查这些网段没有与主机、NFS、HCCL 或内网路由重叠。两台机器之间至少需要放通 Kubernetes API、kubelet、containerd/CNI
所需端口和所有 Pod 间通信。若保留主机防火墙，按 Kubernetes 和所选 Calico 模式的官方端口表配置；不要在共享网络里直接永久关闭防火墙。

### 2.3 内网需要准备的制品

在能访问公网的中转机下载并导入内网仓库或离线包：

| 制品                                                               | 用途                                                       |
| ------------------------------------------------------------------ | ---------------------------------------------------------- |
| Kubernetes 1.34.10 的 kubeadm/kubelet/kubectl 和控制面镜像         | 建集群                                                     |
| containerd 1.6.x 或已验证的内网版本                                | CRI                                                        |
| Calico 3.32.1 manifest 和其中引用的镜像                            | Pod 网络                                                   |
| Helm 3.x 二进制                                                    | 安装 KubeRay                                               |
| KubeRay Operator 1.5.2 chart 和 operator 镜像                      | 管理 RayCluster                                            |
| MindCluster 7.3.0 Ascend Docker Runtime、Device Plugin YAML 和镜像 | NPU 容器化                                                 |
| 当前 AReaL 分支源码或源码压缩包                                    | 在 node-1 执行仓库脚本和复制部署模板                       |
| AReaL NPU 镜像，固定 Ray 2.53.0                                    | head、worker、driver                                       |
| Qwen2.5-1.5B-Instruct 或实际验证模型                               | 真实推理和训练                                             |
| `openai/gsm8k` 的 Hugging Face cache                               | 放在共享盘 `/shared/areal/huggingface`，避免训练时访问公网 |

本节只登记制品，不在空白裸机上执行 `kubeadm`、`ctr`、`kubectl` 或 `helm`。完成 containerd 安装后在第 4.4 节导入容器镜像；完成
kubeadm 安装后在第 5.4 节生成并核对 Kubernetes 控制面镜像清单。中转机如需提前生成清单，也必须先安装与目标集群完全一致的
`kubeadm v1.34.10`。

在与验证镜像依赖一致、能够访问 Hugging Face 的准备环境中预热 GSM8K cache，然后把整个目录同步到内网共享盘：

```bash
export HF_HOME=<STAGING_DIR>/huggingface
python -c 'from datasets import load_dataset; load_dataset("openai/gsm8k", "main", split="train"); load_dataset("openai/gsm8k", "main", split="test")'
# 把 <STAGING_DIR>/huggingface 完整同步为内网的 /shared/areal/huggingface
```

### 2.4 本节通过标准

- 已记录两台机器的主机名、管理 IP、OS、架构和 NPU 状态；
- Pod/Service CIDR 与现有网络不冲突；
- 两台机器架构一致；
- 所有安装包和容器镜像已经进入内网；
- 共享盘中的模型、数据集和实验目录在两台机器上使用相同绝对路径。

## 3. 两台宿主机前置检查

以下命令在两台机器执行。

### 3.1 检查 NPU 和共享盘

如果共享盘使用 NFS，先确认两台机器都有 NFS 客户端。缺少 `mount.nfs` 时按 OS 安装一种：

```bash
command -v mount.nfs

# Debian/Ubuntu
sudo apt-get install -y nfs-common

# RHEL/openEuler
sudo dnf install -y nfs-utils
```

上面两个安装命令只执行与当前 OS 对应的一个。若共享目录尚未挂载，先在两台机器创建相同挂载点并挂载；已经由系统或存储客户端挂载时跳过 `mount`：

```bash
sudo mkdir -p /shared/areal
sudo mount -t nfs -o nfsvers=4.1 \
  <NFS_SERVER>:<NFS_EXPORT_PATH> /shared/areal
```

非 NFS 共享文件系统按照对应存储产品挂载，但最终路径仍统一为 `/shared/areal`。然后检查设备和共享路径：

```bash
npu-smi info
ls -l /dev/davinci* /dev/davinci_manager /dev/devmm_svm
findmnt /shared/areal
touch /shared/areal/areal-write-test-$(hostname)
ls -l /shared/areal/areal-write-test-*
```

两台机器都应看到 8 张健康 910B，并能看到对方写入的测试文件。测试文件可以在确认后手工删除。

### 3.2 关闭 swap

先只读检查：

```bash
swapon --show
free -h
```

如果有 swap，由管理员执行：

```bash
sudo swapoff -a
```

然后由管理员编辑 `/etc/fstab`，注释 swap 条目，重启后再次确认 `swapon --show` 没有输出。

### 3.3 加载 Kubernetes 网络模块

```bash
sudo modprobe overlay
sudo modprobe br_netfilter

sudo tee /etc/modules-load.d/areal-k8s.conf >/dev/null <<'EOF'
overlay
br_netfilter
EOF

sudo tee /etc/sysctl.d/99-areal-k8s.conf >/dev/null <<'EOF'
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

sudo sysctl --system
sysctl net.ipv4.ip_forward
```

### 3.4 检查时间同步

```bash
timedatectl status
date --iso-8601=seconds
```

两台机器时间偏差应小于 1 秒。否则先配置 chrony/NTP。

### 3.5 本节通过标准

- `npu-smi info` 两台均正常且各有 8 卡；
- 两台共享盘读写正常；
- swap 已关闭并持久化；
- `overlay`、`br_netfilter` 已加载；
- `net.ipv4.ip_forward = 1`；
- 时间同步正常。

## 4. 安装和检查 containerd、Ascend Docker Runtime

### 4.1 安装 containerd

两台机器先检查是否已经安装：

```bash
if command -v containerd >/dev/null 2>&1; then
  containerd --version
  sudo systemctl status containerd --no-pager
else
  echo "containerd is not installed"
fi
```

没有 containerd 时，从经过内网验证的 deb/rpm 软件源安装 1.6.x。下面两组只执行与当前 OS 对应的一组：

```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y containerd

# RHEL/openEuler；根据内网仓库实际包名二选一
sudo dnf install -y containerd
# 或 sudo dnf install -y containerd.io
```

安装完成后再执行：

```bash
containerd --version
sudo systemctl enable containerd
```

### 4.2 创建或保留 containerd 配置

先判断配置文件是否存在。已有文件只备份，不覆盖；仅在文件不存在时生成默认配置：

```bash
if sudo test -f /etc/containerd/config.toml; then
  sudo cp -a /etc/containerd/config.toml \
    /etc/containerd/config.toml.before-areal-validation
  echo "existing containerd config backed up"
else
  sudo mkdir -p /etc/containerd
  containerd config default | sudo tee /etc/containerd/config.toml >/dev/null
  echo "default containerd config created"
fi

sudo grep -nE 'disabled_plugins|SystemdCgroup|ascend|runtime' \
  /etc/containerd/config.toml
```

由管理员编辑 `/etc/containerd/config.toml`，需要满足：

- `disabled_plugins` 不包含 `cri`；
- containerd 1.x 的 runc 配置中 `SystemdCgroup = true`；
- 如果原配置已有 Ascend runtime，完整保留；
- CRI socket 是 `/run/containerd/containerd.sock`。

修改后启动并检查 CRI。这里通过后才能使用后面的 `ctr` 命令：

```bash
sudo systemctl restart containerd
sudo systemctl is-active containerd
sudo ctr plugins list | grep -E 'io.containerd.grpc.v1.cri|io.containerd.cri.v1'
```

### 4.3 安装或检查 Ascend Docker Runtime

Ascend Device Plugin 必须在 Ascend Docker Runtime 之后安装。先检查：

```bash
command -v ascend-docker-runtime || true
sudo test -x /usr/local/Ascend/Ascend-Docker-Runtime/ascend-docker-runtime && \
  echo "Ascend Docker Runtime found"
```

未找到时，使用第 2.3 节准备的、与 MindCluster 7.3/CANN 8.5.2 匹配的安装包，按照包内说明安装。安装程序可能修改
`/etc/containerd/config.toml`；安装前使用第 4.2 节的备份，安装后重新检查 CRI 和 `SystemdCgroup = true`，然后执行：

```bash
sudo systemctl restart containerd
sudo systemctl is-active containerd
sudo test -x /usr/local/Ascend/Ascend-Docker-Runtime/ascend-docker-runtime
```

如果实际安装路径不同，后续命令统一替换成实际路径。

### 4.4 导入离线容器镜像

此时 containerd 和 Ascend runtime 已经安装，才开始在两台机器导入第 2.3 节准备的镜像。Kubernetes 使用的镜像必须导入 `k8s.io`
namespace：

```bash
sudo ctr -n k8s.io images import <IMAGE_TAR>
sudo ctr -n k8s.io images list
```

这里先导入 Calico、KubeRay Operator、Ascend Device Plugin、AReaL NPU 和网络 smoke 镜像。Kubernetes
控制面镜像等 kubeadm 安装完成并生成精确清单后，在第 5.4 节导入。也可以使用内网镜像仓库，但 YAML 和 kubeadm 中的完整镜像名必须与仓库一致。

### 4.5 验证 Ascend runtime

确认 AReaL NPU 镜像已经存在，再做单卡容器测试：

```bash
sudo ctr -n k8s.io images list | grep '<AREAL_NPU_IMAGE>'

sudo ctr -n k8s.io run --rm \
  --runtime io.containerd.runc.v2 \
  --runc-binary /usr/local/Ascend/Ascend-Docker-Runtime/ascend-docker-runtime \
  --env ASCEND_VISIBLE_DEVICES=0 \
  <AREAL_NPU_IMAGE> areal-npu-runtime-$(hostname) \
  npu-smi info
```

这个命令直接验证 containerd、Ascend runtime、驱动挂载和镜像是否能协同工作；失败时不要继续安装 Kubernetes。

### 4.6 本节通过标准

- containerd 服务 active；
- CRI plugin 为 `ok`，且未被禁用；
- containerd 和后续 kubelet 都使用 systemd cgroup；
- 两台机器都能用 containerd + Ascend runtime 启动单卡容器并执行 `npu-smi info`。

## 5. 安装 Kubernetes 1.34.10

以下安装方式选择与你的 OS 对应的一种，两台机器使用完全相同版本。命令中的公网软件源只是联网示例；内网环境先把 URL 换成已同步的软件源，或者直接安装第 2.3 节准备的
deb/rpm 包，不能在无公网环境照抄公网 URL。

### 5.1 Debian/Ubuntu 路径

```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates curl gpg
sudo mkdir -p -m 755 /etc/apt/keyrings

curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.34/deb/Release.key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg

echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.34/deb/ /' | \
  sudo tee /etc/apt/sources.list.d/kubernetes.list

sudo apt-get update
apt-cache madison kubeadm | grep 1.34.10
sudo apt-get install -y \
  kubelet=1.34.10-1.1 kubeadm=1.34.10-1.1 kubectl=1.34.10-1.1
sudo apt-mark hold kubelet kubeadm kubectl
sudo systemctl enable --now kubelet
```

如果内网仓库的 deb revision 不是 `-1.1`，以 `apt-cache madison` 显示值为准，但三者版本必须一致。

### 5.2 RHEL/openEuler 路径

```bash
sudo tee /etc/yum.repos.d/kubernetes.repo >/dev/null <<'EOF'
[kubernetes]
name=Kubernetes
baseurl=https://pkgs.k8s.io/core:/stable:/v1.34/rpm/
enabled=1
gpgcheck=1
gpgkey=https://pkgs.k8s.io/core:/stable:/v1.34/rpm/repodata/repomd.xml.key
exclude=kubelet kubeadm kubectl cri-tools kubernetes-cni
EOF

sudo dnf list --showduplicates kubeadm | grep 1.34.10
sudo dnf install -y \
  kubelet-1.34.10 kubeadm-1.34.10 kubectl-1.34.10 \
  --disableexcludes=kubernetes
sudo systemctl enable --now kubelet
```

在 kubeadm 初始化前 kubelet 反复重启是正常现象。

### 5.3 检查版本

两台机器执行：

```bash
kubeadm version -o short
kubelet --version
kubectl version --client
```

三者都应为 `v1.34.10`。

### 5.4 生成清单并准备 Kubernetes 镜像

到这一步 `kubeadm` 和 `ctr` 才都已经安装。两台机器执行清单命令：

```bash
kubeadm config images list --kubernetes-version v1.34.10
```

输出的完整镜像名和 tag 是本次集群的精确清单。按环境二选一：

联网且两台机器能直接访问 `registry.k8s.io` 时，在两台机器拉取：

```bash
sudo kubeadm config images pull \
  --kubernetes-version v1.34.10 \
  --cri-socket unix:///run/containerd/containerd.sock
```

完全离线时，不执行上面的 `pull`。在中转机准备清单中的每个镜像 tar，然后在两台机器逐个导入，并保持原始完整镜像名和 tag：

```bash
sudo ctr -n k8s.io images import <KUBERNETES_IMAGE_TAR>
sudo ctr -n k8s.io images list
```

如果企业镜像仓库改变了 repository 前缀，需要在后续 kubeadm 配置文件中统一设置对应的 `imageRepository`。为保持下面命令可直接执行，本文的
内网路径统一采用 tar 导入并保留 `kubeadm config images list` 输出的原始名称。

本节通过标准：两台机器均已安装同版本 kubeadm/kubelet/kubectl，并且清单中的镜像在各自 containerd `k8s.io` namespace
可见。未满足时不要执行 `kubeadm init`。

## 6. 使用 kubeadm 建立两节点集群

### 6.1 初始化 node-1

确认第 5.4 节镜像门禁已经通过。仅在 `node-1` 执行，替换管理 IP：

```bash
sudo kubeadm init \
  --kubernetes-version v1.34.10 \
  --apiserver-advertise-address <CONTROL_PLANE_IP> \
  --pod-network-cidr 192.168.0.0/16 \
  --service-cidr 10.96.0.0/12 \
  --cri-socket unix:///run/containerd/containerd.sock \
  --node-name node-1
```

保存输出中的 `kubeadm join ...` 命令。然后配置当前用户的 kubectl：

```bash
mkdir -p "$HOME/.kube"
sudo cp -i /etc/kubernetes/admin.conf "$HOME/.kube/config"
sudo chown "$(id -u):$(id -g)" "$HOME/.kube/config"
kubectl get nodes -o wide
```

此时 node-1 是 `NotReady` 属于预期，因为 CNI 尚未安装。

### 6.2 安装 Calico CNI

仅在 `node-1` 执行。先按环境二选一确定 manifest 路径。

联网环境：

```bash
curl -LO https://raw.githubusercontent.com/projectcalico/calico/v3.32.1/manifests/calico.yaml
CALICO_MANIFEST=$PWD/calico.yaml
```

离线环境使用第 2.3 节准备的本地文件，不执行 `curl`；同时确认第 4.4 节已经在两台机器导入 manifest 引用的全部镜像：

```bash
CALICO_MANIFEST=/opt/areal-offline/calico-v3.32.1.yaml
test -f "$CALICO_MANIFEST"
```

然后检查 CIDR 并部署：

```bash
grep -n '192.168.0.0/16' "$CALICO_MANIFEST"
kubectl apply -f "$CALICO_MANIFEST"

kubectl get pods -n kube-system -o wide -w
```

`-w` 会持续观察而不会自行退出。看到 Calico 和 CoreDNS Pod 都进入 Running 后按一次 `Ctrl-C`，再继续第 6.3 节；这只停止观察命令，
不会停止 Pod。

如果 manifest 的 IP pool 与 kubeadm 的 Pod CIDR 不一致，必须在首次 apply 前修正。不要在已经分配 Pod IP 后随意更换
CIDR。

### 6.3 加入 node-2

在 `node-2` 执行之前保存的 join 命令，并补充 CRI socket 和节点名，例如：

```bash
sudo kubeadm join <CONTROL_PLANE_IP>:6443 \
  --token <TOKEN> \
  --discovery-token-ca-cert-hash sha256:<HASH> \
  --cri-socket unix:///run/containerd/containerd.sock \
  --node-name node-2
```

join 命令过期时，在 node-1 重新生成：

```bash
kubeadm token create --print-join-command
```

### 6.4 验证 Kubernetes 和 Pod 网络

在 node-1 执行：

```bash
kubectl get nodes -o wide
kubectl get pods -A -o wide
kubectl wait --for=condition=Ready node/node-1 --timeout=300s
kubectl wait --for=condition=Ready node/node-2 --timeout=300s
```

创建一个短生命周期 Pod 检查 DNS 和跨 Pod 网络：

```bash
kubectl run areal-network-smoke \
  --image=<INTERNAL_BUSYBOX_IMAGE> \
  --restart=Never --command -- sh -c \
  'nslookup kubernetes.default.svc && sleep 5'
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/areal-network-smoke --timeout=120s
kubectl logs areal-network-smoke
kubectl delete pod areal-network-smoke
```

### 6.5 本节通过标准

- 两个节点均为 `Ready` 且版本一致；
- CoreDNS、kube-proxy、Calico Pod 均 Running；
- DNS smoke 能解析 `kubernetes.default.svc`；
- `kubectl get events -A` 没有持续出现 sandbox、CNI 或镜像拉取错误。

## 7. 安装 Ascend Device Plugin 7.3.0

### 7.1 准备 Device Plugin

使用 MindCluster 7.3.0 软件包中的 `device-plugin-910-v7.3.0.yaml`。它是不依赖 Volcano 的 910/A2/A3
清单。本验证不需要 Volcano、Ascend Operator、NodeD 或 gang scheduling。

两台机器都需要能在 containerd `k8s.io` namespace 中看到 Device Plugin 镜像：

```bash
sudo ctr -n k8s.io images list | grep ascend-k8sdeviceplugin
```

检查 YAML：

- 镜像地址已替换成内网地址或本地导入后的完整名称；
- `imagePullPolicy` 与内网方式一致；
- containerd socket 挂载指向 `/run/containerd`；
- containerd 场景删除 Docker socket/目录挂载；
- 不修改 DaemonSet 的 `metadata.name`；
- Ascend Docker Runtime 已先安装。

### 7.2 部署并检查资源

在 node-1 执行：

```bash
kubectl apply -f device-plugin-910-v7.3.0.yaml
kubectl get daemonset -A | grep -i ascend
kubectl get pods -A -o wide | grep -i ascend
```

找到实际 namespace 和 DaemonSet 名后等待：

```bash
kubectl rollout status daemonset/<ASCEND_DEVICE_PLUGIN_DAEMONSET> \
  -n <DEVICE_PLUGIN_NAMESPACE> --timeout=300s

kubectl describe node node-1 | grep -A12 -E 'Capacity:|Allocatable:'
kubectl describe node node-2 | grep -A12 -E 'Capacity:|Allocatable:'
```

两个节点都必须出现：

```text
huawei.com/Ascend910: 8
```

如果实际资源键不同，后续所有 YAML 的 requests/limits 必须使用实际键。Ray 内部逻辑资源仍使用 `NPU`。

### 7.3 在两个节点分别做单卡 Pod smoke

从这里开始，仓库相对路径命令都在 node-1 的 AReaL 仓库根目录执行。先确认源码和模板存在：

```bash
cd <AREAL_REPOSITORY_ROOT>
test -f examples/kuberay/npu-device-smoke.yaml
if command -v git >/dev/null 2>&1; then
  git rev-parse --short HEAD
else
  echo "record the source commit from the offline archive manifest"
fi
```

然后把验证镜像填入 [`npu-device-smoke.yaml`](npu-device-smoke.yaml) 的副本，对两个节点分别执行：

```bash
mkdir -p /tmp/areal-kuberay-validation
cp examples/kuberay/npu-device-smoke.yaml \
  /tmp/areal-kuberay-validation/npu-device-smoke.yaml
sed -i 's|REPLACE_ME_AREAL_NPU_IMAGE_WITH_RAY_2_53|<AREAL_NPU_IMAGE>|g' \
  /tmp/areal-kuberay-validation/npu-device-smoke.yaml

kubectl apply -f examples/kuberay/namespace.yaml

# node-1
kubectl label node node-1 areal.io/npu-smoke=true --overwrite
kubectl apply -f /tmp/areal-kuberay-validation/npu-device-smoke.yaml
kubectl wait -n areal-elastic-validation \
  --for=jsonpath='{.status.phase}'=Succeeded pod/npu-device-smoke --timeout=180s
kubectl logs -n areal-elastic-validation npu-device-smoke
kubectl delete -f /tmp/areal-kuberay-validation/npu-device-smoke.yaml
kubectl label node node-1 areal.io/npu-smoke-

# node-2
kubectl label node node-2 areal.io/npu-smoke=true --overwrite
kubectl apply -f /tmp/areal-kuberay-validation/npu-device-smoke.yaml
kubectl wait -n areal-elastic-validation \
  --for=jsonpath='{.status.phase}'=Succeeded pod/npu-device-smoke --timeout=180s
kubectl logs -n areal-elastic-validation npu-device-smoke
kubectl delete -f /tmp/areal-kuberay-validation/npu-device-smoke.yaml
kubectl label node node-2 areal.io/npu-smoke-
```

### 7.4 本节通过标准

- Device Plugin 在两个节点各有一个 Running Pod；
- 两个节点都上报 `huawei.com/Ascend910: 8`；
- 单卡 smoke 在两个节点分别成功；
- smoke 日志中只分配一个 NPU，并能执行 `npu-smi info` 和 `torch.npu.is_available()`。

## 8. 安装 Helm 和 KubeRay Operator

本节所有命令只在已经配置好 `kubectl` 的 `node-1` 执行。Helm 只负责向当前 Kubernetes 集群提交资源，不需要在 `node-2` 安装，
同一个 KubeRay release 也只能安装一次。

### 8.1 安装 Helm 3

从内网软件源安装，或者安装第 2.3 节准备的 Helm 3 二进制：

```bash
sudo install -m 0755 <HELM_BINARY> /usr/local/bin/helm
```

已经通过软件包安装 Helm 时不执行 `install`，直接检查：

```bash
helm version
```

### 8.2 安装 KubeRay 1.5.2

联网环境：

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm pull kuberay/kuberay-operator --version 1.5.2

helm install kuberay-operator ./kuberay-operator-1.5.2.tgz \
  --namespace kuberay-system \
  --create-namespace
```

离线环境不执行 `helm repo`，直接使用第 2.3 节准备的 chart。若 operator 镜像保留 chart 默认完整名称且已在第 4.4 节导入，在
`node-1` 执行：

```bash
test -f /opt/areal-offline/kuberay-operator-1.5.2.tgz
helm install kuberay-operator \
  /opt/areal-offline/kuberay-operator-1.5.2.tgz \
  --namespace kuberay-system \
  --create-namespace
```

如果镜像被推送到内网仓库并改变了名称，在这条命令额外使用 chart 对应的
`--set image.repository=<INTERNAL_REPOSITORY> --set image.tag=<TAG>`；先用
`helm show values` 核实字段，不能猜测字段名。

验证：

```bash
kubectl wait --for=condition=Available deployment/kuberay-operator \
  -n kuberay-system --timeout=300s
kubectl get pods -n kuberay-system -o wide
kubectl get crd rayclusters.ray.io rayjobs.ray.io rayservices.ray.io
```

### 8.3 本节通过标准

- Helm 显示 3.x；
- KubeRay Operator deployment Available；
- Operator Pod Running；
- RayCluster、RayJob、RayService CRD 存在。

## 9. 部署两节点 RayCluster

### 9.1 给节点添加标签

```bash
kubectl label node node-1 areal.io/ray-head=true --overwrite
kubectl label node node-1 areal.io/npu-worker=true --overwrite
kubectl label node node-2 areal.io/npu-worker=true --overwrite
```

Ray head 固定在 node-1。两个节点都允许调度整机 8 卡 Worker Pod。Worker Pod requests/limits 都申请 8 卡，因此
Kubernetes 保证每台机器最多一个该 Worker Pod。

### 9.2 渲染仓库模板

不要直接修改仓库模板，复制到临时目录：

```bash
mkdir -p /tmp/areal-kuberay-validation
cp examples/kuberay/namespace.yaml \
  examples/kuberay/shared-pv.yaml \
  examples/kuberay/shared-pvc.yaml \
  examples/kuberay/raycluster-2x910b.yaml \
  /tmp/areal-kuberay-validation/

sed -i 's|REPLACE_ME_AREAL_NPU_IMAGE_WITH_RAY_2_53|<AREAL_NPU_IMAGE>|g' \
  /tmp/areal-kuberay-validation/raycluster-2x910b.yaml
sed -i 's|REPLACE_ME_NFS_SERVER|<NFS_SERVER>|g' \
  /tmp/areal-kuberay-validation/shared-pv.yaml
sed -i 's|/REPLACE_ME_NFS_EXPORT|<NFS_EXPORT_PATH>|g' \
  /tmp/areal-kuberay-validation/shared-pv.yaml

grep -R -n REPLACE_ME /tmp/areal-kuberay-validation
```

最后一个 `grep` 必须没有输出。模板默认 Worker Pod 请求 64 CPU、384 GiB 内存；node-1 还需要为 Ray head 预留至少 4
CPU、16 GiB。节点规格不足时，同时修改 Worker Pod requests/limits 和 AReaL `SchedulingSpec`，不能只改一侧。

### 9.3 dry-run 和部署

第 7 节已经创建 namespace。这里仍先幂等 apply 一次，确保后面的 server dry-run 能在真实 namespace 中执行：

```bash
kubectl apply --dry-run=client \
  -f /tmp/areal-kuberay-validation/namespace.yaml
kubectl apply --dry-run=client \
  -f /tmp/areal-kuberay-validation/shared-pv.yaml
kubectl apply --dry-run=client \
  -f /tmp/areal-kuberay-validation/shared-pvc.yaml
kubectl apply -f /tmp/areal-kuberay-validation/namespace.yaml

kubectl apply --dry-run=server \
  -f /tmp/areal-kuberay-validation/shared-pv.yaml
kubectl apply --dry-run=server \
  -f /tmp/areal-kuberay-validation/shared-pvc.yaml
kubectl apply --dry-run=server \
  -f /tmp/areal-kuberay-validation/raycluster-2x910b.yaml

kubectl apply -f /tmp/areal-kuberay-validation/shared-pv.yaml
kubectl apply -f /tmp/areal-kuberay-validation/shared-pvc.yaml
kubectl apply -f /tmp/areal-kuberay-validation/raycluster-2x910b.yaml
```

如果第 7 节已创建 namespace，重复 apply 是幂等的。

### 9.4 等待初始 RayCluster

```bash
NS=areal-elastic-validation
RAYCLUSTER=areal-elastic-npu

kubectl get raycluster -n "$NS" "$RAYCLUSTER" -o wide
kubectl get pods -n "$NS" -o wide -w
```

看到一个 head Pod 和一个 worker Pod 已创建后按一次 `Ctrl-C` 结束观察，再执行下面的 wait 和检查命令。

初始应有：

```text
1 个 head Pod：ray-head + autoscaler 两个容器
1 个 worker Pod：ray-worker，一个 Pod 整机占 8 张 NPU
```

获取 Pod 名并检查：

```bash
HEAD=$(kubectl get pod -n "$NS" -l ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')
WORKER=$(kubectl get pod -n "$NS" -l ray.io/node-type=worker \
  -o jsonpath='{.items[0].metadata.name}')

kubectl wait -n "$NS" --for=condition=Ready pod/"$HEAD" --timeout=600s
kubectl wait -n "$NS" --for=condition=Ready pod/"$WORKER" --timeout=600s

kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  python -c "import ray; assert ray.__version__ == '2.53.0'; ray.init(address='auto'); print(ray.cluster_resources())"

kubectl exec -n "$NS" "$WORKER" -c ray-worker -- npu-smi info
kubectl exec -n "$NS" "$WORKER" -c ray-worker -- \
  env | grep -E 'ASCEND_RT_VISIBLE_DEVICES|ASCEND_VISIBLE_DEVICES'
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  sh -c 'test "$HF_HOME" = /shared/areal/huggingface && echo "$HF_HOME"'
kubectl exec -n "$NS" "$HEAD" -c ray-head -- sh -c 'command -v curl'
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray status -v
```

### 9.5 本节通过标准

- head 和一个 worker Pod Ready；
- head、worker 的 Ray 均为 2.53.0；
- Ray `cluster_resources()` 中有 `NPU: 8`，head 自身不上报 NPU；
- Worker 容器内能看到 8 张 NPU；
- head 镜像包含后续调用 Controller HTTP API 所需的 `curl`；
- PVC Bound，head 和 worker 都能读写 `/shared/areal`；
- autoscaler 容器没有持续报错。

## 10. 建立统一观测和证据目录

后续每个测试至少同时观察 Kubernetes、Ray 和 AReaL。

```bash
NS=areal-elastic-validation
RAYCLUSTER=areal-elastic-npu
HEAD=$(kubectl get pod -n "$NS" -l ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')
EVIDENCE=/shared/areal/evidence/$(date +%Y%m%d-%H%M%S)
kubectl exec -n "$NS" "$HEAD" -c ray-head -- mkdir -p "$EVIDENCE"
```

三个终端分别持续观察：

```bash
# 终端 B：Kubernetes
kubectl get pods -n "$NS" -o wide -w

# 终端 C：KubeRay autoscaler
kubectl logs -n "$NS" "$HEAD" -c autoscaler -f

# 终端 D：按阶段重复执行 Ray 状态快照
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray status -v
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  ray list placement-groups --detail
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray list actors --detail
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray list nodes --detail
```

每个 case 的前、中、后至少保存：

```bash
kubectl get raycluster -n "$NS" "$RAYCLUSTER" -o yaml
kubectl get pods -n "$NS" -o wide
kubectl get events -n "$NS" --sort-by=.lastTimestamp
```

## 11. Case 1：无模型验证 Ray/KubeRay 1→2→1

### 11.1 目的

先排除 Kubernetes、Device Plugin、Ray NPU ID 分配和 KubeRay autoscaler 问题，不加载模型。每个 Ray actor
模拟一个 2 卡 rollout 实例。

### 11.2 执行

```bash
kubectl cp examples/kuberay/npu_autoscaler_smoke.py \
  "$NS/$HEAD:/tmp/npu_autoscaler_smoke.py" -c ray-head

kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  python /tmp/npu_autoscaler_smoke.py \
    --replicas 5 \
    --resources-per-replica 2 \
    --hold-seconds 300
```

前四个 actor 占满第一个 Worker，第五个 actor 产生 `NPU: 2` pending demand，KubeRay 应创建第二个整机 8 卡 Worker
Pod。

### 11.3 通过标准

- 第五个 actor ready 之前，`ray status -v` 出现 `NPU: 2` demand；
- KubeRay Worker replicas 从 1 变成 2；
- 两个 Worker Pod 位于不同裸机；
- 每个 actor 的 `accelerator_ids.NPU` 恰好两个 ID；
- 同一 hostname 内不同 actor 的 NPU ID 不重叠；
- 输出出现两个 hostname；
- smoke 结束并经过 `idleTimeoutSeconds=120` 加调度余量后，Worker Pod 回到 1。

状态码 3 表示 Ray 没有正确分配 NPU ID；不要继续真实模型测试。

## 12. Case 2：真实推理 Controller 1→5→1

### 12.1 目的

不启动训练，验证真实 vLLM 实例、批量 PG、版本状态、HTTP desired、drain、恢复和 Pod 缩容。

### 12.2 镜像和模型检查

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc '
  set -e
  cd /AReaL
  test -f examples/math/rollout_elastic_controller_spike.py
  test -d /shared/areal/models/Qwen2.5-1.5B-Instruct
  python -c "import ray, torch, torch_npu, vllm; print(ray.__version__); print(torch.npu.is_available())"
'
```

### 12.3 执行

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc '
  set -o pipefail
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
    rollout.elastic.startup_timeout_seconds=600 \
    2>&1 | tee /shared/areal/evidence/controller-spike.log
'
```

### 12.4 通过标准

- 日志输出 `Elastic Controller HTTP 1->5->1 spike passed`；
- 扩容时四个新增 PG 在等待第一个 PG ready 前都已提交；
- 每个 PG 申请 `NPU: 2`；
- 五个实例最终均 READY，`loaded_version == serving_version`；
- 缩容先 DRAINING，再删除 actor/PG；
- 原始实例保留；
- Worker Pod 最终从 2 回到 1；
- recovery 后 desired、实例数和 serving version 一致。

## 13. Case 3：训练4卡 + rollout单实例2卡的真实任务

### 13.1 目的和资源预期

启动真实 GRPO 训练，但先关闭自动应用建议，用人工 desired 做确定性验证：

```text
actor fsdp:d4p1t1                    = 4 NPU
rollout vllm:d1p1t2，initial=1       = 2 NPU
初始总量                              = 6 NPU，1 个 Worker Pod
desired=2                             = 8 NPU，仍是 1 个 Worker Pod
desired=5                             = 14 NPU，2 个 Worker Pod
```

### 13.2 启动训练

在终端 A 运行。`max_instances=7` 是为了后续资源不足测试；正常环境上限应设置为 6。

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc '
  set -o pipefail
  cd /AReaL
  export AREAL_SPMD_MODE=false
  python examples/math/gsm8k_rl.py \
    --config examples/math/gsm8k_grpo_npu.yaml \
    experiment_name=elastic-kuberay-validation \
    trial_name=manual-4train-2rollout \
    total_train_steps=100 \
    scheduler.type=ray \
    cluster.n_nodes=2 \
    cluster.n_gpus_per_node=8 \
    cluster.ray_device_resource=NPU \
    cluster.fileroot=/shared/areal/experiments \
    cluster.name_resolve.nfs_record_root=/shared/areal/name_resolve \
    actor.backend=fsdp:d4p1t1 \
    actor.path=/shared/areal/models/Qwen2.5-1.5B-Instruct \
    actor.weight_update_mode=disk \
    rollout.backend=vllm:d1p1t2 \
    rollout.fileroot=/shared/areal/experiments \
    rollout.max_concurrent_rollouts=384 \
    rollout.elastic.enabled=true \
    rollout.elastic.min_instances=1 \
    rollout.elastic.initial_instances=1 \
    rollout.elastic.max_instances=7 \
    rollout.elastic.max_concurrent_rollouts_per_instance=64 \
    rollout.elastic.max_total_concurrent_rollouts=384 \
    rollout.elastic.report_freq_steps=2 \
    rollout.elastic.auto_apply_scaling_recommendations=false \
    rollout.elastic.resource_provision_timeout_seconds=300 \
    rollout.elastic.startup_timeout_seconds=600 \
    2>&1 | tee /shared/areal/evidence/manual-4train-2rollout.log
'
```

模板已经把 head/worker 的 `HF_HOME` 固定到共享盘 `/shared/areal/huggingface`。联网准备阶段先用同版本 `datasets`
把 `openai/gsm8k` 下载到该目录；同步到内网后，训练继续使用配置文件默认的 `openai/gsm8k`。注意 `get_gsm8k_rl_dataset`
调用的是 `load_dataset(path=..., name="main")`，不能把任意 Hugging Face `save_to_disk` 输出目录直接当成
`train_dataset.path`。启动训练前先在 head 容器做离线预检：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc '
  export HF_DATASETS_OFFLINE=1
  python -c "from datasets import load_dataset; print(load_dataset(\"openai/gsm8k\", \"main\", split=\"train\")[:1])"
'
```

预检失败时先修复数据缓存或路径，不要启动训练。

### 13.3 找到 Controller HTTP 地址

终端 B：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc \
  "grep 'Callback server started on' /shared/areal/evidence/manual-4train-2rollout.log | tail -1"
```

日志会显示 `<HEAD_POD_IP>:<PORT>`。在同一个 head 容器内验证，不需要暴露 Service：

```bash
CALLBACK_ADDR=http://<HEAD_POD_IP>:<PORT>
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS "$CALLBACK_ADDR/elastic/instances"
```

### 13.4 初始状态通过标准

- actor 训练 role 使用 4 个 NPU；
- 一个 rollout 实例 READY，使用 2 个 NPU；
- Ray 总使用量约 6 NPU，只有一个 Worker Pod；
- `desired_instances=1`、`ready_instances=1`；
- READY 实例 `loaded_version == serving_version`；
- 训练 step 正常推进，disk 权重版本能被 rollout 加载。

## 14. Case 4：训练运行期间人工 1→5→1

### 14.1 扩容到 5

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS -X PUT "$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":5}'
```

轮询：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS "$CALLBACK_ADDR/elastic/instances"
```

预期链路：

```text
4 个新 PENDING identity
  -> provision_many 提交 4 个 NPU:2 PG
  -> 第一 Worker 剩余 2 卡，容纳 1 个新实例
  -> 其余需求触发第二个 8 卡 Worker Pod
  -> STARTING -> CATCHING_UP -> READY
```

当前 `provision_many` 有批次屏障：四个 PG 会先全部提交并等待全部 ready，然后才批量激活 worker/engine。因此正常资源充足时应看到四个
demand 很快一起跨过屏障；不能要求第一个 PG ready 后对应实例立刻进入 STARTING。

### 14.2 扩容通过标准

- desired 立即变为 5；
- Ray 在资源到位前看到四个新 PG；
- KubeRay Worker Pod 从 1 变成 2；
- 最终 `desired=5, ready=5`；
- 训练4卡 + rollout10卡，总计14卡；
- 所有 READY 实例版本一致、`pending_update_version=null`；
- 训练控制循环没有崩溃。

### 14.3 缩容到 1

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS -X PUT "$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":1}'
```

### 14.4 缩容通过标准

- 四个实例先进入 DRAINING，不再接收新任务；
- active/result/direct/update lease 清零后实例删除；
- 对应 Ray actor 和 PG 删除；
- AReaL 先收敛 `desired=ready=1`；
- Ray idle timeout 后 KubeRay Worker Pod 才从 2 回到 1；
- 缩容期间保留实例继续服务，训练继续推进。

## 15. Case 5：训练4卡时的资源不足

### 15.1 为什么先扩到 6 再申请 7

两台机器总计16卡，训练占4卡，因此最多6个两卡 rollout。先正常收敛到6，再申请7，可以让不足量稳定为一个 PG，避免当前批次屏障让已经 ready 的 PG
等待最慢项超时。

### 15.2 先扩到最大可用6

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS -X PUT "$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":6}'
```

等待 `desired=ready=6`，确认总共16卡已满。

### 15.3 再申请7

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS -X PUT "$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":7}'
```

### 15.4 通过标准

- AReaL 显示 `desired=7, ready=6` 和一个 PENDING 实例；
- Ray 显示一个 `NPU:2` pending PG/demand；
- KubeRay autoscaler 日志说明 Worker group 已达到 `maxReplicas=2`；
- 两个 Worker Pod 均保持 Ready，已有6个 rollout 和训练继续工作；
- 300 秒 resource timeout 后 pending PG 被删除，临时实例失败并移除；
- 下一轮 reconcile 会重新申请缺口，但 pending PG 数不会无限累积；
- `last_reconcile_error=null` 不能证明资源充足，必须同时看 AReaL timeout 日志和 Ray PG 状态。

测试完成后恢复：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS -X PUT "$CALLBACK_ADDR/elastic/desired-instances" \
  -H 'Content-Type: application/json' \
  -d '{"desired_instances":1}'
```

## 16. Case 6：真实训练指标自动应用建议

### 16.1 说明

先结束上一轮任务并使用新的 `trial_name` 启动。不要同时人工 PUT desired。第一份报告用于建立 startup
watermark，容量变化期间和收敛后的第一份报告也可能被 discard；这是预期安全行为。

### 16.2 启动自动模式

先在终端 A 用一次 `Ctrl-C` 结束 Case 3 的任务，等待 Controller 清理 actor 和 PG。不要用 `kill -9` 结束正常测试任务。 确认
Ray 中没有上一任务遗留的 AReaL placement group 后，启动新的自动模式任务：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc '
  set -o pipefail
  cd /AReaL
  export AREAL_SPMD_MODE=false
  python examples/math/gsm8k_rl.py \
    --config examples/math/gsm8k_grpo_npu.yaml \
    experiment_name=elastic-kuberay-validation \
    trial_name=auto-4train-2rollout \
    total_train_steps=100 \
    scheduler.type=ray \
    cluster.n_nodes=2 \
    cluster.n_gpus_per_node=8 \
    cluster.ray_device_resource=NPU \
    cluster.fileroot=/shared/areal/experiments \
    cluster.name_resolve.nfs_record_root=/shared/areal/name_resolve \
    actor.backend=fsdp:d4p1t1 \
    actor.path=/shared/areal/models/Qwen2.5-1.5B-Instruct \
    actor.weight_update_mode=disk \
    rollout.backend=vllm:d1p1t2 \
    rollout.fileroot=/shared/areal/experiments \
    rollout.max_concurrent_rollouts=192 \
    rollout.consumer_batch_size=128 \
    rollout.elastic.enabled=true \
    rollout.elastic.min_instances=1 \
    rollout.elastic.initial_instances=1 \
    rollout.elastic.max_instances=6 \
    rollout.elastic.max_concurrent_rollouts_per_instance=32 \
    rollout.elastic.max_total_concurrent_rollouts=192 \
    rollout.elastic.report_freq_steps=2 \
    rollout.elastic.auto_apply_scaling_recommendations=true \
    rollout.elastic.autoscaler_scale_up_cooldown_seconds=0 \
    rollout.elastic.autoscaler_scale_down_cooldown_seconds=30 \
    rollout.elastic.autoscaler_direction_change_cooldown_seconds=30 \
    rollout.elastic.resource_provision_timeout_seconds=900 \
    rollout.elastic.startup_timeout_seconds=600 \
    train_dataset.batch_size=128 \
    2>&1 | tee /shared/areal/evidence/auto-4train-2rollout.log
'
```

较大的 batch 和较小的单实例并发用于制造 rollout queue wait。实际模型吞吐不同，不能只靠等待时间判断；以生成的 report 为准。

### 16.3 观察

自动模式是一个新的 Controller 进程，回调端口也会变化。重新读取日志并更新 `CALLBACK_ADDR`：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- bash -lc \
  "grep 'Callback server started on' /shared/areal/evidence/auto-4train-2rollout.log | tail -1"
CALLBACK_ADDR=http://<HEAD_POD_IP>:<NEW_PORT>
```

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS "$CALLBACK_ADDR/elastic/scaling-recommendation"
kubectl exec -n "$NS" "$HEAD" -c ray-head -- \
  curl -fsS "$CALLBACK_ADDR/elastic/instances"
```

按时间关联：

```text
训练 step/version
  -> latest_report_version
  -> report.branch=scale_up
  -> last_autoscaler_decision.action=apply
  -> desired 增加
  -> PG demand
  -> 第二个 Worker Pod
  -> 新实例追平 serving_version
  -> last_autoscaler_decision.action=converged
```

### 16.4 通过标准

- 报告由真实训练 step 产生，不是仅空等；
- 首份报告出现 `discard: establishing startup watermark` 属于预期；
- 有效高等待窗口出现 `scale_up`；
- Controller 自动修改 desired，无人工 PUT；
- PG、Pod、READY 实例链路与 Case 4 一致；
- 所有 READY 实例最终版本一致；
- capacity 未稳定、旧窗口或 ready 快照不一致的报告被 discard，而不是错误应用。

如果多份有效报告始终为 `hold`，说明当前负载没有产生足够等待，不代表 autoscaler 故障。先保存报告，再逐步提高 batch
或降低单实例并发；不要同时修改多个参数。

## 17. Case 7：Worker Pod 故障替换

只在专用测试集群执行。不要删除 node-1 的 head Pod，也不要把 node-1 故障测试当成 HA 验收。

### 17.1 准备

在训练4卡、rollout desired=5 的稳定状态下，第二个 Worker Pod 通常只承载新增 rollout。先用 Ray actor/node 状态和 Pod
IP 确认，不能只按 Pod 创建时间猜测：

```bash
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray list actors --detail
kubectl exec -n "$NS" "$HEAD" -c ray-head -- ray list nodes --detail
kubectl get pods -n "$NS" -o wide
```

### 17.2 删除只承载 rollout 的 Worker Pod

```bash
kubectl delete pod -n "$NS" <ROLLOUT_ONLY_WORKER_POD>
```

### 17.3 通过标准

- Ray 旧 node/actor 变 DEAD；
- AReaL 连续健康探测失败后 fence 旧实例，停止向其路由新任务；
- KubeRay 因现有 demand 重建 Worker Pod；
- desired-state 创建新的 rollout role/实例；
- 新实例加载共享盘 checkpoint 并追平 serving version；
- READY 数允许短暂下降，但最终恢复到 desired；
- 其他实例和训练控制循环继续工作；
- 故障 Pod 上的在途请求允许失败，当前实现不会迁移或自动重试该请求；
- 旧 PG/actor 最终没有泄漏。

## 18. 清理验证资源

先停止训练任务，确认没有需要保留的证据，再删除实验资源：

```bash
kubectl delete -f /tmp/areal-kuberay-validation/raycluster-2x910b.yaml
kubectl delete -f /tmp/areal-kuberay-validation/shared-pvc.yaml
kubectl delete -f /tmp/areal-kuberay-validation/shared-pv.yaml
kubectl delete -f /tmp/areal-kuberay-validation/namespace.yaml
helm uninstall kuberay-operator -n kuberay-system
```

PV 回收策略是 `Retain`，删除 PVC 不会删除 NFS 数据。是否清理 `/shared/areal/evidence`、模型、数据集和实验 checkpoint
由验证负责人单独决定。不要删除 Kubernetes 数据目录或重置节点来清理业务文件。

## 19. 常见问题定位

| 现象                       | 首先检查                                   | 常见原因                                              |
| -------------------------- | ------------------------------------------ | ----------------------------------------------------- |
| node NotReady              | `journalctl -u kubelet`、Calico Pod        | swap、CRI socket、CNI、cgroup 不一致                  |
| Pod 一直 ContainerCreating | `kubectl describe pod`                     | CNI、PVC、Device Plugin、镜像导入失败                 |
| 节点没有 `Ascend910: 8`    | Device Plugin 日志、`npu-smi`              | runtime 顺序、socket 挂载、镜像版本不匹配             |
| Ray 只有 CPU 没有 NPU      | `ray.cluster_resources()`、worker 启动参数 | 缺少 `resources: NPU:8` 或 Worker 未注册              |
| 第五个两卡 actor 不扩 Pod  | `ray status`、autoscaler 日志              | 没有 pending demand、maxReplicas、Pod 调度失败        |
| 第二个 Worker Pod Pending  | `kubectl describe pod`                     | 第二台已有整机 NPU Pod、CPU/内存不足、标签/污点不匹配 |
| PG ready 但引擎启动超时    | AReaL startup 日志                         | 模型路径、vLLM/CANN、端口、共享盘问题                 |
| 新实例版本不一致           | `loaded_version`、checkpoint 日志          | 非 disk 模式、共享路径不同、checkpoint 不完整         |
| 自动报告一直 discard       | `last_autoscaler_decision`                 | startup watermark、容量未稳定、旧窗口、cooldown       |
| 资源不足后反复出现 PG      | AReaL timeout 和 Ray PG                    | 当前没有 backoff，reconcile 会重新申请缺口            |

分布式训练 hang 时先减少到最小复现并检查：

```bash
export TORCH_DISTRIBUTED_DEBUG=DETAIL
export HCCL_EXEC_TIMEOUT=600
export HCCL_CONNECT_TIMEOUT=120
```

本方案的训练4卡应被放在同一个 8 卡 Worker Pod 内，不应产生跨机 HCCL。若训练 role 被拆到两个 Pod，先检查 PG bundle、Ray
节点资源碎片和启动顺序，不要直接增加超时掩盖拓扑问题。

## 20. 最终验收清单

| Case                                | 结果 | 证据位置 | 备注 |
| ----------------------------------- | ---- | -------- | ---- |
| 两节点 Kubernetes/CNI               |      |          |      |
| 两节点 Device Plugin 各上报8卡      |      |          |      |
| 单卡 NPU Pod smoke                  |      |          |      |
| Ray/KubeRay 无模型 5×2卡，Pod 1→2→1 |      |          |      |
| 真实 Controller 1→5→1               |      |          |      |
| 训练4卡 + 初始 rollout2卡共6卡      |      |          |      |
| 训练期间人工 1→5→1                  |      |          |      |
| 最大6实例和第7实例资源不足          |      |          |      |
| 真实训练指标自动扩容                |      |          |      |
| rollout-only Worker Pod 故障替换    |      |          |      |

全部 case 通过后，可以认为已经在两台固定裸机上验证了 AReaL 到 Ray Autoscaler、KubeRay Worker Pod 和 rollout
生命周期的端到端链路。该结论不包含物理机自动创建、control-plane/Ray head HA、在途请求迁移或 head/GCS 灾备。

## 附录 A：独立开发环境的代码回归

本附录不属于两台空白裸机的安装顺序。只在已经安装完整 AReaL 开发依赖的环境执行，直接使用当前 Python 环境，不要求 `uv`：

```bash
python -m pytest -q \
  tests/test_elastic_config.py \
  tests/test_ray_scheduler.py \
  tests/test_rollout_elastic_autoscaler_spike.py \
  tests/infra/controller/elastic/
```

这些测试用 mock/fake 覆盖配置、批量资源申请、部分失败清理、状态机、内部自动决策和恢复文件；它们不启动真实 Kubernetes、KubeRay、NPU 或
vLLM。不要把整个 `tests/test_rollout_controller.py` 加入这个无模型门禁：该文件在 pytest 收集阶段会导入模型测试工具，本地
`/storage/openpsi/models` 缺少模型时会尝试从 Hugging Face 下载，包括大模型。Controller 和真实推理已经由第 12 节覆盖。

这里通过只能说明框架逻辑回归正常，不能替代第 3～17 节的端到端验收。测试环境使用的 Ray 版本不改变集群镜像必须固定 Ray 2.53.0 的要求。
