# JD HPC Discoverer 使用文档

## 📖 概述

JD HPC Discoverer 是 Volcano 项目中用于从京东云 HPC (High Performance Computing) 服务自动发现网络拓扑结构的组件。它能够将 HPC 的层级网络结构转换为 Kubernetes 的 HyperNode 资源，为 Volcano 调度器提供网络拓扑感知能力。

## 🏗️ 架构设计

### 网络拓扑层级

JD HPC 采用三层网络架构，专为大规模 GPU 训练优化：

```
Cluster (集群层)
├── POD (汇聚层)
│   ├── SU (Scale Unit - 汇聚组)
│   │   ├── Leaf Switch (GPU 0 网口)
│   │   ├── Leaf Switch (GPU 1 网口)
│   │   ├── ...
│   │   └── Leaf Switch (GPU 7 网口)
│   │   └── Spine Switch (64个，全互联)
│   └── SU (Scale Unit - 汇聚组)
│       ├── Leaf Switch (GPU 0 网口)
│       ├── Leaf Switch (GPU 1 网口)
│       ├── ...
│       └── Leaf Switch (GPU 7 网口)
│       └── Spine Switch (64个，全互联)
└── POD (汇聚层)
    ├── SU (Scale Unit - 汇聚组)
    └── SU (Scale Unit - 汇聚组)
```

### 网络通信模式

#### GPU 卡间通信
- **同号卡通信**：1号卡与1号卡通信，2号卡与2号卡通信，以此类推
- **网口映射**：每个服务器8个网口对应8个GPU，分别连接8个不同的Leaf交换机
- **汇聚组设计**：8个Leaf交换机组成一个SU（Scale Unit），实现1:1收敛比

#### 网络层级说明
- **SU (Scale Unit)**：8个Leaf交换机 + 64个Spine交换机，实现全互联
- **POD**：一组Spine交换机下所有服务器，无需跨层级即可互通
- **Cluster**：不同POD间通过Core交换机实现RDMA网络互通

### HyperNode 映射

| HPC 层级 | HyperNode Tier | 成员类型 | 说明 |
|----------|----------------|----------|------|
| Cluster  | 3 (TierCluster) | HyperNode | 集群根节点，跨POD通信 |
| POD      | 2 (TierPOD)     | HyperNode | 汇聚层，同POD内通信 |
| SU       | 1 (TierSU)      | Node      | 汇聚组，GPU卡间通信 |

### 网络拓扑优势

#### 1. 高性能通信
- **同号卡直连**：避免跨卡通信的延迟和带宽瓶颈
- **1:1收敛比**：Leaf和Spine交换机全互联，无阻塞通信
- **RDMA支持**：通过Core交换机实现跨POD的高效RDMA通信

#### 2. 可扩展性
- **SU级扩展**：每个SU可独立扩展，不影响其他SU
- **POD级隔离**：不同POD间通信通过Core交换机，实现网络隔离
- **集群级管理**：统一管理整个集群的网络拓扑

#### 3. 故障隔离
- **SU级故障**：单个SU故障不影响其他SU
- **POD级故障**：单个POD故障不影响其他POD
- **网络冗余**：多路径设计提供网络冗余

### 实际网络拓扑示例

以下是一个典型的8节点GPU集群的网络拓扑：

```
Cluster: "gpu-cluster-001"
├── POD: "pod-001"
│   ├── SU: "su-001"
│   │   ├── Leaf-0 (连接所有节点的GPU 0网口)
│   │   ├── Leaf-1 (连接所有节点的GPU 1网口)
│   │   ├── ...
│   │   ├── Leaf-7 (连接所有节点的GPU 7网口)
│   │   └── Spine-0~63 (64个Spine交换机全互联)
│   └── SU: "su-002"
│       ├── Leaf-0 (连接所有节点的GPU 0网口)
│       ├── Leaf-1 (连接所有节点的GPU 1网口)
│       ├── ...
│       ├── Leaf-7 (连接所有节点的GPU 7网口)
│       └── Spine-0~63 (64个Spine交换机全互联)
└── POD: "pod-002"
    ├── SU: "su-003"
    └── SU: "su-004"
```

#### GPU通信路径示例
- **同SU内通信**：Node-A的GPU-0 → Leaf-0 → Spine → Leaf-0 → Node-B的GPU-0
  - 说明：同SU内通过Spine交换机实现全互联，1:1收敛比，无阻塞通信
- **同POD跨SU通信**：Node-A的GPU-0 → Leaf-0 → Spine → Core → Spine → Leaf-0 → Node-C的GPU-0
  - 说明：同POD内跨SU通信需要经过Core交换机，因为不同SU的Spine交换机不直接相连
- **跨POD通信**：Node-A的GPU-0 → Leaf-0 → Spine → Core → Spine → Leaf-0 → Node-D的GPU-0
  - 说明：跨POD通信必须经过Core交换机，实现不同POD间的RDMA网络互通

## 🚀 快速开始

### 1. 环境准备

确保你的 Kubernetes 集群已安装：
- Volcano 调度器
- Volcano 网络拓扑控制器
- JD 云 HPC 服务访问权限

### 2. 创建认证密钥

#### 方法一：使用 kubectl 命令创建

```bash
# 创建包含 JD 云访问密钥的 Secret
kubectl create secret generic jdhpc-credentials \
  --from-literal=accessKey=YOUR_ACCESS_KEY \
  --from-literal=secretAccessKey=YOUR_SECRET_KEY \
  -n volcano-system
```

#### 方法二：使用 YAML 文件创建

创建 `jdhpc-credentials-secret.yaml` 文件：

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: jdhpc-credentials
  namespace: volcano-system
type: Opaque
data:
  # 使用 base64 编码的访问密钥
  # 注意：请将 YOUR_ACCESS_KEY 和 YOUR_SECRET_KEY 替换为实际的密钥
  # 可以使用命令进行编码：echo -n "YOUR_ACCESS_KEY" | base64
  accessKey: WU9VUl9BQ0NFU1NfS0VZ  # YOUR_ACCESS_KEY 的 base64 编码
  secretAccessKey: WU9VUl9TRUNSRVRfQUNDRVNTX0tFWQ==  # YOUR_SECRET_KEY 的 base64 编码
```

然后应用 Secret：

```bash
kubectl apply -f jdhpc-credentials-secret.yaml
```

#### 方法三：使用 kubectl 从文件创建

```bash
# 创建包含密钥的文件
echo "YOUR_ACCESS_KEY" > access-key.txt
echo "YOUR_SECRET_KEY" > secret-key.txt

# 从文件创建 Secret
kubectl create secret generic jdhpc-credentials \
  --from-file=accessKey=access-key.txt \
  --from-file=secretAccessKey=secret-key.txt \
  -n volcano-system

# 清理临时文件
rm access-key.txt secret-key.txt
```

#### 验证 Secret 创建

```bash
# 查看 Secret 是否创建成功
kubectl get secret jdhpc-credentials -n volcano-system

# 查看 Secret 详细信息（不显示敏感数据）
kubectl describe secret jdhpc-credentials -n volcano-system
```

#### 安全最佳实践

```bash
# 1. 使用环境变量避免在命令历史中暴露密钥
export JD_ACCESS_KEY="your-actual-access-key"
export JD_SECRET_KEY="your-actual-secret-key"

# 2. 创建 Secret（推荐方式）
kubectl create secret generic jdhpc-credentials \
  --from-literal=accessKey="$JD_ACCESS_KEY" \
  --from-literal=secretAccessKey="$JD_SECRET_KEY" \
  -n volcano-system

# 3. 清理环境变量
unset JD_ACCESS_KEY
unset JD_SECRET_KEY

# 4. 验证 Secret 内容（可选，仅用于调试）
kubectl get secret jdhpc-credentials -n volcano-system -o jsonpath='{.data.accessKey}' | base64 -d
kubectl get secret jdhpc-credentials -n volcano-system -o jsonpath='{.data.secretAccessKey}' | base64 -d
```

### 3. 配置 Discoverer

创建 JD HPC Discoverer 配置文件：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: jdhpc-discoverer-config
  namespace: volcano-system
data:
  config.yaml: |
    discoverers:
    - name: "jdhpc"
      source: "jdhpc"
      interval: "5m"
      config:
        endpoint: "https://hpc.jdcloud-api.com"
        regionId: "cn-north-1"
        vpcId: "vpc-12345678"
        zone: ["cn-north-1a", "cn-north-1b"]
        timeout: "30s"
      credentials:
        secretRef:
          name: "jdhpc-credentials"
          namespace: "volcano-system"
```

## ⚙️ 配置参数

### 基础配置

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|------|------|------|--------|------|
| `endpoint` | string | 否 | `hpc.jdcloud-api.com` | JD 云 HPC API 端点 |
| `regionId` | string | 是 | - | 云区域 ID |
| `vpcId` | string | 否 | - | VPC 网络 ID |
| `zone` | []string | 否 | - | 可用区列表 |
| `timeout` | duration | 否 | `30s` | API 请求超时时间 |

### 认证配置

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `accessKey` | string | 是 | JD 云访问密钥 |
| `secretAccessKey` | string | 是 | JD 云秘密密钥 |

### 发现配置

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|------|------|------|--------|------|
| `interval` | duration | 否 | `5m` | 拓扑发现间隔 |
| `source` | string | 是 | - | 发现器名称，必须为 "jdhpc" |

## 📝 使用示例

### 完整配置示例

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: jdhpc-complete-config
  namespace: volcano-system
data:
  config.yaml: |
    discoverers:
    - name: "jdhpc-prod"
      source: "jdhpc"
      interval: "2m"
      config:
        endpoint: "https://hpc.jdcloud-api.com"
        regionId: "cn-north-1"
        vpcId: "vpc-prod-12345"
        zone: 
          - "cn-north-1a"
          - "cn-north-1b"
          - "cn-north-1c"
        timeout: "60s"
      credentials:
        secretRef:
          name: "jdhpc-prod-credentials"
          namespace: "volcano-system"
```

### 多环境配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: jdhpc-multi-env-config
  namespace: volcano-system
data:
  config.yaml: |
    discoverers:
    # 生产环境
    - name: "jdhpc-prod"
      source: "jdhpc"
      interval: "5m"
      config:
        endpoint: "https://hpc.jdcloud-api.com"
        regionId: "cn-north-1"
        vpcId: "vpc-prod-12345"
        zone: ["cn-north-1a", "cn-north-1b"]
      credentials:
        secretRef:
          name: "jdhpc-prod-credentials"
          namespace: "volcano-system"
    
    # 测试环境
    - name: "jdhpc-test"
      source: "jdhpc"
      interval: "10m"
      config:
        endpoint: "https://hpc-test.jdcloud-api.com"
        regionId: "cn-north-2"
        vpcId: "vpc-test-67890"
        zone: ["cn-north-2a"]
      credentials:
        secretRef:
          name: "jdhpc-test-credentials"
          namespace: "volcano-system"
```

## 🔍 监控和调试

### 查看发现器状态

```bash
# 查看 HyperNode 资源
kubectl get hypernodes -n volcano-system

# 查看特定 HyperNode 详情
kubectl describe hypernode <hypernode-name> -n volcano-system
```

### 查看日志

```bash
# 查看网络拓扑控制器日志
kubectl logs -f deployment/volcano-topology-controller -n volcano-system

# 查看特定 discoverer 日志
kubectl logs -f deployment/volcano-topology-controller -n volcano-system | grep "jdhpc"
```

### 常见日志信息

| 日志级别 | 消息示例 | 说明 |
|----------|----------|------|
| INFO | `JD HPC discoverer initialized` | 发现器初始化成功 |
| INFO | `Starting JD HPC network topology discovery` | 开始拓扑发现 |
| INFO | `Created SU HyperNode` | 创建 SU 层 HyperNode |
| INFO | `Created POD HyperNode` | 创建 POD 层 HyperNode |
| INFO | `Created CLUSTER HyperNode` | 创建 CLUSTER 层 HyperNode |
| ERROR | `Failed to get credentials from secret` | 认证密钥获取失败 |
| ERROR | `Failed to fetch JD HPC data` | HPC 数据获取失败 |

## 🛠️ 故障排除

### 常见问题

#### 1. 认证失败

**问题**：日志显示 "Failed to get credentials from secret"

**解决方案**：
```bash
# 检查 Secret 是否存在
kubectl get secret jdhpc-credentials -n volcano-system

# 检查 Secret 内容
kubectl get secret jdhpc-credentials -n volcano-system -o yaml

# 重新创建 Secret
kubectl delete secret jdhpc-credentials -n volcano-system
kubectl create secret generic jdhpc-credentials \
  --from-literal=accessKey=YOUR_ACCESS_KEY \
  --from-literal=secretAccessKey=YOUR_SECRET_KEY \
  -n volcano-system
```

#### 2. 网络连接问题

**问题**：日志显示 "Failed to fetch JD HPC data"

**解决方案**：
```bash
# 检查网络连接
kubectl run debug-pod --image=busybox -it --rm -- nslookup hpc.jdcloud-api.com

# 检查防火墙规则
kubectl get networkpolicy -n volcano-system

# 验证端点配置
kubectl get configmap jdhpc-discoverer-config -n volcano-system -o yaml
```

#### 3. 配置错误

**问题**：日志显示 "JD HPC endpoint or regionId is not configured"

**解决方案**：
```bash
# 检查配置文件
kubectl get configmap jdhpc-discoverer-config -n volcano-system -o yaml

# 验证必需参数
# 确保 regionId 已配置
# 确保 endpoint 格式正确
```

#### 4. HyperNode 创建失败

**问题**：没有看到预期的 HyperNode 资源

**解决方案**：
```bash
# 检查节点 ProviderID 格式
kubectl get nodes -o custom-columns=NAME:.metadata.name,PROVIDER-ID:.spec.providerID

# 验证实例 ID 映射
kubectl logs deployment/volcano-topology-controller -n volcano-system | grep "Failed to find node name for instance"

# 检查 HPC 数据格式
kubectl logs deployment/volcano-topology-controller -n volcano-system | grep "No valid nodes found"
```

### 调试模式

启用详细日志：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-topology-controller-config
  namespace: volcano-system
data:
  config.yaml: |
    logLevel: 4  # 启用详细日志
    discoverers:
    - name: "jdhpc"
      source: "jdhpc"
      # ... 其他配置
```

## 📊 性能优化

### 发现间隔调优

根据集群规模和变化频率调整发现间隔：

```yaml
# 大规模集群，变化较少
interval: "20m"

# 中等规模集群，变化适中
interval: "10m"

# 小规模集群，变化频繁
interval: "5m"
```

### 资源限制

为网络拓扑控制器设置适当的资源限制：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: volcano-topology-controller
  annotations:
    volcano.sh/network-topology-mode: "hard" # 支持设置soft、hard
    volcano.sh/network-topology-highest-tier: "2"
spec:
  template:
    spec:
      containers:
      - name: topology-controller
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

## 📚 API 参考

### 配置结构

```go
type DiscoveryConfig struct {
    Source    string                 `yaml:"source"`
    Interval  time.Duration          `yaml:"interval"`
    Config    map[string]interface{} `yaml:"config"`
    Credentials *Credentials         `yaml:"credentials"`
}

type Credentials struct {
    SecretRef *SecretRef `yaml:"secretRef"`
}

type SecretRef struct {
    Name      string `yaml:"name"`
    Namespace string `yaml:"namespace"`
}
```
