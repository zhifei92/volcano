# 06 · 全景速查：二进制 / 控制器 / Webhook / 插件 / Action / 新类型

> 行号对应 tag **v1.15.3**。

## 1. 二进制（`cmd/`）

v1.9 有 4 个：`cli`、`controller-manager`、`scheduler`、`webhook-manager`。v1.15 新增 3 个。

| 二进制 | 状态 | 用途 | 入口 |
|---|---|---|---|
| `vc-agent` | 新增 | 节点侧 DaemonSet，负责在离线混部：CPU/内存 QoS、CPU Burst/Throttle、超卖上报、驱逐、网络 QoS | `cmd/agent/`，`pkg/agent/` |
| `agent-scheduler` | 新增 | 面向 AI Agent 场景的低时延"快路径"调度器：逐个 Pod 即时调度，多 worker 并发，带冲突感知绑定（ConflictAwareBinder）；与 vc-scheduler 通过 NodeShard 分工 | `cmd/agent-scheduler/`，`pkg/agentscheduler/`；设计文档 `docs/design/agent-scheduler.md` |
| `network-qos` | 新增 | 一个二进制两种用法：作为 CNI 插件运行，或作为运维 CLI | `cmd/network-qos/`，`pkg/networkqos/` |
| `vcctl` | 变更 | 新增 `jobtemplate`、`jobflow`、`pod` 子命令 | `cmd/cli/` |
| `scheduler` | 变更 | 新增 `--scheduler-sharding-mode/name`、`--gate-removal-worker-num`、`--disable-default-scheduler-config` | `cmd/scheduler/app/options/options.go` |
| `controller-manager` | 变更 | 默认启用 `"*,-sharding-controller"`，即 sharding controller 默认关闭 | `cmd/controller-manager/app/options/options.go:47` |

## 2. 控制器（`pkg/controllers/`）

| 控制器 | 状态 | 用途 |
|---|---|---|
| `cronjob-controller` | 新增 | `batch.volcano.sh/CronJob`，按 cron 周期性创建 vcjob，受 feature gate `CronVolcanoJobSupport` 控制 |
| `hyperNode-controller` | 新增 | 自动发现网络拓扑并维护 HyperNode，发现源支持 label、ufm |
| `sharding-controller` | 新增（默认关闭） | 把节点切分成 NodeShard。内置策略有 allocation-rate、node-limit、warmup |
| `colocation-config-controller` | 新增 | 把 `config.volcano.sh/ColocationConfiguration` 下发到匹配的 Pod |
| `pg-controller` | 大改 | 新增 StatefulSet informer；从上层工作负载继承注解和 minMember；从 Pod 注解解析 NetworkTopology |
| `job-controller` | 大改 | 新增分布式框架插件 `ray`、`hcclrank`（昇腾 HCCL rank）；把 PartitionPolicy 转换为 SubGroupPolicy；计算 networkTopology |
| `queue-controller` | 中改 | 层级队列状态联动；不再往 status 里写计数 |
| `jobflow` / `jobtemplate` / `gc` | 小改 | — |

## 3. Webhook 与准入策略

| 资源 | 路径 | 状态 |
|---|---|---|
| cronjobs | `/cronjobs/validate` | 新增 |
| hypernodes | `/hypernodes/validate` | 新增 |
| jobflows | `/jobflows/validate` | 新增 |
| podgroups | `/podgroups/validate` | 新增（v1.9 只有 mutate） |
| pods | `/pods/mutate` | 变更：注入队列准入 gate |
| queues | `/queues/validate` | 变更：增加层级校验、资源关系校验、root 保护 |

**新增基于 CEL 的 ValidatingAdmissionPolicy / MutatingAdmissionPolicy**，可替代部分 webhook 逻辑：

- Helm 模板：`installer/helm/chart/volcano/templates/{validating,mutating}_admission_policy.yaml`；
- 开关：`custom.vap_enable` / `custom.map_enable`；
- 策略本体：`installer/helm/chart/volcano/policy/`。

**在 k8s 1.19 上这两个开关必须关闭。**

## 4. 调度插件（`pkg/scheduler/plugins/`）

### 4.1 新增插件

| 插件 | 作用 | 主要参数 |
|---|---|---|
| `network-topology-aware` | 网络拓扑感知调度，实现了 HyperNodeOrder、HyperNodeGradient、BatchNodeOrder，见 [03](./03-network-topology-hypernode-subjob.md) | `weight`、`hypernode.binpack.*`、`hypernode.gradient.evict.max-hypernodes` |
| `resource-strategy-fit` | 按资源维度配置 MostAllocated/LeastAllocated 打分（可用 Pod 注解覆盖），另含两个子功能：<br>- **`sra`**：稀缺资源规避<br>- **`proportional`**：GPU:CPU:Mem 比例过滤，从 v1.9 的 predicates 迁移过来 | `resources.<res>.{type,weight}`、`sra.*`、`proportional.{enable,resources,resourceProportion}` |

**升级必改的配置**：v1.9 predicates 插件的 `predicate.ProportionalEnable` / `predicate.resources` 参数已删除，逻辑迁到了 `resource-strategy-fit` 的 `proportional.*` 下。内部基于 zone 的比例逻辑也写在 predicates 里，迁移时需要一并处理。

### 4.2 变化较大的已有插件

| 插件 | 要点 |
|---|---|
| `predicates` | 全面改用 k8s 原生 framework 插件；新增 VolumeBinding、**DRA**（Filter/PreFilter/Reserve/PreBind）；新增 Simulate* 扩展点；删除 proportional |
| `nodeorder` | 参数名不变，内部改用 k8s 原生打分插件 |
| `deviceshare` | 在 NVIDIA gpushare/vGPU（HAMi-core，新增 MIG）基础上，新增**昇腾 vNPU**（MindCluster 310P 和 HAMi Ascend 两套实现）、GPU 互斥规则，以及 KnownGeometries ConfigMap |
| `gang` | 新增 SubJobOrder/Ready/Pipelined 和 UnifiedEvictable |
| `capacity` | 层级队列、DRA 配额、`ancestorReclaimLevel`、VictimQueueOrder、UnifiedEvictable、Simulate*，见 [05](./05-queue-capacity-enqueue.md) |
| `proportion` | 新增 Preemptive、Simulate*、PrePredicate；增加 priority 和 Open 检查 |
| `overcommit` | 不计入被准入 gate 卡住的资源；只比较 Job 实际请求的那些资源维度 |
| `extender` | 新增 `allocateFuncVerb` / `deallocateFuncVerb`；可用 `managedResources` 限定只对指定资源调用 extender |
| `drf` / `binpack` / `usage` / `numaaware` | 以重构为主，参数不变（binpack 只是 import 改成了 `fwk`） |

新增的插件开关：`enabledHyperNodeOrder`、`enabledSubJobReady`、`enabledSubJobPipelined`、`enabledSubJobOrder`、`enabledHyperNodeGradient`。

## 5. Action

| Action | 状态 | 一句话说明 |
|---|---|---|
| `enqueue` | 不变 | PodGroup 从 Pending 转为 Inqueue |
| `allocate` | 大改 | 分为两条路径，见 [01](./01-allocate-action.md) |
| `backfill` | 小改 | 为 BestEffort 任务回填 |
| `preempt` | 改 | 事务化；新增拓扑感知抢占；跳过带拓扑约束的作业 |
| `reclaim` | 改 | 事务化；Preemptive 改为按 task 判断；不再调用 Allocatable |
| `shuffle` | 不变 | 唯一还会调用 VictimTasks 的 action |
| `gangpreempt` / `gangreclaim` | **新增** | 按 gang/bundle 粒度驱逐，并按 HyperNode 划定搜索域，见 [04](./04-preempt-reclaim-gang-eviction.md) |

## 6. 默认调度配置

`pkg/scheduler/util.go:38` 中内置的 `DefaultSchedulerConf`，以及 Helm 的 `volcano-scheduler.conf`，**与 v1.9 完全相同**：actions 为 `enqueue, allocate, backfill`，tier1 是 priority/gang/conformance，tier2 是 overcommit/drf/predicates/proportion/nodeorder。

所有新插件和新 action 都需要手动配置。agent-scheduler 有自己的配置（`actions: "allocate"`，只启用 predicates 和 nodeorder 两个插件）。

## 7. 混部（volcano-agent）

| 能力 | Feature | 入口 |
|---|---|---|
| CPU QoS / Burst / Throttle | `CPUQoS` / `CPUBurst` / `CPUThrottle` | `pkg/agent/events/handlers/{cpuqos,cpuburst,cputhrottle}` |
| 内存 QoS（cgroup v1/v2） | `MemoryQoS` | `handlers/{memoryqos,memoryqosv2}` |
| 网络 QoS | `NetworkQoS` | `handlers/networkqos` + `pkg/networkqos`（tc + eBPF） |
| 超卖 | `OverSubscription` | `handlers/oversubscription`、`pkg/agent/oversubscription` |
| 驱逐 / 资源上报 | `Eviction` / `Resources` | `handlers/{eviction,resources}` |

- QoS 等级分 LC / HLS / LS / BE 四档，通过 Pod 注解 `volcano.sh/qos-level` 指定。
- 节点标签：`volcano.sh/colocation` 开启混部，`volcano.sh/oversubscription` 开启超卖。

## 8. `pkg/scheduler/api` 新增的重要类型

| 类型 / 字段 | 位置 | 说明 |
|---|---|---|
| `SubJobInfo` / `SubJobID` / `SubJobGID` | `api/sub_job_info.go` | 子 gang |
| `HyperNodeInfo` / `HyperNodesInfo` / `HyperNodeInfoMap` | `api/hyper_node_info.go` | 拓扑树 |
| `NodeShardInfo` | `api/node_shard_info.go` | 分片 |
| `EvictionKind` / `EvictionContext` / `UnifiedEvictableFn` | `api/types.go:326-349` | 统一的驱逐上下文 |
| `HyperNodeOrderFn` / `HyperNodeGradientFor{Job,SubJob}Fn` / `SearchPurpose` | `api/types.go:355-416` | 拓扑相关扩展点 |
| `Simulate{Add,Remove}TaskFn` / `SimulatePredicateFn` / `SimulateAllocatableFn` | `api/types.go:387-399` | 抢占模拟 |
| `TaskInfo` 新增字段 | `api/job_info.go` | `TaskRole`、`DRAResreq`、`ResourceClaimKeys`、`SchGated`、`EvictionOccurred`、`JobAllocatedHyperNode`。**`PodVolumes` 已删除** |
| `JobInfo` 新增字段 | `api/job_info.go:381-385` | `AllocatedHyperNode`、`NetworkTopology`、`SubJobs`、`TaskToSubJob`、`MinSubJobs` |
| `ClusterInfo` 新增字段 | `api/cluster_info.go` | HyperNode 相关字段、`NodesInShard` |

## 9. 新增的 API 组（staging）

从 **v1.14** 开始，volcano 的 API 类型被放进主仓库的 `staging/src/volcano.sh/apis/`（v1.13 及以前依赖外部仓库 `volcano.sh/apis`）。目前包含以下 API 组：

- batch、bus、flow、scheduling、nodeinfo（v1.9 已有）
- topology（HyperNode）、shard（NodeShard）、config（ColocationConfiguration）、training、datadependency

内部 fork 过 apis 仓库，增加了 Reservation CRD（`scheduling.volcano.sh/v1alpha1`）。升级后改为在 staging 目录内维护这部分代码，并且需要重新执行 codegen。
