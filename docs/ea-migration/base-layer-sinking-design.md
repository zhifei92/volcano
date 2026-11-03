# 分层拆分第一步：把基础层的业务逻辑下沉到插件层（详细设计）

> 适用范围：内部 Volcano（`origin/gpu-numa`）升级到社区 v1.16（apiserver 1.19）。
> 关联文档：
> - `scheduler-decoupling-proposal.md`（总提案，3.1 / 3.2 / 3.3 节与本文直接相关）
> - `jobbackoff-plugin-design.md`、`job-lifecycle-plugin-design.md`、`node-metrics-plugin-design.md`（本文涉及但不重复的三项）
>
> 文中内部分支代码行号基于 `origin/gpu-numa`，社区代码行号基于 `v1.16.0`。

## 0. 结论

- **目标**：`api` / `framework` / `actions` / `cache` / `metrics` 这些**社区已有文件**里，不再出现任何业务概念（可回收、弹性、实验高优、SLA 整卡、在线超时、GPU 型号、JDos 注解等）。
- **做法**是三种手法，按优先级使用：
  1. **集中解析**：新建内部库 `pkg/scheduler/ea/profile` 和插件 `eaprofile`，统一解析业务注解，形成"业务画像"。插件、内部新增文件改为读画像，`JobInfo` / `QueueInfo` / `NodeInfo` 上不再挂业务字段。
  2. **翻译成社区字段**：在线超时翻译成 `task.SchGated`，受害者资格翻译成 `task.Preemptable`。v1.16 的 action 原样就能处理，**不用改 action**。
  3. **挂到通用钩子**：
     - 抢占者过滤用 `ActionExcludedFn`；
     - 受害者细则用 `PreemptableFn` / `ReclaimableFn`；
     - PendingReason 分类放到 capacity 的 `OnSessionClose`；
     - 统计类读取放到 `JobUpdateHook`。
- **分两段实施**：
  - **1a**：在 `gpu-numa` 上做纯搬迁，行为不变，可以先上线验证。
  - **1b**：移植到 v1.16 时做语义翻译，社区 action 不带内部改动。
- **留在基础层的**只能是通用扩展点：`ActionExcluded`、`JobUpdateHook`、`Overused(queue, job)`、SchedulingUnit 系列。用 CI 守卫保证不回退。
- **工作量约 10.5 人天**，不含已有单独文档的 jobbackoff、job-lifecycle、node-metrics。
- **`eaprofile` 插件的完整实现**（目录结构、画像表、翻译、钩子、配置自检、指标、测试）见 4.6。
- **具体改造示例**（`task-property` 插件、preempt action、一个抢占场景的前后对比、其他插件的改法）见附录 A。
- **1b 继续使用 v1.16 的 `preempt` / `reclaim`，不切换到 `gangpreempt` / `gangreclaim`**。能力对照和原因见附录 B，待确认项见第 10 节第 10 条。

## 1. 范围与分层定义

| 层 | 内容 | 允许什么 |
|---|---|---|
| **L0 社区文件** | `pkg/scheduler` 下所有在 `v1.16.0` 中存在的文件 | 只允许通用扩展点：不出现业务名词，默认什么都不做，可以向社区提 PR。内部必须加的，放进同目录的 `*_ext.go` 新文件 |
| **L1 内部库** | 新建 `pkg/scheduler/ea/profile`、`pkg/scheduler/ea/gpumodel` | 纯函数、业务注解解析、会话级画像表；不依赖任何插件包 |
| **L2 内部插件** | `pkg/scheduler/plugins/*` 中的内部插件，以及新增的 `eaprofile` | 业务策略本体，可以依赖 L1 |
| **内部新增 action 文件** | `actions/preempt/relax.go`、`actions/allocate/allocate_replica_gang.go`、`actions/reserve/*` | 整个文件是内部新增的，不和社区冲突；可以依赖 L1，**不能**依赖 L2 |

依赖方向：**L0 ← L1 ← L2**，内部 action 文件只能 → L1。L0 不得 import `pkg/scheduler/ea/...`。

**本文不包含：**
- action 里 `IsSuperPodJob` 触发的拓扑感知分支。它是 SuperPod 拓扑能力本身，移植时对接 v1.16 的 HyperNode / 网络拓扑，属于第二步。
- `pluginutil.WriteDetail` 日志改造。它不是业务逻辑，单独处理。
- replica-gang。它是通用的 SchedulingUnit 扩展点加插件，已单独分析。

## 2. 现状盘点

内部分支相对社区基线 `60a7f7536` 的改动规模（不含插件、测试和 archimedes-scheduler）：

| 文件 | 新增行数（约） |
|---|---|
| `api/job_info.go` | 800 |
| `framework/session.go` | 690 |
| `cache/cache.go` | 700 |
| `framework/session_plugins.go` | 470 |
| `api/node_info.go` | 235 |
| `api/constants.go` | 180 |
| `actions/preempt/preempt.go` | 580 |
| `actions/reclaim/reclaim.go` | 460 |
| `actions/allocate/allocate.go` | 290 |

其中通用扩展点（`IsActionExcluded`、SchedulingUnit、Simulate 系列、Statement 扩展等）可以留下。需要下沉的业务逻辑如下。

### 2.1 api 层的业务字段与解析

| 业务字段 / 函数 | 位置 | 消费方 |
|---|---|---|
| `JobInfo.ReclaimAble` | `job_info.go:605` | capacity 19 处、allocate 5、queue-reclaim 4、tdm 3、relax 3、preempt 2，以及 slaNode / property / notebook / cooldown / reclaim_victim / cache 各 1 |
| `JobInfo.Elasticable` | `job_info.go:606` | capacity 13、allocate 3、queue-reclaim 3、elastic 2，以及 gang / slaNode / property / cooldown / reclaim / cache 各 1 |
| `JobInfo.Preemptable`（**内部语义**：可被队列内高优抢占） | `setDefaultPreemptProperty` | preempt 5、tdm 3、allocate 2，以及 slaNode / elastic / capacity / relax |
| `JobProperty`、`JobType` | `job_info.go:613-614` | property、notebook、cooldown |
| `ReqSLAMachine` | `job_info.go:615` | jobbackoff、`metrics/job_backoff.go`、queue-reclaim、slaNode，以及 session / allocate / preempt / reclaim（只用于指标标签） |
| `PermitedOnGPU` | `job_info.go:610` | reserve |
| `setDefaultPreemptProperty`（按 `JobProperty` 推导可回收、可抢占、弹性） | `job_info.go:631-653` | 上述三个字段的来源 |
| `TaskInfo.Elastic`（Pod 级弹性） | `job_info.go:244`，`extractElasticable(pod)` | preempt、elastic、queue-reclaim 等 |
| `TaskInfo.OnlineTaskTimeout` | `getOnlineQueuePodTimeoutStatus`，读 JDos 注解 | allocate、backfill、preempt、reclaim |
| `TaskInfo.GPUResID`、`ResreqReplaceScalar`、`GetGPUTypeID`、`GetGPUDeviceName`、`IsTrain` | `job_info.go`、`resource_info.go` | capacity 16 处，reserve、relax、priority、predicates、queue-reclaim、slaNode |
| `IsElasticInferJob`、`IsReplicaGangJob` | `api/helpers.go:127` | allocate、jobbackoff、replica-gang |
| `QueueInfo.SkipQuotaPreempt`、`PreemptLow`、`HasReclaimableJob` | `api/queue_info.go` | preempt、relax、capacity |
| `NodeInfo.SetReserveNodeInfo`、`SetReserveRatio`、`HasGPUTask`、GPU 型号 | `api/node_info.go:675-708` | reserve、queue-reclaim、slaNode、predicates（proportional）、metrics（明细见 4.2.1） |

### 2.2 action 里的业务判断

| 判断 | 位置 |
|---|---|
| 抢占者过滤："作业可回收就不抢占；队列没开 `PreemptLow` 且队列内没有可回收或弹性作业时也不抢占" | `preempt.go:150`、`relax.go:42` |
| 跨作业受害者：`ReclaimAble \|\| Preemptable \|\| (Elasticable && task.Elastic)` | `preempt.go:239` |
| 作业内受害者：`task.Preemptable \|\| task.Elastic` | `preempt.go:304` |
| relax 受害者：`!ReclaimAble && Preemptable`，跳过弹性部分，且 GPU 型号必须相同 | `relax.go:235-246` |
| reclaim 受害者：作业级 `ReclaimAble \|\| Elasticable` | `reclaim_victim.go:45` |
| 在线超时任务跳过 | `allocate.go:240`、`backfill.go:153/168`、`preempt.go:162/280`、`reclaim.go:142` |
| PendingReason 分类：`NotGuaranteeMin`，以及超用时按可回收区分 `NotEnoughQuota` / `WaitingForMin`，并写 condition 和 phase | `allocate.go:221-225`、`allocate.go:504-516` |
| 退避门控 | `allocate.go:116`、`preempt.go:140`、`reclaim.go:128`（见 jobbackoff 文档） |

### 2.3 framework / cache / metrics 里的业务逻辑

| 内容 | 位置 | 去向 |
|---|---|---|
| `addJobTimeStamp`、`MarkPodGroupDirty`、`OnSessionPreClose` 等 | `framework/session.go`、`job_updater.go` | job-lifecycle 文档 |
| `HandleJobDelayScheduling` | `framework/session.go:959` | **唯一调用方是 delayschedule 插件**，原样剪切过去 |
| `statisticDelayMetric`（按 `ReqSLAMachine` 打标签） | `framework/session.go:609` | job-lifecycle 文档 |
| `UpdateNodeInfoMetrics`（按 `ReclaimAble` / `Elasticable` 统计可回收资源）和后台协程 | `cache/cache.go:2050-2070` | node-metrics 文档 |
| `metrics/job_backoff.go`、`metrics/job.go`、`metrics/node.go` | `metrics/` | 各自的插件 |
| capacity 的 PendingReason 改写 | `plugins/capacity/capacity.go:674-718`（`OnSessionPreClose`） | 见 4.4 节 |

## 3. 总体设计

```
            ┌──────────────── L2 内部插件 ─────────────────────────────────┐
            │ eaprofile（tier1 第一位）                                    │
            │   OnSessionOpen : 建画像表 → 翻译 SchGated / Preemptable     │
            │                   → 回写 job.Preemptable（内部语义）         │
            │   ActionExcludedFn : 抢占者过滤（只拦 preempt）              │
            │   PreemptableFn / ReclaimableFn : 受害者细则                 │
            │ capacity / queue-reclaim / tdm / slaNode / property / ...   │
            │   读 profile.Of(job) 代替 job.ReclaimAble 等                 │
            └───────────────┬──────────────────────────────────────────────┘
                            │ 依赖
            ┌───────────────▼──────── L1 内部库 ───────────────────────────┐
            │ ea/profile  : ParseJob / ParseQueue / TaskElastic / Of ...   │
            │ ea/gpumodel : TaskModel / NodeModel / RenameByLabel ...      │
            └───────────────┬──────────────────────────────────────────────┘
                            │ 只依赖 api 类型
            ┌───────────────▼──────── L0 社区文件 ─────────────────────────┐
            │ api / framework / actions / cache / metrics                  │
            │ 只保留通用扩展点：ActionExcluded、JobUpdateHook、            │
            │ Overused(queue, job)、SchedulingUnit 系列                    │
            └──────────────────────────────────────────────────────────────┘
```

## 4. 详细设计

### 4.1 手法一：业务画像（`ea/profile` + `eaprofile` 插件）

#### 4.1.1 库接口

```go
package profile // pkg/scheduler/ea/profile

type JobProfile struct {
	Property           JobProperty // prod-high / prod-medium / exp-high / exp-medium / elastic-sla
	Type               JobType     // online_infer / offline_infer / train
	Reclaimable        bool        // 原 ji.ReclaimAble
	InQueuePreemptable bool        // 原 ji.Preemptable：可被队列内高优抢占
	Elastic            bool        // 原 ji.Elasticable
	ReqSLA             bool        // 原 ji.ReqSLAMachine
	PermitOnGPU        uint8       // 原 ji.PermitedOnGPU
}

type QueueProfile struct {
	SkipQuotaPreempt bool
	PreemptLow       bool
	HasReclaimable   bool // 原 QueueInfo.HasReclaimableJob，会话内预计算
}

// 纯函数：从 job_info.go / queue_info.go 原样搬来，包含 setDefaultPreemptProperty。
func ParseJob(pg *api.PodGroup) JobProfile
func ParseQueue(q *scheduling.Queue) QueueProfile

// Pod 级属性，内部缺省值与 api 原实现一致。
func TaskElastic(pod *v1.Pod) bool          // 原 extractElasticable(pod)
func TaskPodPreemptable(pod *v1.Pod) bool   // 原内部 GetPodPreemptable，缺省 false
func TaskOnlineTimeout(pod *v1.Pod) bool    // 原 getOnlineQueuePodTimeoutStatus

func IsElasticInferJob(job *api.JobInfo) bool // 原 api.IsElasticInferJob

// 会话画像表：查表，查不到时现场解析（不依赖插件开会话的顺序）。
func Of(job *api.JobInfo) JobProfile
func QueueOf(q *api.QueueInfo) QueueProfile
```

**会话画像表的实现：**
- 包级变量 `current atomic.Pointer[sessionTable]`，表中有 `jobs map[api.JobID]JobProfile` 和 `queues map[api.QueueID]QueueProfile`。
- `eaprofile.OnSessionOpen` 构建新表并 `Store`；`OnSessionClose` 中 `Store(nil)` 清空，避免插件被移出配置后一直读到旧表。实现细节见 4.6.3。
- `Of(job)` 先查表，查不到时调用 `ParseJob(job.PodGroup)`。
  - 不写回表，避免并发写。preempt / reclaim 的拓扑感知路径有 worker 并发。
  - 画像表构建好以后只读，不需要加锁。
- **为什么不挂回 `JobInfo`**：v1.16 的 `JobInfo` 没有扩展字段，挂回去就要改社区文件，和目标矛盾。

**`PendingReason`、`LastReason`、`TargetScheduleTime`、`RunningAt` 不进画像。** 它们是调度过程中产生的状态，不是业务属性。按 job-lifecycle 文档的结论，这些字段保留在 `api/job_info_ext.go`（L0 的 ext 文件）。

#### 4.1.2 消费方替换

消费方是机械替换，规则如下：

| 原写法 | 新写法 |
|---|---|
| `job.ReclaimAble` | `profile.Of(job).Reclaimable` |
| `job.Elasticable` | `profile.Of(job).Elastic` |
| `job.Preemptable`（内部语义） | `profile.Of(job).InQueuePreemptable` |
| `job.JobProperty` / `job.JobType` / `job.ReqSLAMachine` / `job.PermitedOnGPU` | `profile.Of(job).Property` / `.Type` / `.ReqSLA` / `.PermitOnGPU` |
| `task.Elastic` | `profile.TaskElastic(task.Pod)` |
| `queue.PreemptLow` / `queue.SkipQuotaPreempt` / `queue.HasReclaimableJob(...)` | `profile.QueueOf(queue).PreemptLow` / `.SkipQuotaPreempt` / `.HasReclaimable` |
| `api.IsElasticInferJob(job)` | `profile.IsElasticInferJob(job)` |

涉及的插件：capacity、queue-reclaim、tdm、slaNode、property、notebook、cooldown、elastic、gang、reserve、jobbackoff。内部新增的 action 文件 `relax.go`、`reclaim_victim.go` 也要替换，在 1a 阶段连社区 action 里的读取一起替换。

**热点函数里的写法**：capacity 的 `ReclaimableFn` 等函数会在循环里反复调用 `Of`。在函数开头取一次 `p := profile.Of(job)`，后面复用。查一次 map 的开销远低于原来 `Clone` 的开销，可以忽略。

### 4.2 GPU 型号与节点画像（`ea/gpumodel` + 画像表扩展）

#### 4.2.1 现状

基础层里与 GPU 型号、节点类型相关的逻辑分三组，全部依赖私有模块 eaframework（`coding.jd.com/EasyAlgorithm/eaframework/resource`，v1.5.27）中写死的映射表：

| 组 | 函数 / 字段 | 位置 | 做什么 |
|---|---|---|---|
| 任务 | `TaskInfo.GPUResID`（在 `NewTaskInfo` 中由 `GetGPUTypeID` 算出） | `job_info.go:243` | 有 `jdos.jd.com/gpu.model` 标签时（依次查 labels、annotations、nodeSelector）用 `GPUModuleToResNum[标签值]`，否则扫描 `Resreq` 中第一个在 `GPUResNameToID` 里的标量资源 |
| | `GetGPUDeviceName` | `job_info.go` | 有标签时用 `GPUModuleToDevice[标签值]`，否则用 `GPUIDToResName[GPUResID]` |
| | `ResreqReplaceScalar` | `job_info.go` | 有标签且型号已知时，把 `Resreq` 里的 `GPUModuleToDevice[标签值]`（如 `nvidia.com/gpu`）改名为 `GPUIDToResName[id]`（如 `nvidia.com/V100`），用于按型号记配额 |
| | `IsTrain` | `job_info.go` | Pod 标签 `easy-algo` 或 `easyalgo.jd.com/hybrid-app-type` 等于 `train` |
| 节点 | `GPUTypeMark`、`GPUResourceName` | `node_info.go`，`NewNodeInfo` / `SetNode` 中赋值 | 节点上的型号 ID 和设备资源名，规则与任务一致：节点标签优先，其次看 `Allocatable` |
| | `ReserveForSLATask` / `SloTask` / `NtbTask` / `CPUTask` | `SetReserveNodeInfo` | 节点标签 `scheduling-type` 取值为 `slaNode` / `bufferNode` / `notebook` / `permitCPUNode` |
| | `ResourceNameGPU`、`RatioCPU`、`RatioMem` | `SetReserveRatio` | 节点上只有一个 GPU 厂商时，按 `Allocatable` 算出每卡对应的 CPU（毫核）和内存（GiB×1000），作为比例校验的缺省值 |
| | `HasGPUTask(jobID)` | `node_info.go:675` | 遍历 `node.Tasks`，判断某作业在该节点上是否有 GPU 任务 |
| 资源 | `Resource.GetGPUTypeID`、`GetGPUCanonicalAndDeviceResourceNames`、`ReplaceScalar` | `resource_info.go` | 由资源名反查型号；求 Pod/API 中的规范资源名和型号资源名；改名 |
| | `FilteredIgnoredScalarResources`、`IntersectionWithIgnoredScalarResources` | `resource_info.go` | 求两个资源的交集，忽略 `pods` |

消费方（不含 `api` 内部互相调用）：

| 消费方 | 用到的 |
|---|---|
| reserve | `GPUResID`、`GPUTypeMark`、`IsTrain`、`HasGPUTask`、`PermitedOnGPU`、`GPUIDToResName` |
| queue-reclaim | `GPUTypeMark`、`GPUResourceName`、`GPUResID`、`GetGPUDeviceName`、`IsTrain`、`ReserveFor*`、`GPUResNameToID` / `GPUIDToResName` |
| slaNode | `ReserveForSLATask`、`IsTrain`、`GetGPUDeviceName` |
| predicates（proportional） | `GetGPUDeviceName`、`ResourceNameGPU`、`RatioCPU`、`RatioMem` |
| capacity | `ResreqReplaceScalar`（16 处）、`GetGPUCanonicalAndDeviceResourceNames`、`ReplaceScalar`、`IntersectionWithIgnoredScalarResources` |
| priority | `GPUResID > 0`（TaskOrder 中 GPU 副本优先） |
| relax（内部 action 文件） | `GPUResID` 相等比较 |
| replica-gang、`api/scheduling_unit.go` | `GetGPUCanonicalAndDeviceResourceNames`、`ResreqReplaceScalar` |
| `metrics/metrics.go`、`metrics/queue.go`、`metrics/node.go` | `ScalarResourceTypesConvert`、几个 vGPU 资源名常量、`GPUTypeMark` |
| archimedes（`archimedes-scheduler/pkg/util/util.go`） | `api.LookupGPUModel`、`IsTrain`、`GetGPUTypeID` |

**读代码时发现的几个事实：**

1. **节点信息本来就是每个会话重算一次。** `NodeInfo.Clone` 调用 `NewNodeInfo(ni.Node.DeepCopy())`，而 `NewNodeInfo` 每次都会执行 `SetReserveNodeInfo`、`GetGPUTypeID`、`SetReserveRatio`。Snapshot 每个会话都会克隆所有节点，所以挪进画像表后计算量不变，cache 侧处理节点更新时还能少算一次。
2. **eaframework 中调度器实际用到的只有映射表。** `ScalarResourceTypesConvert` 的特殊转换器只在第二个参数非空时生效，而调度器调用时第二个参数一律传 `""`，所以全部可以用表驱动替代。
3. **reserve 插件疑似有缺陷。** 它用 `GPUIDToResName[node.GPUTypeMark]` 查 `node.Idle`，即型号资源名，比如 `nvidia.com/V100`。但 jdos 节点的 device plugin 上报的是 `nvidia.com/gpu`（`GPUModuleToDevice`），所以在 jdos 节点上永远查到 0，"节点有空闲 GPU"的判断（`reserve.go:262`、`reserve.go:285`）一直不成立。迁移时**先等价保留**，修复单独立项（第 10 节第 8 条）。
4. **v1.16 的 proportional 已经从 predicates 挪到 `resource-strategy-fit`，配置格式也变了。** 见 `v1.15-feature-inventory.md` 和 `v1.15-guide/06`。本节只处理它对节点画像的依赖，配置迁移不在本文范围。

#### 4.2.2 设计概览

```
ea/gpumodel  叶子包，只依赖 k8s.io/api/core/v1
             型号注册表（内置表 + 可选配置文件，支持热加载）
             纯函数：标签/资源名 → 型号，型号 → 资源名/设备名/类型
                 ▲
ea/profile   画像表扩展：NodeProfile、任务型号、按型号改名后的请求、IsTrain
             在 eaprofile 的 OnSessionOpen 中与作业画像一起构建
                 ▲
插件          reserve / queue-reclaim / slaNode / capacity / priority / predicates(→resource-strategy-fit) ...
             读 profile.NodeOf / profile.TaskGPU / profile.TaskRequest，不再读 NodeInfo/TaskInfo 上的内部字段
```

- **`gpumodel` 设计成叶子包，不 import volcano 的 `api`。** 这样：
  - 任何地方都能用，包括内部新增的 `api/scheduling_unit.go`，不会产生 `api → gpumodel → api` 的循环依赖；
  - 它的单测不需要构造 `JobInfo`。
- **会话缓存放在 `profile` 画像表里，不放在 `gpumodel` 里。** 原因是 `gpumodel` 只管"规则"，`profile` 管"本会话每个对象的结果"，和作业画像同一套生命周期（4.6.3）。
- **全部内容都可以在 1a 阶段落地。** 不依赖任何 v1.16 机制，同时把 eaframework 从 `go.mod` 里去掉。archimedes 也要一起切换，否则 eaframework 去不掉。

#### 4.2.3 型号注册表

**数据模型：**

```go
package gpumodel // pkg/scheduler/ea/gpumodel

type ID uint8

const None ID = 0

const LabelKey = "jdos.jd.com/gpu.model"

type Model struct {
	ID        ID
	Label     string          // jdos 标签值，如 "V100"；为空表示没有标签映射（如 K40）
	Type      string          // 业务类型，如 "v100"；用于指标的 gpu_type 标签
	Resource  v1.ResourceName // 型号专属资源名，如 nvidia.com/V100；按型号记配额用
	Device    v1.ResourceName // 打了 jdos 标签的节点上 device plugin 实际上报的资源名，如 nvidia.com/gpu
	Canonical v1.ResourceName // Pod/API 中使用的规范资源名；缺省等于 Resource，HC 例外
	Virtual   bool            // 虚拟卡（vcuda-core）：有型号 ID，但不参与按型号的配额回收
}
```

| 原 eaframework 映射 | 新注册表中的对应 |
|---|---|
| `GPUModuleToResNum[label]` | `byLabel[label].ID` |
| `GPUModuleToDevice[label]` | `byLabel[label].Device` |
| `GPUIDToResName[id]` | `byID[id].Resource` |
| `GPUResNameToID[res]` | `byResource[res].ID` |
| `ScalarResourceTypesConvert(res, "")` | `typeOf[res]`，包含型号和虚拟资源 |
| `ScalarResourceTypes[type]`（只在求规范名时用） | `Model.Canonical` |
| `extractVendorSet` 用到的 `GPUIDToResName` 值 + `GPUJdsVRatioResoure` | 所有 `Model.Resource` 的厂商前缀 + `vendorResources` |
| `getSingleVendorResource` 的优先资源 | `preferredResources` |

**内置表**：从 eaframework v1.5.27 逐项抄写，ID 保持原值，方便对照日志。

```yaml
models:
- {id: 1,  label: "",        type: vcuda-core, resource: tencent.com/vcuda-core, virtual: true, canonical: ""}
- {id: 2,  label: P40,       type: p40,        resource: nvidia.com/gpu,       device: nvidia.com/gpu}
- {id: 3,  label: "",        type: k40,        resource: nvidia.com/K40}            # 原 GPUModuleToResNum 中没有 K40
- {id: 4,  label: V100,      type: v100,       resource: nvidia.com/V100,      device: nvidia.com/gpu}
- {id: 5,  label: A10,       type: a10,        resource: nvidia.com/A10,       device: nvidia.com/gpu}
- {id: 6,  label: A30,       type: a30,        resource: nvidia.com/A30,       device: nvidia.com/gpu}
- {id: 7,  label: A100,      type: a100,       resource: nvidia.com/A100,      device: nvidia.com/gpu}
- {id: 8,  label: A100-RDMA, type: a100-rdma,  resource: nvidia.com/A100-RDMA, device: nvidia.com/gpu}
- {id: 9,  label: H800,      type: h800,       resource: nvidia.com/H800,      device: nvidia.com/gpu}
- {id: 10, label: Ascend910, type: ascend910,  resource: huawei.com/Ascend910, device: huawei.com/Ascend910}
- {id: 11, label: "4090",    type: "4090",     resource: nvidia.com/4090,      device: nvidia.com/gpu}
- {id: 12, label: H20,       type: h20,        resource: nvidia.com/H20,       device: nvidia.com/gpu}
- {id: 13, label: G1,        type: g1,         resource: nvidia.com/G1,        device: nvidia.com/gpu}
- {id: 14, label: G3,        type: g3,         resource: nvidia.com/G3,        device: nvidia.com/gpu}
- {id: 15, label: 4090D,     type: 4090d,      resource: nvidia.com/4090D,     device: nvidia.com/gpu}
- {id: 16, label: G2,        type: g2,         resource: nvidia.com/G2,        device: nvidia.com/gpu}
- {id: 17, label: G4,        type: g4,         resource: nvidia.com/G4,        device: nvidia.com/gpu}
- {id: 18, label: G5,        type: g5,         resource: nvidia.com/G5,        device: nvidia.com/gpu}
- {id: 19, label: 910B4,     type: 910b4,      resource: huawei.com/910B4,     device: huawei.com/910B4}
- {id: 20, label: HC,        type: hc,         resource: huawei.com/HC,        device: huawei.com/Ascend910, canonical: huawei.com/Ascend910}
- {id: 21, label: L40S,      type: l40s,       resource: nvidia.com/L40S,      device: nvidia.com/gpu}
- {id: 22, label: P4,        type: p4,         resource: nvidia.com/P4,        device: nvidia.com/gpu}
- {id: 23, label: MLU590,    type: mlu590,     resource: cambricon.com/mlu,    device: cambricon.com/mlu}
- {id: 24, label: G6,        type: g6,         resource: nvidia.com/G6,        device: nvidia.com/gpu}
- {id: 25, label: G12,       type: g12,        resource: nvidia.com/G12,       device: nvidia.com/gpu}
- {id: 26, label: G14,       type: g14,        resource: nvidia.com/G14,       device: nvidia.com/gpu}
- {id: 27, label: S5000,     type: s5000,      resource: mthreads.com/gpu,     device: mthreads.com/gpu}
- {id: 28, label: H350,      type: h350,       resource: huawei.com/npu,       device: huawei.com/npu}
- {id: 29, label: G6-LQ,     type: g6-lq,      resource: nvidia.com/G6-LQ,     device: nvidia.com/gpu}
virtualResources:          # 不是型号，但指标需要识别类型
- {resource: vgpu.jdos.io/vcuda-core,   type: jdsvcore}
- {resource: vgpu.jdos.io/vcuda-memory, type: jdsvmem}
- {resource: vgpu.jdos.io/vcuda-ratio,  type: jdsvratio}
vendorResources: [vgpu.jdos.io/vcuda-ratio]                       # 额外参与"单厂商"判断的资源
preferredResources: [vgpu.jdos.io/vcuda-ratio, tencent.com/vcuda-core]
skipScalarResources: [vgpu.jdos.io/vcuda-core, vgpu.jdos.io/vcuda-memory, tencent.com/vcuda-memory]  # 指标中跳过
```

**配置文件与热加载：**

- `eaprofile` 新增参数 `eaprofile.gpu-model-file`。不配置时使用内置表；配置后读该文件，文件通过 ConfigMap 挂载。
- **加载时机**：每个会话 `OnSessionOpen` 的第一步调用 `gpumodel.ReloadIfChanged(path)`。它先 `stat` 文件，mtime 和 size 都没变就直接返回，开销是一次系统调用。
- **校验**：
  - ID 在 1 到 255 之间且唯一，0 保留给 None；
  - `label`、`resource`、`type` 各自唯一；
  - 有 `label` 的条目必须有 `device`。
- **校验失败时**：保留上一份注册表，指标 `volcano_eaprofile_gpu_model_config_error` 置 1，并打 Error 日志（按文件内容的哈希去重）。
- **切换方式**：注册表存放在 `atomic.Pointer[Registry]` 中，整体替换。会话中途不会变化，因为只在 `OnSessionOpen` 时加载。
- **ID 只在进程内使用**（比较、日志），没有写进注解或指标标签，所以配置里调整 ID 不影响外部。但为了日志可读，建议沿用原值。
- **收益**：新增型号原来要升级 eaframework、重新编译调度器，现在只要改 ConfigMap。

#### 4.2.4 `gpumodel` 接口

```go
func Current() *Registry
func ReloadIfChanged(path string) (changed bool, err error)

// LookupLabel 依次查 labels、annotations、nodeSelector，与原 api.LookupGPUModel 一致。
func LookupLabel(pod *v1.Pod) (string, bool)

// 任务
func (r *Registry) TaskModel(pod *v1.Pod, req map[v1.ResourceName]float64) ID
func (r *Registry) TaskDevice(pod *v1.Pod, id ID) v1.ResourceName
func (r *Registry) RenameByLabel(pod *v1.Pod, req map[v1.ResourceName]float64) (map[v1.ResourceName]float64, bool)

// 节点
func (r *Registry) NodeModel(node *v1.Node, alloc map[v1.ResourceName]float64) ID
func (r *Registry) NodeDevice(node *v1.Node, id ID) v1.ResourceName
func (r *Registry) SingleVendorResource(alloc map[v1.ResourceName]float64) (v1.ResourceName, bool)

// 资源
func (r *Registry) ModelByResources(res map[v1.ResourceName]float64) ID
func (r *Registry) ModelByResourceName(name v1.ResourceName) (ID, bool)
func (r *Registry) CanonicalAndModelResource(res map[v1.ResourceName]float64) (canonical, model v1.ResourceName, ok bool)
func (r *Registry) ResourceOf(id ID) v1.ResourceName
func (r *Registry) IsVirtual(id ID) bool
func (r *Registry) TypeOf(name v1.ResourceName) (string, bool)
func (r *Registry) SkipScalar(name v1.ResourceName) bool
```

与原函数的对应关系和语义：

| 原函数 | 新函数 | 语义要点（等价性测试逐条覆盖） |
|---|---|---|
| `TaskInfo.GetGPUTypeID` | `TaskModel` | 有标签时只看标签，**标签值未知返回 None**，不回退去扫描资源；没有标签时扫描 `req` 中值大于 0、且在 `byResource` 里的第一个资源 |
| `TaskInfo.GetGPUDeviceName` | `TaskDevice` | 有标签时返回 `byLabel[label].Device`，标签未知返回 `""`；没有标签时返回 `ResourceOf(id)` |
| `TaskInfo.ResreqReplaceScalar` | `RenameByLabel` | 只有"有标签、型号已知且不是 None"时才把 `Device` 改名为 `Resource`，否则返回 `false`，调用方直接用原 `Resreq` |
| `NodeInfo.GetGPUTypeID` / `GetGPUDeviceName` | `NodeModel` / `NodeDevice` | 规则与任务相同，只是标签只看 `node.Labels` |
| `getSingleVendorResource` | `SingleVendorResource` | 只统计值不为 0 的资源；厂商数不等于 1 时返回 false；同一厂商有多个资源时，取 `preferredResources` 中的第一个命中项 |
| `Resource.GetGPUTypeID` | `ModelByResources` | 同"没有标签"的扫描规则 |
| `GetGPUCanonicalAndDeviceResourceNames` | `CanonicalAndModelResource` | **原函数名里的"device"实际返回的是型号资源名（`GPUIDToResName[id]`），不是设备名**。新函数改名以免误用；`Canonical` 为空时返回 false（vcuda-core） |
| `ScalarResourceTypesConvert(res, "")` | `TypeOf` | 包含 `virtualResources` |

**一处有意的行为差异：** 原实现扫描资源时遍历的是 Go map，如果一个 Pod 同时请求了多种 GPU 资源，结果是随机的。新实现改为按资源名排序后取第一个，结果确定。正常的 Pod 不会同时请求多种 GPU，等价性测试中跳过这种情况，并在 7.1 中注明。

#### 4.2.5 画像表扩展（`ea/profile`）

```go
type NodeScheduleType uint8

const (
	NodeScheduleNone NodeScheduleType = iota
	NodeScheduleSLA       // scheduling-type=slaNode       原 ReserveForSLATask
	NodeScheduleBuffer    // scheduling-type=bufferNode    原 ReserveForSloTask
	NodeScheduleNotebook  // scheduling-type=notebook      原 ReserveForNtbTask
	NodeScheduleCPUPermit // scheduling-type=permitCPUNode 原 ReserveForCPUTask
)

func (t NodeScheduleType) Reserved() bool { return t != NodeScheduleNone }

type NodeProfile struct {
	GPU           gpumodel.ID      // 原 GPUTypeMark
	GPUDevice     v1.ResourceName  // 原 GPUResourceName
	ScheduleType  NodeScheduleType // 原 ReserveFor* 四个布尔值；同一标签只可能命中一个
	RatioResource v1.ResourceName  // 原 ResourceNameGPU
	RatioCPU      float64          // 原 RatioCPU，毫核/卡
	RatioMem      float64          // 原 RatioMem，GiB×1000/卡
}

func NodeOf(node *api.NodeInfo) NodeProfile
func TaskGPU(task *api.TaskInfo) gpumodel.ID
func TaskRequest(task *api.TaskInfo) *api.Resource // 原 ResreqReplaceScalar；返回值只读
func TaskIsTrain(pod *v1.Pod) bool                  // 原 TaskInfo.IsTrain
```

**`Build` 增加两部分：**

```go
func Build(jobs map[api.JobID]*api.JobInfo, queues map[api.QueueID]*api.QueueInfo,
	nodes map[string]*api.NodeInfo) BuildStats {
	reg := gpumodel.Current()
	// ...作业、队列画像同 4.6.3...
	t.nodes = make(map[string]NodeProfile, len(nodes))
	for name, n := range nodes {
		t.nodes[name] = parseNode(reg, n)
	}
	t.taskGPU = make(map[api.TaskID]gpumodel.ID)
	t.taskReq = make(map[api.TaskID]*api.Resource)
	seen := make(map[api.TaskID]struct{})
	addTask := func(task *api.TaskInfo) {
		if _, ok := seen[task.UID]; ok {
			return
		}
		seen[task.UID] = struct{}{}
		if g := reg.TaskModel(task.Pod, task.Resreq.ScalarResources); g != gpumodel.None {
			t.taskGPU[task.UID] = g
		}
		if renamed, ok := reg.RenameByLabel(task.Pod, task.Resreq.ScalarResources); ok {
			r := task.Resreq.Clone()
			r.ScalarResources = renamed
			t.taskReq[task.UID] = r
		}
	}
	for _, job := range jobs {
		for _, task := range job.Tasks {
			addTask(task)
		}
	}
	for _, n := range nodes {
		for _, task := range n.Tasks { // 作业不在 Snapshot 中的任务（如 PodGroup 缺失）也要覆盖
			addTask(task)
		}
	}
	// ...
}
```

- **为什么只存非 None 和改过名的条目**：绝大多数任务是 CPU 任务，或者没有 jdos 标签，不存可以省内存。会话里所有任务都已经在 `Build` 中处理过，所以查不到就说明是 None 或者没改名：
  - `TaskGPU` 查不到时，表存在就返回 None；表不存在（关闭阶段，或插件不在配置里）就现场计算。
  - `TaskRequest` 查不到时，表存在就直接返回 `task.Resreq`，和原函数一样返回原指针；表不存在就现场计算。
- **`node.Tasks` 中的任务是 `job.Tasks` 的克隆，但 UID 相同**，所以按 UID 查表对两边都适用。会话中 Statement 克隆出来的任务也保持原 UID。
- **`node.Tasks` 也要遍历**：作业不在 `ssn.Jobs` 中的任务仍然会留在节点上，比如 PodGroup 缺失的作业。只遍历 `job.Tasks` 的话，reserve 会把这类 GPU 任务当成 CPU 任务计数。
- **`parseNode`** 依次调用 `NodeModel`、`NodeDevice`、读 `scheduling-type` 标签、`SingleVendorResource`，再按原公式计算比例：

```go
func parseNode(reg *gpumodel.Registry, n *api.NodeInfo) NodeProfile {
	p := NodeProfile{}
	if n == nil || n.Node == nil {
		return p
	}
	p.GPU = reg.NodeModel(n.Node, n.Allocatable.ScalarResources)
	p.GPUDevice = reg.NodeDevice(n.Node, p.GPU)
	p.ScheduleType = parseScheduleType(n.Node.Labels[MachineScheduleTypeKey])
	if res, ok := reg.SingleVendorResource(n.Allocatable.ScalarResources); ok {
		if total := n.Allocatable.ScalarResources[res]; total != 0 {
			p.RatioResource = res
			p.RatioCPU = math.Max(n.Allocatable.MilliCPU, 0) / total
			p.RatioMem = math.Max(n.Allocatable.Memory, 0) / GiB / total * 1000
		}
	}
	return p
}
```

- **开销估算**：
  - 节点部分与原来 `NodeInfo.Clone` 中的计算量相同。
  - 任务部分：以 3 万个任务计，每个任务做几次 map 查找，约 5 到 10ms。7.4 节基准测试的目标是 `OnSessionOpen` 总耗时在 20ms 以内。
  - 如果超标，改为跨会话缓存：键用任务 UID 加 Pod 的 ResourceVersion，每个会话剔除已消失的任务。

#### 4.2.6 消费方改造

| 消费方 | 原写法 | 新写法 |
|---|---|---|
| reserve | `task.GPUResID > NoGPUResNum` | `profile.TaskGPU(task) != gpumodel.None` |
| | `node.GPUTypeMark` | `profile.NodeOf(node).GPU` |
| | `task.IsTrain()` | `profile.TaskIsTrain(task.Pod)` |
| | `job.PermitedOnGPU` | `profile.Of(job).PermitOnGPU` |
| | `node.HasGPUTask(job)` | 插件私有函数 `hasGPUTask(node, job)`，见 4.2.7 |
| | `earesource.GPUIDToResName[mark]` | `gpumodel.Current().ResourceOf(mark)`，先等价保留 4.2.1 第 3 条的行为 |
| queue-reclaim | `gpuTypeName(id)`（排除 None 和 VGPU） | `reg.ResourceOf(id)`，并用 `id == None \|\| reg.IsVirtual(id)` 排除 |
| | `node.GPUTypeMark`、`node.GPUResourceName` | `NodeOf(node).GPU`、`NodeOf(node).GPUDevice` |
| | `task.GetGPUDeviceName()` | `reg.TaskDevice(task.Pod, profile.TaskGPU(task))` |
| | `node.ReserveForSLATask \|\| ...`（四个） | `NodeOf(node).ScheduleType.Reserved()` |
| | `earesource.GPUResNameToID[name]` | `reg.ModelByResourceName(name)` |
| slaNode | `node.ReserveForSLATask`、`task.IsTrain()`、`node.GetGPUDeviceName()` | `NodeOf(node).ScheduleType == NodeScheduleSLA`、`TaskIsTrain`、`NodeOf(node).GPUDevice` |
| capacity | `t.ResreqReplaceScalar()` | `profile.TaskRequest(t)` |
| | `res.GetGPUCanonicalAndDeviceResourceNames()` | `reg.CanonicalAndModelResource(res.ScalarResources)` |
| | `res.ReplaceScalar(a, b)` | 插件私有函数 `replaceScalar(res, a, b)`，原样搬过来 |
| | `api.IntersectionWithIgnoredScalarResources` | 插件私有函数，忽略 `v1.ResourcePods`。这是唯一的调用方，所以不留在 api |
| priority（社区插件，内部改了 TaskOrder） | `lv.GPUResID > 0` | `profile.TaskGPU(lv) != gpumodel.None` |
| predicates proportional → v1.16 `resource-strategy-fit` | `node.GetGPUDeviceName()`、`node.ResourceNameGPU` / `RatioCPU` / `RatioMem` | `NodeOf(node).GPUDevice`、`NodeOf(node).RatioResource` / `RatioCPU` / `RatioMem`；按型号改名用 `gpumodel.LookupLabel` 读节点标签 |
| relax | `task.GPUResID != preemptor.GPUResID` | `profile.TaskGPU(task) != profile.TaskGPU(preemptor)` |
| replica-gang | `GetGPUCanonicalAndDeviceResourceNames` | `reg.CanonicalAndModelResource` |
| `api/scheduling_unit.go`（内部新增的 api 文件） | `t.ResreqReplaceScalar()` | 直接调用 `gpumodel.RenameByLabel`（叶子包，无循环依赖）。更好的做法是把汇总逻辑挪进 replica-gang 插件，届时改用 `profile.TaskRequest` |
| metrics | `ScalarResourceTypesConvert`、vGPU 常量、`GPUTypeMark` | `reg.TypeOf`、`reg.SkipScalar`、`NodeOf(node).GPU`；随 node-metrics 及队列指标的插件化一起搬走 |
| archimedes | `api.LookupGPUModel`、`IsTrain`、`GetGPUTypeID` | `gpumodel.LookupLabel`、`profile.TaskIsTrain`、`reg.NodeModel` |

改完后可以从 `api` 中删除：
- `TaskInfo` 的 `GPUResID`；
- `NodeInfo` 的 `GPUTypeMark`、`GPUResourceName`、`ResourceNameGPU`、`RatioCPU`、`RatioMem`、`ReserveFor*` 字段；
- 上述所有方法，以及 `vendorSet` 全局变量；
- `api` 包对 eaframework 的 import。

#### 4.2.7 reserve 插件改造示例

`HasGPUTask` 原本遍历 `node.Tasks`，改成插件私有函数后语义不变，只把字段读取换成查画像表：

```go
func hasGPUTask(node *api.NodeInfo, job api.JobID) bool {
	for _, t := range node.Tasks {
		if t.Job == job && profile.TaskGPU(t) != gpumodel.None {
			return true
		}
	}
	return false
}
```

**为什么不改成预先计算的计数表：** 原函数会看到 `node.Tasks` 中所有状态的任务。被驱逐的任务在会话里仍以 Releasing 状态留在节点上，但同时会触发 `DeallocateFunc`。如果用事件维护计数，驱逐后计数会减掉，结果就和原来不一样了。所以保持遍历，每次的开销和原来相同。

`getCandidateNodes` 与 predicate 的改动：

```go
for _, task := range node.Tasks {
	if profile.TaskGPU(task) == gpumodel.None {         // 原 task.GPUResID <= NoGPUResNum
		used.cpuPods++
		// ...
	}
}
rs.available[node.Name] = profile.NodeOf(node).GPU      // 原 node.GPUTypeMark

predictFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
	np := profile.NodeOf(node)
	if profile.TaskGPU(task) != gpumodel.None || np.GPU == gpumodel.None {
		return nil
	}
	isTrain := profile.TaskIsTrain(task.Pod)
	if !rs.enableServing && !isTrain {
		return nil
	}
	// ...
	if isTrain {
		if profile.Of(job).PermitOnGPU == profile.GPUNodeAdmit && rs.permitedOnGPU || hasGPUTask(node, task.Job) {
			return nil
		}
	}
	// ...
	if isTrain && !rs.permitRunOnFree {
		gpuRes := gpumodel.Current().ResourceOf(nodeGPUTypeMark) // nodeGPUTypeMark 来自 rs.available；等价保留，修复时改为 np.GPUDevice
		if node.Idle.ScalarResources[gpuRes] > 0 {
			return api.NewFitError(task, node, ErrNoPermit.Error())
		}
	}
	return nil
}
```

`AllocateFunc` / `DeallocateFunc` 中的 `event.Task.GPUResID > NoGPUResNum` 也改为 `profile.TaskGPU(event.Task) != gpumodel.None`。

#### 4.2.8 测试

| 测试 | 内容 |
|---|---|
| 注册表等价性 | 1a 阶段把 eaframework 保留为**仅测试依赖**：对内置表中的每个标签、每个资源名，以及未知标签、空标签、`K40` 这类没有标签的型号，逐一比较新旧函数的输出（型号 ID、设备名、改名结果、规范名、类型）。PR 6 删除旧实现时，把期望值固化成常量表，同时去掉测试依赖 |
| 节点画像 | 四种 `scheduling-type`；单厂商、多厂商、vGPU 加物理卡混合、优先资源命中；`Allocatable` 为 0 或负数 |
| 配置加载 | 文件不存在时用内置表；文件未变化时不重新解析；校验失败时保留旧表并上报指标；`-race` 下加载与并发查询同时进行 |
| 消费方 | reserve、queue-reclaim、slaNode、capacity 的现有单测全部通过；reserve 增加"驱逐后的任务仍算节点上有 GPU 任务"的用例 |
| 回放对比 | 7.3 节的日志对比中，增加 reserve / slaNode 的过滤结果和 capacity 按型号统计的队列用量 |

### 4.3 手法二：翻译成社区字段（在 `eaprofile.OnSessionOpen` 中）

v1.16 的 action 已经根据社区字段做过滤，所以把业务语义翻译过去即可，action 不用改。

#### 4.3.1 在线超时 → `task.SchGated = true`

```go
for _, job := range ssn.Jobs {
	for _, t := range job.TaskStatusIndex[api.Pending] {
		if profile.TaskOnlineTimeout(t.Pod) {
			t.SchGated = true
		}
	}
}
```

- **为什么会被跳过：** v1.16 中 allocate:309、backfill:169、preempt:146/243、reclaim:98 都会跳过 `SchGated` 的任务。
  - allocate:309 的条件是 `task.SchGated && !api.HasOnlyVolcanoSchedulingGate(task.Pod)`。
  - 对没有 `spec.schedulingGates` 的 Pod，`HasOnlyVolcanoSchedulingGate` 返回 false，所以会被**直接跳过**，不会进到 allocate:966 那条带告警的路径。
  - k8s 1.19 没有 schedulingGates 字段，Pod 上一定没有 gate，满足这个前提。
- **InitCycleState：** `session.go:858` 也会跳过 `SchGated` 的任务，正好省去为这些任务做 PreFilter。
- **副作用：** capacity、proportion、overcommit 通过 `DeductSchGatedResources` 把这些任务从 inqueue 资源里扣掉。超时任务不该继续占用入队配额，这是合理的，但属于行为变化，要写进上线说明。
- **顺序要求：** capacity 在 `OnSessionOpen` 中计算 inqueue，所以 eaprofile **必须排在 tier1 第一位**。v1.16 的 `OpenSession` 按配置顺序逐个调用 `OnSessionOpen`（`framework.go` `OpenSession`），这个顺序是确定的。
- 只有 Pending 任务需要设置。超时注解只对等待调度的任务有意义。

#### 4.3.2 受害者资格 → `task.Preemptable`（并集）

v1.16 在把候选交给插件**之前**，会先用 `!task.Preemptable` 预过滤：preempt 跨作业 205、作业内 266，reclaim 194。所以要先把它设成所有内部规则的**并集**，让该进的候选能进到插件，再由 4.4 节的钩子收窄：

```go
// U(t) = Pod 级可抢占 || Pod 级弹性 || 作业可回收 || 作业可被队列内抢占
func union(t *api.TaskInfo, p profile.JobProfile) bool {
	return profile.TaskPodPreemptable(t.Pod) || profile.TaskElastic(t.Pod) ||
		p.Reclaimable || p.InQueuePreemptable
}
```

`(Elastic && task.Elastic)` 已经被 `TaskElastic` 覆盖，不用单独写。

**两个坑：**

1. **缺省值不同。**
   - v1.16 的 `GetPodPreemptable` 在没有注解和标签时**返回 true**（`pod_info.go:201`）。
   - 内部分支返回 **false**（`pod_info.go:97`）。
   - 所以必须用 `profile.TaskPodPreemptable` 整体覆盖 `task.Preemptable`，不能在社区值的基础上做或运算。否则所有没打注解的 Pod 都会变成可抢占。
2. **Job 和 Node 上的 Task 是两份。**
   - 社区 Snapshot 会分别 `Clone` Node 和 Job，所以 `ssn.Jobs[j].Tasks[id]` 和 `ssn.Nodes[n].Tasks[id]` 是**不同的指针**。
   - preempt 和 reclaim 从 `node.Tasks` 里取候选，所以两边都要写：

```go
for _, node := range ssn.Nodes {
	for id, nt := range node.Tasks {
		job, ok := ssn.Jobs[nt.Job]
		if !ok {
			nt.Preemptable = false // 不在会话里的作业不参与驱逐
			continue
		}
		v := union(nt, profile.Of(job))
		nt.Preemptable = v
		if jt, ok := job.Tasks[id]; ok {
			jt.Preemptable = v
		}
	}
}
```

#### 4.3.3 作业级 `job.Preemptable` → 内部语义

- tdm 的 `JobOrderFn` 会比较 `job.Preemptable`（v1.16 `tdm.go:270`），线上 tdm 只启用了 JobOrder。
- 为了让排序结果和现在一致，eaprofile 把 `job.Preemptable` 覆盖为 `profile.Of(job).InQueuePreemptable`。
- tdm 的 Predicate、Preemptable、Victim 在线上都已关闭，不受 `task.Preemptable` 并集的影响。如果以后要启用，需要重新评估。

### 4.4 手法三：挂到通用钩子

#### 4.4.1 抢占者过滤 → `ActionExcludedFn`

```go
ssn.AddActionExcludedFn(name, func(obj interface{}, action string) int {
	if action != preemptActionName {
		return util.Abstain
	}
	job, ok := obj.(*api.JobInfo)
	if !ok {
		return util.Abstain
	}
	p, q := profile.Of(job), profile.QueueOf(ssn.Queues[job.Queue])
	if p.Reclaimable || (!q.PreemptLow && !q.HasReclaimable) {
		return util.Reject
	}
	return util.Abstain // 永不 Permit
})
```

- **为什么不用 `JobStarvingFn`：** reclaim 也调用 `JobStarving`（v1.16 `reclaim.go:91/129`），它区分不了是哪个 action，会把 reclaim 的抢占者也拦掉。
- **为什么永不 Permit：** 在 ActionExcluded 的语义里，一旦某个 tier 投了 Permit，后续 tier 就不再判断，会绕过 delay-schedule、reservation、jobbackoff 等门控。这条约束和 jobbackoff 文档一致。
- **v1.16 上要做的事：** ActionExcluded 是内部扩展点，移植时需要在 preempt 入口加一次 `ssn.IsActionExcluded(job, "preempt")`。这是通用扩展点，jobbackoff 文档里已经计入。
- `relax.go:42` 的 `RelaxJobPipelined` 是内部新增文件，直接改为调用 `profile.Of` / `profile.QueueOf`。

#### 4.4.2 受害者细则 → `PreemptableFn` / `ReclaimableFn`

判定表要求和现在逐格一致：

| 场景 | 现在的规则（内部分支） | 新规则（eaprofile 钩子） |
|---|---|---|
| preempt，作业内（`preemptor.Job == victim.Job`） | `task.Preemptable \|\| task.Elastic`（`preempt.go:304`，Pod 级） | `PreemptableFn`：保留 `TaskPodPreemptable(v) \|\| TaskElastic(v)` |
| preempt，跨作业、同队列 | `Reclaimable \|\| InQueuePreemptable \|\| (Elastic && task.Elastic)`（`preempt.go:239`，作业级） | `PreemptableFn`：保留同样的条件 |
| reclaim，跨队列 | 作业级 `Reclaimable \|\| Elastic`（`reclaim_victim.go:45`） | `ReclaimableFn`：保留 `Reclaimable \|\| Elastic` |
| relax | `!Reclaimable && InQueuePreemptable`，跳过弹性部分，GPU 型号相同（`relax.go:235-246`） | 在内部文件 `relax.go` 里预过滤（改为调用 `profile` / `gpumodel`），之后仍走 `ssn.Preemptable`。预过滤比跨作业规则更严，是子集，结果一致 |

```go
ssn.AddPreemptableFn(name, func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
	var victims []*api.TaskInfo
	for _, v := range preemptees {
		if v.Job == preemptor.Job {
			if profile.TaskPodPreemptable(v.Pod) || profile.TaskElastic(v.Pod) {
				victims = append(victims, v)
			}
			continue
		}
		job, ok := ssn.Jobs[v.Job]
		if !ok {
			continue
		}
		p := profile.Of(job)
		if p.Reclaimable || p.InQueuePreemptable || (p.Elastic && profile.TaskElastic(v.Pod)) {
			victims = append(victims, v)
		}
	}
	return victims, util.Permit
})

ssn.AddReclaimableFn(name, func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
	var victims []*api.TaskInfo
	for _, v := range reclaimees {
		if job, ok := ssn.Jobs[v.Job]; ok {
			if p := profile.Of(job); p.Reclaimable || p.Elastic {
				victims = append(victims, v)
			}
		}
	}
	return victims, util.Permit
})
```

- **BestEffort 和任务状态检查**：仍由社区 action 负责（v1.16 preempt 198-203、reclaim 194），不用重复写。
- **队列条件**：同队列 / 跨队列、`q.Reclaimable()` 由社区 action 判断，不用重复写。

**tier 语义约束（重要）：**
- v1.16 的 `ssn.Preemptable` / `ssn.Reclaimable` 中，只要某个插件返回空列表，就会 `victims = nil; break`，然后**落到下一个 tier**，由下一个 tier 决定（`session_plugins.go` 279-325 行）。内部分支的实现也是这样。
- 所以 eaprofile 的"一个都不准驱逐"，只有在**后续 tier 都没有启用受害者插件**时才成立。
- 线上配置中，tier1 有 conformance、elastic、capacity 三个受害者插件；tier2 是 cooldown、nodeorder、binpack、net-topology、redis，**都没有注册 Preemptable / Reclaimable**，所以当前是安全的。
- 加两道防护：
  1. eaprofile 的 `OnSessionOpen` 检查 `ssn.Tiers`：如果 eaprofile 所在 tier 之后还有启用了 `EnabledPreemptable` / `EnabledReclaimable` 的插件（按已知受害者插件的名单判断），打 Error 日志并输出指标 `volcano_eaprofile_config_violation`。
  2. 在配置评审清单里写明：受害者插件不得放在 eaprofile 之后的 tier。

#### 4.4.3 PendingReason 分类 → capacity `OnSessionClose`

**现在的流程：**
1. allocate:221-225：作业有待调度任务且队列未超用时，`PendingReason = NotEnoughResources`；如果 `!Elastic && !Reclaimable`，改为 `NotGuaranteeMin`。
2. allocate:504-516：作业维度超用时，按 `Reclaimable` 写 `NotEnoughQuota` 或 `WaitingForMin`，同时写 condition。后者还要把 phase 设为 `WaitingForMin`。
3. capacity 的 `OnSessionPreClose`（`capacity.go:674-718`）：对仍未就绪且收尾时超用的作业，`Reclaimable && NotEnoughResources → NotEnoughQuota`，`!Reclaimable && NotGuaranteeMin → WaitingForMin`。

**新的流程：**
- **allocate 只写通用原因：**
  - 未超用时写 `NotEnoughResources`；
  - 超用时写 `QueueOverused`，这是新增的通用常量，放在 ext 文件；
  - 不再读任何业务字段，也不写 condition。
- **capacity 的 `OnSessionClose`** 用纯函数 `classify` 定稿原因，并对 `QueueOverused` 补写 condition 和 phase。

| allocate 写入的原因 | 收尾时是否超用 | 可回收 | 弹性 | 最终原因 | condition / phase |
|---|---|---|---|---|---|
| `QueueOverused` | 不看 | 是 | 不看 | `NotEnoughQuota` | 写 condition |
| `QueueOverused` | 不看 | 否 | 不看 | `WaitingForMin` | 写 condition，phase = `WaitingForMin` |
| `NotEnoughResources` | 是 | 是 | 不看 | `NotEnoughQuota` | 不写 |
| `NotEnoughResources` | 是 | 否 | 否 | `WaitingForMin` | 不写 |
| `NotEnoughResources` | 是 | 否 | 是 | `NotEnoughResources` | 不写 |
| `NotEnoughResources` | 否 | 否 | 否 | `NotGuaranteeMin` | 不写 |
| `NotEnoughResources` | 否 | 其他组合 | | `NotEnoughResources` | 不写 |

这张表和现在的行为逐行等价，单测按表逐行断言。

**读写顺序规则：**
- v1.16 的 `CloseSession` 用 `for _, plugin := range ssn.plugins` 遍历一个 map，**`OnSessionClose` 的顺序是随机的**。
- 因此约定：
  - **写最终原因 / phase**：只能在 action 执行期间，或者在插件的 `OnSessionClose` 里。
  - **读最终原因 / phase 做统计**：只能在 `JobUpdateHook` 里（它在所有 `OnSessionClose` 之后、PodGroup 回写之前执行），或者在下一个会话的 `OnSessionOpen` 里。
- 已知要搬的读取方：
  - gang 的 `OnSessionClose` 统计 `NotGuaranteeMin` 和 `PodGroupWaitingForMin`（`gang.go:240-270`）。这部分是内部新增的，挪到 `JobUpdateHook`。
  - job-lifecycle 本来就在 `JobUpdateHook` 里。
  - jobbackoff 如果在 `OnSessionClose` 里读本会话的 PendingReason，也要挪（见第 10 节待确认问题）。
- 这样一来，capacity 不再需要 `OnSessionPreClose`，**Plugin 接口可以恢复社区原样**，和 job-lifecycle 文档的 F1 / F2 合起来就能删掉。

**行为差异：** 超用 condition 的写入从 allocate 执行期间推迟到 `OnSessionClose`。由于 PodGroup 是在会话结束后由 JobUpdater 统一回写的，**外部看到的结果不变**。会话内部只有一处会受影响：在 allocate 之后执行的 action 如果读 `PodGroupWaitingForMin`，读到的值会不同。目前没有找到这种读取，上线前用 `rg "PodGroupWaitingForMin" pkg/scheduler/actions` 再确认一次。

### 4.5 其余条目（各有单独文档，本文只列去向）

| 条目 | 去向 | 文档 |
|---|---|---|
| 退避门控（allocate / preempt / reclaim） | jobbackoff 插件，用 `ActionExcludedFn` | `jobbackoff-plugin-design.md` |
| 生命周期时间戳、脏标记、`OnSessionPreClose`、`statisticDelayMetric` | job-lifecycle 插件，用 `JobUpdateHook` | `job-lifecycle-plugin-design.md` |
| `cache` 节点指标协程、`metrics/node.go` | node-metrics 插件 | `node-metrics-plugin-design.md` |
| `HandleJobDelayScheduling`、`clearJobDelaySchedulingCondition` | 原样剪切到 delayschedule 插件；`JobInfo` 上与延迟调度相关的方法挪到插件私有函数 | 本文 |

### 4.6 `eaprofile` 插件实现

本节把前面分散在 4.1、4.3、4.4 中的内容收拢成完整实现。如果第 10 节第 7 条决定由 `task-property` 兼任，把本节的文件原样并入 `plugins/property` 即可，见 4.6.9。

#### 4.6.1 职责

| 职责 | 时机 | 1a（`gpu-numa`） | 1b（v1.16） |
|---|---|---|---|
| 热加载 GPU 型号配置（4.2.3） | `OnSessionOpen` 第一步 | 有 | 有 |
| 构建会话画像表（作业、队列、节点、任务型号） | `OnSessionOpen` | 有 | 有 |
| 统计注解解析错误 | `OnSessionOpen` | 有 | 有 |
| 配置自检（tier 位置、开关） | `OnSessionOpen` | 无 | 有 |
| 在线超时 → `task.SchGated` | `OnSessionOpen` | 无（内部分支没有该字段） | 有 |
| 受害者资格 → `task.Preemptable` 并集 | `OnSessionOpen` | 无（内部 action 仍自己判断） | 有 |
| `job.Preemptable` 写回内部语义 | `OnSessionOpen` | 无 | 有 |
| 抢占者过滤（`ActionExcludedFn`） | preempt 调用时 | 有（6.1 节 PR 5） | 有 |
| 受害者细则（`PreemptableFn` / `ReclaimableFn`） | preempt / reclaim 调用时 | 无 | 有 |
| 清空画像表 | `OnSessionClose` | 有 | 有 |

1a 和 1b 的差别在代码里体现，不做成运行时开关：两个分支的 action 不同，开关只会制造无效组合。

#### 4.6.2 目录结构

```
pkg/scheduler/ea/profile/              # L1 内部库，不依赖任何插件和 prometheus
├── types.go          JobProperty / JobType 枚举、注解 key 常量、JobProfile / QueueProfile
├── parse_job.go      ParseJob（含 setDefaultPreemptProperty）及各 extract 函数
├── parse_queue.go    ParseQueue
├── parse_task.go     TaskElastic / TaskPodPreemptable / TaskOnlineTimeout
├── table.go          会话画像表：Build / Of / QueueOf / Reset
├── node.go           NodeProfile / NodeScheduleType、parseNode、NodeOf（4.2.5）
├── task_gpu.go       TaskGPU / TaskRequest / TaskIsTrain（4.2.5）
├── helpers.go        IsElasticInferJob、compare（task-property 排序用）
└── *_test.go         等价性测试（7.1）、并发测试

pkg/scheduler/ea/gpumodel/             # L1 叶子包，只依赖 k8s.io/api/core/v1
├── model.go          ID / Model / Registry 及查询方法（4.2.4）
├── builtin.go        内置型号表（4.2.2 的 YAML 嵌入）
├── load.go           Load / Validate / ReloadIfChanged / Current（4.2.3）
└── *_test.go         与 eaframework 映射表逐项对比的等价性测试（4.2.8）

pkg/scheduler/plugins/eaprofile/       # L2 插件
├── eaprofile.go      New / Name / OnSessionOpen / OnSessionClose、参数解析
├── translate.go      translateSchGated / translatePreemptable / overrideJobPreemptable
├── victim.go         preemptableFn / reclaimableFn
├── gate.go           preemptorGateFn
├── selfcheck.go      配置自检
├── metrics.go        插件指标
└── *_test.go
```

插件在 `plugins/factory.go` 中注册：`framework.RegisterPluginBuilder(eaprofile.PluginName, eaprofile.New)`。`factory.go` 是社区文件，加一行注册属于 L0 允许的改动。内部已有插件也是这样注册的。

#### 4.6.3 画像表（`ea/profile/table.go`）

```go
type sessionTable struct {
	jobs   map[api.JobID]JobProfile
	queues map[api.QueueID]QueueProfile
}

var current atomic.Pointer[sessionTable]

// BuildStats 汇总本次构建中遇到的非法注解，由插件负责打日志和上报指标，库本身不依赖 prometheus。
type BuildStats struct {
	Jobs        int
	InvalidKeys map[string]int // 注解 key → 出现次数
}

// nodes 参数与节点画像、任务型号的构建见 4.2.5，此处只列作业和队列部分。
func Build(jobs map[api.JobID]*api.JobInfo, queues map[api.QueueID]*api.QueueInfo,
	nodes map[string]*api.NodeInfo) BuildStats {
	stats := BuildStats{Jobs: len(jobs), InvalidKeys: map[string]int{}}
	report := func(key string) { stats.InvalidKeys[key]++ }

	t := &sessionTable{
		jobs:   make(map[api.JobID]JobProfile, len(jobs)),
		queues: make(map[api.QueueID]QueueProfile, len(queues)),
	}
	hasReclaimable := map[api.QueueID]bool{}
	for id, job := range jobs {
		p := parseJob(job.PodGroup, report)
		t.jobs[id] = p
		if p.Reclaimable || p.Elastic {
			hasReclaimable[job.Queue] = true
		}
	}
	for id, q := range queues {
		qp := parseQueue(q.Queue, report)
		qp.HasReclaimable = hasReclaimable[id]
		t.queues[id] = qp
	}
	current.Store(t)
	return stats
}

func Reset() { current.Store(nil) }

func Of(job *api.JobInfo) JobProfile {
	if job == nil {
		return JobProfile{}
	}
	if t := current.Load(); t != nil {
		if p, ok := t.jobs[job.UID]; ok {
			return p
		}
	}
	return parseJob(job.PodGroup, nil)
}

func QueueOf(q *api.QueueInfo) QueueProfile {
	if q == nil || q.Queue == nil {
		return QueueProfile{}
	}
	if t := current.Load(); t != nil {
		if p, ok := t.queues[q.UID]; ok {
			return p
		}
	}
	return parseQueue(q.Queue, nil) // 查不到时 HasReclaimable 为 false
}
```

**实现要点：**

- **`parseJob(pg, report)` 是内部函数**：导出的 `ParseJob(pg)` 等于 `parseJob(pg, nil)`。
  - 原来的 `klog.Warningf("invalid ...")` 改成调用 `report(key)`。
  - 原实现在 `NewJobInfo` 里解析，只在 PodGroup 变化时执行一次；现在每个会话都要解析，如果照旧打 Warning，一个非法注解每秒会打一次日志。所以改为汇总后由插件去重上报。
- **`pg == nil` 返回零值画像**：`ParseJobProperty(nil)` 返回 `JobPropertyNone`，`setDefaultPreemptProperty` 对 None 不做修改，和原来一致。只有 Pod、还没有 PodGroup 的作业会出现这种情况。
- **`HasReclaimable` 的口径**：原 `QueueInfo.HasReclaimableJob` 基于内部会话字段 `ssn.JobsByQueue`，现在基于 `ssn.Jobs` 按队列分组。等价性测试要覆盖"两者包含的作业集合相同"这一前提。
- **并发**：
  - 表在 `OnSessionOpen` 中单线程构建，`Store` 之后只读；
  - `Of` 查不到时现场解析，结果不写回表；
  - 所以 preempt / reclaim 拓扑感知路径的 worker 并发调用 `Of` 不需要加锁。
  - 用 `go test -race` 跑一组"一边 `Build` 一边并发 `Of`"的测试。
- **`OnSessionClose` 中 `Reset`**：
  - 关闭阶段其他插件的 `OnSessionClose`（顺序随机），以及 `JobUpdateHook` 调用 `Of` 时都会走现场解析，结果正确，只是稍慢。按每个作业解析一次估算，5000 个作业约 1 到 2ms。
  - 好处是：插件被误从配置中移除后，不会一直读到某个旧会话的画像。

#### 4.6.4 插件主体（`eaprofile.go`，1b 版本）

```go
const (
	PluginName = "eaprofile"

	argPreemptorGate     = "eaprofile.preemptor-gate"      // 默认 true
	argOnlineTimeoutGate = "eaprofile.online-timeout-gate" // 默认 true
	argGPUModelFile      = "eaprofile.gpu-model-file"      // 默认空，使用内置型号表（4.2.3）
)

type eaProfilePlugin struct {
	preemptorGate     bool
	onlineTimeoutGate bool
	gpuModelFile      string
}

func New(args framework.Arguments) framework.Plugin {
	p := &eaProfilePlugin{preemptorGate: true, onlineTimeoutGate: true}
	args.GetBool(&p.preemptorGate, argPreemptorGate)
	args.GetBool(&p.onlineTimeoutGate, argOnlineTimeoutGate)
	args.GetString(&p.gpuModelFile, argGPUModelFile)
	return p
}

func (p *eaProfilePlugin) Name() string { return PluginName }

func (p *eaProfilePlugin) OnSessionOpen(ssn *framework.Session) {
	if p.gpuModelFile != "" {
		_, err := gpumodel.ReloadIfChanged(p.gpuModelFile)
		reportGPUModelConfig(err) // 失败时沿用上一份注册表
	}
	stats := profile.Build(ssn.Jobs, ssn.Queues, ssn.Nodes)
	reportBuildStats(stats)

	violations := selfCheck(ssn.Tiers)
	reportViolations(violations)

	gated := 0
	if p.onlineTimeoutGate {
		gated = translateSchGated(ssn)
	}
	failClosed := violations.Has(ruleSelfHooksDisabled)
	preemptable := translatePreemptable(ssn, failClosed)
	overrideJobPreemptable(ssn)
	updateTranslatedGauge(gated, preemptable)

	ssn.AddPreemptableFn(PluginName, preemptableFn(ssn))
	ssn.AddReclaimableFn(PluginName, reclaimableFn(ssn))
	if p.preemptorGate {
		ssn.AddActionExcludedFn(PluginName, preemptorGateFn(ssn))
	}
}

func (p *eaProfilePlugin) OnSessionClose(ssn *framework.Session) {
	profile.Reset()
}
```

- **插件没有跨会话状态。** 插件实例每个会话都会重建；画像表是包级变量，每个会话覆盖；日志去重用的"上次违规签名"也是包级变量，见 4.6.7。
- **开关的含义：**
  - `preemptor-gate`：关闭后，可回收作业也会去抢占，等于回到"不做抢占者过滤"。只用于紧急止血。
  - `online-timeout-gate`：关闭后，在线超时任务会重新参与调度，并重新计入 inqueue。只用于紧急止血。
  - **受害者细则没有开关。** 一旦关闭，A.3 中的 D 类作业就会被误杀，所以不提供。
- **1a 版本**只保留 `Build`、`reportBuildStats`、`AddActionExcludedFn`（PR 5 之后）和 `Reset`，其余都在 1b 加入。

#### 4.6.5 翻译（`translate.go`）

```go
func translateSchGated(ssn *framework.Session) int {
	n := 0
	for _, job := range ssn.Jobs {
		for _, t := range job.TaskStatusIndex[api.Pending] {
			if !t.SchGated && profile.TaskOnlineTimeout(t.Pod) {
				t.SchGated = true
				n++
			}
		}
	}
	return n
}

func translatePreemptable(ssn *framework.Session, failClosed bool) int {
	eligible := func(t *api.TaskInfo, job *api.JobInfo) bool {
		if failClosed {
			return false
		}
		p := profile.Of(job)
		return profile.TaskPodPreemptable(t.Pod) || profile.TaskElastic(t.Pod) ||
			p.Reclaimable || p.InQueuePreemptable
	}
	for _, job := range ssn.Jobs {
		for _, t := range job.Tasks {
			t.Preemptable = eligible(t, job)
		}
	}
	n := 0
	for _, node := range ssn.Nodes {
		for id, nt := range node.Tasks {
			job, ok := ssn.Jobs[nt.Job]
			switch {
			case !ok:
				nt.Preemptable = false
			case job.Tasks[id] != nil:
				nt.Preemptable = job.Tasks[id].Preemptable
			default:
				nt.Preemptable = eligible(nt, job)
			}
			if nt.Preemptable {
				n++
			}
		}
	}
	return n
}

func overrideJobPreemptable(ssn *framework.Session) {
	for _, job := range ssn.Jobs {
		job.Preemptable = profile.Of(job).InQueuePreemptable
	}
}
```

- **为什么能安全地改字段**：`ssn.Jobs` 和 `ssn.Nodes` 是 Snapshot 克隆出来的会话私有副本，改了不会回写到 cache。action 中的 `task.Clone()` 会复制 `Preemptable` 和 `SchGated`，所以翻译结果在 Statement、模拟路径里都能保留。
- **只给 Pending 任务设 `SchGated`**：超时注解只对等待调度的任务有意义。`!t.SchGated` 的判断保证不会重复计数，也不会覆盖 Pod 自带的 gate。
- **`job.Tasks`、`TaskStatusIndex` 和 v1.16 的 `SubJobs` 共享同一个 `*TaskInfo`**，所以写 `job.Tasks` 就够了。单测中要断言这一点，防止社区以后改成分别克隆。
- **`failClosed`**：插件自己的 Preemptable、Reclaimable 或 ActionExcluded 开关被关掉时（4.6.7 规则 R1），把所有 `task.Preemptable` 置为 false，preempt / reclaim 实际上停止驱逐。原则是宁可少抢，不可误杀。如果此时用并集，D 类作业会被误杀；如果只用 Pod 级原值，D 类作业照样会进候选。

#### 4.6.6 钩子（`victim.go`、`gate.go`）

```go
func preemptableFn(ssn *framework.Session) api.EvictableFn {
	return func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		victims := make([]*api.TaskInfo, 0, len(preemptees))
		for _, v := range preemptees {
			if v.Job == preemptor.Job {
				if profile.TaskPodPreemptable(v.Pod) || profile.TaskElastic(v.Pod) {
					victims = append(victims, v)
				}
				continue
			}
			job, ok := ssn.Jobs[v.Job]
			if !ok {
				continue
			}
			p := profile.Of(job)
			if p.Reclaimable || p.InQueuePreemptable || (p.Elastic && profile.TaskElastic(v.Pod)) {
				victims = append(victims, v)
			}
		}
		return victims, util.Permit
	}
}

func reclaimableFn(ssn *framework.Session) api.EvictableFn {
	return func(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		victims := make([]*api.TaskInfo, 0, len(reclaimees))
		for _, v := range reclaimees {
			if job, ok := ssn.Jobs[v.Job]; ok {
				if p := profile.Of(job); p.Reclaimable || p.Elastic {
					victims = append(victims, v)
				}
			}
		}
		return victims, util.Permit
	}
}

func preemptorGateFn(ssn *framework.Session) api.ActionExcludedFn {
	return func(obj interface{}, action string) int {
		if action != preemptActionName {
			return util.Abstain
		}
		job, ok := obj.(*api.JobInfo)
		if !ok {
			return util.Abstain
		}
		p, q := profile.Of(job), profile.QueueOf(ssn.Queues[job.Queue])
		if p.Reclaimable || (!q.PreemptLow && !q.HasReclaimable) {
			gateRejected.Inc()
			return util.Reject
		}
		return util.Abstain
	}
}
```

- **`preemptActionName`** 用字符串常量 `"preempt"`，不 import `actions/preempt` 包，避免插件依赖 action。
- **钩子不会返回 Abstain**：`preemptableFn` / `reclaimableFn` 总是表态，哪怕返回空列表也表示"一个都不准"。空列表会落到下一个 tier，由 4.6.7 的规则 R2 兜底。
- **`preemptorGateFn` 永不 Permit**：原因见 4.4.1。单测对随机输入断言返回值只可能是 0 或 -1。
- **性能**：`preemptableFn` 每次调用对每个候选做一次 map 查找和最多三次注解查找，比原来 action 里直接读字段稍慢，但只是候选数量级的开销，远小于预选和打分。7.4 节的基准测试会覆盖。

#### 4.6.7 配置自检（`selfcheck.go`）

每个会话检查一次 `ssn.Tiers`，规则如下：

| 规则 | 内容 | 违规时 |
|---|---|---|
| R1 | 本插件的 `enablePreemptable`、`enableReclaimable`、`enableActionExcluded` 必须为 true | **fail-closed**：`task.Preemptable` 全部置 false，见 4.6.5；打 Error 日志 |
| R2 | 本插件所在 tier 之后的 tier 中，不得有已知受害者插件开启了 `enablePreemptable` 或 `enableReclaimable` | 打 Error 日志，指标置 1。线上当前不违规（4.4.2） |
| R3 | 本插件在配置中的位置必须早于 capacity、proportion、overcommit，它们在 `OnSessionOpen` 中读 `SchGated` | 打 Error 日志，指标置 1 |

```go
var victimPlugins = map[string]struct{}{
	"conformance": {}, "elastic": {}, "capacity": {}, "proportion": {}, "drf": {},
	"cooldown": {}, "tdm": {}, "priority": {}, "gang": {}, "task-property": {},
	"queue-reclaim": {}, "extender": {},
}

var sessionOpenReaders = map[string]struct{}{"capacity": {}, "proportion": {}, "overcommit": {}}

func on(b *bool) bool { return b != nil && *b } // 与 framework.isEnabled 一致；缺省值已由 ApplyPluginConfDefaults 填成 true
```

- **判断方式**：
  - R1 读本插件 `PluginOption` 上的三个 `*bool`；
  - R2 从本插件所在 tier 的下一个 tier 开始，扫描所有插件；
  - R3 按 tiers 展开成一维顺序后比较下标。
- **日志去重**：把违规集合拼成签名存进包级变量，签名变化时才打 Error，避免每秒刷屏。指标每个会话都更新。
- **名单维护**：victimPlugins 是写死的名单，新增受害者插件时需要同步更新。这是第 10 节第 5 条的待确认项。

#### 4.6.8 指标（`metrics.go`）

指标命名沿用 node-metrics 文档的方式：`prometheus.BuildFQName("", metrics.VolcanoSubSystemName, name)`，用 `promauto` 注册。

| 指标 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `volcano_eaprofile_jobs` | Gauge | 无 | 本会话构建画像的作业数 |
| `volcano_eaprofile_translated_tasks` | Gauge | `kind`（`schgated` / `preemptable`） | 本会话翻译后为 true 的任务数 |
| `volcano_eaprofile_preempt_gate_rejected_total` | Counter | 无 | 抢占者过滤累计拒绝次数 |
| `volcano_eaprofile_invalid_annotations` | Gauge | `key` | 本会话发现的非法注解个数 |
| `volcano_eaprofile_config_violation` | Gauge | `rule`（`R1` / `R2` / `R3`） | 1 表示违规 |
| `volcano_eaprofile_gpu_model_config_error` | Gauge | 无 | 1 表示型号配置文件加载或校验失败，正在使用上一份注册表（4.2.3） |

- `invalid_annotations` 用 Gauge 而不用 Counter，是因为每个会话都会重新解析，Counter 会按会话频率不断累加，失去意义。
- 插件耗时由框架的 `UpdatePluginDuration` 自动上报，不另加。
- 建议的告警：`config_violation == 1` 持续 5 分钟就告警；`translated_tasks{kind="schgated"}` 突然大幅上涨，说明在线超时数量异常，提示排查上游。

#### 4.6.9 配置

```yaml
tiers:
- plugins:
  - name: jobbackoff
    enableBackoffCheck: false
    arguments:
      jobbackoff.max-backoff-seconds: 600
      jobbackoff.queue-ttl-seconds: 900
  - name: eaprofile          # 必须早于 capacity；本插件的 Preemptable / Reclaimable / ActionExcluded 不得关闭
    arguments:
      eaprofile.gpu-model-file: /etc/volcano/gpu-models.yaml   # 可选，不配则用内置表；ConfigMap 挂载
  - name: task-property
  # ... 其余 tier1 插件不变
- plugins:
  - name: cooldown           # 此 tier 不得放受害者插件（规则 R2）
  # ...
```

**由 `task-property` 兼任时**：
- 把 `translate.go`、`victim.go`、`gate.go`、`selfcheck.go`、`metrics.go` 并入 `plugins/property`；
- 在 `OnSessionOpen` 开头执行 4.6.4 中 `AddPreemptableFn` 之前的所有步骤；
- `preemptableFn` 中先执行 4.6.6 的作业级条件，再执行原有的"`PreemptLow` + 属性高低"规则（见 A.1）；
- 指标前缀改为 `volcano_task_property_`，参数 `eaprofile.gpu-model-file` 改为 `task-property.gpu-model-file`；
- 配置里去掉 `eaprofile` 这一行。`task-property` 已经在 tier1 第二位，满足 R3。

#### 4.6.10 测试

| 测试 | 内容 |
|---|---|
| `profile` 等价性 | 见 7.1 |
| 画像表 | 查得到、查不到时现场解析、`Reset` 之后现场解析；`-race` 下 `Build` 与并发 `Of` |
| 非法注解 | 非法布尔值计入 `InvalidKeys`，解析结果与原实现一致（false） |
| `translateSchGated` | 只有带超时注解的 Pending 任务被设置；用 v1.16 的 allocate、backfill、preempt、reclaim 各跑一遍，断言该任务不被调度，capacity 的 inqueue 扣除了它的资源 |
| `translatePreemptable` | `job.Tasks`、`node.Tasks`、`SubJobs` 三处一致；不在会话里的作业的任务为 false；**没有注解的 Pod 为 false**（防止 v1.16 缺省 true 的回归）；fail-closed 时全部为 false |
| `overrideJobPreemptable` | `job.Preemptable` 等于 `InQueuePreemptable`，与 PodGroup 上的 `volcano.sh/preemptable` 无关 |
| 受害者钩子 | 7.2 节判定表，加上 A.3 场景 |
| 抢占者过滤 | 只对 preempt 生效；随机输入下永不返回 Permit |
| 配置自检 | R1、R2、R3 各构造一份违规配置；日志签名去重 |

## 5. 留在 L0 的内容与 CI 守卫

### 5.1 允许留在 L0 的通用扩展点

| 扩展点 | 用途 | 是否计划提交社区 |
|---|---|---|
| `AddActionExcludedFn` / `IsActionExcluded` | 按 action 门控作业 | 可以提 |
| `AddJobUpdateHook` | PodGroup 状态计算完成后的回调 | 可以提 |
| `Overused(queue, job)` | 作业维度的超用判断 | 可以提（会改变社区签名，需要讨论） |
| SchedulingUnit 系列、`VictimProtection`、`Coeviction` | replica-gang | 中期收敛到 SubGroupPolicy |
| `PendingReason` / `LastReason` 字段、`QueueOverused` 常量 | 调度过程状态 | 放在 `api/job_info_ext.go` |

### 5.2 CI 守卫

只检查社区文件，也就是 `v1.16.0` 中存在的文件，不误伤内部新增文件：

```bash
#!/usr/bin/env bash
# hack/verify-base-layer.sh
set -euo pipefail
BASE=${BASE:-v1.16.0}
files=$(git ls-tree -r --name-only "$BASE" pkg/scheduler \
  | grep -E '^pkg/scheduler/(api|framework|actions|cache|metrics|conf|util)/' \
  | grep -v '_test.go$' | while read -r f; do [ -f "$f" ] && echo "$f"; done)

pattern='ReclaimAble|Elasticable|JobProperty|ReqSLAMachine|OnlineTaskTimeout|PermitedOnGPU|GPUResID|eaframework|EasyAlgo|JDos|scheduler/ea/'
if echo "$files" | xargs rg -n "$pattern"; then
  echo "business logic leaked into community files" >&2
  exit 1
fi
```

- 1a 阶段以"只报告"模式运行，把输出当作剩余工作清单；1b 结束时改为阻断模式。
- `*_ext.go` 是内部新增文件，不在 `v1.16.0` 的文件列表里，自然不会被检查。但 ext 文件里**也不能出现业务名词**，需要在代码评审时把关。

## 6. 实施步骤

### 6.1 第一段（1a）：在 `gpu-numa` 上做纯搬迁，行为不变

内部分支没有 `SchGated`，内部 preempt 的跨作业过滤也不看 `task.Preemptable`，所以 1a 只做不改变行为的搬迁。

| PR | 内容 | 验证 |
|---|---|---|
| 1 | 新建 `ea/profile`，解析函数原样复制；新建 `eaprofile` 插件，只构建画像表；`hack/verify-base-layer.sh` 以报告模式接入 | 等价性测试（7.1） |
| 2 | 插件消费方机械替换（4.1.2 节），`relax.go`、`reclaim_victim.go` 一并替换 | 现有单测全部通过；`rg` 确认插件里没有 `job.ReclaimAble` 等残留 |
| 3 | 社区 action 中对业务字段的**读取**改为调用 `profile`，判断逻辑暂时不动 | 现有单测，以及详细日志对比（7.3） |
| 4 | 新建 `ea/gpumodel`（内置型号表，暂不开放配置文件）；画像表增加节点画像和任务型号（4.2.5）；按 4.2.6 替换 reserve、queue-reclaim、slaNode、predicates、metrics 等消费方；删除 `NodeInfo` / `TaskInfo` 上的 GPU 字段和 `resource_info.go` 中的 GPU 函数 | 4.2.8 的等价性测试（eaframework 只作为测试依赖），以及 reserve、queue-reclaim 单测 |
| 4' | 开放 `eaprofile.gpu-model-file`，ConfigMap 挂载 | 加载、校验、热更新失败回退的单测；灰度时看 `volcano_eaprofile_gpu_model_config_error` |
| 5 | 剪切 `HandleJobDelayScheduling` 到 delayschedule；`preempt.go:150` 改为 eaprofile 的 `ActionExcludedFn` | preempt 单测 |
| 6 | 删除 api 上的业务字段和解析函数 | 编译通过即证明没有残留引用 |

**1a 的完成标准：** api 层干净；action 里只剩"判断逻辑"，不再读 api 业务字段。可以单独上线观察一周。

### 6.2 第二段（1b）：移植到 v1.16 时做语义翻译

| 步骤 | 内容 |
|---|---|
| 7 | 移植 `ea/*`、`eaprofile` 和已改造的插件 |
| 8 | 在线超时改为 `SchGated`（4.3.1）；社区 action 不带 `OnlineTaskTimeout` 改动 |
| 9 | 受害者改为"`task.Preemptable` 并集 + `PreemptableFn` / `ReclaimableFn`"（4.3.2、4.4.2）；preempt / reclaim 用社区原版，只加 `IsActionExcluded` 入口 |
| 10 | PendingReason 分类挪到 capacity 的 `OnSessionClose`；gang 的统计挪到 `JobUpdateHook`（4.4.3）；去掉 `OnSessionPreClose` |
| 11 | 按各自文档落地 jobbackoff、job-lifecycle、node-metrics |
| 12 | `verify-base-layer.sh` 改为阻断模式 |

## 7. 测试与验证

### 7.1 解析等价性测试（1a）

- 搬迁期间保留旧实现。用表驱动测试覆盖以下组合：
  - `task_property` 各取值，以及缺失、非法值；
  - `reclaimable` / `elasticable` 的注解和标签各种组合（包括 `ParseAnnotationsToBool` 在 annotations 或 labels 为空时直接返回 false 的分支）；
  - `elastic-sla`。
- 断言 `profile.ParseJob(pg)` 与旧的 `api.NewJobInfo` 设置的字段逐字段相等。
- Pod 级和 Queue 级的解析也做同样的测试。
- PR 6 删除旧实现时，把期望值固化成常量表。

### 7.2 受害者判定表测试（1b）

- **组合维度：**
  - 作业属性 5 类：prod-high、prod-medium、exp-high、exp-medium、elastic-sla；
  - Pod 级可抢占和弹性：2 × 2；
  - 场景 4 类：作业内、同队列跨作业、跨队列、relax。
- 每一格都断言新实现（社区 action + eaprofile）与内部分支旧实现的结论相同。
- 期望值用内部分支的现有逻辑生成，两边各跑一遍对比。

### 7.3 决策日志对比

- 在测试集群上回放同一批作业，比较改造前后的 `session-action-*` 详细日志：
  - 抢占者被跳过的作业集合；
  - 每次 preempt / reclaim 选出的受害者；
  - 每个会话结束时各作业的 PendingReason。
- 允许的差异只有一项：在线超时任务的 inqueue 资源被扣除（4.3.1）。

### 7.4 性能

- 记录 `eaprofile.OnSessionOpen` 的耗时：v1.16 通过 `UpdatePluginDuration` 自动上报。目标是 5000 个作业、2000 个节点时在 20ms 以内。
- preempt / reclaim 的耗时与改造前对比，`Of` 查表增加的开销应在噪声范围内。

## 8. 上线与回滚

- 1a 的 PR 都不改变行为，可以逐个合入上线；回滚就是回退 PR。
- 1b 随 v1.16 一起上线，回滚到旧版调度器即可。
  - 本方案没有修改任何注解的 key 或格式，PodGroup 上的数据在新旧版本之间可以互相读取。
- 上线说明里写明两处行为变化：
  1. 在线超时任务不再计入队列的 inqueue 资源；
  2. 超用 condition 改在会话结束时写入（外部观察不到差别）。

## 9. 工作量

| 项 | 人天 |
|---|---|
| `ea/profile`、`eaprofile` 插件（含配置自检、指标）、等价性测试 | 2 |
| 插件消费方替换 | 1 |
| `ea/gpumodel`（注册表、配置热加载、等价性测试）、节点画像、各插件消费方替换 | 2.5 |
| 抢占者和受害者钩子、判定表测试 | 2 |
| `SchGated` 翻译 | 0.5 |
| PendingReason 分类、gang 统计迁移 | 1.5 |
| CI 守卫、清理、日志对比 | 1 |
| **合计** | **约 10.5** |

不含 jobbackoff（约 2.5）、job-lifecycle（约 3~4）、node-metrics（约 2.5）。

## 10. 待确认问题

1. **jobbackoff 读 PendingReason 的时机**：如果它在 `OnSessionClose` 里读本会话的原因，在随机顺序下可能读到 capacity 改写前的值，需要挪到 `JobUpdateHook` 或下个会话。需要对照 `jobbackoff.go` 中读 `PendingReason` 的 10 处逐一核对。
2. **gang 统计的归属**：gang 的内部统计（`NotGuaranteeMin` 作业数、`WaitingForMin` 资源）挪到 job-lifecycle 插件的 `JobUpdateHook`，还是新建一个队列指标插件？
3. ~~**`FilteredIgnoredScalarResources` 的归属**~~：已确认唯一调用方是 capacity（`capacity.go:865`），挪进 capacity 作为私有函数（4.2.6）。
4. **`Overused(queue, job)` 签名**：是否向社区提 PR。如果不提，需要作为内部扩展点长期维护。
5. **受害者插件的名单**：4.4.2 中的配置检查需要一份"已知受害者插件"名单（conformance、elastic、capacity、cooldown、tdm、priority、gang 等），名单的维护方式待定。
6. **`job.Preemptable` 覆盖的影响面**：除了 tdm 的 JobOrder，v1.16 中还有没有其他读作业级 `Preemptable` 的地方？移植时用 `rg "\.Preemptable\b" pkg/scheduler` 复核。
7. **画像插件是否由 `task-property` 兼任**：建议兼任，理由见附录 A.1。如果采纳，正文中的 `eaprofile` 插件统一改为 `task-property`，不用新增插件，线上配置也不用改。
8. **reserve 在 jdos 节点上查空闲 GPU 的疑似缺陷**（4.2.1 第 3 条）：
   - 迁移时先等价保留；
   - 是否修复（改用 `NodeOf(node).GPUDevice`），需要业务确认。修复后，"不允许使用有空闲 GPU 的节点"会在 jdos 节点上真正生效，CPU 任务可用的节点会变少，打分也会变化。
9. **eaframework 能否在 1a 去掉**：调度器之外，archimedes 也依赖 eaframework。如果 archimedes 暂时不改，eaframework 就会在 `go.mod` 里多留一段时间，但调度器的 `api` 层已经不再引用它，不影响本文目标。
10. **1b 不切换到 `gangpreempt` / `gangreclaim`**（附录 B）：
    - **结论（待确认）**：1b 的 actions 保持 `allocate, reclaim, preempt, backfill`。本文的受害者规则、抢占者过滤、`task.Preemptable` 并集、`SchGated` 翻译都是按 v1.16 的 `preempt` / `reclaim` 设计的，对 gang 系列不成立。
    - **原因**：
      - gang 系列在 action 里写死了 PriorityClass 优先级比较；
      - 它和 4.3.3 的 `job.Preemptable` 覆盖方案冲突；
      - 它会丢掉 relaxEvictByJob、作业内抢占、弹性作业部分扩容、Coeviction 等线上在用的能力；
      - 社区要求它不能和 `preempt` / `reclaim` 同时配置，没法灰度。
    - **以后评估切换的前提**见 B.5。评估单独立项，不计入本文工作量。

## 附录 A：改造示例

本附录用 `task-property` 插件和 preempt action 举例，说明两段改造后代码分别长什么样，再用一个具体场景验证抢占结果没有变化。要点有三个：

- **插件**基本是机械替换：把 `job.ReclaimAble` 这类字段改成读业务画像，判断逻辑本身不动。
- **action** 改动最大：业务判断整段删掉，回到社区原版，只保留一个通用的 `IsActionExcluded` 入口。
- **删掉的判断**由画像插件的钩子接住。这个画像插件可以直接由 `task-property` 兼任。

### A.1 `task-property` 插件

现在这个插件做两件事：
- 按 `JobType` / `JobProperty` 给作业排序；
- 在 `PreemptableFn` 里判断"高优能否抢低优"，用到 `ReclaimAble`、`Elasticable`、`queue.PreemptLow`、`JobProperty`。

这些都是挂在 `api` 上的业务字段。

**改造前**（`plugins/property/property.go`，节选）：

```go
lvType, lvProperty := lv.JobType, lv.JobProperty
rvType, rvProperty := rv.JobType, rv.JobProperty
// ...
if preempteeJob.ReclaimAble || preempteeJob.Elasticable {
	victims = append(victims, preemptee)
	continue
}
preemptorQueue := ssn.Queues[preemptorJob.Queue]
preempteeQueue := ssn.Queues[preempteeJob.Queue]
if !pp.enablePreempt && (preemptorQueue == nil || !preemptorQueue.PreemptLow || preempteeQueue == nil || !preempteeQueue.PreemptLow) {
	continue
}
if preemptorJob.JobProperty > preempteeJob.JobProperty {
	victims = append(victims, preemptee)
}
```

**第一段改造后**（1a，`gpu-numa`）：只换数据来源，判断逻辑一行不动。

```go
import "volcano.sh/volcano/pkg/scheduler/ea/profile"

lp, rp := profile.Of(lv), profile.Of(rv)
return compare(lp.Type, rp.Type, lp.Property, rp.Property)

// preemptableFn
pj := profile.Of(preemptorJob)
for _, preemptee := range preemptees {
	preempteeJob := ssn.Jobs[preemptee.Job]
	if preempteeJob == nil {
		continue
	}
	ej := profile.Of(preempteeJob)
	if ej.Reclaimable || ej.Elastic {
		victims = append(victims, preemptee)
		continue
	}
	pq := profile.QueueOf(ssn.Queues[preemptorJob.Queue])
	eq := profile.QueueOf(ssn.Queues[preempteeJob.Queue])
	if !pp.enablePreempt && (!pq.PreemptLow || !eq.PreemptLow) {
		continue
	}
	if pj.Property > ej.Property {
		victims = append(victims, preemptee)
	}
}
```

`JobType`、`JobProperty` 这些枚举和 `compare()` 一起搬到 `ea/profile`。`QueueOf(nil)` 返回零值，`PreemptLow` 为 false，和原来的 nil 判断等价。改完后，这个插件不再依赖 `api` 上的任何业务字段。

**第二段改造后**（1b，v1.16）：property 自身的规则不用再改。建议让它**兼任画像插件**，把正文中 `eaprofile` 的职责合并进来，原因是：
- 线上配置里 `task-property` 已经在 tier1 第二位（只排在 jobbackoff 后面），早于 capacity，满足 4.3.1 节"必须早于 capacity 执行 `OnSessionOpen`"的要求。jobbackoff 的 `OnSessionOpen` 不读 `SchGated` 和 `task.Preemptable`，所以排在它后面没有影响。
- 它本来就是"业务属性"插件，合并后不用新增插件，配置也不用改。

合并后的骨架：

```go
func (pp *propertyPlugin) OnSessionOpen(ssn *framework.Session) {
	profile.BuildSessionTable(ssn) // 构建画像表（4.1.1）
	translateSchGated(ssn)         // 在线超时 → task.SchGated（4.3.1）
	translatePreemptable(ssn)      // 受害者资格并集，job.Tasks 和 node.Tasks 两份都写（4.3.2）
	overrideJobPreemptable(ssn)    // job.Preemptable 写回内部语义，给 tdm 排序用（4.3.3）

	ssn.AddJobOrderFn(pp.Name(), jobOrderFn)               // 原有，不变
	ssn.AddPreemptableFn(pp.Name(), pp.preemptableFn)      // 原有规则，加上 4.4.2 的受害者细则
	ssn.AddReclaimableFn(pp.Name(), pp.reclaimableFn)      // 新增：Reclaimable || Elastic（4.4.2）
	ssn.AddActionExcludedFn(pp.Name(), pp.preemptorGateFn) // 新增：抢占者过滤，只拦 preempt（4.4.1）
}
```

合并后，`pp.preemptableFn` 中先执行 4.4.2 节的作业级条件（作业内要求 Pod 级可抢占或弹性；跨作业要求 `Reclaimable || InQueuePreemptable || (Elastic && TaskElastic)`），通过的再走原有的"同队列 `PreemptLow` + 属性高低"规则。两段规则在同一个函数里依次过滤，效果和分成两个插件取交集一样。

**注意 `enablePreemptable` 开关：** 线上配置没有给 `task-property` 设置 `enablePreemptable: false`，所以它的 `PreemptableFn` 是生效的。兼任之后，这个开关同时控制 4.4.2 节的受害者细则，**不能再关掉**；关掉的话，D 这类作业（见 A.3）会被误杀。需要在配置评审清单里注明。

### A.2 preempt action

**改造前**（内部 `actions/preempt/preempt.go`）：抢占者筛选循环里加了三段业务判断。

```go
if ssn.IsActionExcluded(job, pmpt.Name()) { continue }
if ssn.IsBackoffBlocked(job) {                                     // ① 退避
	metrics.RegisterJobBackoffBlocked(..., job.ReqSLAMachine)
	continue
}
preemptorQueue := ssn.Queues[job.Queue]
if job.ReclaimAble ||                                              // ② 抢占者过滤
	(!preemptorQueue.PreemptLow && !preemptorQueue.HasReclaimableJob(ssn.JobsByQueue)) {
	continue
}
for _, task := range job.TaskStatusIndex[api.Pending] {
	if task.OnlineTaskTimeout { continue }                         // ③ 在线超时
	preemptorTasks[job.UID].Push(task)
}
```

受害者过滤函数里，跨作业的地方还有一段（`preempt.go:239`）：

```go
if !job.ReclaimAble && !job.Preemptable && !(job.Elasticable && task.Elastic) {
	return false
}
```

**第一段改造后**（1a）：判断逻辑保留，只换数据来源，目的是先把 `api` 上的字段删掉。

```go
p, q := profile.Of(job), profile.QueueOf(ssn.Queues[job.Queue])
if p.Reclaimable || (!q.PreemptLow && !q.HasReclaimable) { continue }
// ...
if profile.TaskOnlineTimeout(task.Pod) { continue } // 内部分支没有 SchGated，这一段暂时保留
// ...
vp := profile.Of(job)
if !vp.Reclaimable && !vp.InQueuePreemptable && !(vp.Elastic && profile.TaskElastic(task.Pod)) {
	return false
}
```

1a 阶段 ② 也可以按 6.1 节 PR 5 直接改成 `ActionExcludedFn`，因为内部分支已经有这个钩子。

**第二段改造后**（1b）：直接用 v1.16 的 `preempt.go` 原版，只在 `JobStarving` 后面加一行通用入口。

```go
// check job if starving for more resources.
if !ssn.JobStarving(job) {
	continue
}
if ssn.IsActionExcluded(job, pmpt.Name()) { // 唯一的内部改动，通用扩展点
	continue
}
// 以下全部是社区原版：
for _, task := range job.TaskStatusIndex[api.Pending] {
	if task.SchGated { continue }           // 在线超时已经被翻译成 SchGated
	preemptorTasks[job.UID].Push(task)
}
// ...
if !task.Preemptable { return false }       // task.Preemptable 已经被翻译成并集
```

删掉的三段业务判断分别由插件接住：

| 原来在 action 里的判断 | 现在由谁处理 |
|---|---|
| ① 退避 | jobbackoff 插件的 `ActionExcludedFn`（见 jobbackoff 文档） |
| ② 抢占者过滤 | `task-property` 的 `ActionExcludedFn`，只在 `action == "preempt"` 时投 Reject |
| ③ 在线超时 | `task-property` 在 `OnSessionOpen` 中设置 `task.SchGated = true` |
| 跨作业受害者条件 | `task.Preemptable` 并集用来放行，`task-property` 的 `PreemptableFn` 再收窄 |

改完以后，`preempt.go` 和社区版只差一行，升级时基本不会冲突。reclaim 的改法相同：入口加一行 `IsActionExcluded`；`reclaim_victim.go` 中的作业级条件改由 `task-property` 的 `ReclaimableFn` 负责。

### A.3 场景验证：改造前后抢占结果一致

队列 Q 开启了 `PreemptLow`。prod-high 作业 A 有任务在等待，作为抢占者；同队列里有四个作业在运行：

| 作业 | 属性 | 推导出的画像（`setDefaultPreemptProperty`） |
|---|---|---|
| B | exp-medium | 可回收，可被队列内抢占 |
| C | prod-medium | 不可回收，可被队列内抢占 |
| D | prod-high | 都不可，但它的 Pod 上打了 `volcano.sh/preemptable=true` |
| E | elastic-sla | 弹性（强制不可回收、不可被队列内抢占）；任务 e1 是弹性副本，e0 不是 |

属性优先级（`api/types.go:102-107`）从高到低依次是：ProdHigh、ProdMedium、ExpHigh、ElasticSLA、ExpMedium。

**改造前**的筛选过程（内部分支）：

1. **action 的跨作业条件**（`preempt.go:239`）只放行 B、C 和 e1（e1 是"弹性作业的弹性任务"）。D 和 e0 被挡掉，D 的 Pod 注解在跨作业场景下不起作用。
2. **`task-property`**：B（可回收）、e1（所属作业是弹性的）直接通过；C 的属性比 A 低，同队列且开了 `PreemptLow`，也通过。
3. **elastic 插件**：B、C 不是弹性作业，直接放行；E 的就绪副本数大于 min，e1 是弹性任务，通过。
4. **同一 tier 内取交集**，最终受害者是 **B、C、e1**。

**改造后**的筛选过程（v1.16）：

1. **`task.Preemptable` 并集**：B（可回收）、C（可被队列内抢占）、e1（Pod 级弹性）为 true；**D 因为 Pod 上有注解也是 true**；e0 为 false。社区 action 的预过滤（`if !task.Preemptable`）放行 B、C、D、e1。
2. **`task-property` 的 `PreemptableFn`**：先按作业级条件 `Reclaimable || InQueuePreemptable || (Elastic && TaskElastic)` 收窄，**D 被剔除**，B、C、e1 保留；再走原有的优先级规则，结果不变。
3. **elastic 插件**：结果和原来一样。
4. **取交集**，最终受害者是 **B、C、e1**，和改造前一致。

D 这一格说明了为什么一定要有"先放宽、再收窄"的两步：
- 只有并集、没有 `PreemptableFn` 收窄，D 会被误杀；
- 只有 `PreemptableFn`、没有并集，社区 action 的 `!task.Preemptable` 预过滤会先把 B、C 挡掉，插件根本看不到它们。

**抢占者一侧：** 如果换成 B 去抢占，B 是可回收作业，`ActionExcludedFn` 在 preempt 时投 Reject，B 不会进入抢占流程，和原来 `preempt.go:150` 的效果一样。

这个场景应作为 7.2 节判定表测试中的一组固定用例。

### A.4 其他插件的改法

| 插件 | 改法 | 注意点 |
|---|---|---|
| capacity | 19 处 `ReclaimAble`、13 处 `Elasticable` 机械替换；`ResreqReplaceScalar` 改为调用 `gpumodel`；PendingReason 改写从 `OnSessionPreClose` 挪到 `OnSessionClose`，按 4.4.3 节的分类表 | 改动最多，但都是机械替换 |
| elastic | `job.Elasticable` 改为 `profile.Of(job).Elastic`；**`preemptee.Preemptable` 改为 `profile.TaskPodPreemptable(preemptee.Pod)`** | 1b 之后 `task.Preemptable` 的含义变成了"并集"，插件里凡是读它的都要改读 Pod 级原值 |
| queue-reclaim、slaNode、cooldown | 机械替换 | 无 |
| notebook | `JobType` 改为读画像；`RunningAt` 按 job-lifecycle 文档处理 | 无 |
| gang | `Elasticable` 改为读画像；`NotGuaranteeMin` 和 `WaitingForMin` 的统计挪到 `JobUpdateHook` | 因为 `OnSessionClose` 的执行顺序是随机的（4.4.3） |
| tdm | 线上只用到 JobOrder，读 `job.Preemptable`，靠画像插件写回内部语义（4.3.3）来保持排序不变 | 以后如果启用 tdm 的受害者功能，需要重新评估 |
| reserve | `PermitedOnGPU` 改为读画像；节点预留信息改为插件在 `OnSessionOpen` 中计算（4.2） | 无 |

**关于 elastic 那一行：** elastic-sla 作业会被 `setDefaultPreemptProperty` 强制设成"不可回收、不可被队列内抢占"，所以对弹性作业来说，"并集"和"Pod 级原值"恰好相等，现在直接读也不会出错。但这种巧合靠不住，还是统一按规则改读 Pod 级原值。

**同一个注解的两种含义：**
- 内部的"可回收"（`ReclaimAble`）读的是 PodGroup 上的 `volcano.sh/preemptable` 注解（`extractReclaimable`）。
- v1.16 社区的 `job.Preemptable` 读的也是这个注解。

如果画像插件不把 `job.Preemptable` 覆盖成内部语义（4.3.3），tdm 排序拿到的就会是"可回收"的含义。

## 附录 B：`gangpreempt` / `gangreclaim` 能否替代线上的 `preempt` / `reclaim`

**结论：不能直接替代，1b 继续使用 v1.16 的 `preempt` / `reclaim`（第 10 节第 10 条）。**

v1.16 新增的 `gangpreempt` / `gangreclaim` 是另一套模型：
- 按 HyperNode 拓扑域选受害者；
- 按受害者作业的 gang 约束把候选任务切成 safe / whole 两种 bundle；
- 驱逐后用和 allocate 相同的逻辑模拟整个抢占作业能否放下。

线上 `preempt` / `reclaim` 依赖的内部能力，有一半以上在它们里面没有对应，其中几处是写死在 action 代码里的规则，插件改不了。

另外，社区设计文档 `docs/design/gang-aware-eviction-design.md` 明确要求：gang 系列不能和 `preempt` / `reclaim` 同时出现在 actions 配置里。所以只能整体切换，没法按场景混用或灰度。

### B.1 线上实际用到的能力

- actions 是 `allocate, reclaim, preempt, backfill`。
- `preempt`、`reclaim` 都配了 `overusedCheckGangEnable: true`，以及 `enableTopologyAwarePreemption` / `enableTopologyAwareReclamation`。因此：
  - `preempt` 实际走 `withoutFitNodePreempt`，满足 relax 条件时走 `relaxEvictByJob`（`relax.go`）；
  - `reclaim` 实际走"按作业调用 `Preemptive(queue, job)`"，不做队列级 `Overused` 和逐任务的 `Allocatable` / `Preemptive` 检查；
  - SuperPod 作业走拓扑感知分支（`topology_aware.go`）。
- 插件侧：
  - priority 配了 `enablePreemptable: false`，gang 配了 `enablePreemptable: false`、`enableReclaimable: false`；
  - 真正决定受害者的是 task-property、elastic、notebook、conformance、capacity；
  - replica-gang 提供 `VictimProtection` 和 `Coeviction`；
  - tdm 关闭了受害者功能，pdb 和 rescheduling 没有配置，所以 `VictimTasks` 线上没有作用。

### B.2 preempt 能力对照

| 内部能力 | `gangpreempt` |
|---|---|
| 抢占者过滤：可回收作业不发起抢占，检查队列 `PreemptLow` / `HasReclaimableJob`（`preempt.go:150`） | 没有 |
| `ActionExcluded`（delay-schedule、reservation） | 没有，action 里不调用 `IsActionExcluded` |
| jobbackoff 退避拦截（`IsBackoffBlocked`） | 没有 |
| 在线超时任务跳过 | 有，前提是翻译成 `SchGated`（4.3.1） |
| 受害者规则：task-property 按 JobProperty 和 `PreemptLow` 判断 | **冲突**：`gangpreempt.go:211` 写死"受害者作业的 PriorityClass 优先级必须更低" |
| 作业内抢占（同一作业的任务之间） | 没有 |
| relaxEvictByJob：只为腾出配额而驱逐，不要求抢占作业 Pipelined 就提交 | 没有，必须 `JobPipelined` 才提交 |
| 逐节点按需驱逐，使用 `VictimTaskOrder`、`VictimProtection`、`StableSortNodesByReclaimCost` | 换成按 bundle 排序，这三项都不调用 |
| SuperPod 拓扑感知抢占 | 换成 HyperNode 拓扑，机制不同 |
| 弹性作业部分扩容（逐个任务抢，抢到几个算几个） | 全有或全无 |

### B.3 reclaim 能力对照

| 内部能力 | `gangreclaim` |
|---|---|
| 队列门禁：按作业调用 `Preemptive(queue, job)`，含内部 capacity 的 nonPreemptAlloc 和弹性逻辑 | 先 `Overused(queue)` 再 `Preemptive(queue, []*TaskInfo)`，签名不同，capacity 要重写 |
| 受害者资格：`ReclaimAble \|\| Elasticable`（`reclaim_victim.go:45`） | **冲突**：见 B.4 第 2 条 |
| capacity 的回收规则 | 要重写成 `UnifiedEvictableFn` |
| 受害者作业排序 `ReclaimOrderFn`（queue-reclaim、notebook） | 不调用，换成 `VictimQueueOrderFn` 加 ROI |
| replica-gang 连带驱逐（`Coeviction`） | 不调用 |
| SuperPod 拓扑感知回收 | 同 B.2 |

ROI 指 bundle 腾出的资源和抢占需求的匹配度，见 `actions/utils/bundle.go` 的 `bundleROI`。

### B.4 关键问题

1. **抢占的受害者规则写死在 action 里。** `gangpreempt.go:211` 只认 `job.Priority`，内部的 `job.Priority` 来自 PriorityClass（`cache.go:1728-1732`），和 JobProperty 是两套东西。要沿用"Prod 抢 Exp、按队列开 `PreemptLow`"的语义，只有两条路：
   - 改社区 action 文件，违反第 1 节的分层原则；
   - 业务侧让所有作业的 PriorityClass 和 JobProperty 严格对齐。
2. **和 4.3.3 的 `job.Preemptable` 覆盖冲突。**
   - `IsJobPreemptableForGangEviction`（`actions/utils/util.go:120`）在 PodGroup 带 `volcano.sh/preemptable` 注解时读 `job.Preemptable`。
   - 内部这个注解的含义是"可回收"，而 4.3.3 要把 `job.Preemptable` 改写成"可被队列内抢占"。
   - 两者叠加，"可回收但不可被队列内抢占"的作业会被 `gangreclaim` 排除。
   - 弹性作业也受影响：`setDefaultPreemptProperty` 会把它的 `Reclaimable` 和 `Preemptable` 都强制置为 false。
3. **`UnifiedEvictable` 没有开关。**
   - `session_plugins.go:330` 不检查 `isEnabled`。priority 配了 `enablePreemptable: false` 也没用，它的 `UnifiedEvictableFn` 照样会在 `gangpreempt` 里按优先级过滤。
   - task-property、elastic、notebook 都要各自补一份 `UnifiedEvictableFn`，4.4.2 的受害者判定表要再实现一遍。
   - `EvictionContext` 只带抢占作业，不带抢占任务，任务级规则要改成作业级。
4. **可能多驱逐。**
   - 没有 HyperNode 时，候选域退化成整个集群（`GetCandidateDomains` 回退到 `ClusterTopHyperNode`，它包含所有节点）。
   - 选择方式是按顺序累加 bundle，直到"域内空闲 + 已选受害者"在总量上够用，再模拟放置；放不下就加下一个 bundle 重试。
   - 最终提交的是整个已选集合，不会剔除放置时没用上的节点上的受害者。GPU 碎片化严重时，驱逐量可能明显大于内部逐节点按需驱逐的结果。
5. **弹性作业部分扩容变成全有或全无。** 已就绪的作业没有配置 SubJob 时，所有任务都在默认 SubJob 里，`runSimulateTrialAtHyperNode` 中任何一个待调度任务放不下，整次模拟就失败。内部的做法是逐个任务抢占，能扩几个算几个。
6. **和 replica-gang 的语义对不上。** bundle 模型允许打破一个 SubJob，但同组其余成员会继续运行，资源被白占。内部 `Coeviction` 的做法是整组一起驱逐。

### B.5 gang 系列的优点，以及以后切换的前提

**优点：**
- 不会无谓地打碎受害者的 gang（线上 gang 插件的受害者过滤是关着的）；
- 驱逐后的放置模拟和 allocate 是同一套逻辑；
- capacity 在每个拓扑域只被调用一次，配额核算更准确；
- 原生支持 HyperNode 和 SubJob。

这些对"大训练作业按拓扑整体抢占"的场景有价值，但当前线上并不依赖。

**以后评估切换，至少要先满足以下前提：**

| 前提 | 对应问题 |
|---|---|
| PriorityClass 和 JobProperty 对齐；或者向社区提 PR，把写死的优先级比较改成插件钩子 | B.4 第 1 条 |
| `IsJobPreemptableForGangEviction` 和内部注解语义的冲突有解法，例如社区改为读 `task.Preemptable`，或内部换一个注解 key | B.4 第 2 条 |
| 内部受害者插件都实现 `UnifiedEvictableFn`；priority 的行为通过配置或代码处理掉 | B.4 第 3 条 |
| 多驱逐控制在可接受范围：配置 HyperNode 缩小候选域，或向社区提议剔除未用上的受害者 | B.4 第 4 条 |
| 接受失去或替代掉：relaxEvictByJob、作业内抢占、弹性部分扩容、`Coeviction`、`VictimProtection`、`ReclaimOrderFn`、`ActionExcluded`、backoff 拦截 | B.2、B.3、B.4 第 5、6 条 |
| SuperPod 作业改用 HyperNode 描述拓扑，配好 network-topology-aware 插件 | B.2 |
| 有独立集群或影子调度器做验证，因为不能和 `preempt` / `reclaim` 共存，没法在线灰度 | 社区约束 |
