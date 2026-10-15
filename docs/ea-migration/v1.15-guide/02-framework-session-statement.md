# 02 · 调度框架变化：Session / 扩展点 / Statement / Bind

> 行号对应 tag **v1.15.3**；以 `v1.9:` 开头的引用对应 tag v1.9.0。

## 0. 一页速览

| 维度 | v1.9 | v1.15 |
|---|---|---|
| 主循环 | `runOnce`：OpenSession → 依次执行 actions → CloseSession | **结构不变**（`pkg/scheduler/scheduler.go:124-153`），只多了 `SetSchGateManager` |
| Session 拓扑视图 | 无 | `HyperNodes`、`RealNodesList/Set`、`HyperNodesTiers`、`HyperNodesSetByTier`，以及虚拟根 `<cluster-top-hypernode>` |
| CycleState | predicates 插件在整个 session 中共用一个 | 按 task UID 各自隔离（`session.go:156-161, 842-875`），可跨 PreFilter → Filter → Reserve → PreBind 共享 |
| PredicateFn | `func(*TaskInfo,*NodeInfo) ([]*Status, error)` | `func(*TaskInfo,*NodeInfo) error` |
| 扩展点数量 | 26 个 `Add*Fn` | 37 个，其中新增 11 个、改签名 3 个 |
| Statement | Evict / Pipeline / Allocate / Commit / Discard | 新增 `Merge`、`SaveOperations`、`RecoverOperations`、`UnPipeline`、`Operations()`；`Pipeline` 多一个 `evictionOccurred` 参数；`Evict` 不再返回 error |
| 卷绑定 | Statement 和 cache 里有 `GetPodVolumes/AllocateVolumes/BindVolumes` | 全部删除，改由 k8s 原生插件处理：predicates 插件包装了 VolumeBinding/DRA，走 Reserve 和 **PreBind** |
| 绑定上下文 | `AddBindTask(*TaskInfo)` | `AddBindTask(*BindContext)`，可携带插件扩展数据，并支持 `PreBinder` |
| 关闭 session 时回写 | `UpdateJobStatus(job, updatePG)` | `UpdateJobStatus(job, updatePGStatus, updatePGAnnotations, updateJobInfo)`，配合 `DirtyJobs` |
| 多调度器 | 无 | 节点分片 `NodesInShard`，参数 `--scheduler-sharding-mode=none\|hard\|soft` |
| 队列准入 | 无 | `SchGateManager`，受 feature gate `SchedulingGatesQueueAdmission` 控制 |

---

## 1. Session 新增字段

结构体见 `pkg/scheduler/framework/session.go:65-164`（v1.9 的结构体在 `v1.9:session.go:44-104`）。

| 字段 | 用途 | 写入方 → 消费方 |
|---|---|---|
| `PodGroupOldState`（替换 v1.9 的 `podGroupStatus`） | 记录 session 开始时每个 PodGroup 的 **Status 和 Annotations** 快照 | `openSession`（232-237）写入；`job_updater.go:105-117` 用它判断状态或注解是否变化 |
| `DirtyJobs` | 记录哪些 Job 需要在 session 结束时，把内存中的 `AllocatedHyperNode` 回写到 cache | `MarkJobDirty`（1012-1014）写入；JobUpdater 读取后调用 `cache.updateJobInfo`（`cache.go:1732-1744`） |
| `schGateManager` | 异步移除 Pod 上的队列准入 gate | `scheduler.go:142` 设置；allocate 中使用 |
| `NodeMap map[string]fwk.NodeInfo` | k8s 的 NodeInfo。v1.9 存的是 `*k8sframework.NodeInfo` 具体类型，现在改为**接口** | — |
| `HyperNodes` / `HyperNodeTierNameMap` / `HyperNodesSetByTier` / `HyperNodesTiers` / `RealNodesList` / `RealNodesSet` / `HyperNodesReadyToSchedule` | 网络拓扑视图 | 见 [03](./03-network-topology-hypernode-subjob.md) |
| `cycleStatesMap` | 每个 task 一份 k8s `CycleState` | `InitCycleState`（842-862）创建，`GetCycleState` 懒加载（864-875）；predicates、capacity、proportion 插件和 preempt action 使用 |
| `NodesInShard` | 当前调度器分片内的节点集合 | 来自 `snapshot.NodesInShard`；见第 6 节 |

### 1.1 openSession 的变化

v1.15 的 `openSession` 在 `session.go:166-283`，v1.9 的在 `v1.9:session.go:106-194`。主要变化如下：

1. 构建 HyperNode 视图（240-247）：
   - 先 `addClusterTopHyperNode` 添加虚拟根；
   - 再 `parseHyperNodesTiers`；
   - 最后 `adjustNetworkTopologySpec`：把 `highestTierName` 翻译成 tier，并把 **soft 模式统一改写为 hard 模式**。
2. HyperNode 就绪后，对每个 Job 执行 `removeInvalidAllocatedHyperNode` 和 `recoverAllocatedHyperNode`。这一步用于调度器重启后，根据已运行 Pod 所在的节点反推出 `AllocatedHyperNode`。
3. **删除了 v1.9 在 openSession 中调用 `JobValid` 并删除不合法 Job 的逻辑**（`v1.9:session.go:160-177`），改为由各 action 自行检查。
4. 插件的 `OnSessionOpen` 执行完后，`framework.go:56` 会调用 `ssn.InitCycleState()`，为每个 task 预先创建 CycleState。

### 1.2 closeSession 的变化

v1.15 在 `session.go:558-578`。

- JobUpdater 现在会计算三个标志位：
  - PG Status 是否变化；
  - PG Annotations 是否变化；
  - 是否属于 dirty job。
- `updateQueueStatus` 中 `allocated` 的统计口径有三处变化：
  - 从只统计 `Running` 改为统计所有 AllocatedStatus；
  - **逐级累加到父队列**（层级队列）；
  - 合并 DRA 资源。

---

## 2. 扩展点（`session_plugins.go`）

### 2.1 新增和改签名的扩展点

| 注册函数 | 状态 | 用途 | 调用方 |
|---|---|---|---|
| `AddVictimQueueOrderFn` | 新增 | 跨队列选受害者时的队列排序，会结合抢占方信息 | `BuildVictimsPriorityQueue`、gangreclaim；目前只有 capacity 插件实现 |
| `AddUnifiedEvictableFn` | 新增 | gang 级别统一过滤受害者，入参是 `EvictionContext{Kind, Job, Task, HyperNode}` | gangpreempt / gangreclaim；由 gang、priority、conformance、capacity 实现 |
| `AddHyperNodeOrderFn` | 新增 | 给候选 HyperNode 打分（按 SubJob） | allocate；由 network-topology-aware 实现 |
| `AddHyperNodeGradientForJobFn` / `ForSubJobFn` | 新增 | 生成按 tier 分层的候选 HyperNode 列表，参数 `SearchPurpose` 区分 Allocate 和 Evict。**只采用第一个注册插件的结果** | allocate、gang* 系列 |
| `AddSubJobReadyFn` / `AddSubJobPipelinedFn` / `AddSubJobOrderFn` | 新增 | SubJob 级的 gang 判定和排序 | allocate、simulate；由 gang、priority 实现 |
| `AddSimulateAddTaskFn` / `RemoveTaskFn` / `AllocatableFn` / `PredicateFn` | 新增 | preempt 拓扑感知模式下，在 **CycleState 副本**上模拟增删 Pod，再判断能否放下 | preempt；由 predicates、capacity、proportion 实现 |
| `AddPredicateFn` | **改签名** | 只返回 `error` | 通过 `PredicateForAllocateAction` / `PredicateForPreemptAction` 间接调用 |
| `AddPreemptiveFn` | **改签名** | `ValidateFn` 改为 `ValidateWithCandidateFn`，多了 `candidates []*TaskInfo` 参数 | reclaim、gangreclaim |
| `AddNodeReduceFn` | 改类型 | 参数改为 `fwk.NodeScoreList` | — |

其余扩展点（JobOrder、QueueOrder、TaskOrder、Preemptable、Reclaimable、JobReady、JobPipelined、PrePredicate、BestNode、NodeOrder、BatchNodeOrder、Overused、Allocatable、JobValid、JobEnqueueable、JobEnqueued、TargetJob、ReservedNodes、VictimTasks、JobStarving）**签名都没有变化**。

另外有一个新的非 Add 类注册入口：`ssn.RegisterBinder(name, binder)`，用来注册 PreBinder（1218-1220）。

### 2.2 其他语义变化

- 插件配置新增 5 个开关：`enabledHyperNodeOrder`、`enabledSubJobReady`、`enabledSubJobPipelined`、`enabledSubJobOrder`、`enabledHyperNodeGradient`（`pkg/scheduler/conf/scheduler_conf.go:96-104`）。
- `BuildVictimsPriorityQueue(victims, preemptor)` 多了 preemptor 参数，处理逻辑如下：
  - 孤儿 task 优先被驱逐；
  - 跨队列时使用 VictimQueueOrderFn；
  - 同队列时先按 Job 排序，再按 Task 排序。
- `Event` 新增 `Err` 字段。插件可以在 AllocateFunc 中报告失败（例如 predicates 插件 Reserve 失败），Statement 会据此回滚（`statement.go:186-190, 293-297`）。
- `Preemptable` / `Reclaimable` 求交集时，初始化方式从 `init` 标志改为 `victims==nil`（`session_plugins.go:297`）。可能带来的语义差异（推断）：某个插件把交集过滤为空后，下一个插件可能重新填充。**内部自定义的 Reclaimable 插件迁移后要回归测试。**
- `framework/arguments.go` 新增泛型方法 `Get[T]` 和 `GetString`；`GetBool` 现在也能接受字符串值。

---

## 3. Statement（`pkg/scheduler/framework/statement.go`）

| API | v1.9 | v1.15 |
|---|---|---|
| `Evict(task, reason)` | 返回 error | **不再返回值**（72-100）。只修改 session 状态并记录操作，真正的驱逐在 Commit 时执行 |
| `Pipeline(task, host, evictionOccurred)` | `(task, host) error` | 多了 `evictionOccurred` 参数（140-205）。出错时会通过 defer 自动 `unPipeline`；`evictionOccurred=true` 时，会在 Pod 状态中写入 `nominatedNodeName`（`api/job_info.go:870-875`） |
| `Allocate(task, node)` | 包含卷处理 | **删除了卷处理**（246-315）；出错时自动 `unallocate` |
| `Commit()` / `Discard()` | 不清空 operations | 执行完后清空 operations（388、414），防止重复执行 |
| `Operations()` | — | 新增，返回当前的操作列表 |
| `Merge(stmts...)` | — | 新增（423-428），接管子 Statement 的全部操作 |
| `SaveOperations(stmts...)` | — | 新增（430-445），把操作**深拷贝**成离线方案 |
| `RecoverOperations(plan)` | — | 新增（447-472），在新的 Statement 上重放 Evict / Pipeline / Allocate |

三种典型用法：

1. **allocate 路径 B**：在每个候选 HyperNode 上试算，`SaveOperations` 后 `Discard`；选出最优方案后用 `RecoverOperations` 重放，再 `Commit`。
2. **preempt / reclaim**：每个节点用一个子 Statement 试算，成功就 `Merge` 到外层，失败就 `Discard`。v1.9 的 reclaim 是直接调用 `ssn.Evict`，执行后**无法回滚**。
3. **gangpreempt / gangreclaim**：先由 `BuildNominationPlanInDomain` 生成方案，再 `RecoverOperations` 重放，最后 `Commit`。

`ssn.Allocate` / `ssn.Pipeline` / `ssn.Evict` 这类不经过 Statement 的直接操作仍然存在。目前只有 backfill 和 shuffle 还在使用。

---

## 4. 插件接口与 k8s 调度框架复用

### 4.1 新增的可选接口

- `framework.BindContextHandler`（`framework/interface.go:53-56`）：`SetupBindContextExtension(state, bindCtx)`。Session 在生成 BindContext 时调用它，把该 task 的 CycleState 中插件需要的数据放进 `bindCtx.Extensions`。
- `cache.PreBinder`（`cache/interface.go:132-137`）：包含 `PreBind(ctx, bindCtx)` 和 `PreBindRollBack(ctx, bindCtx)`。插件需要在 `OnSessionOpen` 中通过 `ssn.RegisterBinder` 注册。
- 目前只有 **predicates 插件**同时实现了这两个接口，用于 VolumeBinding 和 DRA 的 PreBind（`plugins/predicates/predicates.go:831-889`）。

`Plugin` 接口本身（Name / OnSessionOpen / OnSessionClose）和 `Action` 接口**都没有变化**。

### 4.2 `fwk` 与 `k8sframework`

v1.15 的代码里同时 import 了两个包：

```go
fwk "k8s.io/kube-scheduler/framework"                 // 接口和类型：NodeInfo、CycleState、Handle、FilterPlugin、Status、NodeScoreList、SharedDRAManager…
k8sframework "k8s.io/kubernetes/pkg/scheduler/framework" // 具体实现的构造函数：NewCycleState()、NewNodeInfo()、NewPodInfo()
```

- 上游 kube-scheduler 把框架接口拆到了 staging 仓库 `k8s.io/kube-scheduler/framework`，Volcano 跟进了这一调整。读代码时可以这样区分：**`fwk.X` 是接口，`k8sframework.NewX` 是实现**。
- `pkg/scheduler/plugins/util/k8s/framework.go` 实现了 `fwk.Handle`，构造方式为 `NewFramework(nodeMap, WithSharedDRAManager(...), WithClientSet(...), ...)`。predicates 插件用它来实例化 k8s 原生插件。

**内部插件迁移时的影响**：所有直接使用 `k8sframework.NodeInfo`、`CycleState`、`NodeScoreList` 类型的代码，都要改用 `fwk` 中对应的接口类型。

---

## 5. Cache 与 Bind 流程

### 5.1 Cache 接口变化（`cache/interface.go:41-137`）

- `AddBindTask(*TaskInfo)` 改为 `AddBindTask(*BindContext)`。
- 删除了 `GetPodVolumes`、`AllocateVolumes`、`BindVolumes`、`RevertVolumes` 和 `VolumeBinder`。
- 新增 `VCClient`、`RegisterBinder`、`SharedDRAManager`、`IsJobTerminated`、`UpdateNodeShardStatus`、`OnSessionOpen`、`OnSessionClose`。
- `Binder.Bind` 的返回值改为 `map[TaskID]string`，逐个 task 返回错误信息。
- `StatusUpdater.UpdatePodCondition` 改为 `UpdatePodStatus`。

### 5.2 从 Commit 到真正 Bind

```text
Statement.Commit
  └─ Statement.allocate
       ├─ Session.CreateBindContext          // 各 BindContextHandler 填入扩展数据
       └─ cache.AddBindTask(bindCtx)         // task 置为 Binding，加到节点上，放入 BindFlowChannel（容量 5000）
后台 processBindTask（每 20ms 一次，按环境变量 BATCH_BIND_NUM 批量处理，默认 1）
  └─ BindTask（goroutine）
       ├─ executePreBinds                    // 失败：逆序 PreBindRollBack → resyncTask → 置为 Unschedulable
       └─ Bind → Binder.Bind                 // 失败：所有 PreBinder 回滚 → resyncTask；成功：发 Scheduled 事件
```

代码位置：`cache.go:1342-1475`、`984-1016`。v1.9 的流程是 `BindVolumes` → `Bind`，失败时 `RevertVolumes`，没有 PreBind 这层抽象。

**冲突感知绑定（ConflictAwareBinder）只存在于新的 `agent-scheduler` 中**（`pkg/agentscheduler/cache/binder.go`），批调度器 vc-scheduler 没有。

---

## 6. 调度器分片与队列准入 gate

### 6.1 节点分片（NodeShard）

- **要解决的问题**：多个调度器（例如 vc-scheduler 和 agent-scheduler）并行工作时，如果共用全局视图，资源冲突会很多。做法是由 `sharding-controller`（默认关闭）把节点划分成若干 `NodeShard`（`shard.volcano.sh/v1alpha1`），各调度器在自己的分片内调度。
- **参数**：`--scheduler-sharding-mode=none|hard|soft`（默认 none），`--scheduler-sharding-name`（默认 `volcano`）。
- **hard 模式**：`PredicateNodes` 直接把分片外的节点判为不可用（`util/predicate_helper.go:102-111`）。
- **soft 模式**：分片外的节点降为低优先级候选。allocate 的四级候选（见 [01](./01-allocate-action.md) 3.5 节），以及 backfill / preempt / reclaim 中的 `GetPredicatedNodeByShard`，都是为这个模式服务的。
- 默认 `none` 模式下，所有分片相关代码都不生效。

### 6.2 SchedulingGates 队列准入（QueueAllocationGate）

- **要解决的问题**：队列配额不足时，Volcano 会给 Pod 打上 Unschedulable，Cluster Autoscaler 或 Karpenter 会误以为集群资源不够而扩容。启用后，Pod 只有在**真正缺少集群资源**时才会显示 Unschedulable。
- **流程**：
  1. Pod 带注解 `scheduling.volcano.sh/queue-allocation-gate: "true"`。
  2. webhook 为该 Pod 注入同名的 schedulingGate。
  3. allocate 中，该 Pod 通过 `Allocatable` 检查后，交给 `SchGateManager`（5 个 worker，可通过 `--gate-removal-worker-num` 调整）异步移除 gate。
  4. capacity 插件维护一份"已放行、待分配"的预留账本，防止这部分额度被其他 Pod 抢走。
- 前提条件：feature gate `SchedulingGatesQueueAdmission`（Alpha，默认关闭），并且 k8s 开启 `PodSchedulingReadiness`。
- 详见 [05](./05-queue-capacity-enqueue.md)。

---

## 7. Feature gate

Volcano 自己的 feature gate 定义在 `pkg/features/volcano_features.go`。v1.15 相比 v1.9 新增两个：

- `CronVolcanoJobSupport`（默认 true，供 controller 使用）
- `SchedulingGatesQueueAdmission`（Alpha，默认 false）

调度器还会读取 k8s 的 feature gate：

- `PodSchedulingReadiness`：不开启时 `SchGated` 恒为 false。
- `DynamicResourceAllocation`、`DRA*`
- `InPlacePodVerticalScaling`、`PodLevelResources`

这些 gate 的默认值都随调度器编译时依赖的 k8s 版本而定。**在 k8s 1.19 集群上运行时，需要显式设置 emulation version 或关闭相关 gate**，详见 [../v1.9-to-v1.15-upstream-changes.md](../v1.9-to-v1.15-upstream-changes.md)。

---

## 8. 建议阅读顺序

1. `framework/framework.go`，再看 `session.go:166-283`：了解一个 session 是怎么建立起来的。
2. `session_plugins.go:35-222`：浏览全部扩展点。
3. `statement.go`，再看 `cache.go:1342-1475`：从 Commit 一直跟到 Bind。
4. `plugins/predicates/predicates.go:192-335, 825-893`：k8s 插件、CycleState、PreBind 是怎么接起来的。
5. 回到 [01 allocate](./01-allocate-action.md)，看上面这些机制在主路径上如何组合使用。
