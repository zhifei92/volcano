# 07 · allocate 路径 A 深入分析（v1.15.3）

> **版本说明**：仓库当前 HEAD 是 v1.16.0-rc.0，IDE 里打开的 `allocate.go` 第 398-399 行是 master 版本。
> 本文全部基于 **v1.15.3**，同一段代码在 v1.15.3 的 `pkg/scheduler/actions/allocate/allocate.go` 第 325-326 行。
> 本文所有 `allocate.go:NNN` 行号都指 v1.15.3。查看方式：`git show v1.15.3:pkg/scheduler/actions/allocate/allocate.go`，或者用 `git worktree add ../volcano-v1.15.3 v1.15.3`。

路径 A 就是"普通作业"走的那条路：作业没有 hard 网络拓扑（soft 拓扑在 openSession 时已被转换成 hard），也没有配置 subGroupPolicy。内部绝大多数作业都会走这条路，所以它是迁移时最需要吃透的部分。

---

## 1. 一句话总结

**路径 A 本质上就是 v1.9 的 allocate。** 它只是换了一层 SubJob 的外壳（默认 SubJob 等于整个 Job），然后在 v1.9 的基础上加了 5 样东西：

1. 调度门控（SchedulingGates）的处理；
2. 抢占"提名 HyperNode"的快速路径（在 v1.15.3 中基本走不到，见第 6 节）；
3. 节点分片（sharding）带来的 4 桶节点优选；
4. `PrePredicate` 和 `PredicateForAllocateAction` 的新错误语义；
5. 按"角色"（TaskRole）缓存预选失败，并在后续跳过同角色任务。

---

## 2. 路径 A 的入口条件与数据准备

### 2.1 哪些作业进路径 A

```go
// allocate.go:305 附近（v1.15.3），分派条件
if job.ContainsHardTopology() || job.ContainsSubJobPolicy() {
    // 路径 B：allocateForJob（HyperNode 梯度 + dry-run）
} else {
    // 路径 A：本文主题
}
```

| 作业形态 | 走哪条路 |
|---|---|
| 普通 Job / PodGroup，没有 networkTopology，也没有 subGroupPolicy | **A** |
| `networkTopology.mode: hard`，并且设置了 `highestTierAllowed` 或 `highestTierName`（openSession 会把名字翻译成 tier） | B |
| `networkTopology.mode: soft` | B（openSession 时转成以 ClusterTopHyperNode 为最高层的 hard 模式） |
| `mode: hard`，但 `highestTierAllowed` 和 `highestTierName` 都**没有**设置 | **A**。`IsHardTopologyMode()` 要求 tier 非空（`api/job_info.go:1265`），所以这类作业会被当成无拓扑作业，这是个容易踩的坑 |
| 配置了 subGroupPolicy（不论有没有拓扑） | B |

### 2.2 `tasksNoHardTopology` 是怎么来的

```go
// allocate.go:142-206  buildAllocateContext（节选）
for _, job := range ssn.Jobs {
    // ... 过滤：Pending 状态的 PodGroup、JobValid 校验失败、queue 不存在 等
    worksheet := alloc.organizeJobWorksheet(job)
    if worksheet.Empty() { continue }
    // ...
    if !job.ContainsHardTopology() {
        subJobWorksheet := worksheet.subJobWorksheets[job.DefaultSubJobID()]
        actx.tasksNoHardTopology[job.UID] = subJobWorksheet.tasks   // allocate.go:200
    }
}
```

需要注意 4 点：

1. **`tasksNoHardTopology[job.UID]` 和 `worksheet.subJobWorksheets[默认ID].tasks` 是同一个 PriorityQueue 对象。** 路径 A 里 Pop 掉的任务，worksheet 里也随之消失。
2. 有 subGroupPolicy 但没有拓扑的作业，在这里也会被写入 `tasksNoHardTopology`，但分派时它们走路径 B，这个 entry 用不到。
3. `organizeJobWorksheet`（allocate.go:208-281）构建队列时会跳过两类任务：
   - 带**外部** scheduling gate 的任务（Volcano 自己的 QueueAllocationGate 除外）；
   - BestEffort 任务（`Resreq` 为空），它们留给 backfill 处理。

   没有剩余 Pending 任务的作业，worksheet 为空，不会进入 `jobsByQueue`。已经 Ready 的弹性作业只要还有 Pending 任务，照样会进来。
4. 这个 PQ 按 `TaskOrderFn` 排序，**每次执行 allocate action 只构建一次**。任务被 Pop 后不会再 Push 回去，所以**每个任务在一次 allocate 中最多只被尝试一次**。这是理解后面所有"任务丢失"现象的关键。

---

## 3. 外层循环：queue → job → 路径 A

```go
// allocate.go:283-368  allocateResources（路径 A 相关部分）
for !queues.Empty() {
    queue := queues.Pop().(*api.QueueInfo)
    if ssn.Overused(queue) { continue }            // ① 本次 action 中这个 queue 被永久丢弃
    jobs := actx.jobsByQueue[queue.UID]
    if jobs == nil || jobs.Empty() { continue }    // ② queue 不再 Push 回去，循环靠这一步收敛
    job := jobs.Pop().(*api.JobInfo)

    // 路径 A
    subJob, sjExist := job.SubJobs[job.DefaultSubJobID()]       // :325
    tasks, tasksExist := actx.tasksNoHardTopology[job.UID]      // :326
    if sjExist && tasksExist {
        var stmt *framework.Statement
        if subJob.NominatedHyperNode != "" { /* 提名快速路径，见第 6 节 */ }
        if stmt == nil {
            stmt = alloc.allocateResourcesForTasks(subJob, tasks, framework.ClusterTopHyperNode)
        }
        if stmt != nil && ssn.JobReady(job) {      // ③ 只有 Ready 才 Commit；只 Pipelined 不 Commit
            stmt.Commit()
            if subJob.NominatedHyperNode != "" { subJob.NominatedHyperNode = "" }
            if tasks.Len() > 0 { jobs.Push(job) }  // ④ 还有剩余任务才 Push 回去
        }
    } else {
        klog.ErrorS(nil, "Can not find default subJob or tasks for job", ...)
    }
    queues.Push(queue)                             // ⑤ 每处理一个 job 都把 queue Push 回去，触发重排
}
```

### 3.1 一个 job 被 Pop 一次的 3 种结局

| `allocateResourcesForTasks` 结束时的状态 | stmt 怎么处理 | job 是否 Push 回去 | session 里的效果 |
|---|---|---|---|
| `JobReady`（gang 已满足） | 返回 stmt，然后 **Commit** | 剩余任务 > 0 时 Push 回去 | Allocated 的任务进入 Binding；Pipeline 操作在 Commit 时是空操作，只留在 session 内 |
| 没 Ready，但 `JobPipelined` | 返回 stmt，**既不 Commit 也不 Discard** | **不 Push** | 这些任务以 Pipelined 状态留在 session 中，占住节点的 FutureIdle，后续作业看不到这部分资源 |
| 两者都不满足 | `Discard()` 回滚，返回 nil | **不 Push** | 本次 action 中这个 job 就此结束，而且它已经被 Pop 过的任务也都消耗掉了 |

### 3.2 关键行为：Ready 之后"每次 Pop 只分配 1 个任务"

`allocateResourcesForTasks` 里，每成功分配一个任务就会检查 `SubJobReady`（无 subGroupPolicy 时等价于 `JobReady`），一旦满足就 `break`（allocate.go:849）。所以：

- **第一次 Pop**：连续分配，直到凑满 minAvailable，然后 Commit，再 Push 回去；
- **之后每次 Pop**：作业本来就已经 Ready，分配完 1 个任务后立刻 break，Commit 这 1 个任务，再 Push 回去；
- 每轮结束 queue 也会被 Push 回去，重新按 `QueueOrderFn` 排序，job 也会重新按 `JobOrderFn` 排序（gang 插件把未 Ready 的作业排在前面）。

结果就是：**作业中超出 minAvailable 的弹性部分，会以"单个任务"为粒度和其他作业、其他队列交错分配。** 这一点和 v1.9 一致，但内部如果有"一次把一个作业的副本全部分完"的假设（比如 replica-gang），要特别留意。

### 3.3 例子

两个队列权重相同，都在 proportion/capacity 允许的范围内：

- job A（q1）：minAvailable=2，replicas=4
- job B（q2）：minAvailable=1，replicas=2

| 轮次 | Pop 出的 queue / job | 本轮分配 | 结局 |
|---|---|---|---|
| 1 | q1 / A | A-0、A-1（凑满 gang） | Commit，A 还剩 2 个任务，Push 回去 |
| 2 | q2 / B | B-0 | Commit，B 还剩 1 个，Push 回去 |
| 3 | 按份额重排后，假设轮到 q2 / B | B-1 | Commit，B 没有剩余任务，不 Push |
| 4 | q1 / A | A-2（Ready 后只分 1 个） | Commit，Push 回去 |
| 5 | q2：jobs 为空 | — | q2 不再 Push 回去，退出 |
| 6 | q1 / A | A-3 | Commit，A 没有剩余任务 |
| 7 | q1：jobs 为空 | — | 循环结束 |

---

## 4. `allocateResourcesForTasks` 逐步拆解（allocate.go:719-867）

入参：`subJob` 是默认 SubJob；`tasks` 是第 2 节的 PQ；`hyperNode` 是 `ClusterTopHyperNode`，它在 `RealNodesList` 中对应的就是 session 里的全部节点（`ssn.NodeList`）。

函数开头会创建一个新的 `stmt` 和一个新的 `PredicateHelper`。**因此预选错误缓存的作用域只是"这一次调用"，也就是某个 job 的一次 Pop。**

对每个 Pop 出的任务，依次执行：

### 步骤 1：`ssn.Allocatable(queue, task)`（:744）

- 按 tier 遍历插件，任意一个启用了的插件返回 false，结果就是 false（capacity / proportion 都实现了这个钩子）。
- 失败时直接 `continue`，**不检查 `NeedContinueAllocating`**。如果队列额度在中途用完，后面所有任务都会被 Pop 出来，并一个个失败。对 gang 作业来说，最后 Discard，整个作业本轮结束。
- 这只是一次判定，不扣减额度；真正的扣减发生在 `stmt.Allocate` 触发的事件回调里。

### 步骤 2：调度门控（:750-765）

- 开启 `SchedulingGatesQueueAdmission` 特性门控、任务带有 Volcano 的 QueueAllocationGate、并且通过了步骤 1：把任务加入异步摘除 gate 的工作队列。
- 只要任务仍然处于 `SchGated` 状态，**本轮就跳过**。gate 被摘掉后，最早下一个调度周期才会真正分配，所以有**至少一个周期的延迟**。
- 坑：步骤 1 不扣减额度，同一轮里多个带 gate 的任务都会拿"同一份剩余额度"做判定，于是可能一次摘掉的 gate 总量超过队列剩余额度。下一轮它们再正常竞争，超出的部分会重新 Pending。

### 步骤 3：`job.TaskHasFitErrors(subJob.UID, task)`（:768）

- 通过 `FitFailedRoles(subJob)` 得到"已经有任务预选失败过的角色"集合（数据来自 `job.NodesFitErrors`，按 SubJob 过滤），如果当前任务的角色在集合里，就直接跳过。
- `TaskRole` 为空时这一步不生效。
- **`job.NodesFitErrors` 在路径 A 中不会被重置**（`ResetFitErr` 只在路径 B 的 :391 调用），而且挂在 session 的 JobInfo 上。所以**某个角色在本 session 失败过一次以后，即使 job 之后被 Push 回去再 Pop，这个角色剩下的任务也全部会被跳过**，哪怕期间别的作业的 Discard 已经释放了资源。
- 跳过时同样不检查 `NeedContinueAllocating`，直接 `continue`。

### 步骤 4：`ssn.PrePredicateFn(task)`（:779）

- 失败时把**所有节点**都记为这个错误，写进 `NodesFitErrors`，然后根据 `NeedContinueAllocating` 决定 `continue` 还是 `break`。
- 下一个同角色任务会在步骤 3 被跳过。

### 步骤 5：优先尝试 `NominatedNodeName`（:798-805）

条件：Pod 的 `Status.NominatedNodeName` 非空，该节点在本次叶子节点集合里，并且 `InitResreq <= FutureIdle`。满足时**只对这一个节点做预选**；通过的话，步骤 7 中桶里只有 1 个节点，会被直接选中，不打分。所以 preempt 留下的提名节点在路径 A 里是"强绑定"。

### 步骤 6：`ph.PredicateNodes(task, nodes, alloc.predicate, enableCache, ssn.NodesInShard)`（:808）

`util/predicate_helper.go` 内部逻辑：

- **只找"够用"的节点，不扫全部**：`numNodesToFind = CalculateNumOfFeasibleNodesToFind(len(nodes))`。默认参数为 `minimum-feasible-nodes=100`、`percentage-nodes-to-find=0`（自适应）、`minimum-percentage-nodes-to-find=5`：
  - 节点数 ≤ 100：全部扫描；
  - 否则比例 = `50 - N/125`，下限 5%。例如 1000 个节点时是 42%，即找到 420 个可行节点就 cancel；5000 个节点时是 10%，即 500 个。
- **起点轮转**：包级全局变量 `lastProcessedNodeIndex` 记录上次扫到的位置，下次从那里接着扫。
- **16 路并发**（`workqueue.ParallelizeUntil(ctx, 16, ...)`），找够数量后 cancel。
- **结论**：打分只在"本次碰巧找到的那部分可行节点"中进行，选出的是**局部最优**；同一个任务两次调度可能看到不同的候选集。这和 kube-scheduler 的 `percentageOfNodesToScore` 是同一个思路。**大集群里如果发现 binpack 效果不理想，先查这几个参数**，必要时设置 `--percentage-nodes-to-find=100`。
- **错误缓存**：key 是 `job/TaskRole`（`TaskRole` 为空时不缓存）。只有当这一个 helper 中该角色之前失败过，才会读取缓存。由于步骤 3 会直接跳过整体失败的角色，缓存实际上只在"部分节点失败、但仍有可行节点"的场景下起作用。
- **硬分片**：`ShardingMode=hard` 时，不在本分片的节点直接记为失败。

单节点预选函数 `alloc.predicate`（:985-993）：

1. 先检查资源：`InitResreq <= node.FutureIdle()`，不满足就返回 `Unschedulable`。注意比较对象是 **FutureIdle = Idle + Releasing − Pipelined**，不是 Idle。
2. 再调用 `ssn.PredicateForAllocateAction`（`framework/session.go:666`）：
   - 插件返回的不是 `*FitError`：包装成 FitError，视为失败；
   - 是 FitError，并且 Status 中包含 `Unschedulable`、`UnschedulableAndUnresolvable` 或 `ErrorSkipOrWait`：失败；
   - **是 FitError，但 Status 不属于上面三种（例如只有 `Error`）：视为通过。** 内部插件迁移时，如果返回的 FitError 里写的是 `api.Error` 之类的 code，会被 allocate 静默放行。v1.9 没有这层语义，要逐个检查。

### 步骤 7：预选全部失败（:810-831）

- 写入 `job.NodesFitErrors[task.UID]`，后续同角色任务会在步骤 3 被跳过。
- `NeedContinueAllocating(subJob)`（`api/job_info.go:949`）决定是否继续：
  - `MinAvailable == len(Tasks)`（作业要求全部到齐）或者 `subJob.MinAvailable >= len(subJob.Tasks)`：返回 false，直接 break，因为少一个都不可能凑成 gang；
  - 作业带 subJob policy：返回 true（路径 A 不会遇到）；
  - 其余情况：
    - 没有按角色设置 minMember（`MinAvailable < TaskMinAvailableTotal`）时，看 `ReadyTaskNum + 剩余待分配数 >= MinAvailable` 是否还有可能成立；
    - 按角色设置了 `TaskMinAvailable` 时，逐个角色检查剩余任务是否还能凑够。
- break 后回到函数末尾的 Ready / Pipelined 判断。

### 步骤 8：`prioritizeNodes`（:882-951）—— 4 个桶

| 优先级 | 桶 | 条件 |
|---|---|---|
| 1 | 本分片、Idle 够用 | `InitResreq <= Idle` |
| 2 | 其他分片、Idle 够用 | 只在 soft 分片模式下非空 |
| 3 | 本分片、FutureIdle 够用 | `InitResreq <= FutureIdle` |
| 4 | 其他分片、FutureIdle 够用 | 只在 soft 分片模式下非空 |

- 按顺序取第一个非空的桶。桶里只有 1 个节点时**不打分，直接选中**；多个节点时调用 `util.PrioritizeNodes`（16 路并发执行 `NodeOrderMapFn`，然后 `NodeOrderReduceFn`，再叠加 `BatchNodeOrderFn`）。
- 打分后先让插件的 `BestNodeFn` 决定，它返回 nil 时再用 `SelectBestNodeAndScore`，**最高分并列时随机选一个**。
- 含义：**只要有任何一个节点能用 Idle 直接放下，就绝不会选"需要等待资源释放"的节点**，即使后者打分更高。这一点和 v1.9 的两档逻辑相同，只是 v1.15 多了分片维度。
- 返回 `nil` 时直接 `continue`，**不记录 fit error**，比较隐蔽。

### 步骤 9：`allocateResourcesForTask`（:953-983）

- `InitResreq <= node.Idle`：调用 `stmt.Allocate(task, node)`，依次：
  1. 设置 `task.Pod.Spec.NodeName`；
  2. 更新任务状态为 Allocated；
  3. 调用 `node.AddTask`；
  4. 触发各插件的 `AllocateFunc` 事件回调（capacity / proportion / drf 等在这里扣减额度）。
  任何一个回调设置了 `Event.Err`，都会通过 defer 中的 `unallocate` 回滚。
- 否则如果 `InitResreq <= FutureIdle`：调用 `stmt.Pipeline(task, node, evictionOccurred=false)`。
- 出错时只打日志，外层 `continue`，**该任务本轮丢失**。

### 步骤 10：`SubJobReady` 时 break（:849）

gang 插件的 `JobReady` 判定是 `CheckTaskReady() && CheckSubJobReady() && IsReady()`，其中 `ReadyTaskNum` 统计的是 Bound / Binding / Running / **Allocated** / Succeeded 状态的任务。所以本 stmt 中刚 Allocate 的任务**立即计入**，Pipelined 的任务不计入。

### 函数末尾（:854-866）

```go
if ssn.SubJobReady(job, subJob)          { return stmt }  // soft 拓扑写回 AllocatedHyperNode（路径 A 不涉及）
else if ssn.SubJobPipelined(job, subJob) { return stmt }  // 交给外层：不 Commit，也不 Discard
stmt.Discard(); return nil
```

---

## 5. Commit 之后

`Statement.Commit()`（`framework/statement.go:392`）逐个执行记录下来的操作：

- **Allocate**：`s.allocate(task)` 依次执行 `CreateBindContext`（v1.15 新增，PreBinder 插件在这里准备数据）、`cache.AddBindTask` 进入异步 bind 流程，并把状态置为 Binding。失败时调用 `unallocate` 回滚。
- **Pipeline**：`s.pipeline(task)` 是**空函数**。Pipelined 状态只存在于 session 内，session 关闭后不会写回任何东西（`NominatedNodeName` 只在 `evictionOccurred=true` 时由 preempt / reclaim 设置）。
- **Evict**：路径 A 不会产生。

所以一个"已 Ready 但部分任务是 Pipeline"的作业：Allocated 的任务进入 bind，Pipelined 的任务下个周期重新调度。

---

## 6. 提名 HyperNode 快速路径（:595-717）

```go
if subJob.NominatedHyperNode != "" {
    if s, _, ok := alloc.allocateFromNomination(subJob, sjWorksheet, ssn.HyperNodes[ClusterTopHyperNode]); ok { stmt = s }
}
```

设计意图：gangpreempt / gangreclaim 为整个 gang 腾出某个 HyperNode，并为每个任务写入 `NominatedNodeName`；下一轮 allocate 直接按提名逐个校验并落位（`validateNomination`：Allocatable、NominatedNodeName 在叶子节点集合中、PrePredicate、单节点 Predicate）。任何一个任务不满足，就调用 `invalidateSubJobNomination` 清空提名，然后回退到普通的 `allocateResourcesForTasks`。

**在 v1.15.3 中这条路径基本走不到：**

1. `NominatedHyperNode` 只是 session 内 SubJobInfo 上的字段，跨 session 时不会被保留（`UpdateJobInfo` 时丢失）。master 的 [#5476](https://github.com/volcano-sh/volcano/pull/5476)（commit `691bd2d88`，测试用例 `TestUpdateJobInfo_PropagatesNominatedHyperNode`）修复了这个问题。
2. 常见的 action 顺序是 `enqueue, allocate, gangpreempt, ...`，同一个 session 里 allocate 先于 gangpreempt 执行，所以 allocate 看不到本 session 设置的提名。

**潜在的副作用（真正触发时才有）：** `TaskInfo.Clone()` 会共享 `Pod` 指针（`Pod: ti.Pod`）。`invalidateSubJobNomination` 直接执行 `task.Pod.Status.NominatedNodeName = ""`，修改的是 scheduler cache 乃至 informer 共享的 Pod 对象。`Statement.Allocate` 设置 `task.Pod.Spec.NodeName` 也是同样的模式（v1.9 就已存在）。目前不影响功能，但如果内部有代码直接读 cache 里 Pod 的这些字段，结果可能和预期不一致。

---

## 7. 任务、作业、队列在一次 allocate 中的去向汇总

| 情况 | 任务 | 作业 | 队列 |
|---|---|---|---|
| `Overused(queue)` | 不涉及 | 该队列的所有作业本轮都不处理 | **本轮被丢弃** |
| `Allocatable` 失败 / 被门控 / 角色已失败 | Pop 后跳过，**本轮丢失** | 继续处理下一个任务 | — |
| 预选失败 + `NeedContinue=true` | 本轮丢失 | 继续 | — |
| 预选失败 + `NeedContinue=false` | 剩余任务仍在 PQ 中 | 走 Ready / Pipelined / Discard 判断；Discard 时作业本轮结束 | — |
| Discard | 已 Pop 的任务丢失，剩余任务在 PQ 中但没人再处理 | **本轮结束** | Push 回去，继续处理该队列的其他作业 |
| 只 Pipelined | 以 Pipelined 状态留在 session | **本轮结束**，stmt 悬空，资源被预留 | Push 回去 |
| Ready + Commit | Allocated 的任务进入 Binding | 有剩余任务就 Push 回去 | Push 回去 |
| 队列下没有作业了 | — | — | 不再 Push 回去（循环靠这一步收敛） |

后续 action 的兜底：`backfill` 处理 BestEffort 任务；`preempt` / `reclaim` / `gangpreempt` / `gangreclaim` 处理"放不下"的任务；其余任务等下一个调度周期（默认 1s）重新排队。

---

## 8. 和 v1.9 的差异速查（只看路径 A）

| 点 | v1.9 | v1.15.3 路径 A |
|---|---|---|
| 入口 | `allocateResources` 直接处理 job | 多了 SubJob 外壳，通过 `tasksNoHardTopology` 拿任务队列 |
| 门控 | 无 | 带 QueueAllocationGate 的任务：通过 Allocatable 后异步摘 gate，本轮跳过 |
| PrePredicate 失败 | 所有节点记为失败，然后**直接 break** | 所有节点记为失败，然后检查 `NeedContinue`：弹性作业会继续尝试其他任务 |
| 插件预选返回值 | `PredicateFn` 返回 `([]*Status, error)` | 只返回 `error`，经过 `PredicateForAllocateAction` 按 Status code 过滤，**非"不可调度"类 code 会被放行** |
| 节点优选 | Idle / FutureIdle 两档 | 4 个桶（多了 soft 分片维度） |
| 硬分片 | 无 | `PredicateNodes` 直接拒绝分片外的节点 |
| 提名 HyperNode | 无 | 有，但在 v1.15.3 基本走不到 |
| Commit 后 | AddBindTask | 先 `CreateBindContext`（PreBinder），再 AddBindTask |
| 一次 Pop 的分配量 | Ready 后每次 1 个 | 相同 |

---

## 9. 内部功能迁移到路径 A 时的挂载点建议

| 内部能力 | 在 v1.9 中的位置（推测） | v1.15.3 路径 A 中建议的挂载点 |
|---|---|---|
| `Overused(queue, req)`（按请求量判断是否超用） | allocate 外层 | 社区 `Overused(queue)` 没有 req 参数。建议把按任务的判断放到 `AllocatableFn`，它在步骤 1 被逐任务调用；不要改外层签名 |
| 可驱逐 / 不可驱逐任务使用不同额度（内部 capacity 语义） | capacity 插件 | 同样放在 `AllocatableFn`，再配合 `AllocateFunc` 事件回调扣减；同时注意社区 capacity 没有实现 `OverusedFn` |
| replica-gang / SchedulingUnit | allocate 内部循环 | 路径 A 的"Ready 后每次分 1 个"会把副本拆开分配。如果要求以单元为粒度，优先考虑 subGroupPolicy（走路径 B），而不是改路径 A 的循环 |
| ActionExcluded（某些作业不参与某些 action） | 各 action 入口 | 建议在 `buildAllocateContext` 的作业过滤阶段（:142-206）判断，避免改动分派逻辑 |
| 自定义预选插件 | `PredicateFn` | 改成只返回 `error`，**确保不可调度时返回的 FitError 的 code 是 `Unschedulable` 或 `UnschedulableAndUnresolvable`** |

---

## 10. master（v1.16.0-rc.0）在这一段的变化

`git diff v1.15.3 HEAD -- pkg/scheduler/actions/allocate/allocate.go` 共 +404/−60 行，和路径 A 相关的有：

- 分派条件变为 `job.RequiresHyperNodeAllocate()`，等于 `ContainsHardTopology() || ContainsSubJobPolicy() || ContainsHardPodGroupAntiAffinity() || HasPreferredPodGroupAntiAffinity()`。也就是说，**配置了 PodGroup 级反亲和的作业也会离开路径 A**，改走路径 B。
- `allocateResources` 改为返回 `committed bool`，用来判断本轮是否有提交。
- `NominatedHyperNode` 能跨 session 保留（#5476），提名快速路径开始真正生效。

升级目标是 v1.15.x 的话，可以暂时忽略这些变化，但后续再升 v1.16 时需要回头看。
