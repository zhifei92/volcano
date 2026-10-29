# Volcano v1.15 速通指南（写给熟悉 v1.9 的人）

> 本系列以 **tag v1.15.3** 的源码为准，所有行号都对应这个 tag；以 `v1.9:` 开头的引用对应 v1.9.0。
> 文档里标"推断"或"待确认"的内容，是读代码得出的结论，没有经过运行验证。

## 0. 本地对照阅读的准备

```bash
# 在仓库外各检出一份只读源码，方便左右对照
git worktree add ../volcano-v1.15.3 v1.15.3
git worktree add ../volcano-v1.9.0  v1.9.0
# 用完删除：git worktree remove ../volcano-v1.15.3
```

## 1. 一页看懂 v1.9 → v1.15 变了什么

**没变的骨架**：`runOnce` 仍然是 OpenSession → 依次执行 actions → CloseSession。Plugin 和 Action 接口没变。大部分扩展点（JobOrder、QueueOrder、Allocatable、Preemptable、Reclaimable、JobReady……）的签名也没变。默认调度配置与 v1.9 **完全相同**。

**变了的部分，按读代码时遇到的先后顺序排列：**

1. **allocate 拆成两条路径。**
   - 普通作业走"老路径"，与 v1.9 基本相同，只是节点集合换成了虚拟根 HyperNode 下的全部节点。
   - 拓扑作业和 SubJob 作业走"新路径"：按 HyperNode 梯度逐个试算，选出最优方案后重放。
   - → [01](./01-allocate-action.md)
2. **每个 Job 至少有一个 SubJob。** 没有配置 subGroupPolicy 时，Job 只有一个"默认 SubJob"，它就代表整个 Job。代码里随处可见的 `subJob` 都可以这样理解。→ [01](./01-allocate-action.md)、[03](./03-network-topology-hypernode-subjob.md)
3. **Statement 成了事务和试算工具。**
   - `Evict` 不再返回 error，改为在 Commit 时才真正执行。
   - `Pipeline` 多了 `evictionOccurred` 参数。
   - 新增 `Merge`、`SaveOperations`、`RecoverOperations`。
   - preempt 和 reclaim 都改成了"每个节点开一个子 stmt，失败就 Discard"。
   - → [02](./02-framework-session-statement.md)
4. **PredicateFn 改为只返回 `error`。** 谓词逻辑收敛到 `PredicateForAllocateAction` 和 `PredicateForPreemptAction` 两个函数。→ [02](./02-framework-session-statement.md)
5. **CycleState 按 task 隔离，引入 `fwk` 接口包。**
   - 卷绑定和 DRA 改为 k8s 原生插件 + PreBind 实现，`AddBindTask` 改为接收 `BindContext`。
   - 代码里同时出现 `fwk.X` 和 `k8sframework.NewX`：前者是接口，后者是实现。
   - → [02](./02-framework-session-statement.md)
6. **新增网络拓扑调度能力。** 包括 HyperNode CRD、虚拟根 `<cluster-top-hypernode>`、network-topology-aware 插件，以及 Job/PodGroup 上的 `networkTopology` 和 `subGroupPolicy`/`partitionPolicy`。soft 模式会在 openSession 时被改写为 hard 模式。→ [03](./03-network-topology-hypernode-subjob.md)
7. **驱逐体系有三处变化。**
   - 新增 gangpreempt / gangreclaim，以 bundle 为驱逐粒度，并按 HyperNode 划定驱逐域。
   - 新增 `UnifiedEvictableFn`。
   - preempt 会跳过拓扑作业，并且不再调用 VictimTasks。
   - → [04](./04-preempt-reclaim-gang-eviction.md)
8. **队列体系有四处变化。**
   - capacity 插件支持层级队列、队列优先级和 DRA 配额。
   - 调度器启动时会自动创建 root 队列。
   - queue controller 不再把计数写进 status。
   - 新增 SchedulingGates 队列准入。
   - → [05](./05-queue-capacity-enqueue.md)
9. **新组件。**
   - agent-scheduler：面向 AI Agent 场景的快速调度器。
   - vc-agent：混部 / QoS。
   - 节点分片（sharding）、CronJob、HyperNode 自动发现。
   - CEL 准入策略（VAP/MAP）。
   - API 从 v1.14 起移入 staging。
   - → [06](./06-landscape-components-plugins.md)
10. **配置层面必须改的一项**：predicates 插件里的 `proportional` 比例过滤已经迁到 `resource-strategy-fit` 插件。→ [06](./06-landscape-components-plugins.md)

## 2. 文档列表与建议阅读顺序

| 顺序 | 文档 | 读完能回答的问题 | 建议用时 |
|---|---|---|---|
| 1 | [01 · allocate action 精读](./01-allocate-action.md) | allocate 为什么变得这么长？普通作业走哪条路？和 v1.9 差在哪？ | 1h |
| 2 | [02 · 框架：Session / 扩展点 / Statement / Bind](./02-framework-session-statement.md) | 新增了哪些扩展点？Statement 有哪些新用法？Commit 之后怎么一步步走到 Bind？ | 1h |
| 3 | [05 · 队列与配额](./05-queue-capacity-enqueue.md) | 社区 capacity 现在是什么语义？层级队列怎么用？和内部 capacity 有什么冲突？ | 45min |
| 4 | [04 · 驱逐类 action](./04-preempt-reclaim-gang-eviction.md) | preempt 和 reclaim 改了什么？gangpreempt 解决什么问题？ | 45min |
| 5 | [03 · 网络拓扑与 SubJob](./03-network-topology-hypernode-subjob.md) | HyperNode 怎么建？hard 和 soft 模式分别怎么调度？ | 1h（需要拓扑调度时再读） |
| 6 | [06 · 全景速查](./06-landscape-components-plugins.md) | 多了哪些二进制、控制器、插件、action、类型？ | 随时查阅 |
| 7 | [07 · allocate 路径 A 深入分析](./07-allocate-path-a-deep-dive.md) | 普通作业在 allocate 里每一步发生了什么？任务和作业在什么情况下会被"丢掉"？内部功能该挂在哪？ | 1h（读完 01 之后） |
| 8 | [08 · 拓扑调度、抢占、回收深入分析](./08-topology-preempt-reclaim-deep-dive.md) | 框架为拓扑和驱逐做了哪些准备？四个驱逐 action 怎么分工？gang 驱逐和 allocate 之间怎么交接？有哪些坑？（基于 v1.16.0-rc.0 行号） | 1.5h（读完 02、03、04 之后） |

## 3. 读源码的最短路径

1. `pkg/scheduler/scheduler.go:124-153`：`runOnce`，与 v1.9 几乎一样。
2. `pkg/scheduler/framework/session.go:166-283`：openSession。重点看 HyperNode 视图的构建，以及 soft 改 hard 的处理。
3. `pkg/scheduler/actions/allocate/allocate.go:283-368`：`allocateResources` 的分派逻辑，先看懂两条路径。
4. `allocate.go:719-867`：`allocateResourcesForTasks`，与 v1.9 对照着读。
5. `pkg/scheduler/framework/statement.go:423-472`：Merge、Save、Recover。
6. `allocate.go:370-539`：路径 B，按需阅读。
7. `pkg/scheduler/actions/preempt/preempt.go` 和 `reclaim/reclaim.go`：事务化之后的驱逐流程。
8. `pkg/scheduler/plugins/capacity/capacity.go:441-994`：所有钩子的注册入口。

## 4. 与本次迁移相关的发现

以下几点是读代码时发现、会直接影响内部迁移的。前五点各篇文档里有详细说明，最后一点在 [04](./04-preempt-reclaim-gang-eviction.md) 第 2 节：

- 社区 capacity **没有注册 OverusedFn**，allocate 只在队列级别调用 `Overused(queue)`。内部的 `Overused(queue, req)` 语义需要重新安排放在哪里。
- v1.15 的 preempt **跳过所有带 networkTopology 的作业**。如果启用 HyperNode，这类作业的抢占要改用 gangpreempt / gangreclaim。
- v1.15.3 里 gangpreempt 写入的 `NominatedHyperNode` 不会被持久化，上游 [#5476](https://github.com/volcano-sh/volcano/pull/5476) 已修复，建议 cherry-pick。
- gang 类 action 的名字**必须全小写**。设计文档中的 `gangPreempt` 写法会导致调度器启动失败。
- `dequeueStrategy` 只有 API 定义，**没有实现**。
- reclaim **不再调用 Allocatable**，配额判断完全交给 PreemptiveFn。

## 5. 相关文档

- [../v1.15-feature-inventory.md](../v1.15-feature-inventory.md)：内部功能清单与迁移决策
- [../v1.9-to-v1.15-upstream-changes.md](../v1.9-to-v1.15-upstream-changes.md)：社区版本变化与 k8s 1.19 兼容性
- [../scheduler-decoupling-proposal.md](../scheduler-decoupling-proposal.md)：哪些功能应从调度器剥离
