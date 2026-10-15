# 03 · 网络拓扑感知调度（HyperNode）与 SubJob

> 行号对应 tag **v1.15.3**。为简洁起见，`apis/` 指 `staging/src/volcano.sh/apis/pkg/apis/`。
> v1.9 中完全没有这部分代码，是整块新增的能力。

## 0. 一句话概览

1. 用户用 **HyperNode CRD** 描述交换机层级：tier 越小，网络距离越近。
2. Job / PodGroup 用 **networkTopology**（hard/soft 模式 + highestTierAllowed）约束"整个作业最多能跨到第几层"。
3. PodGroup 还可以用 **subGroupPolicy** 把 Pod 再切成若干 **SubJob**，每个 SubJob 有自己的 gang（成组调度）约束和拓扑约束。
4. 调度器在 allocate 中分三层做试算（dry-run）："Job 级梯度 → 每个候选 HyperNode 内的 SubJob 级梯度 → 节点"，最后选出最优的 HyperNode（流程见 [01](./01-allocate-action.md) 第 4 节）。

---

## 1. API

### 1.1 HyperNode（`topology.volcano.sh/v1alpha1`，集群级资源，短名 `hn`）

```yaml
apiVersion: topology.volcano.sh/v1alpha1
kind: HyperNode
metadata: {name: leaf-0}
spec:
  tier: 1                 # 层级，越小越近
  tierName: leaf          # 可选，可以被 highestTierName 引用
  members:
  - type: Node            # Node | HyperNode
    selector:             # 三选一：exactMatch / regexMatch / labelMatch（labelMatch 只能用于 Node 类型）
      regexMatch: {pattern: "gpu-node-[0-3]"}
```

- 类型定义：`apis/topology/v1alpha1/hypernode_types.go:23-157`。
- 互斥规则由 CRD 中的 CEL 表达式校验（`:115-129`）。**k8s 1.19 不支持 CRD CEL 校验，安装前需要删掉这部分**，详见上一级目录的 upstream-changes 文档。
- `status.nodeCount` 由 hypernode controller 维护。

### 1.2 networkTopology（Job 和 PodGroup 各有一份，结构相同）

| 字段 | 含义 |
|---|---|
| `mode` | `hard`（默认）或 `soft` |
| `highestTierAllowed` | 作业能跨越的最高 tier |
| `highestTierName` | 按名字指定 tier，与 `highestTierAllowed` 二选一 |

- 定义位置：`apis/scheduling/v1beta1/types.go:268-296`、`apis/batch/v1alpha1/job.go:123-157`。
- 普通 Pod（不是 vcjob）可以通过注解 `volcano.sh/network-topology-mode` 和 `volcano.sh/network-topology-highest-tier` 指定。PodGroup controller 会把它们写到自动创建的 PG 上（`pkg/controllers/podgroup/pg_controller_handler.go:418-472`）。

### 1.3 PodGroup.subGroupPolicy（SubJob 的来源）

定义在 `apis/scheduling/v1beta1/types.go:217-266`。注释说明它是 `minTaskMember` 的长期替代方案。

| 字段 | 含义 |
|---|---|
| `name` | 策略名 |
| `labelSelector` | 哪些 Pod 属于这个策略 |
| `matchLabelKeys` | 按这些 label 的取值，把 Pod 再分成若干子组 |
| `subGroupSize` | 每个子组的 Pod 数，默认 1 |
| `minSubGroups` | 子组级 gang：至少要有 N 个子组满足 |
| `networkTopology` | 子组自己的拓扑约束 |

### 1.4 vcjob 的写法：`TaskSpec.PartitionPolicy`

- 字段：`totalPartitions`、`partitionSize`、`minPartitions`、`networkTopology`（`apis/batch/v1alpha1/job.go:279-303`）。webhook 会校验 `replicas == totalPartitions × partitionSize`。
- job controller 给每个 Pod 打上 `volcano.sh/partition-id = index / partitionSize`，并把每个带 PartitionPolicy 的 task 转换成一条 SubGroupPolicy（`pkg/controllers/job/job_controller_actions.go:1082-1153`）：

  | SubGroupPolicy 字段 | 取值 |
  |---|---|
  | `name` | task 名 |
  | `subGroupSize` | partitionSize |
  | `minSubGroups` | minPartitions |
  | `labelSelector` | task-spec |
  | `matchLabelKeys` | `[partition-id]` |

---

## 2. HyperNode 从哪里来：自动发现

- 由 `hyperNode-controller` 负责（`pkg/controllers/hypernode/`），它读取 ConfigMap 中的 `networkTopologyDiscovery[]` 配置。每个 source 对应一个 discoverer，controller 按标签 `volcano.sh/network-topology-source` 对 HyperNode 做增、改、删的对账。
- 已经实现的发现源：
  - **ufm**（InfiniBand UFM）：按 leaf 交换机分组，生成 tier1 的 HyperNode；再生成一个 tier2 的 spine，包含所有 leaf。
  - **label**：按配置的 label 键顺序，第 i 个键对应 tier=i+1。
- `roce` 只出现在示例配置里，**没有对应实现**。
- 内部如果已经有节点的网络拓扑数据（交换机或 SuperPod 信息），推荐用 **label 发现**，或者由上层组件直接生成 HyperNode。

---

## 3. 调度器中的拓扑视图

### 3.1 Cache：`HyperNodesInfo`（`pkg/scheduler/api/hyper_node_info.go`）

- 为每个 HyperNode 维护 Parent、Children、tier，以及展开后的真实节点集合（`realNodesSet`）。展开方式：
  - Node 类型成员：按 exact / regex / label 规则展开；
  - HyperNode 类型成员：只支持 exactMatch，递归展开。
- 两条约束：不允许出现环；一个子节点只能有一个 parent。构建出错时 `ready=false`，带拓扑约束的作业会被**整体跳过**。
- 增量更新：
  - HyperNode 事件：进入队列后重建；
  - Node 增删：只重建使用 regex / label 匹配的叶子 HyperNode。

### 3.2 Session 中的字段与 openSession 时的处理

| 字段 | 含义 |
|---|---|
| `HyperNodes` | HyperNode 映射（包含虚拟根） |
| `HyperNodesTiers` | 升序排列的 tier 列表（**包含虚拟根的 tier**） |
| `HyperNodesSetByTier` | tier → HyperNode 名集合 |
| `RealNodesList` / `RealNodesSet` | HyperNode → 其下的真实节点 |
| `HyperNodesReadyToSchedule` | cache 是否构建成功 |

openSession 时依次做三件事（`session.go:240-259`）：

1. **添加虚拟根 `<cluster-top-hypernode>`**：它的 tier 为最大 tier + 1，是所有没有 parent 的 HyperNode 的父节点，并且**包含集群全部节点**（包括不属于任何 HyperNode 的节点）。
2. **调用 `adjustNetworkTopologySpec`**：
   - 把 tierName 翻译成 tier；
   - 把 soft 模式改写为 hard 模式。Job 级的上限设为虚拟根的 tier；SubJob 级的上限设为 Job 的 tier。
3. **恢复或清理 `AllocatedHyperNode`**：调度器重启后，根据已运行 Pod 所在节点，反推出 tier 最低的 HyperNode。

---

## 4. SubJob 数据结构

### 4.1 `SubJobInfo`（`pkg/scheduler/api/sub_job_info.go:40-60`）

- 主要字段：`MinAvailable`、`Priority`、`MatchIndex`、`Tasks`、`TaskStatusIndex`、`AllocatedHyperNode`、`NominatedHyperNode`（只保存在内存中）、`NetworkTopology`。
- ID 规则：
  - `GID` = `<jobUID>/<policyName>`，同一个策略下的子组共享同一个 GID；
  - `ID` = `<GID>-<matchValues>`，超过 128 个字符时取 hash。
- `IsReady`：ready 数 + bestEffort 数 ≥ MinAvailable。

### 4.2 `JobInfo` 新增字段（`job_info.go:381-385`）

- 新增字段：`AllocatedHyperNode`、`NetworkTopology`、`SubJobs`、`TaskToSubJob`、`MinSubJobs`。
- **默认 SubJob**：task 匹配不上任何策略时，归入 ID 为 job UID 的默认 SubJob。没有 subGroupPolicy 时，默认 SubJob 的 MinAvailable 等于 job.MinAvailable，并继承 Job 级的拓扑约束。
- 判定函数：
  - `ContainsSubJobPolicy()`
  - `ContainsHardTopology()`：Job 或任一 SubJob 为 hard 模式
  - `ContainsNetworkTopology()`
- gang 插件的 `JobValid`、`JobReady`、`JobPipelined` 都会额外检查"满足条件的子组数 ≥ minSubGroups"（`plugins/gang/gang.go:75-81, 191-219`）。

### 4.3 `TaskInfo.JobAllocatedHyperNode`

allocate 在给节点打分之前，会把当前已累积的 HyperNode 写到 task 上，供 network-topology-aware 插件在节点打分时计算 LCA 距离。unallocate 或 unpipeline 时清空。

---

## 5. 扩展点与 network-topology-aware 插件

### 5.1 扩展点的聚合方式

| Session 函数 | 聚合方式 | 实现插件 |
|---|---|---|
| `HyperNodeGradientForJobFn` / `ForSubJobFn` | **只用第一个注册插件的结果**；没有插件时返回 `[[输入节点]]`，即不做任何过滤 | network-topology-aware |
| `HyperNodeOrderMapFn` | 各插件打分后求和 | network-topology-aware |
| `SubJobReady` / `SubJobPipelined` | 没有 subGroupPolicy 时退化为 `JobReady` / `JobPipelined` | gang |
| `SubJobOrderFn` | 按插件逐个比较 | gang（未 ready 的优先）、priority（优先级高的优先） |

**注意：没有启用 network-topology-aware 插件时，hard 约束实际上不生效。** 原因是梯度函数不做任何过滤（`session_plugins.go:1130-1131`）。

### 5.2 Gradient 是怎么算的（`network_topology_aware.go:591-724`）

1. **确定搜索根**：没分配过时，用传入的 HyperNode；已经有 `AllocatedHyperNode` 时，先找它在 `tier ≤ highestTier` 范围内最高的祖先，再与传入节点取交集。这样保证同一个作业后续扩容的 Pod 不会跑出原来的网络域。
2. **BFS 遍历子树**，逐个检查以下条件，不满足的剔除：
   - `tier ≤ highestTierAllowed`；
   - 作业还没有分配 HyperNode 时，`minResource` ≤ 该 HyperNode 的 idle 或 futureIdle。驱逐场景下比较的是 allocatable。
3. **按 tier 升序分组**，得到 `[][]*HyperNodeInfo`。

驱逐场景（PurposeEvict）会把顺序**反过来**：先尝试更大的域，并截断到 `hypernode.gradient.evict.max-hypernodes`（默认 8）。

### 5.3 打分

插件参数：`weight`、`hypernode.binpack.{cpu,memory,resources}`、`hypernode.binpack.normal-pod.{enable,fading}`（默认 true / 0.8）、`hypernode.gradient.evict.max-hypernodes`。

- **HyperNode 级打分**（`HyperNodeOrderFn`）：用 SubJob 的全部请求对这个 HyperNode 做 binpack，计算 `Σ w·(used+req)/allocatable`。分数并列时，"已有更多 task 在其中"的 HyperNode 优先。
- **节点级打分（带拓扑约束的 Pod）**：计算节点所在的最低层 HyperNode 与 `task.JobAllocatedHyperNode` 的 LCA。与已分配的 HyperNode 相同得 1 分，否则按 LCA 的 tier 递减。效果是**先填满同一个 leaf**。
- **节点级打分（普通 Pod）**：对节点所在各层 HyperNode 的 binpack 分做加权，权重为 `fading^(tier-1)`，目的是减少拓扑碎片。

### 5.4 hard 和 soft 的精确行为

- **hard + highestTierAllowed=N**：只在 tier ≤ N 的子树内找"能放下整个 Job（或 SubJob）"的解，从最低层开始逐层往上。找不到解，作业就保持 Pending。
- **soft**：被改写为 hard，上限为虚拟根的 tier。效果是"尽量放在同一个网络域，实在不行就跨整个集群"。
- **Job 为 hard、SubJob 为 soft**：SubJob 的上限会被限制在 Job 的 tier 以内。
- **以下情况会退化为"没有域约束"**（推断）：`mode=hard` 但没有设置任何 tier，或者 tierName 找不到对应的 tier。

---

## 6. 跨 session 的持久化

- **`AllocatedHyperNode` 只保存在内存中。** 设计文档说明：原先存在 PG 注解 `volcano.sh/job-allocated-hypernode` 里，因为 SubJob 太多导致对象膨胀，改成了只放内存（`docs/design/Network Topology Aware Scheduling.md:742-748`）。注解常量还在，但 v1.15.3 中已经没有生产代码写入它。
- **回写链路**：`MarkJobDirty` → closeSession 时 JobUpdater 处理 → `cache.updateJobInfo` 写回 cache 中的 JobInfo → 下一轮 Snapshot 时 Clone 出来。
- **调度器重启后**：在 openSession 中，根据已分配 task 所在的节点重新推导。代码选的是 **tier 最低**的 HyperNode；设计文档写的是"节点数最少"的，**以代码为准**。
- **`NominatedHyperNode` 在 v1.15.3 中不会回写 cache**，跨 session 会丢失。上游 [#5476](https://github.com/volcano-sh/volcano/pull/5476) 已修复。

---

## 7. 示例：hard 模式，highestTierAllowed=2

假设每个节点有 8 张 GPU：

```text
                     <cluster-top-hypernode>   (虚拟根, tier=3, 包含全部节点)
                      /                      \
             spine-0 (tier2)               spine-1 (tier2)
             /           \                       |
      leaf-0 (tier1)   leaf-1 (tier1)      leaf-2 (tier1)
      n0 n1 n2 n3      n4 n5 n6 n7         n8 n9 n10 n11
空闲:  4 个节点(32卡)    2 个节点(16卡)        4 个节点(32卡)
```

作业：6 个副本，每个副本 8 卡，`networkTopology: {mode: hard, highestTierAllowed: 2}`，没有 partitionPolicy。

1. **controller**：创建 PG，设置 `networkTopology{hard,2}`、`minMember=6`、`MinResources≈48卡`。
2. **openSession**：添加虚拟根（tier=3），`HyperNodesTiers=[1,2,3]`。作业只有一个默认 SubJob，MinAvailable=6，继承 `{hard,2}`。
3. **allocate**：`ContainsHardTopology()` 为 true，进入路径 B，调用 `allocateForJob(job, ws, ClusterTop)`。
4. **计算 Job 级梯度**：
   - 虚拟根 tier3 > 2，剔除；
   - spine-0 有 48 卡空闲，满足 ≥ 48，保留；
   - spine-1 只有 32 卡，剔除；
   - 各个 leaf 都不够 48 卡，全部剔除。
   - 结果为 `[[spine-0]]`。如果某个 leaf 有 ≥6 个空闲节点，结果会是 `[[leaf-x],[spine-0,…]]`，并且会优先在那个 leaf 内找到解。
5. **在 spine-0 内试算**：SubJob 级梯度同样是 `[[spine-0]]`。随后在 n0–n7 上逐个分配 Pod：
   - 第 1 个 Pod 没有拓扑分，由其他插件决定，假设落在 n0。此时累计的 HyperNode 变为 leaf-0。
   - 之后的 Pod：leaf-0 内的节点得 1 分；leaf-1 内节点的 LCA 是 spine-0，得 0.5 分。所以会先填满 leaf-0，再用 leaf-1。
   - 6 个 Pod 都分配完后 `SubJobReady`，得到方案。
6. **选出 spine-0**：`RecoverOperations` 重放方案，然后 Commit。`job.AllocatedHyperNode` 和 SubJob 的 AllocatedHyperNode 都设为 spine-0，closeSession 时写回 cache。
7. **后续轮次**（例如某个 Pod 失败后重建）：搜索根被锁定在 spine-0 的子树内，新 Pod 不会被调度到 spine-1。

两种变体：

- **soft 模式**：tier 上限被改写为 3。spine-0 放不下时，会回落到全集群。
- **再加 `partitionPolicy{partitionSize:4}` 且分区级为 `{hard,1}`**：Job 仍被限制在一个 spine 内，并且每 4 个 Pod 组成的子组必须落在同一个 leaf 内。

---

## 8. 对内部的意义

- 内部的 SuperPod 拓扑路径、net-topology 插件，与上游 HyperNode 解决的是**同一类问题**。如果迁移到上游 HyperNode，需要做三件事：
  - 由上层组件或 label 发现机制生成 HyperNode；
  - 把作业约束改写成 `networkTopology` 或 `partitionPolicy`；
  - 评估内部特有的打分逻辑能否以插件形式挂到 `HyperNodeOrderFn` 或 `BatchNodeOrderFn` 上。
- 迁移前需要注意的限制：
  - **v1.15 的 preempt action 会直接跳过带 networkTopology 的作业**，这类作业的抢占只能依赖 gangpreempt / gangreclaim。
  - HyperNode CRD 的 CEL 校验在 k8s 1.19 上不能用。

## 9. 相关文档（源码 `docs/` 目录下）

| 文件 | 内容 |
|---|---|
| `design/Network Topology Aware Scheduling.md` | 主设计文档，涵盖 API、梯度、allocate 流程，以及 AllocatedHyperNode 只放内存的决策 |
| `design/hyperNode-auto-discovery.md` | 自动发现的架构设计 |
| `design/gang-aware-eviction-design.md` | gangpreempt / gangreclaim 与 NominatedHyperNode |
| `user-guide/how_to_use_network_topology_aware_scheduling.md` | 用户指南 |
| `user-guide/how_to_use_hypernode_auto_discovery.md` | 自动发现的配置方法 |
