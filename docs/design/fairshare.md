# 公平共享调度

本文档涵盖两个相关的设计提案，用于在同一 `Queue` 内的多个租户之间实现公平共享调度：

1. [命名空间公平共享](#命名空间公平共享2019)（2019，[@lminzhw](http://github.com/lminzhw)）——
   记录在 `ResourceQuota` 上的静态每命名空间权重，通过新的
   `NamespaceOrderFn` 调度循环阶段进行比较。
2. [基于衰减使用量追踪的每命名空间公平共享](#基于衰减使用量追踪的每命名空间公平共享2026)
   （2026）——一个自包含的调度器插件，通过指数衰减追踪每个命名空间的历史资源秒数，
   在不引入新的 `Session`/`SchedulerCache` 字段或 `NamespaceOrderFn` 阶段的情况下，
   解决相同的底层问题。

有关使用说明（参数、配置示例、与其他插件的交互），请参阅
[fairshare 用户指南](../user-guide/how_to_use_fairshare_plugin.md)。

---

## 命名空间公平共享（2019）

[@lminzhw](http://github.com/lminzhw)；2019 年 5 月 8 日

### 动机

`Queue` 在 [kube-batch](http://github.com/kubernetes-sigs/kube-batch) 中被引入，用于在用户之间共享资源。

但是，同一个 `Queue` 中的用户在调度时是等价的。例如，我们有一个资源量很少的 `Queue`，其中 UserA 有 10 个 Pod，UserB 有 1000 个 Pod。在这种情况下，UserA 的 Pod 绑定到节点的概率会更低。

因此，我们需要一种更细粒度的策略来平衡同一 `Queue` 中用户的资源使用。

考虑到 Kubernetes 的多用户模型，我们使用命名空间来区分不同的用户。每个命名空间都有自己的权重来控制资源使用。

### 功能规格

权重具有以下特性：
> 1. `Queue` 级别
> 2. 一个 `整数`，默认值为 1
> 3. 记录在命名空间的 `quota` 中
> 4. 值越高，平衡后获得的资源越多

#### 权重的位置

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  namespace: default
spec:
  hard:
    limits.memory: 2Gi
    volcano.sh/namespace.weight: 1  <- 此字段代表该命名空间的权重
```

如果同一命名空间中的多个 `ResourceQuota` 都有权重，则该命名空间的权重取其中的最大值。

该权重必须为正数，任何无效值都将被视为默认值 1。

### 调度器框架

在 SchedulerCache 中引入两个新字段

```go
type NamespaceInfo struct {
    Weight int
}

type SchedulerCache struct {
    ...
    quotaInformer    infov1.ResourceQuotaInformer
    ...
    NamespaceInfo  map[string]*kbapi.NamespaceInfo
    ...
}
```

调度器将通过 `quotaInformer` 监听 `ResourceQuota` 的生命周期，并刷新 `NamespaceInfo` 中的信息。

在 `openSession` 函数中，我们应通过 `cache.Snapshot` 函数将 `NamespaceInfo` 传递到 `Session` 中，方法是在 `Session`/`ClusterInfo` 结构体中使用一个新字段。

```go
type Session struct {
    ...
    NamespaceInfo  map[string]*kbapi.NamespaceInfo
    ...
}
type ClusterInfo struct {
    ...
    NamespaceInfo  map[string]*kbapi.NamespaceInfo
    ...
}
```

### Allocate 动作

#### 调度循环

`allocate` 动作的行为是在 `Queue` 中逐个调度作业。

在调度循环开始时，它会从 `Queue` 中取出优先级最高的作业，并尝试调度属于该作业的任务，直到作业就绪（满足 minMember），然后进入下一轮。

上述作业的优先级由插件注册的 `JobOrder` 函数定义。例如 Gang 插件的作业就绪排序、Priority 插件的优先级排序，以及 DRF 插件的共享排序。

#### JobOrder

命名空间权重 `不应` 通过 JobOrder 函数实现。因为作业的调度会影响其他作业的优先级。

> 例如
>
> ns1 有 job1、job2，ns2 有 job3、job4。原始顺序是 job1-job2-job3-job4。
>
> 在 job1 被调度后，正确的顺序应该是 job3-job4-job2。但在优先级队列中，我们没有机会修正 job2 的优先级。

#### 命名空间排序

为了添加命名空间权重，我们在 `Session` 中引入一个新的排序函数，名为 `NamespaceOrder`。

```go
type Session struct {
    ...
    NamespaceOrderFns map[string]api.CompareFn
    ...
}
```

allocate 中的调度循环应作如下修改。

在调度循环中，首先通过调用 `NamespaceOrderFn` 选择优先级最高的命名空间，然后在该命名空间中使用 `JobOrderFn` 选择优先级最高的作业。

作业调度完成后，将命名空间和作业按顺序推回优先级队列以刷新其优先级。因为一旦作业被调度，已分配的资源可能会降低该命名空间的优先级，同一命名空间中的其他作业可能会被延后调度。

在每一轮中始终将资源分配给优先级最高（资源使用量较低）的命名空间，将使资源达到平衡。

### DRF 插件

DRF 插件使用抢占和作业排序来平衡作业间的资源。该插件中的 `share` 定义为资源使用量，`share` 越高表示该作业当前占用的资源越多。

#### 命名空间比较

为了将命名空间权重引入该插件，我们首先需要定义如何比较具有权重的命名空间。

对于权重为 w1 的命名空间 n1 和权重为 w2 的命名空间 n2，我们可以计算资源的 `share` 并记为 u1 和 u2。现在，n1 的资源使用量小于 n2 可以定义为 (u1 / w1 < u2 / w2)

`例如` 权重 w1=2 的 ns1 使用 6cpu，权重 w2=1 的 ns2 使用 2cpu。在 cpu 范围内，ns1 使用的资源比 ns2 少。(6 / 3 < 3 / 1)

#### 命名空间排序

使用上述策略注册 `NamespaceOrder` 函数。

#### 抢占

> `preempt` 动作目前已禁用。此项后续再做。

在当前的 `preemption` 函数中，策略只是简单地比较作业间的资源 share。

添加命名空间权重后，我们应首先检查抢占者和被抢占者的命名空间。资源较少的命名空间中的作业可以抢占其他作业，或者当命名空间资源使用量相同时，转而比较作业的 share。

### 特性交互

#### preempt 动作

Preempt 是一组用于选择受害者并最终驱逐它们的策略集合。

选择受害者的方式是一组由插件注册的名为 `Preemptable` 的函数集合。例如 Gang 插件的作业就绪保护、Conformance 插件的特殊 Pod 保护、DRF 插件的作业 share 平衡策略。

所有这些插件会各自选择一些受害者，它们的交集将成为最终的受害者集合。因此，DRF 插件做出的选择永远不会破坏其他插件的要求。

### 简述

1. Preempt 可能导致杀死某些正在运行的 Pod。

### 场景：

- 集群有 __16 cpu__，queue 和命名空间均为默认权重。

    | queue | namespace | 请求量 | queue 已分配 | namespace 已分配 |
    | ----- | --------- | --------- | -------------- | ------------------ |
    | q1    | ns1       | 5 cpu     | 8 cpu          | 4 cpu              |
    |       | ns2       | 10 cpu    |                | 4 cpu              |
    | q2    | ns3       | 10 cpu    | 8 cpu          | 6 cpu              |
    |       | ns4       | 2 cpu     |                | 2 cpu              |

- 集群有 __16 cpu__，q1 权重为 1，q2 权重为 3。ns1 权重为 3，ns2 权重为 1，ns3 权重为 2，ns4 权重为 6。

    | queue | namespace | 请求量 | queue 已分配 | namespace 已分配 |
    | ----- | --------- | --------- | -------------- | ------------------ |
    | q1 w1 | ns1 w3    | 5 cpu     | 4 cpu          | 3 cpu              |
    |       | ns2 w1    | 10 cpu    |                | 1 cpu              |
    | q2 w3 | ns3 w2    | 10 cpu    | 12 cpu         | 10 cpu             |
    |       | ns4 w6    | 2 cpu     |                | 2 cpu              |

- 集群有 __16 cpu__，q1 权重为 1，q2 权重为 3。ns1 权重为 2，ns2 权重为 6。

    | queue | namespace | 请求量 | queue 已分配 | namespace 已分配 |
    | ----- | --------- | --------- | -------------- | ------------------ |
    | q1 w1 | ns1 w2    |           | 4 cpu          |                    |
    | q2 w3 | ns1 w2    | 5 cpu     | 12 cpu         | 3 cpu              |
    |       | ns2 w6    | 20 cpu    |                | 9 cpu              |

---

## 基于衰减使用量追踪的每命名空间公平共享（2026）

### 背景

Volcano 现有的 DRF 插件在命名空间/队列层面提供了主导资源公平性，
但它只考虑**当前分配快照**——没有对历史使用量的记忆。
这在多租户 GPU 集群中导致了一个有据可查的问题（参见 [#4165](https://github.com/volcano-sh/volcano/issues/4165)）：

- 命名空间 A 提交了数百个 GPU 作业并占满了集群
- 命名空间 B 稍后带着少量作业到达
- 随着命名空间 A 的作业完成，其新的待处理作业立即重新获得优先级
  （它们当前的分配量相等或更低），实际上导致命名空间 B 饥饿

这种"提交顺序偏差"意味着第一个涌入队列的租户会无限期地垄断资源，
即使其他租户有正当的需求。SLURM 通过其追踪历史使用量并应用指数衰减的
公平共享算法解决了这个问题。

这与上述 2019 年 [命名空间公平共享](#命名空间公平共享2019)提案针对的是相同的底层缺陷，
但该设计需要新的 `NamespaceOrderFn` 调度循环阶段以及贯穿
`SchedulerCache`/`Session`/`ClusterInfo` 的 `NamespaceInfo` 管道改造，而这些从未落地。
下面的提案有意限定在单个自包含插件内——不需要框架变更——以自动适应租户消耗升降的
基于使用量的度量，取代静态的、管理员配置的 `ResourceQuota` 权重。

还有一个两个设计共同具备的假设值得明确指出：Volcano 已经支持跨队列的公平共享
（通过队列层面的 DRF/proportion）。2019 年提案和本提案都是关于**单个队列内**的
公平共享，适用于多个租户向同一队列提交作业并需要在那里相互平衡的场景——
当租户映射到命名空间而非专用队列时，这种情况很常见。

### 提案

新增一个 `fairshare` 调度器插件，追踪每个命名空间的累计资源秒数，
并应用指数半衰期衰减，使过去的消耗逐渐被宽恕。

### 命名空间身份

租户直接通过其作业的**命名空间**来标识——没有单独的"用户"概念，
因为并非每个集群都将单个用户一一映射到命名空间（一个命名空间本身可能代表
一个由多个用户共享的团队或项目）。不需要额外的标签。

### 算法

每个调度周期（约 1 秒）：

1. **衰减**所有历史使用量：`usage × 2^(-elapsed / halfLife)`
2. **累加**运行中的使用量：对于每个已分配的任务，将 `resource_count × elapsed_seconds`
   加到该命名空间的累计总量上
3. **排序**待处理作业，通过 `JobOrderFn`：
   - **主要**：累计使用量较低者胜出（浮点比较使用 1.0 资源秒的 epsilon）
   - **次要**：当前运行中资源较少者胜出（周期内平局决胜）

### 半衰期衰减

| 使用后经过的时间 | 剩余权重（4 小时半衰期） |
|-----------------|--------------------------------|
| 0 小时         | 100%                           |
| 4 小时         | 50%                            |
| 8 小时         | 25%                            |
| 12 小时        | 12.5%                          |
| 24 小时        | 1.6%                           |

一个消耗了 10 GPU 小时的命名空间，其使用惩罚在每 4 小时的非活动期后会减半。
24 小时后，惩罚实际上已被遗忘。

### 状态持久化

Volcano 每个调度周期都会通过 `New()` 重新创建插件实例，因此实例级状态会在
周期之间丢失。fairshare 插件将使用历史保存在一个跨周期存活的包级结构体中：

```go
var state = &fairShareProcessState{
    usage: make(map[string]map[string]float64), // [queue][namespace] → 资源秒数
}

type fairShareProcessState struct {
    usage     map[string]map[string]float64
    lastCycle time.Time

    persistenceInitialized bool
    lastFlushAt            time.Time
}
```

`state` 有意不加锁保护：`OnSessionOpen`/`OnSessionClose` 只会从
单一调度器 goroutine 顺序调用（Volcano 的 `wait.Until` 循环从不会重叠调用）。
如果 Volcano 将来并发运行会话，这需要真正的重新设计，而不是重新引入全局锁。

#### 持久化存储（ConfigMap）

为了在调度器重启后存活，插件可以选择将状态持久化到 ConfigMap。
当 `fairshare.persistState` 设置为 `"true"` 时：

1. 在**第一个调度周期**，`initPersistence` 将任何现有状态从
   ConfigMap 加载到 `state.usage` / `state.lastCycle` 中，由
   `state.persistenceInitialized` 保护，确保只运行一次。
2. **`OnSessionClose`**（每个调度周期调用一次，约 1 秒）在距上次刷新
   至少经过 `flushIntervalSeconds`（默认：30 秒）时，将当前状态
   刷新到 ConfigMap。写入使用 ConfigMap 的 `resourceVersion` 进行
   乐观并发控制。将刷新与会话生命周期绑定而不是使用独立的
   goroutine+ticker，意味着持久化在插件停止被调用的那一刻（例如通过
   配置热更新从层级列表中移除）就会停止——否则 ticker 会在没有
   关闭钩子的情况下永远写入过期状态。
3. 重启时，加载的 `state.lastCycle` 用于计算经过的时间并应用正确的衰减，
   因此命名空间不会因停机时间而受到不公平的惩罚或宽恕。

ConfigMap 存储在调度器的命名空间中（默认：`volcano-system`）：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fairshare-usage-state
  namespace: volcano-system
  labels:
    app: volcano-scheduler
    component: fairshare
data:
  state.json: |
    {
      "lastCycle": "2026-04-07T12:00:00Z",
      "queues": {
        "gpu-queue": {
          "team-a": 12345.67,
          "team-b": 8901.23
        }
      }
    }
```

**设计考量：**

- **领导者选举**：Volcano 已经选举出单一活跃调度器。只有领导者执行写入。
- **数据丢失窗口**：崩溃时最多丢失 `flushInterval` 秒的使用数据。
  使用默认的 30 秒间隔和 4 小时半衰期，这可以忽略不计。
- **大小**：即使有 50 个队列中的 1000 个命名空间，JSON 负载也约 50 KB——远在
  1 MB 的 ConfigMap 限制之内。
- **向后兼容性**：持久化默认禁用。现有部署不受影响。

### 调度器钩子

| 钩子 | 用途 |
|------|---------|
| `JobOrderFn` | 按累计使用量排序作业（低者胜出），以运行中资源作为平局决胜 |
| `JobEnqueueableFn` | （可选）阻止达到或超过其最大最小公平份额的命名空间进入调度流水线 |
| `EventHandler` | 在调度周期内实时追踪分配/释放 |

### 测试

#### 单元测试（53 个测试）

- 最大最小公平共享算法正确性（单命名空间、等量需求、非对称需求、渐进淘汰）
- 衰减因子数学（一个/两个半衰期、零经过时间、零半衰期、微小经过时间）
- `decayAllUsage`（一个半衰期后减半、清理可忽略条目、多队列）
- 使用量排序（低使用量胜出、等量使用量回退到运行中平局决胜、真实多命名空间场景）
- 衰减场景（10 小时作业在 4 小时和 24 小时的衰减）
- 辅助函数（命名空间提取、资源键默认值/覆盖）
- `targetQueues` 白名单行为（未设置时默认为所有队列，设置时限制为白名单）
- 通过真实 `enqueue` 动作的入队门控：达到或超过其份额的命名空间被阻止，
  无论其待处理作业是否已有 Pod（`jobTotalResource` 和待处理需求对尚未创建 Pod 的作业
  回退到 `PodGroup.Spec.MinResources`，例如 vcjob）
- `shouldAbstainOrdering`（除非两个作业都在同一目标队列中，否则弃权排序）
- 持久化刷新失败会在下一个周期重试，而不是等待完整的 `flushIntervalSeconds`
- 持久化：刷新创建 ConfigMap、刷新更新现有 ConfigMap、加载填充全局变量、
  加载处理缺失的 ConfigMap、加载处理空数据、刷新→加载往返、
  禁用持久化为空操作、损坏的 JSON 返回错误
- `maybeFlush` 速率限制（禁用时为空操作、首次调用立即刷新、
  间隔内的第二次调用被跳过、间隔过后再次刷新）

#### 集成测试

在具有 2 个 GPU 节点和 4 个租户命名空间的测试集群上验证：

1. **无衰减追踪**：FIFO 行为，最后一个租户等待了约 7 分钟
2. **有衰减追踪**：所有租户在约 2 分钟内获得 GPU，调度在租户间轮转
3. **突发不对称**（1 个租户 = 8 个作业，其他各 1 个）：少数租户的作业在约 2 分钟内完成
4. **DAG/工作流模拟**：GPU 步骤按累计使用量在租户间交错
5. **PriorityClass 交互**：高优先级按预期绕过公平共享
6. **调度器重启**：运行中的作业存活，重启后新作业公平调度

### 局限性

- 不启用 `persistState` 时，状态仅存在于内存中，调度器重启后会丢失
- 仅支持基于命名空间的身份标识；不支持任意自定义标签（可以扩展）
- ConfigMap 持久化在崩溃时有一个等于刷新间隔的小数据丢失窗口
- 公平性限定在队列内（与上述 2019 年提案相同）；此插件不会
  改变 DRF/proportion 跨队列平衡资源的方式
- `fairshare` 不注册 `PreemptableFn`/`ReclaimableFn`；它只对待处理队列重新排序，
  并（可选地）对入队进行门控。它无法抢占已在运行的作业，因此
  拥有长期运行作业的命名空间会保留其资源，无论其使用惩罚变得多么陈旧