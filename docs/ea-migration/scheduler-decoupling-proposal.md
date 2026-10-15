# 内部 Volcano 功能剥离方案：哪些应移出调度器，交给上层管理组件

> 背景：内部 fork（`EasyAlgorithm/volcano`，主干 `origin/gpu-numa`）计划升级到社区 v1.15。
> 功能清单见 [v1.15-feature-inventory.md](./v1.15-feature-inventory.md)，社区变化见 [v1.9-to-v1.15-upstream-changes.md](./v1.9-to-v1.15-upstream-changes.md)。
> 本文回答一个问题：**内部新增的功能里，哪些不属于"调度决策"，应在这次升级时剥离到自研上层控制组件（controller / webhook / exporter），以及为什么。**

---

## 1. 结论速览

| # | 功能 | 建议 | 外部组件形态 | 优先级 |
|---|---|---|---|---|
| 1 | 业务属性 → 抢占/回收语义翻译（JobProperty / JobType → Preemptable / ReclaimAble / Elasticable） | **剥离** | Mutating Webhook 或上层作业控制器 | P0 |
| 2 | GPU 型号 → 资源名映射（`jdos.jd.com/gpu.model` + eaframework） | **剥离** | Mutating Webhook 和/或 Device Plugin 按型号上报 | P0 |
| 3 | online-queue Pod 调度超时（`--online-pod-scheduling-timeout`） | **剥离** | Pod 超时控制器 | P0 |
| 4 | Redis 排队序号上报（redis 插件） | **剥离** | 排队信息同步服务 | P0 |
| 5 | PodGroup 生命周期时间戳、排队/运行时长统计 | **剥离** | 作业生命周期控制器 / exporter | P1 |
| 6 | Reservation 生命周期（TTL、GC、删除 CR、owner 清理） | **部分剥离**：生命周期出去，放置/消费留下 | reservation-controller + webhook | P1 |
| 7 | delay-schedule 延迟调度 | **剥离** | 上层作业控制器（到点再提交 / 放行） | P1 |
| 8 | 节点资源指标（45s 全量快照、5min 空闲直方图） | **剥离** | 独立 exporter | P1 |
| 9 | archimedes HTTP 指标源 | **删除或剥离**（`usage` 插件线上未启用） | 节点利用率 exporter → Prometheus | P2 |
| 10 | jobbackoff 退避策略 | **部分剥离（可选）**：策略/状态出去，"未到时间不调度"门控留下 | 退避控制器 | P2 |
| 11 | 节点 GPU 配比（`SetReserveRatio`）、zone 配比配置 | **可选剥离** | 节点打标控制器 | P2 |
| 12 | 节点池 / 队列专属节点（`scheduling-type`、`slaCards`）、队列利用率注解 | **保持外置**（数据源已在外部） | 已有的上层管理组件 | — |

**保留在调度器里的**：capacity 配额语义，preempt / reclaim / relaxEvict，replica-gang，SuperPod 拓扑，cooldown，slanode / reserve / coordinator / net-topology 的过滤和打分，archimedes NUMA 过滤和打分，ActionExcluded 门控框架，PendingReason / NotEnoughQuota 等调度原因 condition，调度日志、skip-if-unchanged、`/api/archimedes/dump`。理由见第 5 节。

---

## 2. 划分原则：什么功能适合在 Volcano 里实现，什么应该放到外部

### 2.1 一句话原则

> **Volcano 负责"通用的批调度机制"，上层组件负责"公司的业务策略"。**
> 在 Volcano 内部，调度器只做"在当前集群快照下，这个 Pod / Job 放哪、能不能放、为了放它该驱逐谁"的决策；生命周期交给 controller，校验和默认值交给 webhook。

判断一个功能放哪里，分两步：

1. **它属于 Volcano，还是属于公司业务？** 如果把它贡献给社区，社区会不会接受？会接受的是通用机制，放在 Volcano 里（最好推回上游）；不会接受、只对公司有意义的是业务策略，放到上层组件。
2. **如果属于 Volcano，放在哪个组件？** 依赖调度快照做决策的，放 scheduler；管理对象状态和生命周期的，放 controller-manager；做对象校验和默认值的，放 admission webhook。

### 2.2 适合在 Volcano（调度器）内实现的功能

满足下面任一条件，就应该在调度器内实现：

1. **必须基于调度快照、和其它决策一起原子计算。** 典型例子是配额是否超限、gang 是否满足、抢占/回收选哪些受害者。这些结果依赖本轮会话里已经分配和驱逐的中间状态，外部组件拿不到一致视图，算出来必然有竞态。
2. **结果就是放置或驱逐本身。** 节点过滤、打分、作业和任务排序、拓扑约束（NUMA、SuperPod、网络拓扑）、replica-gang 这类，最终产出的就是 bind 或 evict。
3. **依赖只有调度过程中才产生的信息。** 例如不可调度原因、每个节点的过滤失败原因、Statement 中的待提交操作。PendingReason、NotEnoughQuota 等 condition 由调度器写，就是因为原因只有它知道。
4. **调度器自身的可观测性。** 例如调度延迟、action / plugin 耗时、会话日志、debug dump。这些用于排查调度器本身，没有对外副作用。
5. **能抽象成通用、可配置的机制。** 例如"带某注解的作业暂不参与调度"的门控、按资源名做配额、按标签做节点池约束。这类机制和具体业务无关，参数由外部提供，有机会推回社区。

### 2.3 应该在外部（上层管理组件）实现的功能

满足下面任一条件，就应该放到调度器外：

1. **时间驱动，而不是快照驱动。** 超时、TTL、延迟到点、退避到期这类逻辑，结果只取决于"现在几点"，跟资源分配无关。放在调度器里，它会跟着调度周期轮询，还要把状态写回 API Server 才能持久化。
   - 例子：online 队列 Pod 超时、Reservation 过期、delay-schedule。
2. **需要调度器写 API，而写的不是调度结果。** 调度器对 API Server 的写操作应只限于 bind、evict、Pod / PodGroup 的调度状态（condition / phase）。往对象上写业务注解、删除 CR，都会挤占调度主路径的 QPS 配额，拖慢调度延迟。
   - 例子：PodGroup 生命周期时间戳、Reservation 删除、超时注解。
3. **公司特有的业务语义翻译。** 把业务概念（作业类型、GPU 型号、机型配比）翻译成调度概念（可抢占、资源名、比例），这类规则跟着业务频繁变化。放在调度器里，每次改规则都要发版调度器；翻译结果只在调度器内存里，其它组件看不到，理解也会不一致。
   - 正确做法：由 webhook 或控制器把翻译结果写成**显式的通用字段**（`volcano.sh/preemptable`、PriorityClass、型号资源名、节点标签），调度器只读这些通用字段。
4. **依赖外部系统或凭据。** Redis、外部 HTTP 服务、私有 Go 模块这类依赖，会把凭据、网络抖动和构建依赖带进调度器，让外部故障变成调度故障。
5. **纯观测或展示。** 结果只给看板、UI、报表用，不影响调度决策，例如节点资源统计、排队序号、作业时长。这类计算却要拿调度器的 cache 锁、做全量快照。
6. **资源对象的生命周期管理。** 创建、过期、回收、清理、状态维护，是 Kubernetes 惯用的 controller 职责。即使对象属于 Volcano（如 Reservation），也应放在 controller，而不是 scheduler。
7. **外部也能用同等精度算出来。** 如果只靠 watch 对象（phase、condition、`lastTransitionTime`、标签）就能得到同样的结果，就不应该占用调度器。

### 2.4 灰色地带：拆成"外部策略 + 内部门控"

很多功能同时具备两类特征，例如 jobbackoff 既有"退避策略"（业务，时间驱动），又有"未到时间不调度"（调度行为）。这类功能不必一刀切，按下面的方式拆：

- **策略、状态机、持久化放到外部**：外部组件计算出结果，写到对象的注解或标签上。
- **调度器只保留一个通用门控**：读这个注解或标签，决定作业是否参与本轮调度，或者某个节点是否可用。门控本身不含业务规则，也不写 API。
- **多个功能尽量复用同一个门控**：例如 delay-schedule 和 jobbackoff 共用一个 `not-before` 注解，online 超时复用一个"停止调度"注解。k8s ≥ 1.26 时，优先用社区的 `schedulingGates`。

如果外部计算会明显损失精度（例如只有调度器知道"本轮为什么失败"），可以暂时留在调度器，但要在代码上把门控和策略分开，为以后外移做准备。

### 2.5 判定清单

新增功能或评估存量功能时，按顺序回答下面的问题，第一个"是"决定归属：

| # | 问题 | 回答"是"时的归属 |
|---|---|---|
| 1 | 是否必须在调度会话内，和配额、gang、抢占等决策原子计算？ | 调度器 |
| 2 | 它的输出是否就是放置、驱逐、排序或调度失败原因？ | 调度器 |
| 3 | 不看调度快照，只看对象和当前时间，能否同样精确地算出来？ | 外部 |
| 4 | 它是否需要调度器写 bind、evict、调度状态以外的 API？ | 外部 |
| 5 | 它是否包含公司特有的业务概念或规则（等级、型号、平台、HDK 版本等）？ | 外部做翻译，调度器只读通用字段 |
| 6 | 它是否依赖外部服务、凭据或私有模块？ | 外部 |
| 7 | 它的结果是否只用于展示或统计？ | 外部（exporter） |
| 8 | 它是否在管理某个对象的生命周期？ | controller（Volcano 自带的或自研的） |
| 9 | 以上都不是，而且能做成通用、可配置的机制？ | 调度器，并考虑推回社区 |

### 2.6 实现约束

**放在 Volcano 内的功能**，应遵守：
- 以插件形式实现，尽量不改 `framework`、`api`、`cache` 这些社区高频改动的核心包；确实需要新扩展点时，做成通用钩子，并争取上游化。
- 不在 `api` 包里引入业务字段或私有模块依赖。
- 调度会话内不做外部 I/O，也不同步写业务 API。
- 业务相关的键统一使用内部前缀，并登记到第 6 节的契约表里；规则参数走配置，不要硬编码。
- 对缺失的输入字段，使用社区的缺省语义。

**放在外部的功能**，应遵守：
- 只通过注解、标签、CRD 字段或只读接口和调度器交互，不依赖调度器的内部状态或内存结构。
- 写入操作幂等，调度器或控制器重启后结果不变；优先基于对象上的 `lastTransitionTime` 等持久化事实计算。
- 每个字段只有一个写入方。和调度器共用的对象（如 Reservation status），用 patch 并划清字段归属。
- 考虑集群 k8s 版本（例如 `schedulingGates` 需要 1.26+），低版本统一使用注解门控。
- 建议多个控制器合并为一个"调度管理组件"二进制部署，降低运维成本。

按以上原则逐项判断内部功能的结果，见第 3 至第 5 节。

---

## 3. 建议剥离的功能（逐项）

### 3.1 业务属性 → 抢占/回收语义翻译（P0）

**现状**
- `pkg/scheduler/api/job_info.go` 的 `setDefaultPreemptProperty`（约 631 行）按 PodGroup / Pod 上的 JobProperty、JobType 推导：
  - ProdHigh → 不可抢占、不可回收；
  - ProdMedium / ExpHigh → 可抢占、不可回收；
  - ExpMedium → 可抢占、可回收；
  - ElasticSLA → 可弹性；
  - 弹性作业强制不可抢占、不可回收。
- 内部还改了 `GetPodPreemptable` 的默认值（缺省 false，社区缺省 true），并引入了 Job 级的 Preemptable / ReclaimAble。

**为什么移出去**
- **业务规则变化频繁。** 新增一个作业类型或调整等级，都要改调度器代码并发版；放到 webhook 里只改翻译表即可。
- **跟社区语义冲突，是升级的主要冲突点。** v1.15 的 `Preemptive`、`UnifiedEvictableFn`、gangpreempt 都基于 `volcano.sh/preemptable` 和 PriorityClass。内部把业务属性解释嵌在 `api` 包里，几乎每个社区抢占相关提交都会和它冲突。
- **结果不可见。** 翻译只发生在调度器内存里，运维、UI、其它控制器从对象上看不到"这个作业到底可不可抢占"。写成显式注解后，所有组件看到的是同一份事实。
- **复用。** 如果以后再引入 agent-scheduler 或其它调度器，翻译逻辑不必在每个调度器里各写一份。

**剥离方案**
- 在 Mutating Webhook（或创建作业的上层控制器）里，按 JobProperty / JobType 写入**显式的标准字段**：
  - `volcano.sh/preemptable`（Pod / PodGroup）；
  - `priorityClassName`（如果要用优先级表达等级）；
  - 社区没有的语义，用内部前缀的显式注解，例如 `scheduling.easyalgo/reclaimable`、`scheduling.easyalgo/elastic`。
- 存量作业：上线前先跑一次补注解的 Job；或者在一个过渡版本里保留"注解缺省时按业务属性推导"的兜底，下一个版本再删掉。

**调度器保留**：读 `preemptable` / `reclaimable` / `elastic` 这些显式注解，以及 Job 级抢占/回收的判定逻辑本身（这属于调度语义）。
**注意**：`GetPodPreemptable` 的缺省值必须明确统一。建议改由 webhook 给所有 Pod 显式打上值，调度器保持社区缺省，这样就不必再改社区函数。

### 3.2 GPU 型号 → 资源名映射（P0）

**现状**
- `job_info.go` 的 `ResreqReplaceScalar`（约 349 行）读取 Pod label、annotation 或 nodeSelector 上的 `jdos.jd.com/gpu.model`，借助私有模块 eaframework（`GPUModuleToResNum` / `GPUModuleToDevice` / `GPUIDToResName`），把 `nvidia.com/gpu` 请求在调度器内存里改写为型号专属资源名。
- `api/node_info.go`、`api/resource_info.go`、`api/job_info.go`、`plugins/reserve`、`plugins/queue-reclaim`、`metrics` 中共有约 43 处 `earesource.` 引用。

**为什么移出去**
- **调度器看到的请求和 kubelet 看到的不一致。** 调度器按型号资源名记账，Pod spec 里却还是 `nvidia.com/gpu`。排查问题时，"调度器认为的资源"无法从对象上直接看出来，quota 统计、指标、外部看板也各算一套。
- **私有模块进入调度器核心包。** `api` 包是社区改动最频繁的包，eaframework 依赖会让每次 rebase 都要处理它。私有模块拉取还要求构建环境具备凭据（这正是仓库里出现 `id_rsa` / `PRIVATE-TOKEN` 的根源之一）。
- **社区已有标准做法。** v1.10 以后社区推荐让 device plugin 按型号上报不同的资源名，capacity 插件原生按资源名做配额，不需要调度器做映射。

**剥离方案（二选一，或组合使用）**
- **方案 A（推荐，彻底）**：device plugin 按型号上报资源名（例如 `nvidia.com/A100`）；webhook 把用户的 `nvidia.com/gpu` + `gpu.model` 改写成对应资源名。改写后，调度器、kubelet、配额看到的是同一份请求。
- **方案 B（过渡）**：device plugin 暂不改，webhook 只做"型号 → 节点亲和"的改写（nodeSelector / affinity），配额按型号拆分则通过 queue 的 `capability` 加上型号标签维度来实现。这种方式改动小，但按型号的配额精度不如方案 A。

**调度器保留**：不再需要 eaframework。reclaim 里的"同型号才能回收"直接比较资源名；`GPUResID` 一类的判定改为从资源名解析。
**风险**：方案 A 需要先改 device plugin 并滚动节点，否则 kubelet 会拒绝改写后的请求。必须先完成节点侧改造，再上 webhook。

### 3.3 online-queue Pod 调度超时（P0）

**现状**
- `pkg/scheduler/cache/unschedule_task.go` 新增全局 flag `--online-pod-scheduling-timeout`（默认 10 分钟）。
- `cache.go` 在记录作业不可调度状态的循环里（约 1886 行），对每个不可调度 task 调用 `handleUnschedulableOnlineTask`：
  - 判断是否为 online 队列（`SkipQuotaPreempt` 队列，需要拿 `sc.Mutex`）；
  - 首次失败时 patch Pod 注解 `jdos.jd.com/OnlineFailedSchedulingPod`、`SchedulingEndTime`、`SchedulingStatus=True`；
  - 超时后 patch 为 `SchedulingStatus=False`，并发出事件；
  - 调度器之后跳过 `SchedulingStatus=False` 的 Pod。

**为什么移出去**
- **纯时间驱动。** 超时判断只用到创建时间、注解和当前时间，与集群快照无关。
- **调度结果回写路径里的同步 API 调用。** 这里在逐个 task 的循环中同步 patch Pod，大批 online Pod 同时 pending 时，会拉长 session 收尾时间，并占用调度器的客户端 QPS。
- **业务专属概念进入通用 cache 层。** "online-queue"、`jdos.jd.com/*` 注解、全局 flag 都放在 `cache` 包里，而 `cache` 包是社区重构最多的地方（v1.15 引入了 fwk `NodeInfo`、BindContext 等）。
- **策略不够灵活。** 超时时长目前只能用全局 flag 或 Pod 注解 `SchedulingMinutes`；放到控制器后，可以按队列、按租户配置，还能做超时后的处置（标记失败、删除、通知上层）。

**剥离方案**
- 新建 Pod 超时控制器：watch online 队列中未绑定的 Pod，按 `SchedulingMinutes` 或默认值计算截止时间；到期后写 `jdos.jd.com/SchedulingStatus=False` 并发事件，必要时通知上层平台。
- "首次调度失败时间"可以从 Pod 的 `PodScheduled=False` condition 的 `lastTransitionTime` 获得，不需要调度器额外写注解。

**调度器保留**：只保留"`SchedulingStatus=False` 的 Pod 不参与调度"这一过滤（几行代码）。k8s ≥ 1.26 的集群可以改用 `schedulingGates`，那时调度器一行都不用改。
**风险**：控制器按 watch 事件或定时扫描判断超时，会比调度器多几秒延迟，在分钟级超时场景下可接受。

### 3.4 Redis 排队序号上报（P0）

**现状**
- `pkg/scheduler/plugins/redis/`（约 750 行）在 `init()` 里用环境变量 `REDIS_HOST_PORT` / `REDIS_PASSWORD` 建立 Redis 连接。
- 会话中按作业计算 `pending_index`，用 pipeline 执行 `HSet` / `HDel`。
- 同时往 PodGroup 写 `scheduling.k8s.io/jobScheduledTime` 注解。

**为什么移出去**
- **外部 I/O 进入调度会话。** Redis 网络抖动或超时会直接拖慢调度周期；Redis 不可用时，插件行为决定了调度会不会受影响，这是一个不必要的故障耦合点。
- **凭据进入调度器。** 调度器 Pod 需要挂 Redis 密码，扩大了凭据的暴露面。
- **`init()` 有副作用。** 只要插件包被 import，就会尝试连接 Redis，单测和其它二进制也会受影响。
- **用途是展示，不是调度。** 排队序号是给用户界面看的，调度器不读这个结果。

**剥离方案**
- 调度器只**暴露**排队顺序：推荐一个只读 HTTP 接口（复用现有 debug 端口，如 `/api/queue-order`），或者每个 pending 作业一个 `pending_index` 指标。
- 由独立的同步服务拉取后写入 Redis，Redis 凭据只给这个服务。
- `jobScheduledTime` 合并到 3.5 的生命周期控制器里写。

**调度器保留**：暴露排序结果的只读接口（无 I/O、无凭据）。
**注意**：如果业务能接受"近似序号"，同步服务也可以按"队列优先级 + 作业等级 + 创建时间"自行排序，调度器完全不用改。但这样和真实调度顺序可能有偏差（例如受 backoff、cooldown 影响的作业），需要业务确认。

### 3.5 PodGroup 生命周期时间戳、排队/运行时长统计（P1）

**现状**
- `pkg/scheduler/framework/session.go` 的 `addJobTimeStamp`（约 478 行，只处理离线任务）往 PodGroup 写入首次调度时间、目标调度时间、运行开始/结束时间、各 pending 原因的累计时长等注解。
- 同时上报 `statisticDelayMetric`、`RegistryJobRunningDuration`、`RegistryJobScheduleDuration` 指标。
- `job_updater.go:132` 用"注解有变化或被标脏"决定是否回写 PodGroup。
- notebook 插件依赖 `scheduling.k8s.io/runningTimestamp`。

**为什么移出去**
- **记录的是"事实"，不是"决策"。** Phase 迁移、condition 变化都能从 PodGroup / Pod 对象上观察到，外部控制器 watch 一下就能精确记录。
- **增加 PodGroup 回写量。** 时间戳和时长注解会让原本"状态没变就不回写"的 PodGroup 也需要回写（内部为此引入了 `MarkPodGroupDirty` 机制），回写量随作业数线性增长。
- **调度器重启后统计不准。** 时长累计依赖调度器内存和"上次看到的状态"，leader 切换或重启时会断档或重复计数；外部控制器可以基于对象上的 `lastTransitionTime` 计算，结果是幂等的。
- **`session.go` / `job_updater.go` 是社区高频改动文件。** 在这里加业务逻辑，是升级冲突的高发区。

**剥离方案**
- 作业生命周期控制器 watch PodGroup 和 Pod：
  - Inqueue / Running / Completed 的迁移时间，取 condition 的 `lastTransitionTime` 或首次观察到的时间；
  - 各 pending 原因的时长，按 `Unschedulable` condition 的 reason 变化累计；
  - 统一写 PodGroup 注解并导出指标。
- notebook 的运行时长改用 Pod 的 `status.startTime`，或者读控制器写的 `runningTimestamp`。

**调度器保留**：PodGroup 的 condition（Unschedulable / PendingReason / NotEnoughQuota 等，这些原因只有调度器知道），以及社区原有的调度延迟指标。
**注意**："首次进入调度器"的时间是调度器独有的事实。如果业务确实需要这个口径，可以保留一个只写一次的 `firstScheduleAttempt` 注解（写一次、不再更新，开销可忽略）；其余时间戳都外移。

### 3.6 Reservation 生命周期（P1，部分剥离）

**现状**
- `pkg/scheduler/reservation/cache.go`（约 1100 行）在调度器里启动 `runStatusSyncWorker` 和 `runGCWorker`，负责：
  - TTL 过期（`IsExpired`）；
  - 终态保留期（注解 `volcano.sh/reservation-gc-retention`）后通过 `vcClient...Reservations().Delete` 删除 CR；
  - 清理占用 Pod 上的注解（`clearReservationAllocatedPods`）；
  - owner 跟踪；
  - 维护 status phase 和 conditions（`status.go`）。

**为什么移出去**
- **CRD 生命周期管理是 controller 的标准职责。** Kubernetes 的惯例是"调度器做放置，控制器做生命周期"，Job、ReplicaSet、PodGroup 的状态维护也都由 controller-manager 负责。
- **删除对象是破坏性操作。** 放在调度器里，调度器 bug 可能误删用户的 Reservation；放在独立控制器里，权限可以单独收敛（调度器的 RBAC 可以去掉 reservations 的 delete 权限）。
- **生命周期跟调度器可用性绑在一起。** 调度器卡在 cache sync（例如 k8s 1.19 上的 PDB informer 问题），或在做 leader 切换时，过期的 Reservation 不会被处理，资源一直被锁着。
- **多调度器场景无法复用。** 如果以后有第二个调度器也要消费 Reservation，生命周期逻辑会出现两份，或者归属不清。
- **减少约 1000 行调度器代码。** 这部分与社区 v1.15 的 cache 重构没有交集，留在调度器里纯属迁移负担。

**剥离方案**
- `reservation-controller`（可以放进 vc-controller-manager 或自研控制器）负责：TTL 到期改 phase、终态 GC、owner / consumer 注解清理、Available / Expired 等 condition 维护。
- Validating / Mutating Webhook 负责 spec 校验和默认值。

**调度器保留**：占位 Pod 的放置、消费者匹配与资源交接、Restricted 过滤、PreAllocation 的受害者选择、`reserve` action，以及只把**调度结果**写入 status（Scheduled / 绑定到哪些节点）。
**注意**：status 由两方写入，必须划清字段归属：调度器只写调度相关的 condition，控制器写生命周期 phase。双方用 patch 而不是 update，避免互相覆盖。

### 3.7 delay-schedule 延迟调度（P1）

**现状**
- `pkg/scheduler/plugins/delayschedule/`，加上 `session.go` 里的 `HandleJobDelayScheduling`（约 980 行）。
- 作业到达目标时间之前，通过 ActionExcluded 钩子把作业排除在所有 action 之外，并写 PodGroup Unschedulable condition（reason 为 `DelayedSchedule`）。

**为什么移出去**
- **纯时间触发，不涉及任何资源判断。**
- **每个调度周期都要遍历所有延迟作业**，并写入或维持 condition。
- **"延迟提交"本来就是作业编排语义。** 上层平台在到点时再创建 vcjob / PodGroup，是最简单、零调度器代码的实现。

**剥离方案（任选其一）**
- **方案 A（推荐）**：上层作业控制器到点才创建作业。
- **方案 B**：作业照常创建，但带上通用的"不早于"注解（见 3.10 的统一门控 `not-before`）。控制器到点后删除注解，或者调度器只做时间比较。
- **方案 C**：k8s ≥ 1.26 用 `schedulingGates`，由控制器到点移除 gate。

**调度器保留**：方案 A 不需要任何代码；方案 B 只需要一个通用的 not-before 门控。
**注意**：如果业务要求"延迟中的作业也要占队列配额或在 UI 显示排队"，方案 A 不满足，应选方案 B。

### 3.8 节点资源指标（P1）

**现状**
- `cache.go` 启动了 `handleUpdateNodeMetrics`（每 45s）和 `updateNodeFreeResourceMetrics`（每 5min）。两者都调用 `sc.Snapshot()` 做**全量深拷贝快照**，然后计算节点已分配、空闲、可回收资源和空闲直方图（`metrics/node.go` 约 190 行）。

**为什么移出去**
- **跟调度主循环抢资源。** `Snapshot()` 要拿 cache 锁并深拷贝所有节点、作业。大集群下，一次快照本身就是秒级开销；额外的快照会让调度周期抖动，并增加内存峰值。
- **不影响任何调度决策。**
- **口径更稳定。** 独立 exporter 可以直接基于 informer 计算，不受调度器重启或 leader 切换影响；多副本调度器也不会重复上报或断档。

**剥离方案**
- 独立 exporter（或者 kube-state-metrics 加自定义 collector），基于 Node / Pod informer 计算已分配、空闲资源和直方图。
- "可回收资源"依赖作业的可回收语义；3.1 完成后这个语义已经写在注解上，exporter 直接读即可。

**调度器保留**：社区原有的调度指标（e2e 延迟、action / plugin 耗时、队列指标）。

### 3.9 archimedes HTTP 指标源（P2）

**现状**：`metrics/source/metrics_client_archimede.go`（约 270 行）通过 HTTP 从 archimedes 拉取节点利用率，供 `usage` 插件使用。

**结论**：线上配置**没有启用 `usage` 插件**，建议这次升级直接不迁移。以后如果需要基于利用率调度，让节点利用率以 Prometheus 指标或节点注解发布，调度器使用社区自带的 prometheus / elasticsearch 指标源即可。

**理由**：调度器不应该依赖某个特定内部服务的私有 HTTP 协议；社区的指标源接口已经足够用。

### 3.10 jobbackoff 退避（P2，可选部分剥离）

**现状**
- `pkg/scheduler/plugins/jobbackoff/`（约 780 行）在调度器里维护退避状态机：失败次数、指数退避、按 HDK 版本分桶、下次重试时间、阻塞原因类型。
- 状态通过 `MarkPodGroupDirty` 持久化到 PodGroup 注解（`annoBlockedSince`、`annoBlockedDuration`、attempts、next-retry-at 等）。

**为什么考虑移出去**
- 退避**策略**（时长、上限、按 HDK 版本分桶）是业务运维策略，会跟着昇腾 HDK 版本演进而变化。
- 每次失败都要回写 PodGroup 注解。
- HDK 版本这类硬件运维概念进入了调度插件。

**为什么只是"可选"**
- "这次调度失败了、失败原因是什么"是调度器独有的信息。外部控制器只能通过 PodGroup Unschedulable condition 的变化间接感知，计数粒度会变粗：一个 session 内的多次尝试无法区分，condition 不变时也感知不到新的失败。

**剥离方案（如果决定剥离）**
- 调度器只保留一个**通用门控**：PodGroup 带有 `scheduling.easyalgo/not-before=<RFC3339>` 时，在该时间之前把作业排除在 action 之外。
- 这个门控可以**与 3.7 的 delay-schedule 合并**，用同一个注解、同一段代码。
- 退避控制器 watch PodGroup condition，计算下一次重试时间，然后写 `not-before`。

**建议**：本次升级先保持 jobbackoff 在调度器内，但把"门控"和"策略"拆成两层代码，为以后外移做准备。delay-schedule 外移时顺便落地 `not-before` 通用门控。

### 3.11 节点 GPU 配比与 zone 配比配置（P2，可选）

**现状**
- `api/node_info.go` 的 `SetReserveRatio` 按节点 allocatable / GPU 数自动计算每卡 CPU、内存的配比。
- predicates 里通过 `jdos.jd.com/zone` 和调度器 configmap 中的比例配置，做按比例的资源校验（社区 v1.15 已用 `resource-strategy-fit` 取代 predicates 的 proportional 逻辑）。

**为什么考虑移出去**
- 配比是**机型属性**，由节点池管理组件计算并写成节点标签（例如 `easyalgo/cpu-per-gpu`、`easyalgo/mem-per-gpu`）后更直观，也便于运维核对。
- 调度器 configmap 里的 zone 比例配置，跟社区 `resource-strategy-fit` 的配置格式会冲突。改成"读节点标签"之后，调度器侧的改动只剩一个读标签的过滤函数。

**为什么只是"可选"**：自动计算本身很轻，留在调度器的成本不高。主要收益是减少 predicates 上的升级冲突。

---

## 4. 已经在外部、应保持外置的部分

以下数据已经由上层组件写入，调度器只读取。升级时**不要**把写入逻辑搬回调度器，并且建议在第 6 节的契约里把它们登记下来：

- 节点池标签 `scheduling-type`、`jdos.jd.com/zone`；
- 队列专属卡数 `slaCards`（队列注解）；
- 队列利用率注解 `scheduling.k8s.io/cpu-utilization` / `gpu-utilization`（queue-reclaim 读取）；
- replica-gang 相关标签（由作业控制器写入）。

---

## 5. 应保留在调度器的功能及理由

| 功能 | 保留理由 |
|---|---|
| capacity 配额语义（含 Overused、队列层级、SLA 卡数核算） | 配额判断需要基于实时分配结果，必须在 session 内与 allocate / reclaim 原子地一起计算 |
| preempt / reclaim / relaxEvict、受害者选择、BatchVictimScore、VictimProtection、Coeviction | 驱逐决策依赖完整快照和 Statement 事务，外部组件无法做到一致 |
| replica-gang、SchedulingUnit、SuperPod 拓扑、net-topology | 本质是放置约束，必须在过滤和打分阶段生效 |
| archimedes NUMA 过滤/打分、slanode、reserve 插件、coordinator | 节点级可行性判断，属于调度核心 |
| cooldown | 依赖 session 内的驱逐事件和跨 session 的短期记忆，外部化会引入竞态；体量小，留下成本低 |
| ActionExcluded 门控框架 | 它是"读外部门控"的统一入口，剥离 delay / backoff 之后反而更通用 |
| PodGroup 调度原因 condition（PendingReason、NotEnoughQuota 等） | 原因只有调度器知道，社区也是由调度器写入 |
| 调度日志、session 快照日志、skip-if-unchanged、`/api/archimedes/dump` | 用于调度器自身排障，没有对外副作用 |

---

## 6. 调度器与上层管理组件的接口契约（建议）

剥离之后，调度器与外部组件之间只通过**对象上的注解和标签**交互。建议统一前缀（例如 `scheduling.easyalgo/*`），并在仓库里维护一份契约表：

| 方向 | 键 | 写入方 | 读取方 | 含义 |
|---|---|---|---|---|
| 输入 | `volcano.sh/preemptable` | Webhook | 调度器 | 是否可被抢占（社区标准） |
| 输入 | `scheduling.easyalgo/reclaimable` | Webhook | 调度器 | 是否可被跨队列回收 |
| 输入 | `scheduling.easyalgo/elastic` | Webhook | 调度器 | 弹性作业 |
| 输入 | 型号专属资源名（如 `nvidia.com/A100`） | Webhook / Device Plugin | 调度器、kubelet | GPU 型号需求 |
| 输入 | `scheduling.easyalgo/not-before` | 延迟/退避控制器 | 调度器 | 早于该时间不参与调度 |
| 输入 | `jdos.jd.com/SchedulingStatus=False` | Pod 超时控制器 | 调度器 | 已超时，停止调度 |
| 输入 | `scheduling-type`、`jdos.jd.com/zone`、配比标签 | 节点池管理组件 | 调度器 | 节点池与机型属性 |
| 输入 | 队列 `slaCards`、利用率注解 | 队列管理组件 | 调度器 | 队列专属卡数、利用率 |
| 输出 | PodGroup conditions（Unschedulable / PendingReason） | 调度器 | 生命周期控制器、退避控制器、UI | 调度失败原因 |
| 输出 | Reservation status 中的调度相关 condition | 调度器 | reservation-controller | 放置结果 |
| 输出 | `/api/queue-order`（只读 HTTP）或指标 | 调度器 | Redis 同步服务 | 排队顺序 |

约定：
- 输入类键只由写入方写，调度器**只读**；输出类键只由调度器写。
- 调度器对缺失的输入键使用社区缺省语义，不再在调度器内部按业务属性推导。

---

## 7. 对本次 v1.15 迁移的收益

- **少迁移约 3000+ 行调度器代码**：redis、delay-schedule、unschedule_task、节点指标、archimedes 指标源、Reservation GC/状态同步、`addJobTimeStamp` 等。
- **去掉调度器对 eaframework 私有模块的依赖**：构建不再需要私有仓库凭据，`api` 包与社区的差异大幅缩小。
- **减少高冲突文件的改动**：`api/job_info.go`、`framework/session.go`、`framework/job_updater.go`、`cache/cache.go`、`plugins/predicates` 都是社区 v1.10–v1.15 大改过的文件。
- **框架钩子可以精简**：`HandleJobDelayScheduling` 和 `MarkPodGroupDirty` 的大部分用途会消失；ActionExcluded 退化为一个通用的 not-before 门控。
- **调度器 RBAC 收敛**：不再需要 reservations 的 delete 权限，也不需要为业务注解 patch Pod，Redis 凭据也不必挂在调度器上。
- **运维解耦**：超时时长、退避策略、业务等级映射这类变更不再需要重启或发版调度器。

---

## 8. 风险与待确认

1. **组件数量增加**：需要新建或扩展 webhook、若干控制器和 exporter。建议合并成一个"调度管理组件"二进制，内部按 controller 拆分。
2. **异步延迟**：控制器基于 watch 工作，超时、到点放行会有秒级延迟。需要业务确认可以接受。
3. **存量数据兼容**：3.1、3.2 上线前必须完成存量 Pod / PodGroup 的补注解，或保留一个版本的兜底推导。3.2 方案 A 必须先完成 device plugin 和节点侧改造。
4. **k8s 版本**：`schedulingGates` 需要 k8s ≥ 1.26。如果生产集群仍是 1.19 / 1.21，就统一使用注解门控（`not-before`、`SchedulingStatus`）。
5. **口径变化**：生命周期时长改为基于 condition 的 `lastTransitionTime` 计算后，会和现有看板的口径略有差异，需要通知看板使用方。
6. **待确认问题**：
   - Redis 排队序号是否接受"近似顺序"？（决定 3.4 是否需要调度器暴露接口）
   - 延迟中的作业是否需要占用或显示在队列中？（决定 3.7 选方案 A 还是 B）
   - device plugin 能否按型号上报资源名？（决定 3.2 选方案 A 还是 B）
   - jobbackoff 这次是否外移，还是先做代码分层？
