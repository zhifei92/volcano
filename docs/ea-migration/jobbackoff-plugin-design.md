# jobbackoff 迁移：复用 ActionExcluded 门控的详细设计

> 适用范围：内部 Volcano 升级到社区 v1.16（apiserver 1.19）。
> 关联文档：`scheduler-decoupling-proposal.md` 3.10 节（本文是对该节的修订）。

## 0. 结论

- **jobbackoff 不剥离，保留为调度器插件。**
- **拦截点复用内部已有的 ActionExcluded 门控（`AddActionExcludedFn` / `IsActionExcluded`），不再保留 jobbackoff 专用的框架钩子。**
  - 线上配置是 `enableBackoffCheck: false`，退避**只拦 reclaim / preempt，不拦 allocate**。
  - ActionExcluded 的回调签名是 `func(obj, action string) int`，**能感知是哪个 action 在调用**，可以精确地"只拦 preempt 和 reclaim"。
  - 它支持**弃权**（Abstain），不受 tier 位置约束。
  - 内部 preempt 和 reclaim 中，`IsActionExcluded` 的调用位置**紧挨着**原来的 `IsBackoffBlocked`（都在 `JobStarving` 之后），门控顺序和时机与现状完全一致。
  - 指标 `job_backoff_blocked_total` 的 `action` 标签可以原样保留，看板不用改。
- **复用不会增加任何框架侵入。** delay-schedule、reservation、spblock 都在用 ActionExcluded，迁移时它一定会保留（提案第 5 节已经把它列为保留项）。
- **jobbackoff 专用的约 15 处侵入全部删除**：`AddBackoffBlockedFn`、`AddBackoffCheckFn`、`IsBackoffBlocked`、`ShouldCheckBackoffBlocked`、两个配置开关、3 个 action 中的门控调用、`JobInfo.BackoffSkipped`，以及 metrics 包中的退避指标。
- **备选方案**：如果将来要彻底去掉 ActionExcluded（例如追求零框架改动），可以退回社区的 `JobStarvingFn`，见附录 A。不选 `JobValidFn`，因为它会连 allocate、backfill 一起拦。
- 注解格式完全不变，新旧版本可以互相读取，滚动升级和回滚都安全。
- 工作量约 2.5 人天。

## 1. 现状

### 1.1 线上配置

```yaml
actions: "reserve, allocate, reclaim, preempt, backfill"   # 没有 enqueue
tiers:
- plugins:
  - name: jobbackoff
    enableBackoffCheck: false
    arguments:
      jobbackoff.max-backoff-seconds: 600
      jobbackoff.queue-ttl-seconds: 900
  - name: task-property
  - name: queue-reclaim
  - name: elastic
  - name: priority
  - name: gang
  - name: replica-gang
  - name: tdm
  # ... capacity, notebook, delay-schedule, reservation, ...
```

### 1.2 线上实际生效的拦截范围

| action | 内部调用 | 线上是否拦截 | 原因 |
|---|---|---|---|
| enqueue | `JobEnqueueableFn` → `backoffGate` | **未启用** | actions 里没有 enqueue。Pending 作业在 allocate 开头被直接改成 Inqueue（`allocate.go:96-109`） |
| allocate | `!IsElasticInferJob && ShouldCheckBackoffBlocked && IsBackoffBlocked`（`allocate.go:116`） | **不拦** | `enableBackoffCheck: false` → 没有插件投票 → `ShouldCheckBackoffBlocked` 返回 false |
| reclaim | `JobStarving` 为 true → `IsActionExcluded` → `IsBackoffBlocked`（`reclaim.go:121-134`） | **拦** | `enableBackoffBlocked` 默认 true |
| preempt | `JobStarving` 为 true → `IsActionExcluded` → `IsBackoffBlocked`（`preempt.go:128-146`） | **拦** | 同上 |
| backfill | 无 | 不拦 | — |

说明：

- `ShouldCheckBackoffBlocked` 的注释写的是 "Default behavior is true when no plugin votes"，但代码在没有投票时返回 **false**。线上正是依赖这个返回值让 allocate 不受拦截的。
- `upsertFailedJob` 的注释也印证了这一点："This avoids pushing the retry window forward when allocate runs without backoff check while reclaim/preempt remain blocked by IsBackoffBlocked."

**线上 jobbackoff 的真实作用是：调度失败的作业，每轮照常参与分配，但抢占和回收按指数退避的节奏进行。** 抢占和回收会驱逐 Pod，代价高，这个设计本身是合理的。

### 1.3 jobbackoff 专用的框架侵入（本方案全部删除）

| 位置 | 内容 |
|---|---|
| `framework/session.go` | 字段 `backoffBlockedFns`、`backoffCheckFns` |
| `framework/session_plugins.go` | `AddBackoffBlockedFn`、`AddBackoffCheckFn`、`IsBackoffBlocked`、`ShouldCheckBackoffBlocked` |
| `conf/scheduler_conf.go` | `EnabledBackoffBlocked`、`EnabledBackoffCheck` |
| `plugins/defaults.go` | 上面两个开关的默认值 |
| `actions/allocate/allocate.go:111-122`、`log_errors.go` | 门控调用、`errBackoffBlocked` |
| `actions/preempt/preempt.go:140-146`、`log_errors.go` | 同上 |
| `actions/reclaim/reclaim.go:128-134`、`log_errors.go` | 同上 |
| `api/job_info.go` | `BackoffSkipped` 字段及其 `Clone` |
| `api/constants.go` | `SkippedByJobBackoff`（遗留的 pending reason） |
| `metrics/job.go` | `job_backoff_queue_size`、`job_backoff_rejected_in_session`、`job_backoff_residence_seconds`、`job_backoff_delay_seconds` |
| `metrics/job_backoff.go` | `job_backoff_blocked_total`、`job_backoff_blocked_seconds`、`job_backoff_blocked_jobs`、`BackoffReasonTypeFromAnnotations` |

### 1.4 ActionExcluded 门控（保留，本方案复用）

| 位置 | 内容 |
|---|---|
| `api/types.go:430-433` | `type ActionExcludedFn func(obj interface{}, action string) int`，返回 -1 排除、1 放行、0 弃权 |
| `framework/session_plugins.go:174-177, 537-566` | `AddActionExcludedFn`、`IsActionExcluded` |
| `conf/scheduler_conf.go`、`plugins/defaults.go` | `enableActionExcluded`，默认 true |
| 调用方 | allocate:141、backfill:193、enqueue:53、preempt:133、reclaim:122、reserve:654、shuffle:63 |
| 现有使用者 | delay-schedule（所有 action）、reservation（所有 action）、spblock（只拦 preempt、reclaim，线上未启用） |

`IsActionExcluded` 的投票语义：

- 按 tier、按插件顺序遍历，**遇到第一个 Reject 立即返回 true**；
- 某个 tier 内有插件返回 Permit，则该 tier 结束后返回 false，**不再看后面的 tier**；
- 全部弃权，返回 false。

## 2. 拦截点选型

### 2.1 候选对比

| 维度 | **ActionExcluded（推荐）** | JobStarvingFn（备选） | JobValidFn | JobEnqueueableFn |
|---|---|---|---|---|
| 来源 | 内部框架（迁移时本来就要保留） | 社区 | 社区 | 社区 |
| 能否区分 action | **能**（参数 `action`） | 不能（只被 preempt/reclaim/gangpreempt/gangreclaim 调用） | 不能（allocate、backfill、preempt、reclaim、gang*、`InitCycleState` 都调用） | 只有 enqueue 调用 |
| 能否弃权 | **能** | 不能（bool），要求同一 tier 里还有其他 starving 插件 | 能（返回 nil） | 能 |
| 在 preempt/reclaim 中的位置 | `JobStarving` 之后，**紧挨原 `IsBackoffBlocked`** | 本身就是 `JobStarving`，需要靠插件顺序和短路复现原顺序 | `JobStarving` 之前 | — |
| 每个作业每个会话的调用次数 | preempt 1 次、reclaim 1 次 | preempt 2 次、reclaim 2 次 | 每个 action 1 次，外加 `InitCycleState` | — |
| `blocked_total` 的 action 标签 | **保留**（allocate/preempt/reclaim） | 无法区分，只能合并 | 无法区分 | — |
| 默认能否和线上语义一致 | **能** | 能（有 tier 约束） | **不能**：会拦 allocate | 不能：线上没开 enqueue |
| 可选的"连 allocate 一起拦" | 配置一下 action 列表即可，backfill 依然不拦 | 需要再注册 JobValidFn，backfill 也会被拦 | 本身就拦 | — |

### 2.2 结论

**复用 ActionExcluded 最好：**

1. **语义最精确。** 用参数 `jobbackoff.excluded-actions` 声明要拦哪些 action，默认 `preempt,reclaim`。原来 `enableBackoffCheck: true` 的语义，改成 `allocate,preempt,reclaim` 就能得到，而且不会误伤 backfill。
2. **行为零偏差。** 内部 preempt（133 行）和 reclaim（122 行）里，`IsActionExcluded` 的调用位置就在原来的 `IsBackoffBlocked` 之前一两行，判断条件也相同，都是在 `JobStarving` 之后。
3. **没有 tier 约束。** 不拦截时返回弃权，jobbackoff 放在哪个 tier、哪个位置都不会截断其他插件的投票。
4. **可观测性不退化。** 回调能拿到 action 名，`job_backoff_blocked_total{action}` 保持原口径。
5. **概念统一。** delay-schedule、reservation、spblock 和 jobbackoff 都是"某些作业暂时不参与某些 action"，用同一个门控，符合提案中"多个功能尽量复用同一个门控"的原则。

**代价和风险：**

- ActionExcluded 是内部扩展，不是社区接口。但它的维护成本已经由 delay-schedule 和 reservation 承担，jobbackoff 复用它不增加新的侵入。
- v1.16 新增的 gangpreempt、gangreclaim 没有调用 `IsActionExcluded`。线上没有启用这两个 action；如果以后启用，需要在 ActionExcluded 的移植工作里给它们补上调用（这是 ActionExcluded 本身的事，和 jobbackoff 无关）。
- **长期建议**：把 ActionExcluded 作为通用扩展点提交到社区。一旦合入，delay-schedule、reservation、spblock、jobbackoff 四个插件都变成零侵入。

## 3. 读代码发现的其他问题

| # | 问题 | 处理 |
|---|---|---|
| B1 | `lastRoundEvicted` 不起作用：插件实例每个会话都会重新 `New`，上一轮写的值下一轮读不到。另外它只在 `ShouldCheckBackoffBlocked` 路径里用到，线上这条路径是关闭的 | 删除 |
| B2 | 门控副作用会重复执行：`backoffGate` 在 preempt、reclaim 里各被调用一次，阻塞计时、`JobFitErrors` 改写会执行两次；`job_backoff_rejected_in_session` 按调用次数计数，不是按作业计数 | 注解类副作用每个会话每个作业只执行一次（4.4）；`blocked_total` 本来就是按 action 计数，保持不变 |
| B3 | `ShouldCheckBackoffBlocked` 的注释和代码相反 | 随框架改动一起删除 |
| B4 | `bp.mu` 读写锁没有意义：插件实例是会话级的，`queue` 每个会话都从注解重建 | 删除，状态只在会话内使用 |
| B5 | HDK 分桶写死读取 `huawei.com/hdkversion_current` | 改成参数 `jobbackoff.bucket-annotation-key` |
| B6 | TTL 和最大退避的配合：线上 `max=600s`、`ttl=900s`，退避序列 10、20、40、80、160、320、600 秒，重试时间点约为 10、30、70、150、310、630 秒；下一次本应在 1230 秒，但 900 秒时 TTL 触发，条目被清空，从 10 秒重新开始。每 15 分钟大约给 7 次抢占/回收机会，600 秒的上限只会出现一次 | 默认保持不变；新增可选的 `ttl-mode`（6.1），由业务决定 |
| B7 | 内部 metrics 注释提到 "production queue-ttl-seconds: 2500"，和线上 configmap 的 900 不一致 | 文档和注释以 configmap 为准 |

## 4. 详细设计

### 4.1 总览

```
OnSessionOpen
  ├─ readArguments()
  ├─ 遍历 ssn.Jobs：recoverFromAnnotations() → decide()       // 纯计算，不改任何东西
  │     结果：bp.decisions[jobUID] = {verdict, entry, version}
  └─ AddActionExcludedFn(bp.excludedFn)

  reserve / allocate / reclaim / preempt / backfill 调用 IsActionExcluded(job, action)
        └─ excludedFn(job, action)
             ├─ action 不在 excluded-actions 中     → Abstain
             ├─ 没有退避条目                         → Abstain
             ├─ 首次命中时 applyOnce()（TTL 删除、阻塞计时、写 JobFitErrors）
             ├─ 按 action 计数 blocked_total
             └─ 冷却中 → Reject；否则 → Abstain（永远不返回 Permit）

OnSessionClose
  └─ 遍历 ssn.Jobs：成功则删除条目；失败且本轮未被拦截则 upsert   // 基本沿用现有逻辑
```

### 4.2 参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `jobbackoff.initial-backoff-seconds` | 10 | 不变 |
| `jobbackoff.max-backoff-seconds` | 300（线上 600） | 不变 |
| `jobbackoff.factor` | 2.0 | 不变 |
| `jobbackoff.jitter-ratio` | 0.1 | 不变 |
| `jobbackoff.queue-ttl-seconds` | 300（线上 900） | 不变 |
| `jobbackoff.allow-backoff-release` | true | 不变 |
| `jobbackoff.excluded-actions` | **`preempt,reclaim`** | **新增**。冷却中的作业要排除在哪些 action 之外。`allocate,preempt,reclaim` 等价于原来的 `enableBackoffCheck: true` |
| `jobbackoff.bucket-annotation-key` | `huawei.com/hdkversion_current` | **新增**。分桶依据的注解 key，为空表示不分桶 |
| `jobbackoff.ttl-mode` | `reset` | **新增，可选**。`reset` 为现有行为；`grace` 见 6.1 |

调度器配置中的 `enableBackoffCheck` / `enableBackoffBlocked` 不再需要，删掉即可。v1.16 用 `gopkg.in/yaml.v2` 非严格解析配置，即使暂时没删，未知字段也会被忽略。整体开关可以用 ActionExcluded 自带的 `enableActionExcluded: false`，或者直接从配置里移除插件。

### 4.3 目录结构

```
pkg/scheduler/plugins/jobbackoff/
├── jobbackoff.go      // 插件入口、参数、OnSessionOpen/Close
├── decision.go        // decide()、applyOnce()、excludedFn()
├── state.go           // backoffEntry、注解读写（基本沿用现有代码）
├── policy.go          // backoffDuration()、shouldPermitAfterBackoff()（纯函数，便于以后把策略外移）
├── metrics.go         // 从 metrics 包搬过来，指标名和标签不变
├── hdk.md             // 保留
└── *_test.go
```

### 4.4 决策、门控与幂等副作用

```go
type verdict int

const (
	verdictNone      verdict = iota // 没有退避条目
	verdictBlock                    // 冷却中，拦截
	verdictBlockHold                // 冷却已到期，但 allow-release=false，继续拦截
	verdictRelease                  // 冷却已到期，放行，并结束阻塞计时
	verdictTTL                      // TTL 到期，放行，并删除当前桶的条目
)

func (v verdict) blocks() bool { return v == verdictBlock || v == verdictBlockHold }

type decision struct {
	verdict verdict
	version string
	entry   *backoffEntry
	applied bool // 注解类副作用是否已经执行过
}

type jobBackoffPlugin struct {
	args      pluginArgs
	queue     map[backoffKey]*backoffEntry // 会话级，从注解恢复
	decisions map[api.JobID]*decision
	ssn       *framework.Session
}

func (bp *jobBackoffPlugin) decide(job *api.JobInfo, now time.Time) *decision {
	version := bucketOf(job, bp.args.bucketKey)
	entry, ok := bp.queue[backoffKey{UID: job.UID, Version: version}]
	if !ok {
		return &decision{verdict: verdictNone}
	}
	d := &decision{version: version, entry: entry}
	switch {
	case !entry.EnqueueAt.IsZero() && now.Sub(entry.EnqueueAt) >= bp.args.queueTTL:
		d.verdict = verdictTTL
	case now.Before(entry.NextRetryAt):
		d.verdict = verdictBlock
	case bp.shouldPermitAfterBackoff(job):
		d.verdict = verdictRelease
	default:
		d.verdict = verdictBlockHold
	}
	return d
}
```

`OnSessionOpen` 只做纯计算：

```go
func (bp *jobBackoffPlugin) OnSessionOpen(ssn *framework.Session) {
	bp.readArguments()
	bp.ssn = ssn
	now := time.Now()
	for _, job := range ssn.Jobs {
		bp.recoverFromAnnotations(job)
	}
	for _, job := range ssn.Jobs {
		if d := bp.decide(job, now); d.verdict != verdictNone {
			bp.decisions[job.UID] = d
		}
	}
	bp.publishInventoryMetrics(now)
	ssn.AddActionExcludedFn(bp.Name(), bp.excludedFn)
}
```

门控函数：

```go
func (bp *jobBackoffPlugin) excludedFn(obj interface{}, action string) int {
	job, ok := obj.(*api.JobInfo)
	if !ok || job == nil {
		return util.Abstain
	}
	if !bp.args.excludedActions.Has(action) {
		return util.Abstain
	}
	if action == api.ActionAllocate && api.IsElasticInferJob(job) {
		return util.Abstain // 与原 allocate.go:116 一致
	}
	d, ok := bp.decisions[job.UID]
	if !ok {
		return util.Abstain
	}
	bp.applyOnce(job, d)
	if !d.verdict.blocks() {
		return util.Abstain // 到期或 TTL：放行，但用弃权而不是 Permit
	}
	jobBackoffBlockedTotal.WithLabelValues(action, string(d.entry.ReasonType), slaLabel(job.ReqSLAMachine)).Inc()
	util.WriteDetail(4, "session-action-jobskip action=%s job=%s/%s reason=%q detail=[queue=%s,nextRetryAt=%s]\n",
		action, job.Namespace, job.Name, "BackoffBlocked", job.Queue, d.entry.NextRetryAt.Format(time.RFC3339))
	return util.Reject
}

func (bp *jobBackoffPlugin) applyOnce(job *api.JobInfo, d *decision) {
	if d.applied {
		return
	}
	d.applied = true
	switch d.verdict {
	case verdictTTL:
		bp.deleteVersionEntry(job, d.version)
		finalizeBackoffBlockedSegment(bp.ssn, job, d.entry.ReasonType)
	case verdictRelease:
		finalizeBackoffBlockedSegment(bp.ssn, job, d.entry.ReasonType)
	case verdictBlock, verdictBlockHold:
		applyBackoffBlock(bp.ssn, job, d.entry, d.verdict == verdictBlockHold) // 沿用：开启阻塞计时、写 JobFitErrors、恢复 PendingReason
		jobBackoffRejectedInSession.Inc()
	}
}
```

要点：

- **永远不返回 Permit。** `IsActionExcluded` 中，一个 tier 里只要有插件返回 Permit，该 tier 结束后就直接返回 false，后面 tier 的排除投票会被忽略。原来的 `backoffGate` 在冷却到期时返回 Permit，在独立的 `IsBackoffBlocked` 里这么做没问题；放进共享门控后必须改成弃权，否则会把同一 tier 之后（以及后面 tier）的 delay-schedule、reservation 的排除投票吞掉。
- **注解类副作用只执行一次。** reclaim 和 preempt 在同一个会话里各调用一次门控，`d.applied` 保证阻塞计时、`JobFitErrors` 改写、TTL 删除只做一次，修正问题 B2。
- **`blocked_total` 按 action 计数。** 每个 action 对每个作业只调用一次 `IsActionExcluded`，所以计数口径和原来完全相同。
- **执行时机不变。** 副作用发生在 reclaim 和 preempt 第一次判断这个作业时。线上 action 顺序是 `reserve, allocate, reclaim, preempt, backfill`，allocate 在前面已经执行完，它写入的失败原因不会被提前覆盖，和现状一致。
- **reserve 和 backfill 也会调用 `IsActionExcluded`**，默认不在 `excluded-actions` 里，直接弃权。
- **不需要加锁。** `IsActionExcluded` 在 action 主循环里串行调用。

### 4.5 插件在 tier 中的位置

ActionExcluded 支持弃权，jobbackoff 放在哪里都不会影响其他插件的结果。唯一的差别是副作用：`IsActionExcluded` 在同一个 tier 里遇到第一个 Reject 就返回，排在前面的插件先被询问。

- 原来的顺序是"先 `IsActionExcluded`（delay-schedule、reservation），再 `IsBackoffBlocked`"。被 delay-schedule 或 reservation 排除的作业，根本走不到退避门控，也就不会开始阻塞计时。
- **建议把 jobbackoff 放在 tier1 中 delay-schedule、reservation 之后**，完全复现原来的顺序。
- 如果仍放在 tier1 第一位，结果相同，只是"既被延迟、又在退避中"的作业也会开始阻塞计时、计入 `blocked_total`。这类作业很少：延迟期间作业不参与 allocate，不会产生失败，也就没有退避条目。

### 4.6 OnSessionClose

整体沿用现有逻辑，只有两处改动：

```go
func (bp *jobBackoffPlugin) OnSessionClose(ssn *framework.Session) {
	now := time.Now()
	for _, job := range ssn.Jobs {
		switch {
		case shouldSkipSchedulingByPhase(job),
			len(job.TaskStatusIndex[api.Pending]) == 0:
			if bp.hasJobEntries(job.UID) || hasBackoffQueueAnnotations(job) {
				bp.deleteJobEntries(ssn, job)
			}
		case len(job.TaskStatusIndex[api.Pipelined]) > 0:
			// 有 pipeline 的作业不进入退避队列；删除 lastRoundEvicted
		case bp.blockedThisSession(job.UID):
			// 原来的 job.BackoffSkipped：本轮被门控拦下，不算新的失败
		case isSchedulingFailed(job):
			bp.upsertFailedJob(ssn, job, now)
		}
	}
	bp.publishInventoryMetrics(now)
}

func (bp *jobBackoffPlugin) blockedThisSession(uid api.JobID) bool {
	d, ok := bp.decisions[uid]
	return ok && d.applied && d.verdict.blocks()
}
```

- 用 `blockedThisSession` 替代 `JobInfo.BackoffSkipped`，不再需要往 `JobInfo` 加字段。
- `isSchedulingFailed` 中对 `job.BackoffSkipped` 的判断删除，其余（`JobFitErrors` / `NodesFitErrors`）不变。
- `upsertFailedJob` 里"冷却未到期则不增加次数"的短路保留。线上 allocate 每轮都会失败，没有这个短路，退避时间会被不断往后推。

### 4.7 指标

全部从 `metrics` 包搬到插件的 `metrics.go`，**指标名、Help、标签、桶边界都不变**：

| 指标 | 变化 |
|---|---|
| `volcano_job_backoff_queue_size` | 不变 |
| `volcano_job_backoff_rejected_in_session` | 改为按作业去重计数（修正 B2），数值会比原来小 |
| `volcano_job_backoff_residence_seconds` | 不变 |
| `volcano_job_backoff_delay_seconds{job_ns,job_id,queue_name}` | 不变 |
| `volcano_job_backoff_blocked_total{action,reason_type,sla}` | **不变**（action 仍为 preempt/reclaim，配置了 allocate 时还有 allocate） |
| `volcano_job_backoff_blocked_seconds{reason_type,sla}` | 不变 |
| `volcano_job_backoff_blocked_jobs{reason_type,sla}` | 不变 |

原来各 action 中计数时调用的 `metrics.BackoffReasonTypeFromAnnotations`，改为直接使用 `d.entry.ReasonType`，不再需要从注解里反解。

### 4.8 注解

完全沿用现有格式，新旧版本可以互相读取：

| 注解 | 说明 |
|---|---|
| `volcano.sh/job-backoff-attempts[.<bucket>]` | 失败次数 |
| `volcano.sh/job-backoff-enqueue-at[.<bucket>]` | 进入退避队列的时间 |
| `volcano.sh/job-backoff-next-retry-at[.<bucket>]` | 下次重试时间 |
| `volcano.sh/job-backoff-reason-type[.<bucket>]` | 失败原因类型 |
| `volcano.sh/job-backoff-allow-release` | 作业级的放行开关（用户写） |
| `volcano.sh/job-backoff-blocked-since` / `-blocked-duration` | 阻塞计时，job-lifecycle 也会读 |

分桶后缀 `<bucket>` 来自 `bucket-annotation-key` 指定注解的值，默认就是原来的 HDK 版本号，所以已有数据无需迁移。

### 4.9 配置示例

```yaml
actions: "reserve, allocate, reclaim, preempt, backfill"
tiers:
- plugins:
  - name: task-property
  - name: queue-reclaim
  - name: elastic
  - name: priority
  - name: gang
  - name: replica-gang
  - name: tdm
  # ...
  - name: delay-schedule
  - name: reservation
  - name: jobbackoff          # 放在 delay-schedule、reservation 之后（4.5）
    arguments:
      jobbackoff.max-backoff-seconds: 600
      jobbackoff.queue-ttl-seconds: 900
      # jobbackoff.excluded-actions: preempt,reclaim     # 默认，和线上一致
      # jobbackoff.bucket-annotation-key: huawei.com/hdkversion_current
  # ...
```

## 5. 行为对照

| 场景 | 原行为（线上） | 新行为（默认） |
|---|---|---|
| 冷却中的作业参与 allocate | 参与 | 参与 |
| 冷却中的作业参与 backfill / reserve | 参与 | 参与 |
| 冷却中的作业作为抢占方或回收方 | 跳过 | 跳过 |
| 冷却中的作业作为被抢占者或被回收者 | 不受影响 | 不受影响 |
| 门控在 preempt/reclaim 中的位置 | `JobStarving` → `IsActionExcluded` → `IsBackoffBlocked` | `JobStarving` → `IsActionExcluded`（含 jobbackoff） |
| 冷却到期且 allow-release=false | 跳过抢占和回收 | 同左 |
| TTL 到期 | 放行，删除当前桶 | 同左 |
| allocate 失败但冷却未到期 | 不增加次数 | 同左 |
| 被门控拦截的那一轮 | 不算新失败（`BackoffSkipped`） | 同左（`blockedThisSession`） |
| PodGroup condition 文案 | reclaim/preempt 门控改写 `JobFitErrors`，出现在 gang 写的 condition 里 | 同左（时机相同，只改写一次） |
| `job_backoff_blocked_total` | 按 action 计数 | 同左 |
| `job_backoff_rejected_in_session` | 按调用次数 | 按作业 |
| 门控返回值 | 冷却到期时返回 Permit（在独立钩子里） | 返回弃权（共享门控中不能返回 Permit） |
| `excluded-actions: allocate,preempt,reclaim` | — | 等价于原来的 `enableBackoffCheck: true`；弹性推理作业在 allocate 中豁免（与原来一致）；`lastRoundEvicted` 豁免不再存在（原来也没有生效过） |

## 6. 可选优化（不在本次必做范围）

### 6.1 TTL 模式

新增 `jobbackoff.ttl-mode`：

- `reset`（默认，现有行为）：TTL 到期后删除条目，下次失败从 `initial-backoff` 重新开始。
- `grace`：TTL 到期只放行一次，然后把 `EnqueueAt` 重置为当前时间，**保留失败次数**。这样之后的退避保持在上限附近，`max-backoff` 才真正起作用。

线上目前每 15 分钟大约给 7 次抢占/回收机会；改成 `grace` 后，稳定状态下约每 600 秒一次，外加每 900 秒一次 TTL 放行。是否切换由业务决定。

### 6.2 注解合并

把多个桶的 4 个字段合并成一个 JSON 注解 `volcano.sh/job-backoff`。读取时兼容旧的分散 key，写入只写新格式。分桶数量多时可以显著减少注解数，但会破坏回滚兼容性（旧版本读不懂新格式），所以建议在新版本稳定运行一段时间后单独上线。

### 6.3 精确唤醒

只有内部的 skip-if-unchanged（"无变化跳过会话"）保留时才需要。插件在 `OnSessionClose` 时算出所有条目中最早的 `NextRetryAt`，写入一个包级的"下次唤醒时间"，`runOnce` 判断是否跳过会话时参考它。否则退避到期后，最多要等一个 `maxSkipDuration` 才会真正重试抢占。

### 6.4 和社区 UnschedulableJobCache 的关系

v1.16 的 UnschedulableJobCache（alpha，默认关闭）跳过的是 **allocate、backfill、enqueue**，目标是减少重复的预选计算；jobbackoff（线上配置下）限制的是**抢占和回收的频率**。两者作用的 action 不重叠，可以同时开启，互不替代。

## 7. 改动清单（内部 fork）

| 文件 | 改动 |
|---|---|
| `framework/session.go` | 删除 `backoffBlockedFns`、`backoffCheckFns` 及其初始化 |
| `framework/session_plugins.go` | 删除 `AddBackoffBlockedFn`、`AddBackoffCheckFn`、`IsBackoffBlocked`、`ShouldCheckBackoffBlocked`；**保留** `AddActionExcludedFn`、`IsActionExcluded` |
| `conf/scheduler_conf.go` | 删除 `EnabledBackoffBlocked`、`EnabledBackoffCheck`；**保留** `EnabledActionExcluded` |
| `plugins/defaults.go` | 删除两个退避开关的默认值 |
| `actions/allocate/allocate.go`、`log_errors.go` | 删除 111-122 行门控和 `errBackoffBlocked`；141 行的 `IsActionExcluded` 保留 |
| `actions/preempt/preempt.go`、`log_errors.go` | 删除 140-146 行门控和 `errBackoffBlocked`；133 行的 `IsActionExcluded` 保留 |
| `actions/reclaim/reclaim.go`、`log_errors.go` | 删除 128-134 行门控和 `errBackoffBlocked`；122 行的 `IsActionExcluded` 保留 |
| `api/job_info.go` | 删除 `BackoffSkipped` 及其 `Clone` |
| `api/constants.go` | `SkippedByJobBackoff` 只用于读取历史注解，随 job-lifecycle 插件迁移（见 `job-lifecycle-plugin-design.md`） |
| `metrics/job.go`、`metrics/job_backoff.go` | 删除退避相关指标，搬到插件 |
| `plugins/jobbackoff/*` | 按第 4 节重构 |
| 调度器配置 | 删除 `enableBackoffCheck`；把 jobbackoff 移到 tier1 中 delay-schedule、reservation 之后 |

移植到 v1.16 时，ActionExcluded 本身需要在 allocate、backfill、preempt、reclaim（以及内部的 reserve、shuffle）中保留调用点。这部分工作属于 ActionExcluded 的迁移，delay-schedule 和 reservation 本来就需要，不计入本方案。

## 8. 测试方案

**单元测试**

- `decide`：覆盖没有条目、冷却中、到期放行、到期但 allow-release=false（全局开关和作业注解两种情况）、TTL 到期五种结果；分桶 key 为空、缺失、有值三种情况。
- `excludedFn`：
  - action 不在 `excluded-actions` 中时弃权（reserve、allocate、backfill）；
  - 冷却中返回 Reject，到期和 TTL 返回弃权，**任何情况下都不返回 Permit**；
  - reclaim 和 preempt 先后调用时，注解只写一次，`blocked_total` 两个 action 各加 1，`rejected_in_session` 只加 1；
  - TTL 到期时只删除当前桶，其他桶保留；
  - `excluded-actions` 包含 allocate 时，弹性推理作业在 allocate 中豁免，在 preempt 中不豁免。
- `OnSessionClose`：
  - 本轮被拦截的作业不增加次数；
  - 冷却未到期时失败不增加次数；
  - 成功或没有 Pending task 时删除全部桶；
  - 有 Pipelined task 时不处理。
- 注解兼容：用旧版本写的注解（包括带 HDK 后缀的）恢复出相同的条目。

**集成测试（framework 级）**

- 配置 `delay-schedule, reservation, jobbackoff` 同在 tier1：
  - 冷却中的作业：`IsActionExcluded(job, "allocate")` 为 false，`IsActionExcluded(job, "preempt")` 为 true；
  - 冷却到期的作业同时被 delay-schedule 排除时，`IsActionExcluded` 仍然返回 true（验证 jobbackoff 不会用 Permit 吞掉其他插件的排除）；
  - 被 delay-schedule 排除的作业，jobbackoff 的副作用不执行（验证 4.5 的顺序）。

**对比测试**

- 回放线上一段时间的 PodGroup 注解快照，分别跑旧插件和新插件，对比每个作业在 preempt、reclaim 中的门控结果和注解变化，只允许出现第 5 节列出的差异。

## 9. 上线、验证与回滚

1. **预发**：新版本加新配置（jobbackoff 移位，删除 `enableBackoffCheck`），观察：
   - `job_backoff_queue_size`、`job_backoff_blocked_jobs`、`job_backoff_blocked_total{action}` 和切换前同量级；
   - 驱逐相关指标（`volcano_pod_preemption_victims`、`volcano_total_preemption_attempts`）没有明显上升；
   - allocate 的调度成功率和延迟没有变化。
2. **灰度**：先小集群再大集群。
3. **回滚**：注解格式没变，直接换回旧镜像和旧配置即可，退避状态会被旧版本正常读取。

## 10. 工作量

| 项 | 人天 |
|---|---|
| 插件重构（决策、门控、副作用幂等、参数） | 0.75 |
| 删除退避专用的框架和 action 侵入，搬迁指标 | 0.5 |
| 单元测试、framework 集成测试 | 0.75 |
| 对比测试、预发验证 | 0.5 |
| **合计** | **约 2.5** |

## 11. 待确认问题

1. **ActionExcluded 的长期归属**：是否计划提交到社区？合入后 jobbackoff、delay-schedule、reservation、spblock 都变成零侵入；不合入的话，ActionExcluded 由内部长期维护（附录 A 是去掉它时的退路）。
2. **TTL 模式**：维持 `reset`，还是切换到 `grace`（6.1）？这会改变抢占和回收的实际频率。
3. **是否还需要在 allocate 阶段做退避**：如果确认不需要，`excluded-actions` 可以不开放，写死为 preempt、reclaim。
4. **`lastRoundEvicted` 的原始意图**："上一轮有驱逐时，让所有退避作业按优先级争抢被释放的资源"。它在线上从来没生效过，本方案直接删除。如果业务确实需要，需要单独设计（作业级而不是全局开关）。
5. **HDK 分桶的长期方案**：`hdk.md` 中提到的方案 B（由 ascend 判定是否试遍所有版本，jobbackoff 只读结果）是否要排期？它和本方案的扩展点选择无关，可以独立推进。

## 附录 A：备选方案——社区 JobStarvingFn

如果决定去掉 ActionExcluded，可以改用社区的 `JobStarvingFn`，做到 jobbackoff 零框架改动：

- **范围**：v1.16 里只有 preempt（128/183 行）、reclaim（91/129 行）、gangpreempt、gangreclaim 调用 `ssn.JobStarving`，和线上"只拦抢占和回收"的语义一致。
- **实现**：`starvingGate(job) bool` 查 `decisions`，冷却中返回 false（不饥饿），其他情况返回 true。副作用同样用 `applyOnce` 去重。
- **约束**：
  - 它是 bool 投票，不能弃权。同一 tier 内取 AND，某个 tier 有注册就由该 tier 决定。jobbackoff **必须和其他 starving 插件（gang、priority、tdm）放在同一个 tier，并且排在它们之后**，借助短路复现"先判断饥饿、再判断退避"。否则它对没有退避的作业返回 true，可能让判断提前结束，跳过后面 tier 的 starving 插件。
  - 每个作业每个会话会被调用 4 次（preempt、reclaim 各 2 次），回调必须廉价、幂等。
  - 拿不到调用方身份，`job_backoff_blocked_total` 的 `action` 标签只能合并成一个值，看板需要调整。
  - 如果要连 allocate 一起拦，只能再注册 `JobValidFn`，backfill 也会被一起拦。
- **不选 `JobValidFn` 作为默认的原因**：allocate、backfill、`InitCycleState` 都会调用它，退避中的作业会连普通分配都做不了，最长要等 600 秒，和线上语义相反。
