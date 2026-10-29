# job-lifecycle 插件详细设计：PodGroup 生命周期时间戳与排队时长统计

> 基于内部分支 `origin/gpu-numa` 的现有实现和社区 v1.16.0 的框架代码。
> 本文修订 [scheduler-decoupling-proposal.md](./scheduler-decoupling-proposal.md) 3.5 节的结论：从"剥离到外部控制器"调整为"**收拢成调度器插件**"。

## 0. 结论

- **不剥离到外部。** 首次调度时间、各排队原因的累计时长，依赖调度器内存里的 `PendingReason`，外部控制器看不到，算不出来。
- **从 `session.go` / `job_updater.go` 里挪出去，做成 `job-lifecycle` 插件。** 业务逻辑全部进插件；框架只加两处通用改动，合计约 30 行，都可以给社区提 PR：
  1. 新增"PodGroup 状态计算完成后"的插件回调 `JobUpdateHook`；
  2. `job_updater` 改成按注解差异判断是否回写。
- **注解 key、指标名保持不变**，下游、看板、notebook 插件都不用改。
- 顺带修复现有实现里的 4 个隐藏问题（见第 7 节）。
- 工作量约 3~4 人天。

## 1. 现状

### 1.1 写了什么

只处理带 `job-name` 标签的离线作业（`isOfflineTask`）。

**注解**（key 全部保持不变）：

| 注解 | 含义 | 写入时机 |
|---|---|---|
| `scheduling.k8s.io/jobScheduledTime` | 首次调度时间 | 只写一次。来源有三处：enqueue 和 allocate 在内存里设置 `ScheduleStartTimestamp`；redis 插件的 `JobEnqueuedFn` 直接写注解；`addJobTimeStamp` 在缺失时补写 |
| `scheduling.k8s.io/targetScheduleTime` | 目标调度时间（延迟调度） | 只写一次，来自 `job.TargetScheduleTime` |
| `scheduling.k8s.io/runningTimestamp` | 运行开始时间 | 只写一次，phase 首次变为 Running |
| `scheduling.k8s.io/finishTimestamp` | 运行结束时间 | 只写一次，phase 变为 Completed |
| `scheduling.k8s.io/lastReason` | 当前排队原因 | 原因变化时 |
| `scheduling.k8s.io/notEnoughResources` 等 10 个 | 对应原因**当前这一段**的开始时间 | 切换到该原因时 |
| `NotEnoughResources` 等**不带前缀的原因名** | 对应原因的**累计时长**（如 `5m30s`） | 离开该原因时累加 |

原因和"开始时间"注解 key 的对应关系（`GetAnnotationKeyForPendingReason`）：

| PendingReason | 开始时间注解 |
|---|---|
| `WaitingForMinResource` | `scheduling.k8s.io/notEnoughMin` |
| `NotEnoughQuota` | `scheduling.k8s.io/notEnoughQuota` |
| `NotGuaranteeMin` | `scheduling.k8s.io/notGuaranteeMin` |
| `SkippedByJobBackoff`（历史兼容） | `scheduling.k8s.io/skippedByJobBackoff` |
| `NotEnoughResources` | `scheduling.k8s.io/notEnoughResources` |
| `InvalidReservation` | `scheduling.k8s.io/invalidReservation` |
| `ReservationOwnerNotMatch` | `scheduling.k8s.io/reservationOwnerNotMatch` |
| `ReservationSpecNotMatch` | `scheduling.k8s.io/reservationSpecNotMatch` |
| `NotEnoughPods`、`NotEnoughPodsOfTask` | `scheduling.k8s.io/notEnoughReplicas` |
| `DelayedSchedule` | `scheduling.k8s.io/delayedSchedule` |

**指标**（定义在 `pkg/scheduler/metrics/job.go`，名字保持不变）：

| 指标 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `volcano_job_scheduling_duration_minutes` | Histogram | `framework` | 首次调度到运行的延迟 |
| `volcano_job_pending_reason_duration_minutes` | Histogram | `reason`, `sla` | 进入运行时，各原因的累计时长 |
| `volcano_job_pending_reason_duration_seconds` | Gauge | `job_ns`, `job_id`, `queue_name`, `reason` | 排队中作业当前原因的累计时长，每个会话刷新 |
| `volcano_job_running_duration` | Gauge | `job_ns`, `job_id` | 作业结束时的运行时长 |

### 1.2 代码分布

| 位置 | 内容 | 是否社区文件 |
|---|---|---|
| `framework/session.go` | `addJobTimeStamp`、`calculateAnnotationWhenChanged`、`updateLastPendingReason`、`calculateDurationForReason`、`GetAnnotationKeyForPendingReason`、`statisticDelayMetric`、`isOfflineTask`、`MarkPodGroupDirty` / `IsPodGroupDirty`，约 200 行 | 是 |
| `framework/job_updater.go` | `updateJob` 里调用 `addJobTimeStamp`，并用"注解有变化或被标脏"决定是否回写 | 是 |
| `framework/framework.go` + 所有插件 | 内部给 Plugin 接口加了 `OnSessionPreClose`，capacity 在其中改写 `PendingReason` | 是（接口级改动） |
| `actions/enqueue/enqueue.go:57`、`actions/allocate/allocate.go:202` | 在内存里设置 `ScheduleStartTimestamp` | 是 |
| `api/job_info.go` | `PendingReason`、`LastReason`、`ScheduleStartTimestamp`、`TargetScheduleTime`、`RunningAt` 字段及注解解析 | 是 |
| `metrics/job.go` | 上表 4 个指标 | 是 |
| `plugins/redis/redis.go` | `JobEnqueuedFn` 里写 `jobScheduledTime` | 否 |
| `plugins/jobbackoff/jobbackoff.go` | 5 处 `MarkPodGroupDirty` | 否 |
| `plugins/notebook/notebook.go` | 读 `RunningAt`（由 `runningTimestamp` 解析） | 否 |

### 1.3 为什么不能外移

- `PendingReason` 在会话中由多处设置：allocate 设成 `NotEnoughResources` / `NotGuaranteeMin`，capacity 根据作业是否可回收改写成 `NotEnoughQuota` / `WaitingForMinResource`，gang、延迟调度、预留各自设置。
- 其中 `NotGuaranteeMin`、经 capacity 改写后的原因都**不会写进 PodGroup 的 Unschedulable condition**，外部控制器 watch PodGroup 也看不到。
- 外部能观察到的只有 phase 变化（运行开始和结束时间），只外移这部分收益很小，反而让 notebook 插件依赖外部组件。

## 2. 设计目标

**目标**

1. 业务逻辑全部移出 `session.go`、`job_updater.go`、enqueue、allocate；社区文件只保留通用的框架扩展点。
2. 注解 key、指标名、语义和现在一致，下游无感知。
3. 时间戳与本轮计算出的新 phase 对齐，不引入一个会话的延迟。
4. 不依赖插件之间 `OnSessionClose` 的执行顺序。
5. 调度器重启后统计连续，不丢失、不重复。

**非目标**

- 不改变 `PendingReason` 的赋值逻辑（allocate、capacity、gang 中的赋值保留，它们是调度决策信息）。
- 不改注解格式。不带前缀的累计时长 key 是历史问题，见第 12 节待确认事项。

## 3. 总体设计

### 3.1 为什么不直接用 `OnSessionClose`

插件最自然的位置是 `OnSessionClose`，但它有两个问题：

1. **拿不到本轮的新 phase。** v1.16 的 `CloseSession` 先执行所有插件的 `OnSessionClose`，然后才在 `closeSession` 里由 `JobUpdater` 调用 `jobStatus` 计算新 phase。插件只能用上一轮的 phase，`runningTimestamp` / `finishTimestamp` 会晚一个会话；作业完成后 PodGroup 很快被删除的话，`finishTimestamp` 和运行时长可能根本来不及记录。
2. **执行顺序不确定。** `CloseSession` 用 `for _, plugin := range ssn.plugins` 遍历一个 map，插件之间的 `OnSessionClose` 顺序是随机的。如果其它插件在 `OnSessionClose` 里改 `PendingReason`（例如 capacity 的改写逻辑迁移时从 `OnSessionPreClose` 挪到 `OnSessionClose`），本插件可能读到改写前的值。

### 3.2 方案：在 JobUpdater 里加一个插件回调

在 `JobUpdater.updateJob` 中，`jobStatus` 计算完新 phase 之后、决定是否回写之前，调用插件注册的 `JobUpdateHook`。这正是现在 `addJobTimeStamp` 所在的位置，只是从硬编码改成了插件注册。

```
CloseSession
 ├─ 所有插件 OnSessionClose（顺序随机；PendingReason 在此之前全部定稿）
 └─ closeSession
     └─ JobUpdater.UpdateAll（多 worker 并行）
         └─ updateJob(job)
             ├─ job.PodGroup.Status = jobStatus(ssn, job)     ← 新 phase
             ├─ syncAllocatedHyperNodeAnnotation(job)
             ├─ ssn.runJobUpdateHooks(job)                    ← 新增：job-lifecycle 在这里写注解、记指标
             ├─ updatePGStatus      = status 有变化
             ├─ updatePGAnnotations = 注解与会话开始时不同       ← 改为通用比较
             └─ cache.UpdateJobStatus(...)                    ← 一次 Update 同时写 status 和注解
```

这样：

- 回调里拿到的 phase 就是本轮要写回的 phase，时间戳和现在完全一致；
- 回调在所有插件的 `OnSessionClose` 之后执行，`PendingReason` 已经定稿，不存在顺序问题；
- 时间戳注解和 status 在同一次 `Update` 里写回，不额外增加 API 调用。

附带效果：既然 `PendingReason` 只要在 `OnSessionClose` 结束前定稿即可，capacity 的改写逻辑可以直接放进它自己的 `OnSessionClose`，**内部给 Plugin 接口加的 `OnSessionPreClose` 可以整个去掉**（是否这样做属于 capacity 迁移的范围，这里只说明不再有障碍）。

## 4. 框架改动（社区文件，约 30 行）

### F1：新增 `JobUpdateHook` 扩展点

```go
// pkg/scheduler/framework/session_plugins.go

// JobUpdateHook 在 JobUpdater 计算完 PodGroup 新状态之后、回写之前调用。
// 实现只能修改传入 job 自身的 PodGroup（注解），且必须是并发安全的：
// JobUpdater 会用多个 worker 并行处理不同的 job。
type JobUpdateHook func(job *api.JobInfo)

func (ssn *Session) AddJobUpdateHook(name string, fn JobUpdateHook) {
	ssn.jobUpdateHooks[name] = fn
}

func (ssn *Session) runJobUpdateHooks(job *api.JobInfo) {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if fn, ok := ssn.jobUpdateHooks[plugin.Name]; ok {
				fn(job)
			}
		}
	}
}
```

- `Session` 结构体加字段 `jobUpdateHooks map[string]JobUpdateHook`，在 `openSession` 里初始化。
- 按 tier 顺序调用，和其它扩展点保持一致，顺序是确定的。

### F2：`updateJob` 调用回调，并按注解差异回写

```go
// pkg/scheduler/framework/job_updater.go
func (ju *JobUpdater) updateJob(index int) {
	job := ju.jobQueue[index]
	ssn := ju.ssn

	job.PodGroup.Status = jobStatus(ssn, job)
	syncAllocatedHyperNodeAnnotation(job)
	ssn.runJobUpdateHooks(job)
	oldStatus, found := ssn.PodGroupOldState.Status[job.UID]
	updatePGStatus := !found || isPodGroupStatusUpdated(job.PodGroup.Status, oldStatus)
	updatePGAnnotations := !maps.Equal(ssn.PodGroupOldState.Annotations[job.UID], job.PodGroup.GetAnnotations())
	updateJobInfo := ssn.DirtyJobs.Has(job.UID)
	// ...
}
```

- v1.16 在会话开始时已经把每个 PodGroup 的全部注解快照到 `ssn.PodGroupOldState.Annotations`（`session.go:265`），这里直接拿来比较。
- 原来的 `isJobAllocatedHyperNodeChanged` 只比较一个 key，新写法是它的超集，社区行为不变，该函数可以删除。
- 任何插件在会话中修改 PodGroup 注解都会被自动回写，**内部的 `MarkPodGroupDirty` / `IsPodGroupDirty` 机制可以整个删掉**（jobbackoff 里 5 处调用对应的都是注解修改，见 9.1）。

### 不需要改的地方

- `cache.UpdateJobStatus` 走 `StatusUpdater.UpdatePodGroup`，是对整个 PodGroup 对象的 `Update`，注解会随之写入，不用改。
- `cache.updateJobAnnotations` 只把 HyperNode 注解同步回缓存，其它注解等 informer 事件刷新即可（一致性分析见 6.2）。

### 提交社区

F1 + F2 是通用能力（"插件可以在回写前修改 PodGroup 注解并被持久化"），可以作为一个 PR 提给社区。合入后，内部在框架层就零改动了。

## 5. 插件设计

### 5.1 目录结构

```
pkg/scheduler/plugins/joblifecycle/
├── joblifecycle.go      插件入口：参数解析、OnSessionOpen 注册回调、OnSessionClose 清理指标
├── hook.go              JobUpdateHook 主流程：按 phase 分派
├── timestamps.go        一次性时间戳：jobScheduledTime / targetScheduleTime / runningTimestamp / finishTimestamp
├── pendingreason.go     排队原因时长累计；原因与注解 key 的映射表
├── metrics.go           4 个指标的定义（名字与现在完全一致）和按作业指标的清理
└── *_test.go
```

在 `plugins/factory.go` 注册一行：

```go
framework.RegisterPluginBuilder(joblifecycle.PluginName, joblifecycle.New)
```

### 5.2 配置

```yaml
tiers:
- plugins:
  - name: priority
  - name: gang
  # ...
  - name: job-lifecycle
    arguments:
      job-lifecycle.jobSelectorLabel: job-name   # 只处理带该标签的 PodGroup，默认 job-name，等价于现在的 isOfflineTask
      job-lifecycle.perJobMetrics: true          # 是否导出按作业打标签的 Gauge，默认 true 保持兼容
      job-lifecycle.reasonMinDuration: 0s        # 原因防抖，默认 0 表示关闭，与现在行为一致，见第 8 节
```

**注意：必须在调度配置里启用这个插件**，否则时间戳和指标会静默缺失。插件放在哪个 tier 都可以，它不参与任何调度决策。

### 5.3 插件入口

```go
const PluginName = "job-lifecycle"

type jobLifecycle struct {
	selectorLabel     string
	perJobMetrics     bool
	reasonMinDuration time.Duration
}

func New(args framework.Arguments) framework.Plugin { /* 解析 5.2 的参数 */ }

func (p *jobLifecycle) Name() string { return PluginName }

func (p *jobLifecycle) OnSessionOpen(ssn *framework.Session) {
	ssn.AddJobUpdateHook(p.Name(), p.onJobUpdate)
}

// OnSessionClose 只做一件事：清理已经不存在的作业的按作业 Gauge，避免指标泄漏
func (p *jobLifecycle) OnSessionClose(ssn *framework.Session) {
	if p.perJobMetrics {
		gcJobMetrics(ssn.Jobs)
	}
}
```

### 5.4 回调主流程

```go
// onJobUpdate 在 JobUpdater 的 worker 中并行调用，只读写传入 job 自己的 PodGroup
func (p *jobLifecycle) onJobUpdate(job *api.JobInfo) {
	pg := job.PodGroup
	if pg == nil || pg.Labels[p.selectorLabel] == "" {
		return
	}
	if pg.Annotations == nil {
		pg.Annotations = map[string]string{}
	}
	now := time.Now()

	setOnce(pg, api.JobFirstScheduledTime, now)
	if !job.TargetScheduleTime.IsZero() {
		setOnce(pg, api.JobTargetScheduleTime, job.TargetScheduleTime.Time)
	}

	switch pg.Status.Phase { // 本轮刚算出的新 phase
	case scheduling.PodGroupRunning:
		p.onRunning(job, now)
	case scheduling.PodGroupCompleted:
		p.onCompleted(job, now)
	default:
		p.onPending(job, now)
	}
}
```

### 5.5 一次性时间戳

```go
func setOnce(pg *api.PodGroup, key string, t time.Time) bool {
	if _, ok := pg.Annotations[key]; ok {
		return false
	}
	pg.Annotations[key] = t.Local().Format(time.RFC3339)
	return true
}

func (p *jobLifecycle) onRunning(job *api.JobInfo, now time.Time) {
	pg := job.PodGroup
	if !setOnce(pg, api.JobRunningTimeStamp, now) {
		return // 已经记录过，之后的 phase 变化不再处理
	}
	finalizeCurrentReason(job, now) // 先把最后一段排队原因的时长折算进累计值
	observeRunningMetrics(job, now) // 再记录延迟直方图和各原因直方图
}

func (p *jobLifecycle) onCompleted(job *api.JobInfo, now time.Time) {
	pg := job.PodGroup
	if !setOnce(pg, api.JobFinishTimeStamp, now) {
		return
	}
	if setOnce(pg, api.JobRunningTimeStamp, now) { // 一个会话内从未经过 Running 直接完成
		finalizeCurrentReason(job, now)
		observeRunningMetrics(job, now)
	}
	if p.perJobMetrics {
		started := parseTime(pg.Annotations[api.JobRunningTimeStamp])
		setJobRunningDuration(job, now.Sub(started))
	}
}
```

`jobScheduledTime` 的语义说明：

- 现在它取"enqueue 或 allocate 第一次看到这个作业"的时间，或者 redis 插件在入队时写入的时间，两者都是作业第一次出现在会话中的那一轮。
- 新实现在回调第一次处理这个作业时写入，也就是作业第一次出现在会话中的那一轮结束时，差别不超过一个会话周期。
- 这样 enqueue、allocate 里设置 `ScheduleStartTimestamp` 的两处代码、redis 插件里的写入都可以删掉，见 9.1。

### 5.6 排队原因时长累计

状态完全保存在注解里，插件不持有任何内存状态（防抖除外，见第 8 节）：

- `lastReason`：当前原因；
- `<原因的开始时间 key>`：当前这一段的开始时间；
- `<原因名>`：该原因此前已累计的时长。

```go
func (p *jobLifecycle) onPending(job *api.JobInfo, now time.Time) {
	pg := job.PodGroup
	if _, ran := pg.Annotations[api.JobRunningTimeStamp]; ran {
		return // 运行过的作业不再统计排队原因，与现在一致
	}

	cur := pg.Annotations[api.JobLastPendingReason]
	next := job.PendingReason

	if next == "" || next == cur {
		// 本轮没有给出新原因，或原因没变：当前段继续累计，只刷新 Gauge
		if cur != "" && p.perJobMetrics {
			setJobPendingReasonDuration(job, cur, accumulated(pg, cur, now))
		}
		return
	}
	if startKey(next) == "" {
		klog.V(4).InfoS("unsupported pending reason, ignored", "job", job.UID, "reason", next)
		return
	}
	if !p.debouncePassed(job, next, now) {
		return
	}

	if cur != "" {
		closeSegment(pg, cur, now) // 累计值 += now - 开始时间；删除 cur 的开始时间
	}
	pg.Annotations[api.JobLastPendingReason] = next
	pg.Annotations[startKey(next)] = now.Local().Format(time.RFC3339)
}

// accumulated 返回某个原因到 now 为止的总时长：已累计值 + 当前段（如果正在进行）
func accumulated(pg *api.PodGroup, reason string, now time.Time) time.Duration {
	total, _ := time.ParseDuration(pg.Annotations[reason])
	if start, ok := pg.Annotations[startKey(reason)]; ok {
		if t, err := time.Parse(time.RFC3339, start); err == nil {
			total += now.Sub(t)
		}
	}
	return total
}

func closeSegment(pg *api.PodGroup, reason string, now time.Time) {
	pg.Annotations[reason] = accumulated(pg, reason, now).String()
	delete(pg.Annotations, startKey(reason))
}

func finalizeCurrentReason(job *api.JobInfo, now time.Time) {
	pg := job.PodGroup
	if cur := pg.Annotations[api.JobLastPendingReason]; cur != "" {
		closeSegment(pg, cur, now)
	}
}
```

状态转换总结：

| 本轮 phase | 条件 | 动作 |
|---|---|---|
| Pending / Inqueue / Unknown / WaitingForMin | 已有 `runningTimestamp` | 不处理 |
| 同上 | `PendingReason` 为空或与 `lastReason` 相同 | 当前段继续，只刷新 Gauge |
| 同上 | `PendingReason` 为新原因 | 结束旧段（累加时长、删开始时间），开始新段 |
| Running | 首次 | 写 `runningTimestamp`，结束最后一段，记录直方图 |
| Completed | 首次 | 写 `finishTimestamp`；如果没有 `runningTimestamp` 一并补上；记录运行时长 |

### 5.7 指标

```go
func observeRunningMetrics(job *api.JobInfo, now time.Time) {
	pg := job.PodGroup
	if start := parseTime(pg.Annotations[api.JobFirstScheduledTime]); !start.IsZero() {
		schedulingDurationHist.WithLabelValues(pg.Labels["framework"]).Observe(now.Sub(start).Minutes())
	}
	sla := strconv.FormatBool(job.ReqSLAMachine)
	for _, reason := range supportedReasons { // 与 5.6 的映射表同源
		if v, ok := pg.Annotations[reason]; ok {
			d, _ := time.ParseDuration(v)
			pendingReasonHist.WithLabelValues(reason, sla).Observe(d.Minutes())
		}
	}
}
```

- 4 个指标的定义从 `metrics/job.go` 移到插件的 `metrics.go`，`Subsystem` 和 `Name` 保持完全一致，Prometheus 里的指标名不变，看板不用改。
- **按作业指标的清理**：插件用一个包级的 `sync.Map` 记录导出过 Gauge 的作业（键为 `ns/name`，值为 queue 和出现过的 reason）。`OnSessionClose` 时，对不在 `ssn.Jobs` 里的作业调用 `DeleteLabelValues`。原因切换时，同时删除旧原因那条序列。

## 6. 并发、一致性与重启

### 6.1 并发

- `JobUpdater.UpdateAll` 用多个 worker 并行调用 `updateJob`，回调也会并行执行。
- 回调只读写传入 job 自己的 `PodGroup`（会话快照里的深拷贝，`JobInfo.Clone` 会 `PodGroup.Clone()`），不同 job 之间没有共享数据。
- Prometheus 指标本身是并发安全的；按作业指标的跟踪表用 `sync.Map`；防抖状态（如果启用）也用 `sync.Map`。

### 6.2 一致性

- 回写走整个 PodGroup 的 `Update`，携带会话快照里的 `resourceVersion`，属于乐观并发。
- 如果 informer 还没把上一轮的写入同步回缓存，下一轮快照里的注解是旧的：
  - 回调会基于旧注解重新计算，例如再次判定"原因发生变化"；
  - 但这次 `Update` 携带的是旧 `resourceVersion`，会被 apiserver 以 409 拒绝，错误数据写不进去；
  - 再下一轮缓存追上后，回调读到正确注解，自动收敛。
- 结论：缓存延迟只会造成偶发的写入冲突，**不会写出错误数据**。这和社区对 status 回写的处理方式一致。

### 6.3 重启和 leader 切换

- 时间戳、当前原因、当前段开始时间、累计时长都在注解里，新进程加载 PodGroup 即可接着算，不丢失、不重复。
- 调度器停机期间，时间计入停机前的那个原因。这是可接受的偏差，现在也是如此。
- 防抖状态和指标跟踪表在内存里，重启后丢失的影响：防抖重新计时；按作业 Gauge 在下一轮会话重新导出。

## 7. 顺带修复的现有问题

| # | 问题 | 现在的表现 | 新实现 |
|---|---|---|---|
| 1 | `PendingReason` 为空时重复计时 | 每轮会话 `JobInfo` 从缓存克隆，`PendingReason` 初始为空。本轮没有任何地方设置原因时（例如作业卡在 enqueue），`LastReason != ""` 且 `PendingReason == ""`：旧段时长被写入累计值，但开始时间没有删除、`lastReason` 也没变，下一轮又从同一个开始时间算起，**同一段时间被累计多次**。jobbackoff 里"把 `LastReason` 回填到 `PendingReason`"就是在绕这个问题 | 空原因视为"原因没变，当前段继续"，不做任何结算。jobbackoff 的回填逻辑可以删除 |
| 2 | 原因直方图漏掉最后一段 | Running 分支先调用 `statisticDelayMetric` 读取累计值，后结算最后一段，**最后一段（通常最长）不计入直方图** | 先 `finalizeCurrentReason`，再记录直方图 |
| 3 | 直接完成的作业运行时长错误 | 一个会话内从 Pending 直接变成 Completed 时，没有 `runningTimestamp`，`RunningAt` 为 0，运行时长等于"当前时间 - 1970 年" | Completed 时补写 `runningTimestamp`，运行时长按 0 左右记录 |
| 4 | 按作业 Gauge 泄漏 | `job_pending_reason_duration_seconds` 和 `job_running_duration` 不在 `DeleteJobMetrics` 的清理范围内，作业删除后序列一直存在；原因切换后旧原因的序列也一直存在 | `OnSessionClose` 清理已消失作业的序列；原因切换时删除旧序列 |

这些修复会让指标数值发生变化（第 1 条会让部分作业的累计时长变小，第 2 条会让直方图变大），上线前需要告知看板使用方。

## 8. 回写量控制（可选）

现在每次原因变化都会触发一次 PodGroup 回写。capacity 会根据当时的队列状态在 `NotEnoughResources` 和 `NotEnoughQuota` 之间改写原因，如果某个作业每轮都在两者之间切换，每个会话都会回写一次。

`reasonMinDuration` 参数提供防抖：

- 新原因第一次出现时，只在内存里记下候选原因和时间，不改注解；
- 候选原因连续保持 `reasonMinDuration` 以上才真正切换，切换时新段的开始时间取候选开始时间，不丢时长；
- 期间如果原因变回 `lastReason`，候选作废。

```go
func (p *jobLifecycle) debouncePassed(job *api.JobInfo, next string, now time.Time) bool {
	if p.reasonMinDuration <= 0 {
		return true
	}
	v, _ := candidates.LoadOrStore(job.UID, candidate{reason: next, since: now})
	c := v.(candidate)
	if c.reason != next {
		candidates.Store(job.UID, candidate{reason: next, since: now})
		return false
	}
	if now.Sub(c.since) < p.reasonMinDuration {
		return false
	}
	candidates.Delete(job.UID)
	return true
}
```

建议先在测试环境统计"仅因注解变化导致的 PodGroup 更新次数"（可以临时加一个计数器：`updatePGAnnotations && !updatePGStatus`），确认确实偏高再开启，取值建议 10s~30s。

## 9. 删除清单与侵入面对比

### 9.1 可以删除的内部代码

| 位置 | 删除内容 |
|---|---|
| `framework/session.go` | `addJobTimeStamp`、`calculateAnnotationWhenChanged`、`updateLastPendingReason`、`calculateDurationForReason`、`GetAnnotationKeyForPendingReason`、`statisticDelayMetric`、`isOfflineTask`、`MarkPodGroupDirty`、`IsPodGroupDirty`、`podGroupDirty` 字段 |
| `framework/job_updater.go` | `addJobTimeStamp` 调用和 `IsPodGroupDirty` 判断（由 F2 取代） |
| `actions/enqueue/enqueue.go:57`、`actions/allocate/allocate.go:202` | 设置 `ScheduleStartTimestamp` 的代码 |
| `api/job_info.go` | `ScheduleStartTimestamp` 字段、`extractJobFirstScheduledTime` 及 Clone 里的对应行（插件直接读注解） |
| `metrics/job.go` | 4 个指标的定义和 `Registry*` 函数（移到插件） |
| `plugins/redis/redis.go` | `JobEnqueuedFn` 里写 `jobScheduledTime` 的部分 |
| `plugins/jobbackoff/jobbackoff.go` | 5 处 `MarkPodGroupDirty`；`applyBackoffBlock` 里把 `LastReason` 回填到 `PendingReason` 的逻辑（第 7 节第 1 条修复后不再需要，需要和 jobbackoff 的负责人确认） |
| `framework/framework.go` 及所有插件 | `OnSessionPreClose`（前提是 capacity 的改写逻辑挪到它自己的 `OnSessionClose`，属于 capacity 迁移范围） |

### 9.2 仍然留在社区文件里的内容

| 位置 | 内容 | 原因 |
|---|---|---|
| `framework/session_plugins.go`、`job_updater.go` | F1 + F2，约 30 行 | 通用扩展点，计划提交社区 |
| `api/job_info.go` | `PendingReason`、`LastReason`、`TargetScheduleTime`、`RunningAt` 字段及解析 | 调度决策信息和延迟调度、notebook 依赖，挪不出去；解析逻辑可以挪到内部新文件 `api/job_info_ext.go` 进一步缩小冲突面 |
| allocate、capacity、gang | 约 10 行设置 `PendingReason` 的代码 | 只有调度器知道"为什么没调度" |

### 9.3 对比

| | 现在 | 改造后 |
|---|---|---|
| `session.go` 内部改动 | 约 200 行业务逻辑 + 脏标记机制 | 0 |
| `job_updater.go` 内部改动 | 业务调用 + 脏标记判断 | 3 行通用改动（计划提交社区） |
| Plugin 接口 | 多一个 `OnSessionPreClose`，所有插件都要实现 | 恢复社区接口 |
| enqueue / allocate | 各有设置首次调度时间的代码 | 0 |
| 注解回写判断 | 每个改注解的插件都要记得调 `MarkPodGroupDirty`，漏调就不回写 | 自动比较，不会漏 |

## 10. 上线、验证与回滚

### 10.1 上线步骤

1. 合入 F1、F2 和 `job-lifecycle` 插件，删除第 9.1 节的旧代码。
2. 调度配置里加上 `- name: job-lifecycle`。**漏配会导致时间戳和指标静默缺失**，建议在配置检查脚本里加一条校验。
3. 告知看板使用方第 7 节带来的数值变化。

### 10.2 验证方法

在测试集群上用新旧两个版本分别跑同一批作业（覆盖：正常调度、配额不足排队、资源不足排队、原因切换、gang 等不齐、直接完成、调度器中途重启），对比：

- 每个 PodGroup 上的时间戳注解：`runningTimestamp`、`finishTimestamp` 应在同一会话写入，误差在会话周期以内；
- 各原因的累计时长：除第 7 节第 1 条修复的场景外应基本一致；
- 4 个指标的序列和数值；
- notebook 作业的排序结果不变；
- PodGroup 的更新次数不增加（时间戳和 status 合并为一次写）。

### 10.3 回滚

注解格式和 key 完全不变，回滚到旧版本调度器后，旧代码能直接读取新版本写下的注解，继续累计，无需数据迁移。

## 11. 测试计划

**单元测试**（插件内，构造 `JobInfo` 直接调用 `onJobUpdate`）：

- 首次处理写入 `jobScheduledTime`，再次处理不覆盖；
- 原因 A → A → B → 空 → B → Running：累计值、开始时间、`lastReason` 每一步都正确，空原因不重复计时；
- Running 时先结算最后一段，再记录直方图（用 `prometheus/testutil` 断言直方图样本）；
- Pending 直接到 Completed：补写 `runningTimestamp`，运行时长合理；
- 已有 `runningTimestamp` 的作业回到 Pending：不再统计原因；
- 不带 `job-name` 标签的 PodGroup：不做任何修改；
- 不支持的原因：忽略且不修改注解；
- 防抖：候选原因未满时长不切换，满时长后切换且开始时间取候选开始时间；
- 指标清理：作业从 `ssn.Jobs` 消失后，对应序列被删除。

**框架测试**（F1、F2）：

- 插件在回调里改了注解，`updatePGAnnotations` 为 true；没改则为 false；
- 只改 HyperNode 注解时行为与社区原逻辑一致；
- 回调按 tier 顺序调用；未启用插件时不调用。

**并发测试**：`go test -race` 下用多个 job 并行跑 `UpdateAll`。

## 12. 工作量与待确认事项

### 12.1 工作量

| 项 | 估计 |
|---|---|
| F1 + F2 及框架单测 | 0.5 天 |
| 插件实现（以搬迁现有逻辑为主） | 1.5 天 |
| 插件单测、并发测试 | 1 天 |
| 删除旧代码、联调 jobbackoff / redis / notebook | 0.5 天 |
| 测试集群新旧对比验证 | 0.5~1 天 |
| **合计** | **约 4 天** |

### 12.2 待确认事项

1. **不带前缀的累计时长注解**（如 `NotEnoughResources: 5m30s`）是否有下游在读？如果没有，建议这次改成带前缀的 key（例如 `scheduling.k8s.io/notEnoughResources.duration`），过渡期双写。
2. **jobbackoff 的 `PendingReason` 回填逻辑**是否只是为了绕开第 7 节第 1 条问题？确认后可以一并删除。
3. **capacity 的原因改写是否随 v1.16 迁移挪到 `OnSessionClose`？** 决定能否去掉 `OnSessionPreClose`。
4. **内部的 `PruneJobs`**（会话开始时被剔除、但仍需回写的作业）如果继续迁移，要确认 `JobUpdater` 会处理它们；回调挂在 `updateJob` 上，会自动覆盖到。
5. **按作业 Gauge 是否还需要？** 如果平台侧可以直接根据注解计算按作业的时长，建议把 `perJobMetrics` 默认改为 false，降低调度器的指标基数。
