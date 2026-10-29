# 节点资源指标迁移为 node-metrics 插件：详细设计

> 适用范围：内部 Volcano 升级到社区 v1.16（apiserver 1.19）。
> 关联文档：`scheduler-decoupling-proposal.md` 3.8 节（本文是对该节"剥离为独立 exporter"建议的修订）。

## 0. 结论

- **不剥离出调度器，改为一个只读的 `node-metrics` 插件。** 三组指标（已用/总量、可回收资源、空闲直方图）全部放进插件。
- **不再额外调用 `sc.Snapshot()`。** 插件在 `OnSessionClose` 中按间隔节流，直接复用本轮会话已有的 `ssn.Nodes` / `ssn.Jobs`。
- **导出方式改为自定义 `prometheus.Collector`。** 会话里只生成一份轻量快照并原子替换，抓取时由 Collector 输出常量指标。这样：
  - 旧序列自动消失，不再需要 `DeletePartialMatch` 和节点事件里的清理逻辑；
  - 直方图从"先删后填"变成原子切换，抓取时不会看到空的或填了一半的数据。
- **指标名、标签名、桶边界保持不变**，现有看板和告警不用改。少数口径修正见第 6 节，其中会影响看板的都有兼容开关。
- 社区文件的改动只剩 `plugins/factory.go` 里的 1 行注册。`cache.go`、`event_handlers.go` 的侵入全部删除，同时去掉对 eaframework 的依赖。
- 工作量约 2.5 人天。

## 1. 现状

### 1.1 代码位置（内部 `origin/gpu-numa`）

| 位置 | 内容 |
|---|---|
| `pkg/scheduler/metrics/node.go`（193 行，内部新增） | 指标定义，以及 `UpdateNodeAllResource` / `UpdateNodeReclaimableMetrics` / `UpdateNodeFreeResourceHist` / `DeleteNode*` |
| `pkg/scheduler/cache/cache.go:909-910` | `go wait.Until(sc.handleUpdateNodeMetrics, 45*time.Second)`、`go wait.Until(sc.updateNodeFreeResourceMetrics, 5*time.Minute)` |
| `cache.go:2022` `handleUpdateNodeMetrics` | `sc.Snapshot()` 之后遍历节点，调用 `UpdateNodeInfoMetrics` |
| `cache.go:2034` `updateNodeFreeResourceMetrics` | 先 `DeleteNodeFreeResourceHist()`，再 `sc.Snapshot()`，逐个节点 Observe |
| `cache.go` `UpdateNodeInfoMetrics` | 只处理带 `easy-algo` 标签的节点：计算已用/总量和可回收资源 |
| `event_handlers.go:557` | 节点删除时调用 `DeleteNodeResourceMetrics` |
| `event_handlers.go:1504-1505` `deleteOldMetrics` | 节点的 `easy-algo` / `scheduling-type` / `unschedulable` 变化时，删掉旧序列并立即重写 |
| `metrics.go:372` `IsSkipScalarResource` | 跳过 `jds-vcore` / `jds-vmem` / `vgpu-memory` |

### 1.2 指标清单

指标名的前缀都是 `volcano_`（`Subsystem: "volcano"`）。

| 指标 | 类型 | 标签 | 取值 |
|---|---|---|---|
| `node_CPU_usage` / `node_CPU_total` | Gauge | `node_name, easy_algo, unschedule, scheduling_type` | `ni.Used.MilliCPU` / `ni.Allocatable.MilliCPU` |
| `node_memory_usage` / `node_memory_total` | Gauge | 同上 | `ni.Used.Memory` / `ni.Allocatable.Memory`（字节） |
| `node_GPU_usage` / `node_GPU_total` | Gauge | 同上 + `gpu_type` | 遍历 `Allocatable.ScalarResources`，经 `ScalarResourceTypesConvert` 能识别出型号的才导出 |
| `node_reclaimable_pods` | Gauge | `node_name, easy_algo` | 节点上满足可回收条件的 task 数 |
| `node_reclaimable_milli_cpu` / `node_reclaimable_memory_bytes` | Gauge | `node_name, easy_algo, unschedule, scheduling_type` | 这些 task 的 `Resreq` 之和 |
| `node_reclaimable_gpu` | Gauge | 同上 + `gpu_type` | 这些 task 的 `Resreq.ScalarResources`，标签值是**原始资源名** |
| `node_free_cpu_histogram` | Histogram | `node_type`（cpu/gpu） | `Idle.MilliCPU/1000`，桶 8…640 核 |
| `node_free_memory_histogram` | Histogram | `node_type` | `Idle.Memory/GiB`，桶 32…4096 |
| `node_free_average_histogram` | Histogram | `node_type` | `(Idle.CPU/Alloc.CPU + Idle.Mem/Alloc.Mem)/2`，桶 0.1…1.0 |

可回收条件为：`job.ReclaimAble || (job.Elasticable && task.Elastic)`。这几个字段由 `JobInfo.SetPodGroup` 中的 `setDefaultPreemptProperty()` 根据 `JobProperty` 推导，属于调度器内部语义。

## 2. 现存问题

| # | 问题 | 影响 |
|---|---|---|
| P1 | 每 45 秒和每 5 分钟各做一次额外的全量 `Snapshot()`（持有 `sc.Mutex` 深拷贝所有节点和作业） | 和会话快照、事件处理抢锁；两份深拷贝同时存在时内存峰值抬高 |
| P2 | 两个 goroutine 在 `WaitForHandlerSync` **之前**就启动 | 启动或切主后的前几轮可能导出不完整的数据 |
| P3 | 直方图"先删后填"不是原子操作 | 如果抓取正好落在删除和填充之间，会看到空的或只有部分节点的分布 |
| P4 | 节点不在快照里时，指标停在最后一次的值 | 内部 `setNodeState` 会把 NodeReady≠True 的节点设为 NotReady，`Snapshot()` 跳过它们，但旧序列不会删除，NotReady 节点一直显示断电前的数值 |
| P5 | 可回收统计遍历 `ni.Tasks` 中所有状态的 task | Pipelined 状态（不计入 Used）和 Releasing 状态（已经在被驱逐）也被算作可回收，可回收量可能大于已用 |
| P6 | `node_reclaimable_gpu` 的 `gpu_type` 用原始资源名，且不过滤非 GPU 标量 | 和 `node_GPU_usage` 的 `gpu_type`（型号短名，如 `a100`）对不上，无法直接做比值 |
| P7 | `nvidia.com/gpu` 不在 `scalarResourceTypesConvert` 映射里 | jdos 集群节点不导出 GPU 已用/总量 |
| P8 | 依赖 eaframework 的 `ScalarResourceTypesConvert`，以及 NodeInfo 上内部新增的 `GPUTypeMark` 字段 | 和 GPU 映射配置化（提案 3.2）的改造冲突 |
| P9 | 侵入 `cache.go`、`event_handlers.go` | 每次 rebase 都要处理冲突 |
| P10 | 没有"指标最后刷新时间" | 调度器卡住时，指标静默变旧，没有办法告警 |

## 3. 目标与非目标

**目标**

1. 指标名、标签名、桶边界和数值口径与现状兼容，看板不用改。
2. 零额外快照：复用会话数据，会话内的耗时控制在毫秒级。
3. 解决 P2-P10。
4. 侵入社区代码 ≤ 1 行（插件注册）。

**非目标**

- 不新增标签，不提高基数。
- 不做实际资源使用率（真实利用率由 node-exporter 或 usage 插件负责），这里只统计调度器视角的 request 口径。
- 不处理 vGPU 和共享 GPU 的设备级指标（社区 `vgpu/metrics.go` 已有）。

## 4. 总体设计

```
                   每个调度周期（默认 1s）
OpenSession ──► actions ──► CloseSession
                               │
                               ▼
                 node-metrics.OnSessionClose
                 ├─ 距上次 usage 刷新 ≥ 45s？── 是 ─► buildUsage(ssn)  ─► usageSnap.Store(ptr)
                 └─ 距上次 hist 刷新 ≥ 5m？──── 是 ─► buildHist(ssn)   ─► histSnap.Store(ptr)
                                                   (只读 ssn.Nodes / ssn.Jobs，输出不可变的小结构体)

Prometheus 抓取 /metrics ──► nodeCollector.Collect()
                             ├─ usageSnap.Load()  → MustNewConstMetric × N
                             ├─ histSnap.Load()   → MustNewConstHistogram × 6
                             └─ last_refresh_timestamp
```

关键决策：

- **复用会话数据，不调用 `Snapshot()`。** `ssn.Nodes` / `ssn.Jobs` 本身就是会话开始时的深拷贝，归会话独占，插件只读，不用加锁。
- **用自定义 Collector + 常量指标，而不是 GaugeVec。**
  - 每次刷新生成一份完整、不可变的快照，用 `atomic.Pointer` 整体替换；
  - 抓取时只输出当前快照里有的序列：节点删除、标签变化、NotReady 节点不在快照里时，旧序列自然消失；
  - 直方图用 `MustNewConstHistogram`，和快照一起原子切换；原来每 5 分钟重置一次再重新 Observe，本来就是"某一时刻的分布"，换成常量直方图语义完全一致。
- **会话内只遍历、拷贝数字**，Prometheus 对象的构造放在抓取的 goroutine 里，不占调度周期。

## 5. 详细设计

### 5.1 目录结构

```
pkg/scheduler/plugins/nodemetrics/
├── nodemetrics.go       // 插件入口、参数、节流
├── build.go             // 从会话构建 usage / hist 快照
├── collector.go         // prometheus.Collector 实现
├── gpu.go               // GPU 型号识别（对接 gpumodel 包）
├── build_test.go
└── collector_test.go
```

注册（`plugins/factory.go`，唯一的社区文件改动）：

```go
framework.RegisterPluginBuilder(nodemetrics.PluginName, nodemetrics.New)
```

### 5.2 插件参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `node-metrics.usageInterval` | `45s` | 已用/总量和可回收资源的刷新间隔 |
| `node-metrics.freeHistInterval` | `5m` | 空闲直方图的刷新间隔 |
| `node-metrics.nodeLabelKey` | `easy-algo` | 只有带这个标签的节点才导出已用/总量和可回收资源，标签值写入 `easy_algo` |
| `node-metrics.scheduleTypeLabelKey` | `scheduling-type` | 取值写入 `scheduling_type` |
| `node-metrics.histRequireNodeLabel` | `false` | 直方图是否也只统计带 `nodeLabelKey` 的节点（现状是统计所有节点） |
| `node-metrics.legacyReclaimableGPULabel` | `true` | `node_reclaimable_gpu` 的 `gpu_type` 是否沿用原始资源名（见 6.2） |

配置示例（放在任意 tier 即可，插件不注册任何 Fn，不影响调度决策）：

```yaml
tiers:
- plugins:
  - name: priority
  - name: gang
  # ...
- plugins:
  - name: capacity
  # ...
  - name: node-metrics
    arguments:
      node-metrics.usageInterval: 45s
      node-metrics.freeHistInterval: 5m
```

### 5.3 数据结构

```go
// 每个节点一条，已经算好最终数值和标签值；构建完成后只读。
type nodeSample struct {
	name, easyAlgo, unschedule, schedType string

	usedCPU, usedMem, totalCPU, totalMem float64
	gpu []gpuValue // 按 gpu_type 排序，只含 GPU 资源

	reclaimPods int
	reclaimCPU  float64
	reclaimMem  float64
	reclaimGPU  []gpuValue
}

type gpuValue struct {
	gpuType     string
	used, total float64 // reclaimGPU 只用 used
}

type usageSnapshot struct {
	at    time.Time
	nodes []nodeSample
}

// 一个直方图的一个 node_type；buckets 为累积计数，与 ConstHistogram 要求一致。
type histData struct {
	count   uint64
	sum     float64
	buckets map[float64]uint64
}

type histSnapshot struct {
	at  time.Time
	cpu map[string]*histData // key: node_type
	mem map[string]*histData
	avg map[string]*histData
}
```

插件实例**每个会话都会重新 `New`**，所以 Collector、快照指针和节流时间戳都必须放在包级别，注册只做一次：

```go
var (
	registerOnce sync.Once
	usageSnap    atomic.Pointer[usageSnapshot]
	histSnap     atomic.Pointer[histSnapshot]
	lastUsage    atomic.Int64 // unix nano
	lastHist     atomic.Int64
)
```

### 5.4 插件入口与节流

```go
const PluginName = "node-metrics"

type nodeMetricsPlugin struct {
	args pluginArgs
}

func New(arguments framework.Arguments) framework.Plugin {
	registerOnce.Do(func() { prometheus.MustRegister(&nodeCollector{}) })
	return &nodeMetricsPlugin{args: parseArgs(arguments)}
}

func (p *nodeMetricsPlugin) Name() string                       { return PluginName }
func (p *nodeMetricsPlugin) OnSessionOpen(_ *framework.Session) {}

func (p *nodeMetricsPlugin) OnSessionClose(ssn *framework.Session) {
	now := time.Now()
	if due(&lastUsage, now, p.args.usageInterval) {
		usageSnap.Store(buildUsage(ssn, &p.args, now))
	}
	if due(&lastHist, now, p.args.freeHistInterval) {
		histSnap.Store(buildHist(ssn, &p.args, now))
	}
}

func due(last *atomic.Int64, now time.Time, interval time.Duration) bool {
	prev := last.Load()
	if prev != 0 && now.Sub(time.Unix(0, prev)) < interval {
		return false
	}
	last.Store(now.UnixNano())
	return true
}
```

`parseArgs` 对持续时间做 `time.ParseDuration`，解析失败或 ≤ 0 时回落到默认值并打 warning。

### 5.5 构建 usage 快照

```go
func buildUsage(ssn *framework.Session, a *pluginArgs, now time.Time) *usageSnapshot {
	s := &usageSnapshot{at: now, nodes: make([]nodeSample, 0, len(ssn.Nodes))}
	for _, ni := range ssn.Nodes {
		if ni.Node == nil {
			continue
		}
		easyAlgo, ok := ni.Node.Labels[a.nodeLabelKey]
		if !ok {
			continue
		}
		n := nodeSample{
			name:       ni.Name,
			easyAlgo:   easyAlgo,
			unschedule: strconv.FormatBool(ni.Node.Spec.Unschedulable),
			schedType:  ni.Node.Labels[a.scheduleTypeLabelKey],
			usedCPU:    ni.Used.MilliCPU,
			usedMem:    ni.Used.Memory,
			totalCPU:   ni.Allocatable.MilliCPU,
			totalMem:   ni.Allocatable.Memory,
			gpu:        gpuUsage(ni),
		}

		reclaim := api.EmptyResource()
		for _, task := range ni.Tasks {
			if !api.AllocatedStatus(task.Status) {
				continue
			}
			job, found := ssn.Jobs[task.Job]
			if !found || !reclaimable(job, task) {
				continue
			}
			n.reclaimPods++
			reclaim.Add(task.Resreq)
		}
		n.reclaimCPU, n.reclaimMem = reclaim.MilliCPU, reclaim.Memory
		n.reclaimGPU = gpuReclaim(reclaim, a.legacyReclaimableGPULabel)

		s.nodes = append(s.nodes, n)
	}
	return s
}

func reclaimable(job *api.JobInfo, task *api.TaskInfo) bool {
	return job.ReclaimAble || (job.Elasticable && task.Elastic)
}
```

说明：

- `reclaimable` 依赖内部 `JobInfo.ReclaimAble/Elasticable` 和 `TaskInfo.Elastic`，这些字段跟着抢占语义一起保留在 fork 里。如果以后按提案 3.1 改成显式注解，只需要替换这一个函数。
- 内部 `openSession` 会把作业从 `ssn.Jobs` 移到 `ssn.PruneJobs`，但只移"没有任何 task"的作业，这类作业在节点上没有 Pod，不影响统计。v1.16 社区版的 `openSession` 不删除作业。

### 5.6 构建直方图快照

```go
var (
	cpuBuckets = []float64{8, 16, 32, 64, 96, 128, 192, 256, 320, 416, 512, 640}
	memBuckets = []float64{32, 64, 128, 256, 384, 512, 640, 768, 896, 1024, 2048, 4096}
	avgBuckets = []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}
)

func buildHist(ssn *framework.Session, a *pluginArgs, now time.Time) *histSnapshot {
	h := newHistSnapshot(now) // 预先建好 cpu/gpu 两个 node_type，没有节点时也输出全 0
	for _, ni := range ssn.Nodes {
		if ni.Node == nil {
			continue
		}
		if a.histRequireNodeLabel {
			if _, ok := ni.Node.Labels[a.nodeLabelKey]; !ok {
				continue
			}
		}
		var cpuRatio, memRatio float64
		if ni.Allocatable.MilliCPU > 0 {
			cpuRatio = ni.Idle.MilliCPU / ni.Allocatable.MilliCPU
		}
		if ni.Allocatable.Memory > 0 {
			memRatio = ni.Idle.Memory / ni.Allocatable.Memory
		}
		nodeType := "cpu"
		if isGPUNode(ni) {
			nodeType = "gpu"
		}
		h.cpu[nodeType].observe(ni.Idle.MilliCPU/1000, cpuBuckets)
		h.mem[nodeType].observe(ni.Idle.Memory/(1<<30), memBuckets)
		h.avg[nodeType].observe((cpuRatio+memRatio)/2, avgBuckets)
	}
	return h
}

func (d *histData) observe(v float64, bounds []float64) {
	d.count++
	d.sum += v
	for _, ub := range bounds {
		if v <= ub {
			d.buckets[ub]++
		}
	}
}
```

`newHistSnapshot` 把三种直方图、两种 `node_type`、所有桶都初始化为 0。原来每轮先删再填，某个 `node_type` 没有节点时就没有这条序列；现在改为输出全 0 序列，PromQL 查询结果更稳定。

### 5.7 Collector

```go
var (
	baseLabels = []string{"node_name", "easy_algo", "unschedule", "scheduling_type"}
	gpuLabels  = append(append([]string{}, baseLabels...), "gpu_type")

	descCPUUsage   = newDesc("node_CPU_usage", "Node cpu usage with mill core", baseLabels)
	descMemUsage   = newDesc("node_memory_usage", "Node memory usage with bytes", baseLabels)
	descGPUUsage   = newDesc("node_GPU_usage", "Node gpu usage with mill core", gpuLabels)
	descCPUTotal   = newDesc("node_CPU_total", "Node total cpu with mill core", baseLabels)
	descMemTotal   = newDesc("node_memory_total", "Node total memory with bytes", baseLabels)
	descGPUTotal   = newDesc("node_GPU_total", "Node total gpu with mill core", gpuLabels)
	descRecPods    = newDesc("node_reclaimable_pods", "Node reclaimable pods number", []string{"node_name", "easy_algo"})
	descRecCPU     = newDesc("node_reclaimable_milli_cpu", "Node reclaimable cpu in million values", baseLabels)
	descRecMem     = newDesc("node_reclaimable_memory_bytes", "Node reclaimable memory in bytes", baseLabels)
	descRecGPU     = newDesc("node_reclaimable_gpu", "Node reclaimable gpu mill core with gpu type", gpuLabels)
	descFreeCPU    = newDesc("node_free_cpu_histogram", "The node free cpu histogram in core", []string{"node_type"})
	descFreeMem    = newDesc("node_free_memory_histogram", "The node free memory histogram in GiB", []string{"node_type"})
	descFreeAvg    = newDesc("node_free_average_histogram", "The node free avarage resource histogram in GiB", []string{"node_type"})
	descLastUpdate = newDesc("node_metrics_last_refresh_timestamp_seconds", "Unix time of the last node metrics refresh", []string{"kind"})
)

func newDesc(name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName("", metrics.VolcanoSubSystemName, name), help, labels, nil)
}

type nodeCollector struct{}

func (c *nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descCPUUsage, descMemUsage, descGPUUsage, descCPUTotal, descMemTotal,
		descGPUTotal, descRecPods, descRecCPU, descRecMem, descRecGPU, descFreeCPU, descFreeMem, descFreeAvg, descLastUpdate} {
		ch <- d
	}
}

func (c *nodeCollector) Collect(ch chan<- prometheus.Metric) {
	if s := usageSnap.Load(); s != nil {
		for i := range s.nodes {
			n := &s.nodes[i]
			lv := []string{n.name, n.easyAlgo, n.unschedule, n.schedType}
			gauge(ch, descCPUUsage, n.usedCPU, lv...)
			gauge(ch, descMemUsage, n.usedMem, lv...)
			gauge(ch, descCPUTotal, n.totalCPU, lv...)
			gauge(ch, descMemTotal, n.totalMem, lv...)
			for _, g := range n.gpu {
				gauge(ch, descGPUUsage, g.used, append(lv, g.gpuType)...)
				gauge(ch, descGPUTotal, g.total, append(lv, g.gpuType)...)
			}
			gauge(ch, descRecPods, float64(n.reclaimPods), n.name, n.easyAlgo)
			gauge(ch, descRecCPU, n.reclaimCPU, lv...)
			gauge(ch, descRecMem, n.reclaimMem, lv...)
			for _, g := range n.reclaimGPU {
				gauge(ch, descRecGPU, g.used, append(lv, g.gpuType)...)
			}
		}
		gauge(ch, descLastUpdate, float64(s.at.Unix()), "usage")
	}
	if h := histSnap.Load(); h != nil {
		for _, nt := range []string{"cpu", "gpu"} {
			hist(ch, descFreeCPU, h.cpu[nt], nt)
			hist(ch, descFreeMem, h.mem[nt], nt)
			hist(ch, descFreeAvg, h.avg[nt], nt)
		}
		gauge(ch, descLastUpdate, float64(h.at.Unix()), "free_hist")
	}
}

func gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, lv ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, lv...)
}

func hist(ch chan<- prometheus.Metric, d *prometheus.Desc, h *histData, lv ...string) {
	ch <- prometheus.MustNewConstHistogram(d, h.count, h.sum, h.buckets, lv...)
}
```

注意：

- `append(lv, g.gpuType)` 会复用 `lv` 的底层数组。`MustNewConstMetric` 会立即拷贝标签值，所以是安全的；但实现时建议显式 `make` 一个新切片，免得以后被人误改出问题。
- scheduler 的 `/metrics` 使用 `prometheus.DefaultGatherer`（`pkg/util/util.go` 里的 `PromHandler`），`prometheus.MustRegister` 注册的 Collector 会被正常抓取。
- **必须在同一次改动里删除 `metrics/node.go` 中的 GaugeVec 定义**，否则同名指标重复注册，`MustRegister` 会 panic。

### 5.8 GPU 型号识别

依赖提案 3.2（GPU 映射配置化）提供的 `gpumodel` 包。本插件只需要两个函数：

```go
// ModelOf 资源名 → 型号短名（如 "nvidia.com/A100" → "a100"）；非 GPU 资源返回 false。
func ModelOf(res v1.ResourceName) (string, bool)
// IsGPUNode 节点 Allocatable 中存在任一数量 > 0 的 GPU 资源，或带有 GPU 型号标签。
func IsGPUNode(ni *api.NodeInfo) bool
```

`gpu.go`：

```go
func gpuUsage(ni *api.NodeInfo) []gpuValue {
	var out []gpuValue
	for res, total := range ni.Allocatable.ScalarResources {
		if skipScalar(res) {
			continue
		}
		model, ok := gpumodel.ModelOf(res)
		if !ok {
			continue
		}
		out = append(out, gpuValue{gpuType: model, used: ni.Used.ScalarResources[res], total: total})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].gpuType < out[j].gpuType })
	return out
}

func gpuReclaim(r *api.Resource, legacy bool) []gpuValue {
	var out []gpuValue
	for res, v := range r.ScalarResources {
		if legacy {
			out = append(out, gpuValue{gpuType: string(res), used: v})
			continue
		}
		if skipScalar(res) {
			continue
		}
		if model, ok := gpumodel.ModelOf(res); ok {
			out = append(out, gpuValue{gpuType: model, used: v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].gpuType < out[j].gpuType })
	return out
}
```

`skipScalar` 用来替代 `metrics.IsSkipScalarResource`，跳过列表来自 `gpumodel` 的配置（`jds-vcore` / `jds-vmem` / `vgpu-memory`），不再引用 eaframework。

- **jdos 集群**：如果 3.2 已经在 `NewNodeInfo` / `setNode` 边界把 `nvidia.com/gpu` 加 `jdos.jd.com/gpu.model` 标签归一化为型号资源名，这里自然能识别；如果还没有归一化，`ModelOf("nvidia.com/gpu")` 需要支持结合节点标签判断，此时 `gpuUsage` 改为传入 `ni.Node.Labels`。
- **依赖顺序**：如果 3.2 晚于本插件落地，先在 `gpu.go` 里内置一张和 eaframework `scalarResourceTypesConvert` 等价的映射表作为过渡，3.2 完成后替换。

## 6. 语义对照

### 6.1 不变的部分

- 13 个指标的名字、Help、标签名，以及三组直方图的桶边界。
- 已用/总量取 `NodeInfo.Used` / `Allocatable`，和原来相同。
- "只有带 `easy-algo` 标签的节点才导出已用/总量和可回收资源"，以及"直方图统计所有节点"。

### 6.2 有变化的部分

| 项 | 原行为 | 新行为 | 影响与开关 |
|---|---|---|---|
| 数据时间点 | 独立快照（时间点随机） | 会话结束时的状态，包含本轮刚分配的 Pod（Allocated 计入 Used） | 已用比原来略早反映新分配；45 秒粒度下可以忽略 |
| 节点状态变化 | 事件触发即时重写 | 最多延迟一个 `usageInterval` | 可以接受 |
| 不在快照中的节点 | 序列停在旧值（P4） | 序列消失 | 见下文"NotReady 节点"的说明 |
| 可回收 task 范围 | `ni.Tasks` 全部状态 | 只算 `AllocatedStatus`（Allocated/Binding/Bound/Running） | 修正 P5；可回收量 ≤ 已用 |
| `node_reclaimable_gpu.gpu_type` | 原始资源名，包含所有标量 | 默认不变（`legacyReclaimableGPULabel: true`）；设为 `false` 时改用型号短名，并只保留 GPU 资源 | 看板改完后再关掉兼容开关 |
| jdos 集群 GPU 已用/总量 | 不导出（P7） | 依赖 3.2 后正常导出 | 新增序列，不影响已有序列 |
| 直方图中没有节点的 `node_type` | 无序列 | 输出全 0 序列 | 查询更稳定 |
| 启动和切主 | 可能导出未同步完的数据（P2） | 第一次会话后才有数据（会话在 cache 同步后才开始） | 修正 |
| 新增指标 | 无 | `volcano_node_metrics_last_refresh_timestamp_seconds{kind}` | 用来告警（P10） |

**NotReady 节点。** 节点是否出现在 `ssn.Nodes` 里，取决于 `setNodeState`：

- **社区 v1.16**：只有 Node 对象为空（UnInitialized）的节点会被排除，NotReady 节点仍在快照里，会照常导出真实数值；
- **内部 fork**：NodeReady≠True 的节点会被设为 NotReady 并排除，迁移到插件后这些节点的序列会消失，而不是停在旧值。

两种行为都比现状正确。升级时内部这个补丁是否保留，在提案的补丁清单里统一决定，本插件不需要额外处理。

## 7. 性能评估

以 5000 个节点、10 万个 Pod 估算：

| 环节 | 开销 |
|---|---|
| `buildUsage`（每 45 秒一次，在会话里） | 遍历 5000 个节点和 10 万个 task，加上少量 map 查找，约 5-15 ms；产出约 5000 × 300 B ≈ 1.5 MB |
| `buildHist`（每 5 分钟一次） | 遍历 5000 个节点，< 2 ms |
| 其余会话 | 一次时间比较，可以忽略 |
| 抓取 `Collect` | 约 5000 × 10 + GPU 节点 × 3 ≈ 5.5 万条常量指标。和原来 GaugeVec 在 Gather 时构造 dto 的开销相当，而且在抓取 goroutine 里执行，不持有调度锁 |
| 省掉的开销 | 每 45 秒和每 5 分钟各一次全量 `Snapshot()`（深拷贝全部节点、作业、队列，还要持有 `sc.Mutex`） |

插件耗时会被框架自动计入 `volcano_plugin_scheduling_latency_milliseconds{plugin="node-metrics",OnSession="OnSessionClose"}`，上线后可以直接观察。

如果大集群下 `buildUsage` 超过 20 ms，可以进一步优化：在会话里只拷贝 `(name, labels, Used, Allocatable, 可回收 task 的 Resreq 列表)`，汇总计算放到后台 goroutine。初版不做。

## 8. 并发与一致性

- **读取会话数据**：`OnSessionClose` 在调度主 goroutine 中执行，`ssn.Nodes` / `ssn.Jobs` 由会话独占。在 `framework.CloseSession` 里，插件的 `OnSessionClose` 先于 `closeSession`（JobUpdater）执行，读取时这些数据还没有被清理。
- **快照发布**：`atomic.Pointer.Store` 整体替换，快照构建完成后不再修改。Collect 拿到的永远是一份完整快照，不需要锁。
- **多副本**：v1.16 只在拿到 leader 后才启动调度（`server.go` 里的 `OnStartedLeading: run`），follower 不开会话，所以没有快照，也不输出这组指标，不存在重复上报。丢失 leader 身份时进程直接退出。
- **切主**：新 leader 的第一个会话会立即刷新（节流时间戳为 0），断档时间等于选主时间加 cache 同步时间。
- **会话被跳过或调度器卡住**：快照停在最后一次，`last_refresh_timestamp` 也随之停止，可以用下面的规则告警：

```promql
time() - volcano_node_metrics_last_refresh_timestamp_seconds{kind="usage"} > 180
```

- **插件被移出配置**：快照不再更新，但 Collector 已经注册，会一直输出最后一份快照。处理方式：在 `OnSessionClose` 里记录 `lastSeen`，`Collect` 发现距离 `lastSeen` 超过 `3 × usageInterval` 就不再输出节点序列，只保留 `last_refresh_timestamp`。

## 9. 改动清单（内部 fork）

| 文件 | 改动 |
|---|---|
| `pkg/scheduler/plugins/nodemetrics/*` | 新增，约 400 行（含测试约 700 行） |
| `pkg/scheduler/plugins/factory.go` | +1 行注册 |
| `pkg/scheduler/metrics/node.go` | **删除整个文件** |
| `pkg/scheduler/metrics/metrics.go` | 删除 `IsSkipScalarResource`（确认没有其他引用后） |
| `pkg/scheduler/cache/cache.go` | 删除第 909-910 行的两个 goroutine，以及 `handleUpdateNodeMetrics`、`updateNodeFreeResourceMetrics`、`UpdateNodeInfoMetrics` |
| `pkg/scheduler/cache/event_handlers.go` | 删除 `deleteOldMetrics` 及其调用（1504-1505 行），删除 `DeleteNodeResourceMetrics` 调用（557 行） |
| 调度器配置 | 在 tier 中加入 `node-metrics` |

## 10. 测试方案

**单元测试（`build_test.go`）**

- 节点过滤：没有 `easy-algo` 标签的节点不出现在 usage 里，但仍计入直方图；`histRequireNodeLabel=true` 时直方图也排除这类节点。
- 已用/总量：构造 NodeInfo，断言 CPU、内存和 GPU（多型号、跳过 vcore/vmem）的数值。
- 可回收：
  - 覆盖 `ReclaimAble` / `Elasticable + Elastic` / `Elasticable + 非 Elastic` / 两者都不满足四种组合；
  - Pipelined、Releasing 状态的 task 不计入；
  - 节点上的 Pod 所属作业不在 `ssn.Jobs` 时跳过。
- GPU 标签：`legacyReclaimableGPULabel` 为 true / false 时的 `gpu_type` 取值。
- 直方图：边界值（恰好等于桶上界）计入该桶；累积计数单调；`Allocatable` 为 0 时比例为 0；没有 GPU 节点时 `gpu` 序列为全 0。
- 节流：间隔内连续调用只刷新一次；时间戳为 0 时立即刷新。

**Collector 测试（`collector_test.go`）**

- 用 `testutil.CollectAndCompare` 对比期望的文本格式输出，校验指标名、标签和数值与旧实现逐项一致（这是兼容性的核心保障）。
- 快照替换后，旧节点的序列消失。
- 新注册表上 `Describe` 和 `Collect` 没有重复或不一致（`testutil.CollectAndLint`）。

**兼容性对比（离线）**

- 抓取同一个集群旧版本的 `/metrics`，导出节点和作业信息，构造等价的会话，跑新实现。按 `(指标, 标签)` 做 diff，只允许出现 6.2 列出的差异。

## 11. 上线、验证与回滚

1. **预发**：先用 `legacyReclaimableGPULabel: true` 上线，对比切换前后 24 小时的看板曲线。已用/总量应当基本重合；可回收量允许因为 P5 的修正而略降。
2. **观察**：
   - `plugin_scheduling_latency{plugin="node-metrics"}` 的 p99 < 20 ms；
   - `last_refresh_timestamp` 正常推进；
   - 调度器内存峰值下降（省掉两次额外快照）。
3. **生产灰度**：先上小集群，再上大集群。
4. **看板迁移**（可选）：把 `node_reclaimable_gpu` 的查询改成型号短名，再把开关改为 `false`。
5. **回滚**：本插件没有写操作，回滚就是换回旧镜像。只从配置里删掉插件会导致指标消失（旧代码已经删除），因此不作为回滚手段。

## 12. 工作量

| 项 | 人天 |
|---|---|
| 插件、Collector、GPU 识别 | 1 |
| 单元测试、Collector 对比测试 | 0.5 |
| 删除旧代码，联调 | 0.5 |
| 预发对比验证 | 0.5 |
| **合计** | **约 2.5** |

## 13. 待确认问题

1. **可回收 task 范围**：同意只统计 `AllocatedStatus` 吗？还是需要和现状完全一致（加开关）？
2. **`node_reclaimable_gpu` 标签**：看板是否依赖原始资源名？什么时候可以关掉 `legacyReclaimableGPULabel`？
3. **jdos 集群**：GPU 已用/总量是"本来就不需要"，还是"漏掉了"？前者可以在插件里按集群关闭 GPU 指标。
4. **NotReady 节点**：升级后是否保留内部 `setNodeState` 的 NodeReady 判断？这决定 NotReady 节点是消失还是照常导出（见 6.2）。
5. **刷新间隔**：直方图是否可以和 usage 统一为 45 秒？插件里的开销很小，统一后看板时间对齐更方便。
6. **3.2 的落地顺序**：如果 GPU 映射配置化晚于本插件，`gpu.go` 先内置过渡映射表（见 5.8）。
