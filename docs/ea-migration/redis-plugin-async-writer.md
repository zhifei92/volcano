# Redis 排队序号上报插件：保留与异步化改造方案

> 基于内部分支 `origin/gpu-numa` 的 `pkg/scheduler/plugins/redis/` 分析，目标版本为社区 v1.16。
> 本文修订 [scheduler-decoupling-proposal.md](./scheduler-decoupling-proposal.md) 3.4 节的结论：从"P0 剥离"调整为"**保留插件，P0 整改**"。

## 1. 结论

**不一定要拆。** 这个功能已经是独立插件（3 个文件约 750 行），对社区代码的侵入只有 `plugins/factory.go` 里的 1 行注册，加上 `go.mod` 里的 `github.com/go-redis/redis/v8` 依赖，rebase 成本很低。

原文档要求剥离，理由其实是三个实现缺陷，都可以在插件内部修掉。而排队序号依赖调度器内部的排序规则，拆出去反而算不准。

需要做的整改（约 1~2 人天，改动全部在 `plugins/redis/` 目录内）：

1. 去掉 `init()`，改成懒加载，并支持断线重连；
2. 会话里只计算排队快照，写 Redis 交给后台 writer 异步执行；
3. 把与上报无关的 `JobFirstScheduledTime` 注解写入挪出插件；
4. 顺带修复 key 丢失 namespace 等隐患。

## 2. 现状

### 2.1 插件做了什么

- **`OnSessionOpen`**：
  - 收集状态为 Pending 或 Inqueue、且带 `job-name` 标签的 PodGroup，按队列放进用 `ssn.JobOrderFn` 排序的优先队列；
  - 注册 `JobEnqueuedFn`，把本轮新入队的作业也加进去；
  - 同时给 PodGroup 写 `JobFirstScheduledTime`（`scheduling.k8s.io/jobScheduledTime`）注解。
- **`OnSessionClose`**：
  - 剔除已分配数达到 minMember 的作业，按调度器的排序算出每个队列的排队序号；
  - 和上一轮结果（包级变量 `cache`）比较，有变化就**同步**执行 3 次 Redis pipeline：
    - 排队中的作业：`HSET <job> pending_index <序号>`；
    - 离开排队、但作业还在（已调度）：`HSET <job> pending_index -1`；
    - 离开排队、且作业已不存在：`HDEL <job> pending_index`。

最关键的一点是：**排队序号用的是 `ssn.JobOrderFn`**，也就是 priority、drf、gang 等插件叠加之后的真实调度顺序。这个顺序只有调度器自己知道，外部服务没法准确复现。

### 2.2 存在的问题

| 问题 | 现象 | 能否在插件内修 |
|---|---|---|
| 同步 I/O 在会话里 | `OnSessionClose` 同步执行 3 次 pipeline，每次带重试和秒级超时，Redis 一抖动就拖慢会话收尾 | 能，见第 4 节 |
| `init()` 有副作用 | 只要 import `plugins` 包就会 ping Redis，单测和其它二进制每次最多等 3 秒 | 能，见 4.7 |
| 没有重连 | `init()` 只执行一次，调度器启动时 Redis 恰好不可用，client 就一直是 nil，功能静默失效，直到重启调度器 | 能，见 4.7 |
| Redis 被清空后不恢复 | 只在顺序变化时才写，Redis 重启或数据被清空后，要等顺序再次变化才会恢复 | 能，见 4.5 的定期全量对账 |
| 职责混杂 | 在 `JobEnqueuedFn` 里顺手写 `JobFirstScheduledTime` 注解，和上报毫无关系 | 挪出去，见 5.1 |
| key 丢失 namespace | key 只取 `ns/name` 里的 name，不同 namespace 的同名作业会互相覆盖 | 需要和下游确认，见 5.2 |
| 凭据进入调度器 | 调度器 Pod 需要挂 Redis 密码 | 留在调度器就避免不了，见第 6 节 |

## 3. 设计目标

- 调度主循环**永远不等 Redis**：Redis 慢、挂、重启都不影响调度。
- Redis 恢复后自动收敛到最新的排队顺序，不需要人工干预。
- 写入语义和现在保持一致，下游不用改。
- 改动只在 `plugins/redis/` 目录内，不增加对社区文件的侵入。

## 4. 异步写入设计

**核心思路**：会话结束时只算出一份"排队快照"交给后台 writer，然后立刻返回。writer 用"上次成功写入的状态"和"最新快照"做差量，算出要执行的 Redis 命令后写入。

这里有一个关键性质：**差量永远相对"上次成功写入的状态"来算**。所以中间的快照可以随便丢，写失败了也可以整体重放，Redis 最终一定会收敛到最新的排队顺序。

### 4.1 整体结构

```
调度会话（单 goroutine）                 后台 writer（单 goroutine，进程内唯一）
OnSessionOpen:  收集 pending 作业
OnSessionClose: 用 JobOrderFn 排序
                → 生成 snapshot（只含字符串）
                → publish(snapshot) ──chan(容量1)──▶ run():
                  立即返回，不等 Redis                  取最新 snapshot
                                                      diff(已写入状态, snapshot)
                                                      分批 pipeline 写 Redis（带超时）
                                                      成功 → 更新"已写入状态"
                                                      失败 → 退避后重放
```

调度主循环和 Redis 之间只隔着一个容量为 1 的 channel，`publish` 永远不会阻塞。

### 4.2 数据结构

```go
// snapshot 是一次会话结束时的排队结果。只含字符串，不引用任何会话对象，
// 交给 writer goroutine 后会话可以被安全回收。
type snapshot struct {
	order map[string][]string // queue -> 按 JobOrderFn 排好序的 job key
	alive map[string]struct{} // 本轮会话里仍然存在的全部 job key
}

type writer struct {
	cli     *goredis.Client
	latest  chan *snapshot // 容量 1，只保留最新一份
	written map[string]int // job key -> 已成功写入 Redis 的排队序号（只记排队中的作业）
}

var (
	w     *writer
	wOnce sync.Once
)
```

为什么快照里要带 `alive`：作业离开排队有两种情况，写法不一样：

- 作业还在（已经被调度）：写 `-1`；
- 作业已经不存在（被删除）：`HDEL`。

writer 自己区分不了这两种情况，所以要由会话把"本轮还存在哪些作业"一起交过来。

### 4.3 会话侧：只计算，不做 I/O

```go
func (rp *redisPlugin) OnSessionClose(ssn *framework.Session) {
	wr := getWriter(rp.cfg)
	if wr == nil { // 没配置 Redis，整个插件空转
		return
	}
	rp.calRunningJobs(ssn)
	rp.calUnscheduleQueue()

	s := &snapshot{
		order: make(map[string][]string, len(rp.unscheduleQueue)),
		alive: make(map[string]struct{}, len(ssn.Jobs)),
	}
	for q, om := range rp.unscheduleQueue {
		s.order[q] = append([]string(nil), om.List()...) // 拷贝，不共享底层数组
	}
	for id := range ssn.Jobs {
		s.alive[string(id)] = struct{}{}
	}
	wr.publish(s)
}
```

- `OnSessionOpen` 里也先判断 `getWriter` 是否为 nil，为 nil 就不收集 pending 作业，省掉排序开销。
- 会话侧的开销只有一次排序加两张 map，几千个作业也就毫秒级。现在是同步执行 3 次 pipeline，Redis 抖动时会卡住秒级。

### 4.4 publish：最新结果覆盖旧结果

```go
func (wr *writer) publish(s *snapshot) {
	select {
	case <-wr.latest: // writer 还没取走的旧快照直接丢掉
		snapshotsDropped.Inc()
	default:
	}
	wr.latest <- s
}
```

只有调度会话这一个生产者，所以"先排空再发送"不会阻塞：

- 排空之后 channel 一定是空的；
- 如果 writer 恰好在这期间取走了旧快照，channel 同样是空的。

丢掉中间快照没有问题。Redis 里只需要最新的顺序，而 writer 的差量是相对"上次成功写入"算的，中间状态没写也不影响最终结果。

### 4.5 writer 主循环：重试和定期全量对账

```go
func (wr *writer) run() {
	var (
		pending *snapshot // 待写入的快照
		last    *snapshot // 最近一次成功写入的快照，全量对账时用
		force   bool      // 是否忽略 written，全量重写
		backoff = time.Second
		retry   = time.NewTimer(time.Hour)
		resync  = time.NewTicker(5 * time.Minute)
	)
	retry.Stop()

	for {
		select {
		case s := <-wr.latest:
			pending = s
		case <-retry.C:
			// 用手上的 pending 重试；期间来了新快照会在上面的 case 里替换掉
		case <-resync.C:
			force = true
			if pending == nil {
				pending = last
			}
		}
		if pending == nil {
			continue
		}

		start := time.Now()
		err := wr.flush(pending, force)
		flushDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			flushErrors.Inc()
			logFailureRateLimited(err) // 只在状态切换时或每 N 次打一条，避免刷屏
			retry.Reset(backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		lastSuccess.SetToCurrentTime()
		last, pending, force, backoff = pending, nil, false, time.Second
	}
}
```

- **失败重试**：失败后不更新 `written`，按 1 秒、2 秒……最长 30 秒退避，然后重放差量。这期间来了新快照，就直接用新快照重算差量。Redis 恢复后第一次成功写入就能追上最新状态。
- **定期全量对账**：每 5 分钟一次，`force=true`，把当前所有排队作业的序号无条件重写一遍。用来修复 Redis 重启、数据被清空、被人手动改过这类"`written` 和实际不一致"的情况。

### 4.6 flush：按作业做差量，语义和现在一致

```go
func (wr *writer) flush(s *snapshot, force bool) error {
	desired := make(map[string]int)
	for _, jobs := range s.order {
		for i, key := range jobs {
			desired[key] = i + 1
		}
	}

	var cmds []redisCmd
	for key, idx := range desired {
		if old, ok := wr.written[key]; force || !ok || old != idx {
			cmds = append(cmds, hset(redisKey(key), idx))
		}
	}
	for key := range wr.written {
		if _, still := desired[key]; still {
			continue
		}
		if _, ok := s.alive[key]; ok {
			cmds = append(cmds, hset(redisKey(key), -1)) // 离开排队且作业还在：已调度
		} else {
			cmds = append(cmds, hdel(redisKey(key))) // 作业已不存在
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, batch := range chunk(cmds, 500) { // 大队列分批，单个 pipeline 不要太大
		if err := wr.exec(ctx, batch); err != nil {
			return err // written 不变，下次整体重放
		}
	}
	wr.written = desired // 只记排队中的作业，和现在的 cache 语义一致
	return nil
}
```

和现在的实现相比：

- **语义不变**：排队中的作业写序号；离开排队但作业还在，写 -1；作业消失，`HDEL`。`redisKey` 沿用现有的"取 `ns/name` 里的 name"规则，是否带 namespace 见 5.2。
- **写入量更小**：现在只要队列成员有变化，就把整个队列的每个作业都重写一遍；改为按作业比较，只写序号真正变了的作业。
  - 队头作业被调度时，后面所有作业的序号都会减 1，这种情况仍然是全量写，这是绝对序号本身决定的；
  - 但"队尾新增一个作业"这类常见变化，只需要写 1 条命令。
- **部分失败也安全**：pipeline 中途失败时，可能已经执行了一部分命令。`HSET` 写的是绝对值，`HDEL` 本身就是幂等的，整体重放不会出错。

### 4.7 连接管理：去掉 `init()`，不依赖启动时 Redis 可用

```go
func getWriter(cfg config) *writer {
	wOnce.Do(func() {
		if cfg.addr == "" {
			klog.Warning("redis plugin: address not configured, queue order reporting disabled")
			return
		}
		w = &writer{
			cli: goredis.NewClient(&goredis.Options{
				Addr:         cfg.addr,
				Password:     os.Getenv(cfg.passwordEnv), // 密码仍然走 Secret 注入的环境变量
				DialTimeout:  time.Second,
				ReadTimeout:  time.Second,
				WriteTimeout: time.Second,
				MaxRetries:   1,
				PoolSize:     2, // 只有一个 writer goroutine
			}),
			latest:  make(chan *snapshot, 1),
			written: map[string]int{},
		}
		go w.run()
	})
	return w
}
```

- **不再在启动时 Ping**。`goredis.NewClient` 不会立即建连，第一次发命令时才连接，断线后也会自动重连。"启动时 Redis 不可用，就永久失效"的问题自然消失。
- 插件对象每个会话都会重建，writer 用 `sync.Once` 做成进程级单例。配置只在第一次会话时读取，之后修改 Redis 地址需要重启调度器，这一点要写进运维说明。
- **配置来源**：地址改成插件参数（例如 `redis.address`），兼容期内也可以继续读 `REDIS_HOST_PORT`；密码继续用 Secret 注入的 `REDIS_PASSWORD`，不写进调度配置。
- **多实例部署时只有 leader 会写**：只有 leader 跑调度会话，也只有它会 publish。Volcano 调度器丢失 leader 身份时进程会直接退出，不存在新旧 leader 同时写的问题。

### 4.8 超时预算

| 项 | 取值 |
|---|---|
| 单条命令 | 连接、读、写超时各 1 秒，最多重试 1 次 |
| 单次 flush | 总超时 5 秒，每批最多 500 条命令 |
| 失败退避 | 1 秒起步，翻倍增长，最长 30 秒 |
| 全量对账 | 每 5 分钟一次 |

这些超时只影响 writer goroutine。Redis 完全挂掉时，调度器每个会话照常运行；writer 大约每 30 秒尝试一次，每次最多耗时 5 秒。

### 4.9 可观测性

在插件包里注册 4 个 Prometheus 指标，调度器的 metrics 端口会自动暴露：

| 指标 | 含义 |
|---|---|
| `volcano_queue_order_redis_last_success_timestamp_seconds` | 最近一次成功写入的时间。告警靠它，例如超过 2 分钟没更新就报警 |
| `volcano_queue_order_redis_flush_duration_seconds` | 每次 flush 的耗时 |
| `volcano_queue_order_redis_flush_errors_total` | 写入失败次数 |
| `volcano_queue_order_redis_snapshots_dropped_total` | 被覆盖丢弃的快照数。偶尔有是正常的；持续增长说明写得比会话慢 |

### 4.10 对下游的一致性保证

- Redis 里的序号**最多落后一个会话周期再加一次 flush 的时间**，正常情况下约 1~2 秒，和现在差不多。
- Redis 故障期间，序号停在故障前的值；恢复后第一次成功写入就追平到最新。
- 调度器重启后 `written` 为空，第一次会把所有排队作业全量写一遍，和现在的行为一致。
- 调度器停机期间被删除的作业，它们的 `pending_index` 不会被清理，这个遗留问题现在也存在。如果下游在意，可以由下游在作业删除时自己清理。

### 4.11 测试方式

- 把 Redis 操作抽成一个接口 `exec(ctx, []redisCmd) error`，单测里用假实现，覆盖这些场景：
  - 连续 publish 3 次，writer 只处理最后一次；
  - flush 失败后重放，结果仍然正确；
  - 作业离开排队时，按是否还存在分别写 -1 或执行 `HDEL`；
  - `force` 全量对账会重写所有排队作业；
  - 未配置地址时插件空转，不启动 writer。
- 集成测试可以用 `miniredis` 在内存里跑一个 Redis，验证 key 和字段符合预期。

## 5. 其它整改项

### 5.1 `JobFirstScheduledTime` 挪出插件

redis 插件只负责上报。`JobEnqueuedFn` 里写 `scheduling.k8s.io/jobScheduledTime` 注解的逻辑，按 [scheduler-decoupling-proposal.md](./scheduler-decoupling-proposal.md) 3.5 节合并到作业生命周期控制器里；如果确认已经没人用，直接删除。

挪之前先确认下游有哪些地方读这个注解，避免迁移期间出现空窗。

### 5.2 key 丢失 namespace

现在的 key 只取 job key（`ns/name`）里的 name，不同 namespace 的同名作业会互相覆盖。处理方式：

- 和下游消费方确认能否改成带 namespace 的 key（例如 `ns:name`）；
- 能改的话，过渡期新旧两种 key 双写，下游切换完再去掉旧 key；
- 不能改的话，至少在文档里写明"作业名必须全局唯一"这个约束。

### 5.3 确认 v1.16 的 PodGroup 命名

插件按 PodGroup 的名字和 `job-name` 标签识别作业。升级到 v1.16 后要确认 vcjob 对应的 PodGroup 命名规则和 `job-name` 标签没有变化，否则下游按名字查不到。

## 6. 什么情况下才值得拆

满足下面任一条再拆：

- **安全规范不允许调度器持有业务 Redis 凭据。**
- **有多个消费方**，或者需要 Redis 以外的输出，比如消息队列、平台 API。
- **启用了 v1.16 的多调度器分片模式**，多个调度器各自只看到部分作业，各自写出来的序号会互相冲突。

拆的话也不能把排序逻辑拆出去，原因见 2.1。可行的拆法是：

- 调度器在已有的 HTTP 服务（metrics / healthz 的监听端口）上加一个只读接口，例如 `/api/queue-order`，返回各队列的排队顺序。计算逻辑就是本文 4.3 的快照生成部分，约 100 行；
- 新建一个同步服务轮询这个接口，再写入 Redis，Redis 凭据只给这个服务。同步服务可以直接复用本文 4.5、4.6 的 writer 逻辑。

不建议把序号写进 PodGroup 的注解或 status：一个作业被调度后，同队列后面所有作业的序号都会变，每次都要更新 N 个对象，对 apiserver 压力很大。

## 7. 改动清单与工作量

| 文件 | 改动 |
|---|---|
| `plugins/redis/redis.go` | 删除 `init()` 和包级 `client`、`cache`；`New` 解析参数；`OnSessionOpen` 去掉 `JobFirstScheduledTime`；`OnSessionClose` 改为生成快照并 publish |
| `plugins/redis/writer.go`（新增） | writer 单例、主循环、flush、重试、对账、指标，约 200 行 |
| `plugins/redis/util.go` | pipeline 封装改为 `exec(ctx, []redisCmd)`，加分批 |
| `plugins/redis/store.go` | `OrderMap` 保留，`Len()` 里的 panic 改成返回错误或打日志，避免后台 goroutine 崩掉整个进程 |
| `plugins/redis/*_test.go`（新增） | 4.11 列出的单测 |
| `plugins/factory.go`、`go.mod` | 不变 |
| 调度器部署 | 增加插件参数 `redis.address`；密码 Secret 保持不变；补充 4.9 的告警规则 |

工作量约 1~2 人天，加上联调下游（确认 5.1、5.2）约 1 天。
