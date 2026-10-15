# Volcano 内部功能迁移清单（dedicated-queue-scheduling → 社区 v1.15）

> 源分支: dedicated-queue-scheduling（基线 v1.9.1-213，277 个内部 commit）
> 目标基线: 社区 release-1.15
> merge-base: 60a7f7536
> 代码差异: plugins +25695/-19059，actions +12163/-12615，api/framework/cache +24646/-34402

## 一、EA 独有调度插件（19 个）

| 插件 | 功能 | 核心代码量 | 依赖独有回调 | 迁移难度 |
|------|------|-----------|-------------|---------|
| replica-gang | 按 replica 组织细粒度 gang 调度 | ~996 行 | NextSchedulingUnitFn 等 | 高 |
| jobbackoff | 失败任务退避机制 | 777 行 | BackoffCheckFn/BlockedFn | 高 |
| queue-reclaim | 同队列 victim 顺序、专属节点池权限、GPU 卡型公共卡配额 | 922 行 | BatchVictimScoreFn 等 | 高 |
| notebook | nb-* 任务特化处理 | 158 行 | - | 低 |
| delayschedule | 延迟调度 | - | - | 低 |
| spblock | super-pod 抢占豁免 | 93 行 | VictimProtectionFn | 中 |
| node-pool | scheduling_type label 节点分池 | 111 行 | - | 低 |
| reserve | permitCPUNode 节点池准入 | 299 行 | - | 中 |
| slanode | SLA 节点准入 | 138 行 | - | 低 |
| cooldown | 节点级故障冷却 | - | - | 中 |
| cdp | cooldown 保护策略 | - | - | 中 |
| net-topology | rack/pod 网络拓扑打分 | 137 行 | - | 低 |
| archimedes | 多 hint 综合评分 | - | - | 高 |
| coordinator | 协同调度 hint | - | - | 中 |
| redis | 调度数据写 Redis sink | 746 行 | - | 低 |
| reservation | 预留时间窗管理 | 575 行 | - | 高 |
| elastic | 弹性 job min/max | 126 行 | - | 低 |
| property | task-property 优先级细化 | 139 行 | - | 低 |
| predicates/proportional | 比例资源 predicate | 109 行 | - | 低 |

## 二、EA 独有/增强 Actions（8 个）

| 模块 | 功能 | 代码量 |
|------|------|--------|
| actions/reserve | 预留 action | 667 行 |
| actions/allocate/allocate_replica_gang.go | replica gang 分配流程 | 370 行 |
| actions/preempt/relax.go | relaxEvictByJob 抢占 | 255 行 |
| actions/preempt/topology_aware.go | 拓扑感知抢占 | 79 行 |
| actions/reclaim/reclaim_victim.go | reclaim victim 选择 | 114 行 |
| actions/reclaim/topology_aware.go | 拓扑感知 reclaim | 123 行 |
| actions/utils/topology_aware.go | superpod 抢占核心 | 599 行 |
| actions/utils/action_log_collector.go | action 日志收集 | 680 行 |

## 三、Session 框架扩展（10 个独有回调）

框架基础，必须最先迁移：

1. SimulatePredicateFn — 模拟谓词
2. SimulateAddTaskFn / SimulateRemoveTaskFn — 模拟任务增删
3. SimulateAllocatableFn — 模拟可分配判断
4. BatchVictimScoreFn — 批量 victim 评分
5. VictimProtectionFn — victim 保护
6. CoevictionFn — 协同驱逐
7. ActionExcludedFn — action 排除
8. BackoffCheckFn / BackoffBlockedFn — 退避检查
9. NextSchedulingUnitFn — 下一调度单元
10. SchedulingUnitManagedTasksFn — 调度单元管理任务

## 四、API / Cache 扩展

| 文件 | 功能 | 代码量 |
|------|------|--------|
| api/scheduling_unit.go | SchedulingUnit 数据结构 | 78 行 |
| api/reservation_info.go | 预留信息结构 | 293 行 |
| cache/reservation.go | 预留缓存管理 | 451 行 |
| cache/unschedule_task.go | 不可调度任务缓存 | 170 行 |
| util/network_utils.go | 网络工具 | 72 行 |

## 五、基础设施（日志/监控/快照）

| 模块 | 功能 | 代码量 |
|------|------|--------|
| plugins/util/session_snapshot.go | session 快照 | 582 行 |
| plugins/util/session_snapshot_gc.go | 快照 GC | 598 行 |
| plugins/util/session_snapshot_metrics.go | 快照指标 | 94 行 |
| plugins/util/logcollector.go | 结构化日志收集 | 645 行 |
| plugins/util/entity_trace_collector.go | 实体追踪收集 | 433 行 |
| plugins/util/archimedes/ | archimedes 框架/快照 | 321 行 |

## 六、第三方依赖

- third_party/volcano.sh/apis — volcano.sh API clientset/informers/listers 移至第三方
- third_party/hami — GPU 虚拟化
- third_party/mindcluster — MindCluster 集成
- third_party/kubernetes — k8s 补丁
- third_party/kube-apiserver-audit-exporter — 审计导出

## 七、构建与部署

- Makefile.ea / Makefile.intl — EA 内部/国际版构建
- installer/ 定制（helm chart、deployment yaml）
- cmd/ 精简（移除 network-qos 等工具，-3221 行）

## 八、EA 删除的社区/旧代码（v1.15 中仍存在，需评估）

- gangpreempt / gangreclaim actions
- network_topology_aware plugin（3890 行测试）
- resource-strategy-fit plugin
- gpuexclusive.go（deviceshare 内）
- shard_coordinator.go 及 shard 相关测试
- bundle.go / simulate.go（actions/utils）
- sub_job_info.go
- network-qos cmd 工具

## 九、git log 提取的完整功能关键词

- 队列调度：queue exclusive node scheduling、queue affinity、queue reclaim、NamespaceQueue、dedicated queue
- 抢占回收：global preemptable、same-queue preemption、relaxEvictByJob、victim 顺序、coeviction
- Gang/副本：replica gang、elastic preempt、gang 语义增强
- 拓扑：GPU NUMA、network topology、task topology、disk topology、hypernode
- 退避/预留：job backoff、reservation policy、reserve action、预留时间窗
- 节点管理：SLA node、node pool、cooldown/cdp、permitCPUNode、tidal scheduling
- 多调度器：multi-scheduler、extender allocate callback
- 硬件特化：MindCluster plugin、Ascend plugin、HAMI GPU 虚拟化
- 可观测：structured log aggregation、entity trace、session snapshot、scheduling metrics、redis data sink
- 策略：BestEffort policy、proportional resource、CPU task predicate、online timeout、podgroup anti-affinity

## 十、迁移优先级建议

**P0 框架基础（必须先迁移）**
1. Session 独有回调（10 个）
2. API 扩展（SchedulingUnit、reservation_info）
3. Cache 扩展（reservation、unschedule_task）

**P1 核心调度能力**
4. replica-gang + allocate_replica_gang
5. jobbackoff
6. queue-reclaim + reclaim_victim + topology_aware
7. preempt/relax + topology_aware

**P2 节点/拓扑管理**
8. node-pool、slanode、reserve、spblock
9. net-topology、cooldown、cdp
10. elastic、property、delayschedule、notebook

**P3 可观测/数据面**
11. session_snapshot 系列、logcollector、entity_trace_collector
12. redis、archimedes、coordinator
13. reservation action + reservation plugin

**P4 构建部署**
14. Makefile.ea/intl、installer 定制
15. third_party 集成