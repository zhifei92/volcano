# 05 · 队列与配额：Queue CRD / capacity / proportion / enqueue / 准入 gate

> 行号对应 tag **v1.15.3**。`apis/` 指 `staging/src/volcano.sh/apis/pkg/apis/`。

## 0. 一页速览

| 主题 | v1.9 | v1.15 |
|---|---|---|
| 层级队列 | 无 | capacity 插件支持层级队列，需设置 `enableHierarchy: true`，用 `spec.parent` 建树 |
| 队列优先级 | 无 | `spec.priority`，QueueOrderFn 先比较它 |
| capacity 插件 | 360 行，平铺队列 | 1965 行，包含层级、DRA 配额、`ancestorReclaimLevel`、UnifiedEvictable、Simulate*、准入 gate 预留账本 |
| root 队列 | 无 | 调度器启动时**无条件创建** `root`（不可回收）和 `default`（`cache/cache.go:464-519`） |
| queue controller | 往 status 写入各状态 PodGroup 的计数 | **只写 `state`**，计数改成 metrics |
| queue webhook | 只校验 state、weight、hdrf | 新增 guarantee ≤ deserved ≤ capability 校验、层级校验、root 保护 |
| enqueue action | — | **逻辑与 v1.9 完全相同**，变化都在插件内部 |
| 准入 gate | 无 | `SchedulingGatesQueueAdmission`（Alpha） |

---

## 1. Queue CRD 字段（`apis/scheduling/v1beta1/types.go:460-520`）

| 字段 | 说明 | 相对 v1.9 |
|---|---|---|
| `weight` | proportion 插件用来计算 deserved 的权重，取值 1~65535 | 已有；校验从 webhook 改为由 CRD 完成 |
| `capability` | 硬上限 | 已有 |
| `reclaimable` | 是否允许被其他队列回收，nil 视为 true | 已有 |
| `guarantee.resource` | 保底资源，会预留出来，其他队列不能借用 | 已有 |
| `deserved` | 应得资源：空闲时可以借给别人，被借走后可以通过回收要回来 | 已有（v1.9 的 capacity 已经在用） |
| `parent` | 父队列名，用于构建层级 | 新增（v1.10 起） |
| `priority` | 队列优先级，数值越大越先调度 | **新增** |
| `dequeueStrategy` | `fifo` 或 `traverse` | **新增，但 v1.15.3 只定义了 API，`pkg/`、`cmd/` 中没有任何代码读取它** |

`status.allocated` 由调度器在关闭 session 时写入，v1.15 会**逐级累加到所有祖先队列**，并合并 DRA 资源（`framework/session.go:517-542`）。

注意：`status.pending/running/inqueue` 这几个计数**不再由 controller 写入**（`pkg/controllers/queue/queue_controller_action.go:92-110`），改成了 metrics。**依赖这些计数的运维脚本或上层平台需要改造**；老版本 vcctl 的 queue get 命令可能一直显示 0。

---

## 2. capacity 插件中各资源概念的语义

| 概念 | 平铺模式 | 层级模式（`enableHierarchy: true`） |
|---|---|---|
| `capability` | 未设置的 cpu/mem 视为无穷大 | 子队列中未设置的维度继承父队列；root 未设置时为无穷大 |
| `guarantee` | 计入 `totalGuarantee` | root 的 guarantee 等于各子队列之和 |
| `deserved` | `max(min(deserved, realCapability), guarantee)`。v1.9 还会再与 request 取 min，v1.15 去掉了这一步 | `max(deserved, guarantee)`；root 的 deserved 等于各子队列之和 |
| `realCapability` | `min(capability, (集群总量 − Σguarantee) + 自身guarantee)` | `min(capability, (父队列realCapability − Σ兄弟guarantee) + 自身guarantee)` |
| `share` | 各维度 `allocated/deserved` 的最大值；**deserved 为空的队列 share 恒为 1** | 同左 |

## 3. capacity 插件的钩子实现

| 钩子 | v1.15 | v1.9 |
|---|---|---|
| QueueOrder | 先比 `priority`，再比 share；share 相同时有 deserved 的队列优先。层级模式下叶子优先，并在分叉处比较祖先的 share | 只比 share |
| VictimQueueOrder（仅层级模式） | 与抢占方亲缘更近（共同祖先更深）的队列优先作为受害者 | — |
| Allocatable | 队列必须是 Open → 层级就绪 → 必须是叶子队列 → **从叶子到 root 逐级**检查 `allocated + 准入预留 + 请求 ≤ realCapability`（只比较请求涉及的维度）→ DRA 检查 | `请求 ≤ realCapability − allocated` |
| Preemptive（判断能否发起回收） | 队列必须是 Open；`allocated + 请求 ≤ realCapability`；**任一维度** `futureUsed ≤ deserved` 即可；否则在 `ancestorReclaimLevel` 层以内检查祖先是否满足 | `!deserved.LessEqual(allocated)` |
| Reclaimable（筛选受害者） | 按受害者优先队列排序；跳过与回收方资源维度没有交集的任务；遵守 guarantee；受害者资源维度与 deserved 没有交集时直接算作受害者；否则要求相关维度上 `allocated > deserved`；层级模式下还要检查祖先 | allocated ≤ deserved，或者回收后会低于 guarantee，则跳过 |
| UnifiedEvictable | GangPreempt 全部放行；GangReclaim 检查 guarantee 和 deserved | — |
| JobEnqueueable | 层级就绪 → 叶子 → Open → 从叶子到 root 逐级检查 `minReq + allocated + inqueue − elastic ≤ realCapability` → DRA；被准入 gate 卡住的资源不计入 inqueue | `minReq + allocated + inqueue − elastic ≤ realCapability` |
| **Overused** | **未注册**，只在 OnSessionClose 时按 `share > 1` 上报指标 | 同样未注册 |

需要特别注意两点：

1. **层级配置有错误时会阻断调度。** 例如 job 挂在非叶子队列上、父队列不存在、队列成环，都会导致 `readyToSchedule=false`。此时**所有钩子都返回拒绝**，本轮由 capacity 管理的调度全部停止（`capacity.go:455-458, 1229-1232`）。
2. **社区 capacity 不区分作业是否可驱逐。** 内部 capacity 的语义是"deserved 只给不可驱逐作业使用，capability 给可驱逐作业使用"，这与社区语义不同，具体差异和迁移建议见上一轮的讨论。v1.15 的 capacity 改动很大，**不建议在社区 capacity 的基础上直接修改**，建议做成独立插件。

插件参数：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `enableHierarchy` | false | 插件级开关，不是 argument |
| `ancestorReclaimLevel` | 0 | — |
| `capacity.DynamicResourceAllocationEnable` / `capacity.DRAConsumableCapacityEnable` | 取对应 k8s feature gate 的值 | — |

DRA 配额通过特殊的资源名表达：`deviceclass/<class>` 表示设备数，`<dim>.deviceclass/<class>` 表示可消耗的容量。

## 4. proportion 插件

- 仍然按 weight 迭代计算 deserved。
- 新增的能力：QueueOrder 先比 priority；Allocatable 增加 Open 检查，并改为 `futureUsed ≤ deserved`；新增 PreemptiveFn 和 Simulate*；JobEnqueueable 增加 Open 检查，并扣除被准入 gate 卡住的资源。
- **不支持层级队列**，层级只在 capacity 中实现。
- **capacity 和 proportion 不能同时启用**，但只有文档这么规定，代码没有做互斥校验。

## 5. enqueue action

- `actions/enqueue/enqueue.go` 与 v1.9 **逻辑完全相同**：按 QueueOrder 弹出队列，按 JobOrder 弹出 Pending 作业；`MinResources` 为空或 `JobEnqueueable` 通过时，把 PodGroup 置为 Inqueue。
- 如果没有配置 enqueue action，allocate 会直接把 Pending 作业置为 Inqueue（`allocate.go:153-164`），与 v1.9 相同。内部生产环境就是这种配置。

## 6. SchedulingGates 队列准入（QueueAllocationGate）

1. Pod 带上注解 `scheduling.volcano.sh/queue-allocation-gate: "true"`，并且开启了 feature gate。webhook 在创建 Pod 时注入同名 gate（`pkg/webhooks/admission/pods/mutate/mutate_pod.go:156-200`）。
2. 被 gate 卡住的资源不计入 inqueue（`api/job_info.go:612-651`）。
3. allocate 中，Pod 通过 `Allocatable` 后：
   - 进入 capacity 的预留账本；
   - 交给 `SchGateManager` 异步移除 gate。worker 数默认 5（`--gate-removal-worker-num`）；channel 满时直接丢弃，属于尽力而为。
   - 本轮仍跳过这个 Pod。
4. 下一轮，Pod 已经没有 gate 但仍带着注解，会**重新进入预留账本**，直到真正分配出去。这样可以防止配额在 gate 移除到 Pod 被分配之间被其他 Pod 抢走（`capacity.go:1608-1663`）。
5. 被 gate 卡住的 Pod 不会被标记为 Unschedulable，CA 和 Karpenter 也就不会因为队列配额不足而误判为集群资源不足。

前提条件：**scheduler 和 webhook 都要开启 `SchedulingGatesQueueAdmission`，并且 k8s 要开启 `PodSchedulingReadiness`**。schedulingGates 在 k8s 1.26 引入（Alpha），1.30 GA，**在 1.19 上不可用**。

## 7. queue controller 与 webhook

**controller**（`pkg/controllers/queue/`）：
- `spec.parent` 为空的非 root 队列会被自动补上 `parent=root`。
- 父队列关闭时级联关闭子队列（打注解 `volcano.sh/closed-by-parent`）；父队列重新打开时，子队列一并重新打开。root 队列不允许关闭。
- 不负责资源聚合，资源聚合由调度器完成。

**webhook**（`pkg/webhooks/admission/queues/validate/validate_queue.go`）：
- 校验 `guarantee ≤ deserved ≤ capability`。capability 中没有的维度视为不限制。
- 层级相关校验：
  - 不能把自己设为父队列；
  - 深度 ≤ `--max-queue-depth`（默认 5）；
  - 父队列必须存在；
  - 如果父队列原来没有子队列、自己已经有已分配的 Pod，则拒绝。
- 兄弟队列之间：Σguarantee ≤ 父队列 guarantee，Σdeserved ≤ 父队列 deserved；子队列的 capability ≤ 祖先的 capability。
- 保护规则：
  - root 的资源字段不能修改（`--enable-root-queue-protection`，默认 true）；
  - root 和 default 不能删除；
  - 有子队列的队列不能删除。

## 8. 其他

- **resourcequota 插件**：与 v1.9 相同，只实现了 JobEnqueueable，按 namespace 的 ResourceQuota 判断。它和队列配额是两套独立的机制。
- 相关设计文档（位于 `docs/design/`）：
  - `capacity-scheduling.md`
  - `hierarchical-queue-on-capacity-plugin.md`
  - `capacity-dra-support.md`
  - `scheduling-gates-queue-admission.md`
  - `queue-priority.md`
  - `podgroup-statistics.md`
  - `pod-scheduling-readiness.md`
