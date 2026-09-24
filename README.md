# 本地虚拟机生命周期编排器（Go）

在本地以“真实虚拟化占位接口 + 进程级模拟后端”承载实例，提供创建、启动、停止、
销毁的完整生命周期编排，保证：

- **并发一致**：所有变更串行化，重复/并发请求幂等，不产生重复实例与资源泄漏；
- **配额守恒**：资源准入先于一切副作用，占用与释放始终与实际状态成对；
- **崩溃可解释**：异常重启后按确定性决策表对账，能判定的自动收敛，
  不能判定的显式标记为 `Failed`（人工介入），绝不静默覆盖；
- **残留可发现**：后端有定义而无记录的“孤儿”只报告、不擅自删除或接管。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/model` | 状态机、资源模型、可区分的哨兵错误 |
| `internal/quota` | 宿主资源总量/用量/分配明细与准入判定 |
| `internal/backend` | 承载后端接口；`ProcessBackend` 以进程模拟；`FaultyBackend` 注入故障 |
| `internal/store` | JSON 快照持久化（临时文件 + fsync + rename 原子替换） |
| `internal/orchestrator` | 生命周期编排：互斥、准入、迁移校验、中间态落盘 |
| `internal/reconcile` | 重启后记录与后端事实的对账决策与报告 |
| `cmd/vm-demo` | 最小演示程序 |

## 1. 状态模型

```
                 Create(Define OK)
   Pending ───────────────────────▶ Stopped
     │  Define 前崩溃且无后端残留      │  ▲
     └──────────────▶ Destroyed      │  │ 启动未生效/进程异常退出
                          ▲          │  │ （重启对账确定收敛）
                          │          ▼  │
   Destroying ──销毁完成───┘      Starting ──确认运行──▶ Running
        ▲  ▲                          │                    │
        │  └──────────── Destroy ─────┤                    │
        │                             ▼                    ▼
   Failed（人工介入）              Stopping ◀──── Stop ─────┘
```

状态集合：`Pending / Stopped / Starting / Running / Stopping /
Destroying / Failed / Destroyed`。

- **`Starting / Stopping / Destroying / Pending` 是显式持久化的中间态**：
  每一步都遵循“**先把意图落盘，再调用后端**”，因此任意时刻崩溃，
  重启后都能依据记录 + 后端事实做出确定判断。
- `Failed` 表示记录与事实无法自动调和（见恢复语义），携带人类可读的 `Reason`；
  常规 `Start/Stop` 被拒绝（`ErrInconsistent`），仅允许 `Destroy` 终结。
- `Destroyed` 为终态；仅当对账发现“终态记录仍有后端残留”这一泄漏证据时，
  才允许受控地翻为 `Failed` 以暴露问题。

非法迁移返回 `TransitionError`（`errors.Is(err, ErrInvalidTransition)`），
其中带实例 ID、起始状态与操作类型；各类失败原因可区分：

| 哨兵错误 | 含义 |
| --- | --- |
| `ErrNotFound` | 实例不存在 |
| `ErrAlreadyExists` | 同 ID/同 Name 重复创建（幂等命中） |
| `ErrInvalidTransition` | 当前状态不允许该操作 |
| `ErrQuotaExceeded` | 资源不足（`QuotaError.Resource` 指明 cpu/memory/disk） |
| `ErrBackendFailed` | 后端执行失败（实例保留在中间态，可重试/对账） |
| `ErrInconsistent` | 实例已标记人工介入 |

### 幂等语义

- `Create(id,…)`：同 ID 重复调用返回已有记录 + `ErrAlreadyExists`，
  不二次占用配额、不二次 Define；同 Name 不同 ID 同样拒绝。
- `Start/Stop`：目标态或中间态重复调用直接成功返回。
- `Destroy`：不存在或已销毁均视为成功。

## 2. 配额规则

- 配额维度：`CPUMillis`（千分之一核）、`MemoryMiB`、`DiskMiB`。
- **准入先于副作用**：`Create` 顺序为
  `重复校验 → 配额 Allocate → Pending 落盘 → 后端 Define`；
  因此被拒请求不会产生任何后端文件或子进程。
- 占用按实例记账（`map[id]Resources`），`Release` 必须能匹配先前的 `Allocate`，
  未占用而释放会直接报错，防止“占用漂移”被静默吞掉。
- 配额在整个生命周期（含 `Stopped`、`Failed`、中间态）保留，
  直到后端事实被确认清除并进入 `Destroyed` 才释放。
- **重启后不盲目信任崩溃前的内存用量**：对账完成后以“收敛后的全部记录”为唯一
  基准重算配额（非 Destroyed 即占用）。若记录总量超过宿主总量，重算返回错误，
  保持现状交由人工处理。

## 3. 恢复语义（重启对账）

启动时（任何外部写操作之前）执行 `reconcile.Run`，对每条记录探测后端事实
（`running / defined / gone / unknown`），按下表得出确定性结论：

| 记录状态 | 后端事实 | 结论 |
| --- | --- | --- |
| Pending | gone（Define 前崩溃） | → `Destroyed`，释放配额 |
| Pending | defined（Define 已完成未落盘） | → `Stopped` |
| Stopped | defined | 确认，不变 |
| Starting | running | → `Running` |
| Starting | defined（启动未生效） | → `Stopped` |
| Running | running | 确认，不变 |
| Running | defined（进程已异常退出） | → `Stopped`，原因保留 |
| Stopping | defined（已停） | → `Stopped` |
| Destroying | gone（已删） | → `Destroyed`，释放配额 |
| Failed | 任意 | 维持标记，**不自动翻案** |
| Destroyed | gone | 确认终态 |
| 任意矛盾组合（如 Stopped 但定义缺失、Destroyed 却有残留、事实 unknown） | — | → `Failed` + 原因 |

原则：

1. **确定的才自动收敛**（结论 `recovered`），且每个收敛都能引用上表规则；
2. **不确定的只标记不覆盖**（结论 `marked`），等待人工 `Destroy` 终结；
3. **后端孤儿**（有定义、无记录）列入报告 `Orphans`，不自动删除/接管；
4. 报告包含操作序列、逐条前后状态与原因、孤儿列表、对账前后配额用量。

进程后端的崩溃识别增强：通过 `/proc/<pid>/stat` 排除僵尸进程（`Z`），
并用进程启动时钟滴答数防止 PID 复用导致的误判。

## 4. 验证方法

```bash
# 全量单测（含并发竞态检测，建议多跑几轮）
go test -race -count=3 ./...

# 仅看并发争用
go test -race ./internal/orchestrator/ -v

# 仅看异常恢复
go test -race ./internal/reconcile/ -v

# 演示：对账报告 + 创建/启动/停止/销毁
go run ./cmd/vm-demo /tmp/vm-demo-data
```

### 覆盖的关键测试

- `TestLifecycleHappyPath`：完整生命周期与销毁后零残留、配额归零。
- `TestIdempotentRepeatedCalls`：重复 Create/Start/Stop/Destroy 与同名冲突。
- `TestInvalidTransitionsAreDistinguishable`：运行态销毁被拒、错误类型可区分。
- `TestQuotaRejectBeforeSideEffects`：超额请求在副作用前拒绝，且错误定位到具体维度。
- `TestConcurrentDuplicateCreate`：32 路并发创建同一实例，恰有 1 个成功。
- `TestConcurrentQuotaContention`：24 路争抢，成功占用之和恒等且不超总量。
- `TestConcurrentStartStop` / `TestConcurrentStartDestroy`：
  任意交错后记录与后端事实一致、无进程与配额泄漏（`-race`）。
- `TestReconcile*`：进程崩溃退出、启动未生效、Define 前崩溃、定义被外部删除、
  Undefine 失败残留、后端孤儿、终态记录与残留矛盾等恢复场景。

测试日志中会打印三类信息（满足排障需要）：

- **操作序列**：例如 `Create -> Start -> Stop -> Destroy`、并发路数与成功/拒绝计数；
- **资源快照**：宿主总量、已用量、实例数/后端定义数；
- **对账结论**：每条记录的前后状态、结论类型（confirmed/recovered/marked）、
  原因与孤儿列表、对账前后配额。
