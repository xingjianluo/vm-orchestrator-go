# 本地虚拟机生命周期编排器（Go）

一套可解释的本地 VM 生命周期编排流程：实例可以由**本地虚拟化能力（模拟后端）**
或**真实子进程（进程级后端）**承载。系统保证：

- 并发/重复请求下不产生重复实例、不泄漏资源、不互相覆盖状态；
  **不同实例的变更并行推进、互不排队，同一实例的变更严格串行**；
- 宿主配额在产生任何后端副作用**之前**完成准入，占用与释放始终成对、守恒；
  任何新进程（无需先做任何操作）查询到的占用都与真正持有资源的实例记录集合一致；
- 异常重启后对「持久化记录 / 后端承载事实 / 配额台账」三方对账，
  能确定的偏差自动收敛到唯一状态，不能确定的**持久化为「待人工处理」
  （PendingManual）并附原因**，绝不静默覆盖；运维通过显式 `resolve` 收口。

## 目录结构

```
internal/domain/       状态机（含 PendingManual）、资源模型、可区分类别的错误
internal/quota/        宿主配额池：预留/释放台账，容量准入，重启重建
internal/backend/      承载后端：SimHandle（模拟）、ProcHandle（真实子进程）
internal/store/        FileStore：每实例一个 JSON，临时文件+rename 原子写
internal/orchestrator/ 编排器：每实例串行 + 全局对账屏障 + 准入 + 回滚 + 人工收口 Resolve
internal/reconcile/    重启对账：Matched / Recovered / Quarantined（持久化挂起）
cmd/vmctl/             命令行入口（含 resolve 人工收口）
cmd/vmguest/           进程后端承载的 guest 程序（SIGTERM 优雅退出）
```

## 一、状态模型

```
                 create(admission+define)
   (none) ───────────────────────────────► Defined
                                             │  ▲
                              start ┌────────┘  │ start
                                    ▼           │
                                 Running ──stop─┘
                                    │  ▲
                          stop ──────┘  │ start（重启/再次启动）
                                    ▼   │
                                 Stopped
                                    │
                    destroy ┌───────┴────────┐
                            ▼                ▼
                        Destroyed        (Defined 也可直接 destroy)
```

合法迁移（`internal/domain/types.go`）：

| From      | To                | 说明 |
|-----------|-------------------|------|
| Defined   | Running           | 启动 |
| Defined   | Destroyed         | 已定义实例可直接销毁（释放配额） |
| Running   | Stopped           | 正常停止 |
| Stopped   | Running           | 再次启动 |
| Stopped   | Destroyed         | 销毁并释放配额 |
| Destroyed | —（终态）          | 墓碑保留，不再迁移 |
| 任意正常态 | **PendingManual**  | **仅由对账**在无法自动判定时写入（非人工不可达） |
| PendingManual | Running / Stopped / Defined | 仅由人工收口 `resolve --keep --target ...` |
| PendingManual | Destroyed       | 仅由人工收口 `resolve --destroy` |

### 待人工处理（PendingManual）

当对账发现「记录与承载事实对不上、系统又无法唯一判定该怎么收」时
（典型：记录 `Running` 但后端承载整体消失——可能是宿主被重建，也可能是被外部清理），
系统把结论**持久化**到实例记录本身：`state=PendingManual`，并附带

- `pending.rule`：触发规则码（如 `running-missing-backend`）；
- `pending.reason`：人类可读原因；
- `pending.record_state`：挂起那一刻记录上的状态；
- `pending.actual_state`：挂起那一刻观测到的后端事实（absent/stopped/running）；
- `pending.detected_at`：判定时间。

语义保证：

1. **跨进程/重启留痕**：结论落在每实例 JSON 里。换新进程、宿主再重启后，
   `get`/`list` 看到的仍是 PendingManual 且带原因；重复对账只会维持挂起，
   不会再自动收敛或覆盖它。
2. **常规动作一律拒绝**：PendingManual 实例的 `create/start/stop/destroy`
   直接返回 `pending_manual`（CLI 退出码 **9**），与 `not_found`(3)、
   `illegal_transition`(5)、`insufficient_resource`(6) 等明确可区分；
   拒绝不改变状态、不动后端承载残留、不释放也不重复占用配额。
3. **唯一收口入口是人工 `resolve`**：
   - `vmctl resolve <id> --keep --target Running|Stopped|Defined [--note ...]`：
     确认它应继续存在。系统先把实际承载对齐到目标（拉起/停止 guest），
     再落定为该正常状态、清空挂起结论，**配额继续占用**，此后恢复为可正常操作的实例；
   - `vmctl resolve <id> --destroy [--note ...]`：确认它该被清理。
     幂等 `Undefine` 清除承载残留 → 释放配额 → 落 `Destroyed` 墓碑并清空挂起结论。
   - 收口结果同样持久化，重启后查询一致。后端动作或配额释放失败时实例**保持挂起**
     并返回 `backend_failure`/`inconsistent_state`，不会出现"半收口"。

### 幂等语义（重复请求不会出错、不会重复生效）

- `create` 相同 ID **且相同规格** → 返回已存在实例（幂等吸收），不新增记录、不重复预留；
  相同 ID 但**规格不同** → `already_exists` 拒绝。
- `start` 在 `Running`、`stop` 在 `Stopped`、`destroy` 在 `Destroyed` 时 → 幂等空操作成功。
- 其他跨态请求（如 `Defined → Stop`、`Running → Destroy`、`Destroyed → Start`）
  → `illegal_transition` 拒绝，**记录状态与配额保持不变**。

### 可区分的失败原因（`domain.ErrorKind`，可用 `errors.As`/`IsKind` 分支）

| Kind                     | 含义 | CLI 退出码 |
|--------------------------|------|-----------|
| `not_found`              | 实例不存在 | 3 |
| `already_exists`         | ID 冲突（规格不一致的重复创建） | 4 |
| `illegal_transition`     | 非法状态迁移 | 5 |
| `insufficient_resource`  | 配额不足，准入拒绝（含首个不足维度与数值） | 6 |
| `backend_failure`        | 承载后端动作失败 | 7 |
| `inconsistent_state`     | 明确的内部不一致（如配额漂移） | 8 |
| `pending_manual`         | 实例处于待人工处理状态，常规动作被拒（请用 `resolve` 收口） | 9 |
| `invalid_argument`       | 参数非法 | 1 |

### 并发模型（不同实例并行，同实例串行）

编排器使用两级锁：

- **全局屏障 `gate`（RWMutex）**：常规变更取读锁（彼此可并发），
  `Reconcile`/启动恢复取写锁（排他，进行期间变更请求等待——这是唯一会"排队"的场景）；
- **每实例互斥锁 `idLocks[id]`**：同一实例的「读记录 → 判定迁移 → 调后端 → 持久化」
  整体串行，保证同实例并发请求有确定先后、不会互相覆盖。

因此：**对互不相干的多个实例同时发起 start/stop/destroy，它们各自推进、互不排队**；
只有同一个实例上的并发请求才会按到达先后串行。锁序固定为
`gate(RLock) → idLocks[id] → 下层（quota/store/backend）锁`，
对账只取 gate 写锁、不嵌套 per-id 锁，无死锁环。配额池的准入在 per-id 临界区内，
故多实例并发下资源同样不会重复占用或泄漏。

下层（配额池、存储、后端）各自带锁，配额池与存储本身也可独立并发使用。

## 二、配额与准入规则

配额维度：`CPU 毫核 / 内存 MB / 磁盘 MB`，宿主容量在启动时配置。

不变量：

1. `used == 所有预留之和`；`available = capacity - used`，任何维度不允许超额。
2. **准入先于副作用**：`create` 的顺序严格为
   `参数校验 → 查记录 → quota.Reserve → backend.Define → 持久化 Defined`。
   资源不足时在触碰后端/磁盘之前直接返回 `insufficient_resource`，错误中带
   `resource=<维度>(requested, available)`。
3. **失败必回滚**：`backend.Define` 失败 → 释放刚预留的配额；持久化失败 →
   撤销后端定义并释放配额。任何失败路径后 `used` 都回到调用前水平。
4. **重复释放即漂移信号**：`Release` 必须对应一次 `Reserve`，无台账记录时返回错误，
   由编排层包装成 `inconsistent_state` 暴露，而不是静默忽略。
5. **销毁才释放**：`Stopped`（含崩溃收敛而来）与 **`PendingManual`（待人工处理）**
   仍持有配额；只有收敛/收口到 `Destroyed` 才释放。
   `vmctl quota` 可查看 capacity/used/available 与逐实例预留台账。
6. **重启即一致（无需先对账）**：`Orchestrator` 在**构造时**就以持久化记录中
   「所有非 Destroyed 记录」自动重建内存台账（等价于 `RestoreFromStore`，重复调用幂等）。
   因此在同一份状态目录上**另起一个新进程**，第一次 `quota`/`ResourceSnapshot`
   看到的 used/逐实例预留就与真正持有资源的实例集合一致，不会出现
   「实例明明占着、新进程却显示为零或漏算」。重建结果超容量时明确报错而非截断。

#### 待人工处理实例是否计入占用？（明确口径）

**计入，且按规格冻结保留，直到人工收口。**

权威占用集合定义为：**持久化记录中所有 `state != Destroyed` 的实例**，即
`Defined / Running / Stopped / PendingManual` 全部计入；`Destroyed` 墓碑不计入。
理由：挂起只表示"系统不敢自动决定"，并不代表资源已被释放——其后端承载残留可能仍在、
规格仍然有效。若此时把它从台账摘出再把容量发给新实例，等于在没回收任何真实资源的
情况下超额分配；而跨进程对账时也无法稳定解释"记录在、承载可能在、占用却是 0"。
所以保守地**冻结保留**最安全且任何进程查询都自洽：

- `resolve --keep`：落定为 Running/Stopped/Defined，**继续计入占用**；
- `resolve --destroy`：清除承载并释放配额，**随即不再计入**。

唯一例外是**无记录的后端孤儿对象**（后端存在、任何记录都不存在）：它规格未知、
归属不明（可能属于另一个编排器实例），无法为其重建一条预留，故**不计入本编排器占用**，
只在对账 Report 的 `Orphans/Quarantine` 中列出、保留残留，等待人工另行裁决。

## 三、异常重启与对账语义

编排器只持久化记录、后端承载事实独立存在，因此「后端动作成功」与「状态落盘」
之间的崩溃会产生有限的几种组合。`reconcile.Run`（或 `vmctl reconcile`、
`Orchestrator.Reconcile`）逐条比对，产出三类结论：

- **Matched**：记录与事实一致，不动。
- **Recovered**：偏差可唯一解释，执行确定动作并持久化收敛。
- **Quarantined**：无法自动判定。对**有记录**的实例，把结论**持久化为
  PendingManual**（带规则/原因/原状态/事实），等待人工 `resolve`；
  对**无记录孤儿**仅列入 `Orphans` 保留残留。二者都不覆盖正常状态、不删除残留、不擅自拉起。

收敛矩阵（详见 `internal/reconcile/reconcile.go` 顶部注释）：

| 记录状态 | 后端事实 | 结论 | 动作 |
|----------|----------|------|------|
| Defined  | stopped  | Matched | — |
| Defined/Stopped | **running** | Recovered→**Running** | 采纳：`Start` 副作用已生效而 Running 未落盘 |
| Running  | running  | Matched | — |
| Running  | **stopped** | Recovered→**Stopped** | guest 崩溃/被杀（异常退出），确定收敛，配额仍占用 |
| Defined/Stopped | **absent** | Recovered→**Destroyed** | 半完成销毁：Undefine 已生效而终态未落盘，台账重建释放配额 |
| Destroyed | absent  | Matched（墓碑） | — |
| Destroyed | 残留 stopped/running | Recovered | 幂等 `Undefine` 清除残留，保持墓碑 |
| Running  | **absent** | **Quarantined→持久化 PendingManual** | 宿主疑似被重建/外部清理，无法判定，不自动重建，配额冻结 |
| PendingManual | 任意事实 | **Quarantined（维持挂起）** | 等待人工收口；不判定、不改记录、不碰残留 |
| （无记录） | 后端独有对象 | **Quarantined（Orphan）** | 归属不明，保留残留并列入 `Orphans`，绝不静默删除 |

配额台账在对账后以「**所有非 Destroyed 记录**（含 PendingManual）」为权威集合重建：
收敛到 Destroyed 的实例其配额随之释放；挂起实例冻结保留（见上一节口径）；
无记录孤儿无法计入。台账重建若超过宿主容量，结论中标记 `quota-ledger`
隔离且 `QuotaConverged=false`。

> 所有权假设：默认由**单个编排器实例**拥有一个后端目录。后端独有残留被视为
> 归属不明而隔离（而非删除），因为它可能属于另一个编排器实例或仍承载重要数据。

## 四、后端

### SimHandle（`internal/backend/sim.go`）

进程内模拟，零外部依赖；支持 `FaultHooks` 在 Define/Start/Stop/Undefine 前注入故障，
用于压测「副作用失败」与各类恢复组合。使用 `--sim` 选择。

### ProcHandle（`internal/backend/proc.go`，非 Windows）

用真实子进程承载实例：

- `Define`：创建承载目录并原子写入 `spec.json`；
- `Start`：拉起 guest（默认同目录 `vmguest`，缺失时回退 `sleep 86400`），
  独立进程组并写入 `pid`；
- `Stop`：先 SIGTERM 并等待 `StopGrace`（默认 5s），超时 SIGKILL，再清理 pid；
- `Inspect`：**只信事实**——读目录与 pid、用信号 0 探活。
  因此用一个全新句柄（模拟编排器重启）也能识别：仍存活、已优雅停止、
  以及「pid 存在但探活失败」的**异常崩溃**。

`cmd/vmguest` 是配套 guest：常驻并打印心跳，收到 SIGTERM/SIGINT 优雅退出，
被 SIGKILL 时即表现为崩溃，供对账识别。

## 五、验证方法

```bash
# 全部单元测试 + 竞态检测（含真实子进程集成测试）
go test ./... -race -count=1

# 只看某一组
go test ./internal/orchestrator/ -race -v   # 状态机/幂等/准入/回滚/并发/挂起收口
go test ./internal/reconcile/ -v            # 崩溃窗口对账矩阵（含持久化挂起）
go test ./internal/backend/ -v              # 真实子进程：探活、跨重启、强杀、优雅退出

# 连跑多轮确认稳定
go test ./... -race -count=10
```

测试在 `-v` 日志中打印：

- **操作序列**：每次 Create/Start/Stop/Destroy 的状态迁移、准入预留/拒绝；
- **资源快照**：每步 used/available 与逐实例预留，最终断言全部释放、零泄漏；
- **对账结论**：每条 `record/actual/outcome/after/action` 以及 orphans、
  quarantine、台账是否收敛。

测试覆盖要点：

- 完整生命周期与终态墓碑；同态幂等、非法迁移可区分、失败后状态不被覆盖；
- 配额：单维超额、逐维耗尽定位、拒绝时零副作用零占用、销毁后可再准入；
- 后端 Define 故障时配额回滚、无记录残留、同 ID 可立即重试；
- 40 个 goroutine 并发创建同一 ID：一条记录、一份预留；
- 12 实例 × 4 轮 start/stop/get 混合并发后串行销毁，配额严格守恒；
- **待人工处理全链路**：判定 PendingManual → 换新进程/再重启仍可见且带原因 →
  start/stop/destroy/create 全部以 `pending_manual` 拒绝（与 not_found 等可区分）→
  人工 `resolve --keep`/`--destroy` → 实例状态与逐实例占用同步更新（两条收口路径各一条用例）；
- **多实例并发互不阻塞**：一个实例卡在后端时另一个实例仍能推进；同实例 30 路
  start/stop 并发后状态确定、恰好一份预留；16 实例 × 5 轮混合并发全程 used 守恒；
- **跨进程占用一致性**：Defined/Running/Stopped/PendingManual/Destroyed 混合后，
  两个全新进程首次查询的 used 与逐实例预留都与持有集合一致；
- 对账：guest 崩溃、Start 崩溃窗口、半完成销毁、Running 但后端消失（持久化挂起）、
  孤儿残留（保留不删）、墓碑残留（幂等清理）、混合矩阵 + 配额守恒。

### CLI 端到端

```bash
go build -o bin/vmctl ./cmd/vmctl && go build -o bin/vmguest ./cmd/vmguest

bin/vmctl create web --cpu 1000 --mem 512 --disk 1024
bin/vmctl start web
bin/vmctl quota
# 模拟 guest 被宿主强杀：
kill -9 $(sed 's/[^0-9]//g' var/run/web/pid)
bin/vmctl reconcile      # Running + guest 已退出 -> 收敛 Stopped
bin/vmctl list
bin/vmctl destroy web
bin/vmctl reconcile --json
```

待人工处理与人工收口（以"记录 Running、后端承载整体消失"为例）：

```bash
# 制造分歧后对账：结论持久化为 PendingManual
bin/vmctl reconcile
bin/vmctl get web                       # 新进程也能看到 PendingManual 与原因
bin/vmctl quota                         # 挂起实例仍冻结计入 reservations
bin/vmctl start web ; echo $?           # -> 9，stderr 输出 kind=pending_manual
# 二选一收口：
bin/vmctl resolve web --keep --target Stopped --note "ops-checked"
bin/vmctl resolve web --destroy --note "ops-confirmed-wipe"
```

标志可放在位置参数前后任意位置（CLI 内部会重排）。失败时 stderr 输出
`kind=<类别>` 并以不同退出码区分原因，便于脚本判定。

## 六、持久化、崩溃安全与兼容范围

- 每条记录独立文件 + `临时文件 → fsync → rename`，单条写入原子；
  同 ID 写入由互斥锁串行，List 忽略临时文件。
- 状态迁移是「后端动作 + 记录落盘」两步，中间崩溃由对账矩阵覆盖；
  系统不假设两步事务，而是把所有可达中间态都做成可收敛或可挂起。
- Destroyed 以**墓碑记录**保留（便于幂等与审计）；如需物理清理记录可另行删除文件。

**对既有记录与既有结果的兼容范围：**

- 记录 JSON 是**向后兼容追加字段**：新增的 `pending` 字段为 `omitempty`，
  旧版本写出的记录（无该字段）可直接被新版本读取，正常实例行为完全不变。
- 既有四种正常状态（Defined/Running/Stopped/Destroyed）及其迁移、幂等、
  准入、回滚语义保持不变；原有销毁结果（Destroyed 墓碑 + 释放配额 + 后端清除）
  仍被识别为 Matched，墓碑残留仍被幂等清理。
- 占用口径的唯一行为差异：**旧版本把"无法判定的有记录实例"从台账摘出，
  新版本改为持久化 PendingManual 并冻结计入占用**（理由见配额章节）。
  新版本首次对账会把这类实例落为 PendingManual，占用可能因此较旧版本"多出"
  这部分冻结预留——这是有意的安全口径，经人工 `resolve --destroy` 后即释放。
- 无记录孤儿对象的处理不变：只列出、保留、不删除，且不计入本编排器占用。
- 单编排器/单进程对同一后端目录的**所有权假设不变**；本版本让"多个脚本/运维
  在同一编排器进程内并发"安全且不同实例并行，但多个独立进程同时**写**同一
  状态目录仍不是受支持的拓扑（读：任意多个新进程查询均一致）。
