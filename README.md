# 本地虚拟机生命周期编排器（Go）

一套可解释的本地 VM 生命周期编排流程：实例可以由**本地虚拟化能力（模拟后端）**
或**真实子进程（进程级后端）**承载。系统保证：

- 并发/重复请求下不产生重复实例、不泄漏资源、不互相覆盖状态；**不同实例的变更真正并行，
  同一实例的变更严格按到达顺序串行**；
- 宿主配额在产生任何后端副作用**之前**完成准入，占用与释放始终成对、守恒；
- 异常重启后对「持久化记录 / 后端承载事实 / 配额台账」三方对账，
  能确定的偏差自动收敛到唯一状态，不能确定的**持久化挂起到「待人工处理（ManualReview）」
  并保留判定原因**，绝不静默覆盖；换进程/宿主重启后该结论仍然可见，直到人工显式收口。

## 目录结构

```
internal/domain/       状态机（含 ManualReview 冻结态与 Review 留痕）、资源模型、可区分类别的错误
internal/quota/        宿主配额池：预留/释放台账，容量准入，重启重建
internal/backend/      承载后端：SimHandle（模拟）、ProcHandle（真实子进程）
internal/store/        FileStore：每实例一个 JSON，临时文件+rename 原子写
internal/orchestrator/ 编排器：每实例锁串行化迁移 + 准入 + 回滚 + 人工收口（resolve）
internal/reconcile/    重启对账：Matched / Recovered / Quarantined（结论持久化为 ManualReview）
cmd/vmctl/             命令行入口（含 resolve / reviews 子命令与退出码 9）
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

   对账发现无法自动判定（Running 但承载消失 / 无记录的后端孤儿）
   ───────────────────────────────────────────────────────────► ManualReview
                                                                   │
                    人工 resolve adopt ─► Defined / Running / Stopped（资源继续占用）
                    人工 resolve discard ─► Destroyed（清承载、释放资源）
```

合法迁移（`internal/domain/types.go`）：

| From      | To                | 说明 |
|-----------|-------------------|------|
| Defined   | Running           | 启动 |
| Defined   | Destroyed         | 已定义实例可直接销毁（释放配额） |
| Running   | Stopped           | 正常停止 |
| Stopped   | Running           | 再次启动 |
| Stopped   | Destroyed         | 销毁并释放配额 |
| 存活态（仅由对账触发） | **ManualReview** | 无法自动判定时**持久化挂起**，记录上带 `Review` 原因与现场 |
| ManualReview | Defined / Running / Stopped | 人工 `resolve adopt`：确认继续存在，承载被收敛到目标态，**资源继续占用** |
| ManualReview | Destroyed      | 人工 `resolve discard`：确认清理，幂等 Undefine 承载并释放资源 |
| Destroyed | —（终态）          | 墓碑保留，不再迁移 |

**ManualReview 是冻结态**：它不参与任何常规状态机迁移。对处于该态的实例发起
`create / start / stop / destroy` 一律返回 `manual_review_pending`（CLI 退出码 9），
拒绝原因与「实例不存在 / 非法迁移 / 资源不足」明确可分；系统不会用别的状态把它覆盖掉，
也不会顺手清理实际承载残留。唯一出路是人工通过
`resolve adopt` / `resolve discard` 显式收口。

### ManualReview 记录上的留痕字段（`domain.Review`）

| 字段 | 含义 |
|------|------|
| `reason` | 判定原因类别：`running_missing_backend`（记录 Running 但承载整体消失）、`orphan_backend`（后端独有对象，无记录） |
| `record_state` / `actual_state` | 判定瞬间的记录状态与后端事实（如 Running / absent） |
| `detail` | 人类可读说明 |
| `raised_at` | 判定时间（UTC） |
| `spec_known` | 规格是否可知；决定该实例能否计入占用台账（见第二节口径） |

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
| `illegal_transition`     | 非法状态迁移（含对非 ManualReview 实例调用 resolve） | 5 |
| `insufficient_resource`  | 配额不足，准入拒绝（含首个不足维度与数值） | 6 |
| `backend_failure`        | 承载后端动作失败 | 7 |
| `inconsistent_state`     | 明确的内部不一致（如配额漂移、记录含非正规格导致无法重建台账） | 8 |
| `manual_review_pending`  | 实例处于待人工处理冻结态，常规变更被拒，需先 resolve | **9** |
| `host_overcommitted`     | 重建占用已超过当前容量（如超大残留），**仅拒绝会新增占用的准入**；查询/收口不受影响 | **10** |
| `invalid_argument`       | 参数非法 | 1 |

### 并发模型

- **不同实例并行**：编排器为每个实例 id 维护一把互斥锁（`keyLocks`）。实例 A 的
  start/stop/destroy 全程不会阻塞实例 B 的任何变更；后端/存储/配额池自身也支持并发。
- **同一实例串行**：同一 id 上「读记录 → 判定迁移 → 调后端 → 持久化」整体在一把
  实例锁内完成，请求按到达顺序推进，不会互相覆盖、不会出现半成品迁移。
- **会退化成全局排队的唯一场景**：进程启动时的 `RestoreFromStore` 与显式 `Reconcile`
  是「全表」动作（要扫描全部记录、重建整张台账），它们持有一把独占的管理屏障
  （`adminMu` 的写锁）；期间新变更需等待。普通变更只持该屏障的读锁，因此彼此仍并行。
- 锁序固定为 `adminMu → 实例锁 → 下层（store/backend/quota）锁`，无环路、无死锁。

## 二、配额与准入规则

配额维度：`CPU 毫核 / 内存 MB / 磁盘 MB`，宿主容量在启动时配置。

不变量：

1. `used == 所有预留之和`，`available = capacity - used`。**正常准入绝不允许把 used 推过
   capacity**；但"既有事实重建出来的占用"可能已经超过容量（见下方超容量口径），此时
   `used` 如实反映事实、`available` 可能为负，并以 `OverCommit` 标记暴露——超容量是
   一种对账结论/告警，而不是台账不自洽（台账仍然是"记录集合 == 预留集合"）。
2. **准入先于副作用**：`create` 的顺序严格为
   `参数校验 → 查记录 → quota.Reserve → backend.Define → 持久化 Defined`。
   资源正常不足时在触碰后端/磁盘之前直接返回 `insufficient_resource`，错误中带
   `resource=<维度>(requested, available)`；宿主**已超卖**（重建占用已超容量）时，
   任何新增准入返回独立的 `host_overcommitted`，两类原因清楚区分。
3. **失败必回滚**：`backend.Define` 失败 → 释放刚预留的配额；持久化失败 →
   撤销后端定义并释放配额。任何失败路径后 `used` 都回到调用前水平。
4. **重复释放即漂移信号**：`Release` 必须对应一次 `Reserve`，无台账记录时返回错误，
   由编排层包装成 `inconsistent_state` 暴露，而不是静默忽略。
5. **销毁才释放 / 待人工处理继续计入**：`Stopped`（含崩溃收敛而来）仍持有配额；
   **ManualReview 在人工收口前也继续按规格占用**（口径见下）；只有收敛到 `Destroyed`
   或人工 `resolve discard` 才释放。`vmctl quota` 可查看 capacity/used/available
   与逐实例预留台账。
6. **重启重建（跨进程一致的关键）**：配额台账是内存结构，进程退出即消失。
   `RestoreFromStore` 依据**所有持有资源状态的持久化记录**（Defined/Running/Stopped/
   ManualReview，Destroyed 除外）重建内存台账。**重建始终如实落地：即使重建占用超过
   宿主容量也不报错、不截断**（只有记录里出现非正规格这类真正非法的数据才报错），
   超容量由快照上的 `OverCommit` / 对账报告上的 `CapacityExceeded` 作为结论暴露。
   **CLI 的每个子命令在执行前都会先跑一次 RestoreFromStore**，因此无论何时、新起几个
   进程去 `quota / get / list / reviews / resolve`，看到的占用都等于「真正持有资源的
   记录集合」之和，不会因为超容量而整条命令失败；同时 `resolve` 也始终可用于把超大
   残留收口、把占用消化回容量以内。作为库嵌入时，应在编排器启动后、查询/变更前调用
   一次 `RestoreFromStore`。

### 重建占用超过当前容量（务必知晓，本次修正的口径）

触发条件：对账时出现一个**没有任何记录对应**、但后端残留自称的规格超过当前整机容量
的孤儿；或更一般地，任何使「持有资源记录集合」重建总额超过当前容量的已落盘情况
（也包括每个对象单独都放得下、但合计超额的情形）。

修正前，这类情况会让 `quota.Rebuild` 直接报错，内存台账无法落地，于是之后每个 CLI
子命令都在开头的 `RestoreFromStore` 上以 `inconsistent_state`（退出码 8）退出，
`list / get / quota / reviews` 全部不可用，连 `resolve` 也被同一错误挡下，只能到磁盘
上手工删文件。修正后的统一口径：

- **台账如实、查询不挂**：`Rebuild`/`RestoreFromStore` 始终把权威集合（所有持有资源
  且规格可知的记录）落地，`used == Σ 规格`、`available = capacity - used`（可能为负）。
  快照新增 `over_commit`（bool）与 `exceeded`（超出维度列表），自洽且可解释：
  读到的每一份占用都能在记录集合里找到唯一对应实例，不漏算、不重算。
- **超容量只是结论/告警**：对账报告新增 `CapacityExceeded` 与 `ExceededDimensions`
  （不再把 `(quota-ledger)` 塞进 `Quarantine`）；`vmctl reconcile` 仍以退出码 0 成功，
  并在 stderr 给出 WARNING；`vmctl quota` 同样展示 WARNING 与负值 available。
- **人工收口始终可用**：`resolve discard` 幂等清除残留并释放该份占用、落 Destroyed
  墓碑；`resolve adopt` 把实例恢复到 Defined/Running/Stopped，承载被收敛到目标态、
  占用按规格如实保留（它确实还在跑这么大的负载）。收口完成后换任意新进程，
  记录集合与台账仍逐项相等；规格不可读孤儿用 `--claim` 补报规格时，补登记不做容量
  准入（人工确认的是既存事实），收口照样落地。
- **真正会超卖的新建仍被拒绝，但原因独立**：超容量期间一切新增占用（`create`）返回
  `host_overcommitted`（退出码 10），与 `manual_review_pending`(9)、`not_found`(3)、
  `illegal_transition`(5)、`insufficient_resource`(6) 明确区分；容量恢复正常后，
  普通容量不足依旧是 `insufficient_resource`，不会混为一谈。
- **不牵连其它实例**：超容量只作用于"新增准入"这一类操作，其它实例的 `get/list` 与
  `start/stop/destroy`（不新增占用）照常进行。

### 待人工处理实例的占用口径（务必知晓）

台账权威集合 = **状态持有资源的记录**（`domain.State.HoldsResources()`：
Defined / Running / Stopped / **ManualReview** 为真，Destroyed 为假）。

- ManualReview 意味着「承载归属未定、系统不敢自动收」。为防止它占着的资源在裁决前
  被再次分配（超卖），**它继续按规格占用**；人工 `resolve adopt` 后占用保持，
  `resolve discard` 后释放。这样「查询占用」始终自洽、可解释：
  `used == Σ(所有未 Destroyed 且规格可知的记录的规格)`。
- **唯一例外**：无记录且后端读不出规格的孤儿（如承载目录里没有 `spec.json`）。
  系统不知道它该占多少，无法计入台账；该实例仍持久化为 ManualReview
  （`review.spec_known=false`），并在对账报告 `unaccounted_orphans`、
  `vmctl reviews` 中显式标出。人工 `resolve adopt --claim --cpu .. --mem .. --disk ..`
  补报规格时才把占用补登记；`resolve discard` 则无需释放（本就没登记）。

## 三、异常重启与对账语义

编排器只持久化记录、后端承载事实独立存在，因此「后端动作成功」与「状态落盘」
之间的崩溃会产生有限的几种组合。`reconcile.Run`（或 `vmctl reconcile`、
`Orchestrator.Reconcile`）逐条比对，产出三类结论：

- **Matched**：记录与事实一致，不动。
- **Recovered**：偏差可唯一解释，执行确定动作并持久化收敛。
- **Quarantined**：无法自动判定。对两类**持久**的不可判定偏差，结论会**随实例记录
  落盘为 ManualReview**（跨进程/重启可见、常规变更被拒、等待人工收口）；其余报告级
  隔离（如墓碑残留清理失败、未知后端状态）只写入本次 `Report.Quarantine`，
  **不覆盖记录、不删除残留、不擅自拉起**。

收敛矩阵（详见 `internal/reconcile/reconcile.go` 顶部注释）：

| 记录状态 | 后端事实 | 结论 | 动作 |
|----------|----------|------|------|
| Defined  | stopped  | Matched | — |
| Defined/Stopped | **running** | Recovered→**Running** | 采纳：`Start` 副作用已生效而 Running 未落盘 |
| Running  | running  | Matched | — |
| Running  | **stopped** | Recovered→**Stopped** | guest 崩溃/被杀（异常退出），确定收敛，配额仍占用 |
| Defined/Stopped | **absent** | Recovered→**Destroyed** | 半完成销毁：Undefine 已生效而终态未落盘，台账重建释放配额 |
| Destroyed | absent  | Matched（墓碑） | — |
| Destroyed | 残留 stopped/running | Recovered | 幂等 `Undefine` 清除残留，保持墓碑；清理失败则仅报告隔离 |
| Running  | **absent** | **Quarantined→持久化 ManualReview** | 宿主疑似被重建/外部清理，无法判定；不自动重建，**资源继续占用** |
| （无记录） | 后端独有对象 | **Quarantined（Orphan）→新建 ManualReview 记录** | 归属不明，**保留残留、新建待人工处理记录**；规格可知则计入占用，绝不静默删除 |
| ManualReview | 任意事实 | Quarantined（保持） | 冻结态原样回报，重复对账**不覆盖、不动作**，仍等人工收口 |

配额台账在对账后以「**持有资源状态**」的记录为权威集合**重建**：
收敛到 Destroyed 的实例配额随之释放；ManualReview（含新建的孤儿记录，规格可知时）
**继续计入**；只有无记录且读不出规格的孤儿不进台账并显式列入 `UnaccountedOrphans`。
台账重建**即使超过宿主容量也照常收敛**（`QuotaConverged=true`、台账与记录自洽），
并以 `CapacityExceeded=true` + `ExceededDimensions` 作为结论/告警暴露；它不阻断任何
查询或人工收口，只让新增准入在此期间得到 `host_overcommitted`（见第二节超容量口径）。
仅当权威集合本身非法（如记录含非正规格）时重建才失败并把 `(quota-ledger)` 列入
`Quarantine`。

### 人工收口（resolve）

`vmctl reviews` 列出全部 ManualReview 实例（含 reason/record/actual/raised 时间），
随后由运维二选一显式收口：

```bash
# 确认它应继续存在：承载被收敛到 --to 目标态，资源继续占用
vmctl resolve adopt <id> --to Running            # 或 Defined / Stopped
vmctl resolve adopt <id> --to Stopped --note "host rebuilt, keep"
# 孤儿残留读不出规格时，先补报规格再认领（此时才补登记占用）
vmctl resolve adopt <id> --to Stopped --claim --cpu 1000 --mem 512 --disk 1024

# 确认它应被清理：幂等 Undefine 承载、释放占用、落为 Destroyed 墓碑
vmctl resolve discard <id> --note "wipe confirmed"
```

收口可审计：收口后记录上的 `review` 被清空，`last_event` 记录
`manual-resolved adopt->... / discard->Destroyed` 以及原判定 reason 与 note。

> 所有权假设：默认由**单个编排器实例**拥有一个后端目录。后端独有残留被视为
> 归属不明而挂起（而非删除），因为它可能属于另一个编排器实例或仍承载重要数据。

## 四、后端

### SimHandle（`internal/backend/sim.go`）

进程内模拟，零外部依赖；支持 `FaultHooks` 在 Define/Start/Stop/Undefine 前注入故障，
用于压测「副作用失败」与各类恢复组合（运行期可用 `SetHooks` 安全地动态替换，
也可注入阻塞钩子来实证不同实例互不排队）。使用 `--sim` 选择。

### ProcHandle（`internal/backend/proc.go`，非 Windows）

用真实子进程承载实例：

- `Define`：创建承载目录并原子写入 `spec.json`；
- `Start`：拉起 guest（默认同目录 `vmguest`，缺失时回退 `sleep 86400`），
  独立进程组并写入 `pid`；
- `Stop`：先 SIGTERM 并等待 `StopGrace`（默认 5s），超时 SIGKILL，再清理 pid；
- `Inspect`：**只信事实**——读目录与 pid、用信号 0 探活，并读取 `spec.json`
  （使孤儿残留也能带上规格进入对账）。
  因此用一个全新句柄（模拟编排器重启）也能识别：仍存活、已优雅停止、
  以及「pid 存在但探活失败」的**异常崩溃**；缺 `spec.json` 的残留标记为规格不可读。

`cmd/vmguest` 是配套 guest：常驻并打印心跳，收到 SIGTERM/SIGINT 优雅退出，
被 SIGKILL 时即表现为崩溃，供对账识别。

## 五、验证方法

```bash
# 全部单元测试 + 竞态检测（含真实子进程集成测试）
go test ./... -race -count=1
# 连跑多轮确认稳定（无竞态、无 flaky）
go test ./... -race -count=10

# 只看某一组
go test ./internal/orchestrator/ -race -v   # 状态机/幂等/准入/回滚/并发/人工收口/跨进程占用
go test ./internal/reconcile/ -v            # 崩溃窗口对账矩阵（含持久化 ManualReview）
go test ./internal/backend/ -v              # 真实子进程：探活、跨重启、强杀、优雅退出
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
- **不同实例并行的实证**：一个实例在后端被阻塞时，另两个实例的操作立即完成
  （`TestDifferentInstancesDoNotBlock`）；同实例 24×2 并发 start/stop 严格串行、
  最终唯一确定（`TestSameInstanceOpsSerialize`）；20 实例 × 3 轮交错混合并发后
  逐条核对「记录是否持有资源 == 台账是否有预留」，全部销毁后归零
  （`TestManyInstancesConcurrentConservation`）；
- **待人工处理整条链路**：判定挂起 → 换新进程/再换进程仍可见 ManualReview 与原因 →
  start/stop/destroy 被拒且为 `manual_review_pending`（退出码 9）→ 人工 adopt 恢复
  Running/Stopped 或 discard 清理 → 实例记录与占用同步更新（`TestManualReviewFullFlowAdopt`、
  `TestManualReviewDiscardFrees`、`TestManualReviewOrphanAdopt`）；
- **跨进程占用一致**：只做 `RestoreFromStore` 的全新进程，对 Running/Stopped/Defined/
  ManualReview/Destroyed 混合集合读到的 used 与事实集合逐次一致，连起三个新进程读数稳定
  （`TestCrossProcessUsageConsistency`）；
- 无 `spec.json` 孤儿不进台账，无 claim 的 adopt 被拒，带 `--claim` 补报后才登记
  （`TestResolveUnknownSpecOrphan`）；
- **重建占用超过当前容量的整条链路（本次修复）**：超大孤儿落盘 → 对账报
  `CapacityExceeded` 且台账照常收敛 → 换新进程后 `list/get/reviews/quota` 全部可用、
  预留集合与记录集合逐项相等（`available` 为负、`over_commit=true`）→ 新增 `create`
  被 `host_overcommitted`（退出码 10）拒绝，而 not_found/illegal_transition/
  manual_review_pending/insufficient_resource 原因各自可分、不相关实例不受牵连 →
  人工 `discard` 释放或 `adopt` 保留 → 再换进程记录与占用仍一致（不漏算不重算），
  见 `TestOversizeOrphanDiscardCrossProcess`、`TestOversizeOrphanAdoptKeepsReservation`、
  `TestTotalExceedsButEachFits`（单对象不超、合计超）、`TestOversizeClaimAdoptUnknownSpec`
  （超容量下补报认领）、reconcile 层 `TestReconcileCapacityExceeded`；
- 对账：guest 崩溃、Start 崩溃窗口、半完成销毁、Running 但后端消失（持久化挂起）、
  孤儿残留（落盘挂起、保留不删）、墓碑残留（幂等清理）、重复对账保持冻结、混合矩阵 + 配额守恒。

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

# 模拟宿主被重建（承载目录整体消失，但记录仍说 Running）：
rm -rf var/run/web
bin/vmctl reconcile          # -> 持久化 ManualReview（reason=running_missing_backend）
bin/vmctl reviews            # 列出所有待人工处理实例及原因
bin/vmctl start web          # 被拒：kind=manual_review_pending，退出码 9
bin/vmctl get web            # 新进程也能看到 ManualReview 与原因
bin/vmctl quota              # 该实例仍计入占用
bin/vmctl resolve adopt web --to Running --note "host rebuilt, keep"
# 或确认清理：
bin/vmctl resolve discard web --note "wipe confirmed"
bin/vmctl quota              # discard 后占用释放

# 模拟"无记录、但自称规格超过整机容量"的后端残留（proc 后端可直接造承载目录）：
mkdir -p var/run/huge
echo '{"resources":{"cpu_millis":99999,"memory_mb":99999,"disk_mb":99999},"image":"x"}' \
  > var/run/huge/spec.json
bin/vmctl reconcile          # 孤儿落盘 ManualReview；输出 capacityExceeded=true，退出码仍是 0
bin/vmctl list               # 仍可用：huge 为 ManualReview（修复前这些查询会直接报内部不一致）
bin/vmctl get huge           # 仍可见判定原因
bin/vmctl reviews            # 待人工处理清单照常列出
bin/vmctl quota              # used 如实超过 capacity、avail 为负，并打印 OVER-COMMIT WARNING
bin/vmctl create other --cpu 1 --mem 1 --disk 1
# 被拒：kind=host_overcommitted，退出码 10（区别于资源不足 6 / 待人工处理 9）
bin/vmctl resolve adopt huge --to Stopped --note "keep, oversized"  # 收口可用：恢复正常态、占用保留
# 或：bin/vmctl resolve discard huge --note "wipe"                  # 收口可用：清残留、释放占用
bin/vmctl quota              # discard 后超容量解除；adopt 后占用如实保留
```

标志可放在位置参数前后任意位置（CLI 内部会重排）。失败时 stderr 输出
`kind=<类别>` 并以不同退出码区分原因，便于脚本判定（ManualReview 为 9；
重建占用超过容量而拒绝新增准入时为 `host_overcommitted`=10，此时查询与
resolve 仍正常可用）。

## 六、持久化与崩溃安全说明

- 每条记录独立文件 + `临时文件 → fsync → rename`，单条写入原子；
  同 ID 写入由实例锁串行，List 忽略临时文件。
- 状态迁移是「后端动作 + 记录落盘」两步，中间崩溃由对账矩阵覆盖；
  系统不假设两步事务，而是把所有可达中间态都做成可收敛或可挂起。
- 「待人工处理」结论不是只活在内存/本次报告里：它就是实例记录本身的
  `state=ManualReview` + `review` 字段，随同一条 JSON 原子落盘，
  因此换进程、宿主重启后依然可见、可审计。
- Destroyed 与收口 discard 都以**墓碑记录**保留（便于幂等与审计）；
  如需物理清理记录可另行删除文件。

### 兼容范围（对既有记录与既有销毁结果）

- **既有记录文件无需迁移**：旧版本写下的 Defined/Running/Stopped/Destroyed 记录
  没有 `review` 字段，反序列化为 `review=null`，行为与旧版一致；新版本只是**新增**
  了一个状态与可选字段，不改动既有字段语义。
- **既有 Destroyed 墓碑**：仍然不占资源、`destroy` 幂等；对账时若墓碑下有后端残留，
  依旧幂等 `Undefine` 清理（与旧版一致），只有清理失败才仅在报告中隔离。
- **既有孤儿/「Running 但后端消失」结论的语义变化（请注意）**：旧版这两类只在对账
  **报告**里隔离、且从配额台账中摘出；新版改为**持久化 ManualReview 且继续计入占用**。
  这是一处有意的口径收紧：保证「待人工处理」跨进程可见、且其资源在裁决前不被再次分配。
  受影响的自动化若依赖旧报告字段 `After="quarantined-*"`，需改为识别
  `After="ManualReview"` 与记录上的 `state/review`；占用读数会把这些实例包含进来。
- **台账口径**统一收敛为 `State.HoldsResources()`：`RestoreFromStore` 与对账重建
  使用同一权威集合，避免「启动恢复」与「对账重建」两套口径不一致。
- **重建超容量的行为变化（本次修复，请注意）**：旧版 `quota.Rebuild`/`RestoreFromStore`
  在重建占用超过容量时直接返回错误，编排器把它包成 `inconsistent_state`；这会导致
  超大残留落盘后所有 CLI 子命令（含查询与 resolve）全部失败。新版改为**台账如实落地 +
  标记告警**：`quota.Snapshot` 新增 `over_commit` 与 `exceeded` 字段；`reconcile.Report`
  新增 `capacity_exceeded` 与 `exceeded_dimensions`，且超容量不再产生
  `Quarantine` 中的 `(quota-ledger)` 项；`LedgerRebuilder` 回调签名相应变为
  `func(alive) (quota.Snapshot, string, error)`。新增错误类别
  `host_overcommitted`（CLI 退出码 **10**），只用于"宿主已超卖时拒绝新增准入"。
  记录文件格式没有变化，**既有落盘记录无需迁移**；依赖旧的"超容量 → 命令报错/退出码 8"
  行为的脚本需要改为识别告警字段与退出码 10。
- 作为库嵌入的调用方：旧代码无需改构造函数；但要获得「新进程占用即一致」的保证，
  需在启动后调用一次 `RestoreFromStore`（CLI 已默认在每个子命令前执行）。
