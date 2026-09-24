# 本地虚拟机生命周期编排器（Go）

一套可解释的本地 VM 生命周期编排流程：实例可以由**本地虚拟化能力（模拟后端）**
或**真实子进程（进程级后端）**承载。系统保证：

- 并发/重复请求下不产生重复实例、不泄漏资源、不互相覆盖状态；
- 宿主配额在产生任何后端副作用**之前**完成准入，占用与释放始终成对、守恒；
- 异常重启后对「持久化记录 / 后端承载事实 / 配额台账」三方对账，
  能确定的偏差自动收敛到唯一状态，不能确定的**明确隔离并给出原因**，绝不静默覆盖。

## 目录结构

```
internal/domain/       状态机、资源模型、可区分类别的错误
internal/quota/        宿主配额池：预留/释放台账，容量准入，重启重建
internal/backend/      承载后端：SimHandle（模拟）、ProcHandle（真实子进程）
internal/store/        FileStore：每实例一个 JSON，临时文件+rename 原子写
internal/orchestrator/ 编排器：串行化状态迁移 + 准入 + 回滚
internal/reconcile/    重启对账：Matched / Recovered / Quarantined
cmd/vmctl/             命令行入口
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
| `invalid_argument`       | 参数非法 | 1 |

### 并发模型

编排器内一把互斥锁，把「读记录 → 判定迁移 → 调后端 → 持久化」整体串行化；
下层（配额池、存储、后端）各自带锁，锁序固定为 `orchestrator.mu → 下层锁`，
因此不会出现：重复实例、状态互相覆盖、半成品迁移。配额池与存储本身也可独立并发使用。

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
5. **销毁才释放**：`Stopped`（含崩溃收敛而来）仍持有配额；只有收敛到 `Destroyed`
   才释放。`vmctl quota` 可查看 capacity/used/available 与逐实例预留台账。
6. **重启重建**：`RestoreFromStore` 依据非 Destroyed 记录重建内存台账；
   若重建结果超容量，明确报错而不是截断。

## 三、异常重启与对账语义

编排器只持久化记录、后端承载事实独立存在，因此「后端动作成功」与「状态落盘」
之间的崩溃会产生有限的几种组合。`reconcile.Run`（或 `vmctl reconcile`、
`Orchestrator.Reconcile`）逐条比对，产出三类结论：

- **Matched**：记录与事实一致，不动。
- **Recovered**：偏差可唯一解释，执行确定动作并持久化收敛。
- **Quarantined**：无法自动判定，写入 `Report.Quarantine` 并附原因，
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
| Destroyed | 残留 stopped/running | Recovered | 幂等 `Undefine` 清除残留，保持墓碑 |
| Running  | **absent** | **Quarantined** | 宿主疑似被重建/外部清理，无法判定，不自动重建 |
| （无记录） | 后端独有对象 | **Quarantined（Orphan）** | 归属不明，保留残留并列入 `Orphans`，绝不静默删除 |

配额台账在对账后以「未销毁且未被隔离」的记录为权威集合**重建**：
收敛到 Destroyed 的实例其配额随之释放；被隔离的实例从台账中显式摘出等待人工裁决，
避免漂移。台账重建若超过宿主容量，结论中标记 `quota-ledger` 隔离且 `QuotaConverged=false`。

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
go test ./internal/orchestrator/ -race -v   # 状态机/幂等/准入/回滚/并发
go test ./internal/reconcile/ -v            # 崩溃窗口对账矩阵
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
- 对账：guest 崩溃、Start 崩溃窗口、半完成销毁、Running 但后端消失（隔离）、
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

标志可放在位置参数前后任意位置（CLI 内部会重排）。失败时 stderr 输出
`kind=<类别>` 并以不同退出码区分原因，便于脚本判定。

## 六、持久化与崩溃安全说明

- 每条记录独立文件 + `临时文件 → fsync → rename`，单条写入原子；
  同 ID 写入由互斥锁串行，List 忽略临时文件。
- 状态迁移是「后端动作 + 记录落盘」两步，中间崩溃由对账矩阵覆盖；
  系统不假设两步事务，而是把所有可达中间态都做成可收敛或可隔离。
- Destroyed 以**墓碑记录**保留（便于幂等与审计）；如需物理清理记录可另行删除文件。
