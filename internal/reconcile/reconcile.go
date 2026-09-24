// Package reconcile 在管理器异常重启后，将持久化记录与后端事实对账：
// 能确定的自动收敛到确定状态；无法判定的标记 Failed(Inconsistent) 并说明原因，
// 绝不静默覆盖；最后以实际记录重算配额，保证占用不漂移。
package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
)

// ItemConclusion 是单实例对账结论。
type ItemConclusion string

const (
	// ConclusionConfirmed：记录与事实一致，无需变更。
	ConclusionConfirmed ItemConclusion = "confirmed"
	// ConclusionRecovered：依据确定规则自动收敛。
	ConclusionRecovered ItemConclusion = "recovered"
	// ConclusionMarked：无法判定，已标记人工介入。
	ConclusionMarked ItemConclusion = "marked"
	// ConclusionOrphan：后端有残留但无记录，仅报告不处置。
	ConclusionOrphan ItemConclusion = "backend-orphan"
)

// Item 是一条对账明细。
type Item struct {
	ID         string         `json:"id"`
	Before     string         `json:"before"`
	After      string         `json:"after"`
	Conclusion ItemConclusion `json:"conclusion"`
	Detail     string         `json:"detail"`
}

// QuotaView 是报告中配额用量的可打印视图。
type QuotaView struct {
	Total model.Resources `json:"total"`
	Used  model.Resources `json:"used"`
}

// Report 是一次对账的完整结论。
type Report struct {
	Items       []Item    `json:"items"`
	Orphans     []string  `json:"orphans"`
	QuotaBefore QuotaView `json:"quotaBefore"`
	QuotaAfter  QuotaView `json:"quotaAfter"`
}

func rstr(r model.Resources) string {
	return fmt.Sprintf("cpu=%dm mem=%dMiB disk=%dMiB", r.CPUMillis, r.MemoryMiB, r.DiskMiB)
}

// String 输出人类可读的对账报告。
func (r *Report) String() string {
	var b strings.Builder
	b.WriteString("==== reconcile report ====\n")
	b.WriteString("[operation sequence]\n")
	items := append([]Item(nil), r.Items...)
	sort.SliceStable(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	if len(items) == 0 {
		b.WriteString("(none)\n")
	}
	for _, it := range items {
		arrow := it.Before
		if it.Before != it.After {
			arrow += " -> " + it.After
		}
		fmt.Fprintf(&b, "  - %s: %s | %s | %s\n", it.ID, arrow, it.Conclusion, it.Detail)
	}
	b.WriteString("[backend orphans]\n")
	if len(r.Orphans) == 0 {
		b.WriteString("(none)\n")
	} else {
		for _, o := range r.Orphans {
			fmt.Fprintf(&b, "  - %s: backend residue without record (not auto-removed)\n", o)
		}
	}
	fmt.Fprintf(&b, "[quota] before used: %s (total %s)\n", rstr(r.QuotaBefore.Used), rstr(r.QuotaBefore.Total))
	fmt.Fprintf(&b, "[quota] after  used: %s (total %s)\n", rstr(r.QuotaAfter.Used), rstr(r.QuotaAfter.Total))
	b.WriteString("==========================")
	return b.String()
}

// Reconciler 执行重启后对账。
type Reconciler struct {
	mgr *orchestrator.Manager
}

// New 创建对账器。
func New(mgr *orchestrator.Manager) *Reconciler { return &Reconciler{mgr: mgr} }

// decision 描述一条对账判定。
type decision struct {
	target model.State
	reason string
	how    ItemConclusion
}

// decide 是对账的核心确定性决策表：输入(记录状态, 后端事实)，输出确定结论。
//
// 设计原则：
//   - 后端事实明确且与操作意图一致 -> 自动收敛（recovered）。
//   - 后端事实与记录矛盾、无法判断运行实体归属 -> 标记 Failed（marked），绝不静默改写。
//   - 已终态 Destroyed 但后端仍有残留 -> 标记 Failed（泄漏的事实必须暴露）。
func decide(state model.State, info backend.RuntimeInfo) decision {
	running := info.Status == backend.StatusRunning
	defined := info.Status == backend.StatusDefined
	gone := info.Status == backend.StatusGone
	switch state {
	case model.StatePending:
		switch {
		case gone:
			// Define 前崩溃：没有任何后端事实，确定地终结并释放配额。
			return decision{model.StateDestroyed, "crashed before backend define; no backend residue", ConclusionRecovered}
		case defined:
			// Define 已完成、结果未落盘：确定地推进到 Stopped。
			return decision{model.StateStopped, "backend define found after restart; converge Pending -> Stopped", ConclusionRecovered}
		default:
			// running/unknown 出现在 Pending：事实无法解释。
			return decision{model.StateFailed, "Pending but backend status=" + string(info.Status), ConclusionMarked}
		}
	case model.StateStopped:
		switch {
		case defined:
			return decision{model.StateStopped, "", ConclusionConfirmed}
		case gone:
			return decision{model.StateFailed, "record Stopped but backend definition missing", ConclusionMarked}
		default:
			return decision{model.StateFailed, "record Stopped but backend status=" + string(info.Status), ConclusionMarked}
		}
	case model.StateStarting:
		switch {
		case running:
			return decision{model.StateRunning, "start confirmed after restart", ConclusionRecovered}
		case defined:
			// 启动失败/未完成：定义仍在且无运行实体，确定地回到 Stopped。
			return decision{model.StateStopped, "start did not take effect; converge Starting -> Stopped", ConclusionRecovered}
		default:
			return decision{model.StateFailed, "Starting and backend status=" + string(info.Status), ConclusionMarked}
		}
	case model.StateRunning:
		switch {
		case running:
			return decision{model.StateRunning, "", ConclusionConfirmed}
		case defined:
			// 进程已退出而定义仍在：异常退出事实明确，收敛到 Stopped 并保留原因。
			return decision{model.StateStopped, "runtime process exited unexpectedly before restart", ConclusionRecovered}
		default:
			return decision{model.StateFailed, "record Running but backend status=" + string(info.Status), ConclusionMarked}
		}
	case model.StateStopping:
		switch {
		case defined: // 已无运行实体（无论曾是 running 与否）
			return decision{model.StateStopped, "stop confirmed after restart", ConclusionRecovered}
		case running:
			return decision{model.StateStopping, "still running; stop incomplete", ConclusionMarked}
		default:
			return decision{model.StateFailed, "Stopping and backend status=" + string(info.Status), ConclusionMarked}
		}
	case model.StateDestroying:
		switch {
		case gone:
			return decision{model.StateDestroyed, "destroy confirmed after restart", ConclusionRecovered}
		default:
			return decision{model.StateFailed, "Destroying unfinished, backend status=" + string(info.Status), ConclusionMarked}
		}
	case model.StateFailed:
		// 上次已标记人工介入：不自动翻案；后端事实消失时可说明但维持标记。
		return decision{model.StateFailed, "", ConclusionConfirmed}
	case model.StateDestroyed:
		if gone {
			return decision{model.StateDestroyed, "", ConclusionConfirmed}
		}
		return decision{model.StateFailed, "terminal Destroyed but backend residue status=" + string(info.Status), ConclusionMarked}
	default:
		return decision{model.StateFailed, "unknown recorded state: " + string(state), ConclusionMarked}
	}
}

// Run 对全部记录逐个与后端对账并收敛状态，再重算配额，返回报告。
// 重启后应在任何外部写操作之前调用。
func (rc *Reconciler) Run(ctx context.Context) (*Report, error) {
	bk := rc.mgr.Backend()

	// 记录对账前配额快照。
	instances, qvBefore, err := rc.mgr.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	report := &Report{
		QuotaBefore: QuotaView{Total: qvBefore.Total, Used: qvBefore.Used},
	}

	recordIDs := make(map[string]bool, len(instances))
	ids := rc.mgr.InstanceIDs(ctx)
	for _, id := range ids {
		recordIDs[id] = true
		info, ierr := bk.Inspect(ctx, id)
		if ierr != nil {
			// 连事实都探测不到：不能猜测，标记人工介入。
			state, reason, _ := rc.mgr.RecordState(ctx, id)
			res, cerr := rc.mgr.ConvergeState(ctx, id, model.StateFailed,
				"backend inspect error: "+ierr.Error())
			_ = res
			report.Items = append(report.Items, Item{
				ID: id, Before: string(state), After: string(model.StateFailed),
				Conclusion: ConclusionMarked,
				Detail:     fmt.Sprintf("inspect failed: %v (prev reason: %s); converged=%v", ierr, reason, cerr == nil),
			})
			continue
		}
		state, reason, _ := rc.mgr.RecordState(ctx, id)
		d := decide(state, info)
		item := Item{ID: id, Before: string(state)}
		res, cerr := rc.mgr.ConvergeState(ctx, id, d.target, d.reason)
		if cerr != nil {
			item.After = string(state)
			item.Conclusion = ConclusionMarked
			item.Detail = fmt.Sprintf("converge refused: %v (prev reason: %s)", cerr, reason)
		} else {
			item.After = string(res.After)
			if d.how == ConclusionConfirmed && reason != "" && d.reason == "" {
				// 保留既有人工标记的说明文字。
				item.Detail = reason
			} else {
				item.Detail = d.reason
			}
			item.Conclusion = d.how
		}
		if item.Detail == "" {
			item.Detail = "record matches backend fact"
		}
		report.Items = append(report.Items, item)
	}

	// 发现后端残留（有定义但无任何记录）：只报告，不自动删除/接管。
	backendIDs, err := bk.List(ctx)
	if err != nil {
		return report, fmt.Errorf("list backend: %w", err)
	}
	sort.Strings(backendIDs)
	for _, id := range backendIDs {
		if !recordIDs[id] {
			report.Orphans = append(report.Orphans, id)
		}
	}

	// 以收敛后的记录为唯一基准重算配额。
	_, after, err := rc.mgr.RebuildQuota(ctx)
	if err != nil {
		return report, fmt.Errorf("rebuild quota: %w", err)
	}
	report.QuotaAfter = QuotaView{Total: qvBefore.Total, Used: after}
	return report, nil
}
