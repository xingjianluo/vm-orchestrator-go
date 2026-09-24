// Package reconcile 在进程重启后对"持久化记录 / 后端承载事实 / 配额台账"三方对账。
//
// 崩溃窗口分析（编排层在"后端动作"与"状态落盘"之间崩溃所产生的组合）：
//
//	记录 Stopped/Defined + 后端 running：Start 已成功、Running 未落盘   -> 采纳为 Running
//	记录 Running        + 后端 stopped：guest 异常退出                -> 收敛为 Stopped
//	记录 Defined/Stopped + 后端 absent： Undefine 已成功、Destroyed 未落盘 -> 收敛为 Destroyed 并释放配额
//	记录 Destroyed      + 后端残留：   外部残留/半完成清理            -> 幂等 Undefine 后保持墓碑
//	记录 Running        + 后端 absent：宿主疑似被重建，无法判定       -> 隔离
//	后端独有对象（无记录）：归属无法判定                              -> 隔离为孤儿，绝不静默删除
//
// 所有"隔离"都在 Report.Quarantine 中显式列出并附带原因，绝不静默覆盖。
package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

// Outcome 是单条对账结论。
type Outcome string

const (
	OutcomeMatched     Outcome = "Matched"
	OutcomeRecovered   Outcome = "Recovered"
	OutcomeQuarantined Outcome = "Quarantined"
)

// Verdict 是单个实例的对账结论。
type Verdict struct {
	InstanceID  string
	RecordState string
	ActualState string
	Outcome     Outcome
	Before      string
	After       string
	Action      string
	Detail      string
}

// Report 是一次完整对账的结论。
type Report struct {
	Verdicts []Verdict
	// Orphans 是后端存在但没有任何记录对应的承载残留。
	Orphans []string
	// Quarantine 包含所有需人工处理的 id（含孤儿、矛盾记录、台账问题）。
	Quarantine []string
	// QuotaConverged 表示配额台账已按权威集合重建。
	QuotaConverged bool
	QuotaDetail    string
}

// LedgerRebuilder 以"仍存活实例的配额集合"重建配额台账，返回人类可读的快照描述。
type LedgerRebuilder func(alive map[string]domain.Resources) (string, error)

// Logf 输出对账过程中的操作序列与结论。
type Logf func(format string, args ...any)

// Run 执行一次完整对账并使系统收敛。
func Run(
	ctx context.Context,
	st store.Store,
	h backend.Handle,
	rebuildLedger LedgerRebuilder,
	logf Logf,
) (*Report, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rep := &Report{}

	records, err := st.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconcile: list records: %w", err)
	}
	infos, err := h.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconcile: list backend: %w", err)
	}
	actual := make(map[string]backend.Info, len(infos))
	for _, in := range infos {
		actual[in.ID] = in
	}

	logf("[reconcile] START records=%d backendObjects=%d", len(records), len(actual))

	recordIDs := make(map[string]struct{}, len(records))
	for _, r := range records {
		recordIDs[r.ID] = struct{}{}
		v := Verdict{InstanceID: r.ID, RecordState: string(r.State), Before: string(r.State), After: string(r.State)}
		info, found := actual[r.ID]
		if !found {
			v.ActualState = "absent"
		} else {
			v.ActualState = string(info.Status)
		}
		v = decide(ctx, st, h, r, info, found, v, logf)
		rep.Verdicts = append(rep.Verdicts, v)
		if v.Outcome == OutcomeQuarantined {
			rep.Quarantine = append(rep.Quarantine, r.ID)
		}
		delete(actual, r.ID)
	}

	// 后端独有对象：归属不明，隔离，绝不自动删除。
	for id, info := range actual {
		logf("[reconcile] ORPHAN id=%s actual=%s -> Quarantined (保留残留，等待人工裁决)", id, info.Status)
		rep.Orphans = append(rep.Orphans, id)
		rep.Quarantine = append(rep.Quarantine, id)
		rep.Verdicts = append(rep.Verdicts, Verdict{
			InstanceID:  id,
			RecordState: "(none)",
			ActualState: string(info.Status),
			Outcome:     OutcomeQuarantined,
			Before:      "(none)",
			After:       "quarantined-orphan",
			Action:      "none",
			Detail:      "backend residue without any record: ownership ambiguous, residue preserved",
		})
	}

	// 配额台账权威集合：未销毁且未被隔离的记录。
	alive := make(map[string]domain.Resources)
	for _, r := range records {
		if r.State == domain.StateDestroyed {
			continue
		}
		if rep.isQuarantined(r.ID) {
			continue
		}
		alive[r.ID] = r.Spec.Resources
	}
	if rebuildLedger != nil {
		detail, err := rebuildLedger(alive)
		if err != nil {
			rep.QuotaDetail = "ledger rebuild rejected: " + err.Error()
			logf("[reconcile] QUOTA DRIFT: %s", rep.QuotaDetail)
			rep.Quarantine = append(rep.Quarantine, "(quota-ledger)")
		} else {
			rep.QuotaConverged = true
			rep.QuotaDetail = detail
			logf("[reconcile] quota ledger CONVERGED alive=%d snapshot=%s", len(alive), detail)
		}
	}

	logf("[reconcile] DONE matched=%d recovered=%d quarantined=%d orphans=%d quotaConverged=%t",
		rep.count(OutcomeMatched), rep.count(OutcomeRecovered), rep.count(OutcomeQuarantined),
		len(rep.Orphans), rep.QuotaConverged)
	return rep, nil
}

// decide 对单条记录做事实比对并在确定时执行收敛。
func decide(
	ctx context.Context,
	st store.Store,
	h backend.Handle,
	r *domain.Instance,
	info backend.Info,
	found bool,
	v Verdict,
	logf Logf,
) Verdict {
	switch r.State {
	case domain.StateDefined, domain.StateStopped:
		if !found {
			// 唯一内部可达路径：Destroy 的 Undefine 已成功而 Destroyed 未落盘。
			// 后端已无残留，收敛到终态并由台账重建释放配额。
			return persist(ctx, st, r, domain.StateDestroyed, "undefine",
				"backend absent: half-done destroy completed to Destroyed", v, logf)
		}
		switch info.Status {
		case backend.StatusStopped:
			if r.State == domain.StateDefined {
				v.Outcome = OutcomeMatched
				v.Detail = "record Defined, backend defined/stopped"
			} else {
				v.Outcome = OutcomeMatched
				v.Detail = "record Stopped, backend stopped"
			}
		case backend.StatusRunning:
			// Start 后端调用已成功而 Running 状态未落盘（崩溃窗口）：采纳事实。
			return persist(ctx, st, r, domain.StateRunning, "adopt-running",
				"guest running while record not Running: Start-side-effect committed before crash, adopted as Running",
				v, logf)
		default:
			return quarantine(v, "quarantined-unknown-actual", "unknown backend status: "+string(info.Status))
		}

	case domain.StateRunning:
		if !found {
			return quarantine(v, "quarantined-running-missing-backend",
				"record Running but backend fully absent: host rebuild or external wipe, cannot auto-recreate")
		}
		switch info.Status {
		case backend.StatusRunning:
			v.Outcome = OutcomeMatched
			v.Detail = "record Running, guest alive"
		case backend.StatusStopped:
			return persist(ctx, st, r, domain.StateStopped, "converge-crash",
				"guest exited while record said Running (crash/unclean kill); converged to Stopped", v, logf)
		default:
			return quarantine(v, "quarantined-unknown-actual", "unknown backend status: "+string(info.Status))
		}

	case domain.StateDestroyed:
		if !found || info.Status == backend.StatusAbsent {
			v.Outcome = OutcomeMatched
			v.Detail = "tombstone and backend both absent"
			return v
		}
		// 墓碑存在但后端有残留：幂等 Undefine 是唯一确定的安全收尾。
		if err := h.Undefine(ctx, r.ID); err != nil {
			return quarantine(v, "quarantined-destroyed-residue",
				"backend residue under tombstone, undefine failed: "+err.Error())
		}
		v.Outcome = OutcomeRecovered
		v.After = string(domain.StateDestroyed)
		v.Action = "undefine-residue"
		v.Detail = "backend residue under tombstone removed idempotently"
		logf("[reconcile] id=%s action=undefine-residue (Destroyed tombstone cleanup)", r.ID)

	default:
		return quarantine(v, "quarantined-unknown-state", "unknown record state: "+string(r.State))
	}
	logf("[reconcile] id=%s record=%s actual=%s -> %s (%s)",
		v.InstanceID, v.RecordState, v.ActualState, v.Outcome, v.Detail)
	return v
}

func persist(
	ctx context.Context,
	st store.Store,
	r *domain.Instance,
	to domain.State,
	action, detail string,
	v Verdict,
	logf Logf,
) Verdict {
	from := r.State
	r.State = to
	r.UpdatedAt = time.Now().UTC()
	r.LastEvent = "reconciled: " + detail
	if err := st.Put(ctx, r); err != nil {
		v.Outcome = OutcomeQuarantined
		v.After = "quarantined-persist-failed"
		v.Action = action
		v.Detail = detail + " (persist failed: " + err.Error() + ")"
		logf("[reconcile] id=%s PERSIST FAILED %s: %v", r.ID, from, err)
		return v
	}
	v.Outcome = OutcomeRecovered
	v.After = string(to)
	v.Action = action
	v.Detail = detail
	logf("[reconcile] id=%s action=%s PERSIST %s -> %s", r.ID, action, from, to)
	return v
}

func quarantine(v Verdict, after, detail string) Verdict {
	v.Outcome = OutcomeQuarantined
	v.After = after
	v.Action = "none"
	v.Detail = detail
	return v
}

func (r *Report) count(o Outcome) int {
	n := 0
	for _, v := range r.Verdicts {
		if v.Outcome == o {
			n++
		}
	}
	return n
}

func (r *Report) isQuarantined(id string) bool {
	for _, q := range r.Quarantine {
		if q == id {
			return true
		}
	}
	return false
}
