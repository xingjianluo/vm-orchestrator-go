// Package reconcile 在进程重启后对"持久化记录 / 后端承载事实 / 配额台账"三方对账。
//
// 崩溃窗口分析（编排层在"后端动作"与"状态落盘"之间崩溃所产生的组合）：
//
//	记录 Stopped/Defined + 后端 running：Start 已成功、Running 未落盘   -> 采纳为 Running
//	记录 Running        + 后端 stopped：guest 异常退出                -> 收敛为 Stopped
//	记录 Defined/Stopped + 后端 absent： Undefine 已成功、Destroyed 未落盘 -> 收敛为 Destroyed 并释放配额
//	记录 Destroyed      + 后端残留：   外部残留/半完成清理            -> 幂等 Undefine 后保持墓碑
//	记录 Running        + 后端 absent：宿主疑似被重建，无法判定       -> 持久化挂起为 ManualReview
//	后端独有对象（无记录）：归属无法判定                              -> 持久化创建 ManualReview 记录（孤儿）
//
// 后两类"待人工处理"不是只存在于本次对账报告里的易失结论：它们会随实例记录落盘，
// 换新进程乃至宿主重启后，实例状态仍是 ManualReview 且带判定原因，常规变更一律拒绝，
// 等待人工通过 orchestrator 的 ResolveAdopt（确认继续存在）/ResolveDiscard（确认清理）收口。
//
// 占用口径：台账权威集合为"状态持有资源"的记录（Defined/Running/Stopped/ManualReview）。
// ManualReview 实例在收口前继续按规格占用，防止资源被再次分配；收口到具体正常状态后
// 占用保持，收口到 Destroyed 后释放。唯一例外：无记录且后端读不出规格的孤儿，
// 无法知道该占多少，台账无法计入（报告与日志显式标出），需人工收口时补报规格。
//
// 重建后总额超过当前容量（如存在规格大于本机容量的孤儿）属于告警级结论：台账仍按权威
// 集合重建（QuotaConverged=true），通过 Report.QuotaOverCapacity 暴露，不进 Quarantine、
// 不返回错误、不阻断任何命令；在线新增准入仍由配额池按容量拒绝。
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
	// Orphans 是后端存在但没有任何记录对应的承载残留（首次发现时已落盘为 ManualReview）。
	Orphans []string
	// Quarantine 包含需人工处理的 id（含已持久化的 ManualReview、孤儿）以及
	// 真正的台账重建失败（数据损坏）。注意：「重建占用超过当前容量」**不进**这里，
	// 它是告警级结论，见 QuotaOverCapacity。
	Quarantine []string
	// ManualReviewIDs 是当前处于 ManualReview 冻结态的实例（结论已随记录持久化）。
	ManualReviewIDs []string
	// UnaccountedOrphans 是无记录且后端读不出规格、无法计入占用台账的孤儿 id。
	UnaccountedOrphans []string
	// QuotaConverged 表示配额台账已按权威集合重建（即便重建结果超过容量，
	// 台账也已落账并自洽；此时 QuotaOverCapacity=true 作为告警暴露）。
	QuotaConverged bool
	// QuotaOverCapacity 表示按权威集合重建后的占用总额超过当前宿主容量。
	// 这是对账结论/告警：不否决任何命令，查询与人工收口照常，
	// 新增准入仍由配额池按容量拒绝（insufficient_resource）。
	QuotaOverCapacity bool
	QuotaDetail       string
}

// LedgerResult 是台账重建的结果：台账总是先按权威集合落账，
// OverCapacity 仅表示落账后总额超过容量（告警，不是失败）。
type LedgerResult struct {
	Detail       string
	OverCapacity bool
}

// LedgerRebuilder 以"仍存活实例的配额集合"重建配额台账，返回人类可读的快照描述。
// 返回 error 仅用于真正的损坏（如非正规格）；超容量必须通过 LedgerResult.OverCapacity
// 作为告警暴露，且台账已经完成重建。
type LedgerRebuilder func(alive map[string]domain.Resources) (LedgerResult, error)

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

	for _, r := range records {
		v := Verdict{InstanceID: r.ID, RecordState: string(r.State), Before: string(r.State), After: string(r.State)}
		info, found := actual[r.ID]
		if !found {
			v.ActualState = "absent"
		} else {
			v.ActualState = string(info.Status)
		}
		v = decide(ctx, st, h, r, info, found, v, rep, logf)
		rep.Verdicts = append(rep.Verdicts, v)
		if v.Outcome == OutcomeQuarantined {
			rep.Quarantine = append(rep.Quarantine, r.ID)
		}
		if r.State == domain.StateManualReview {
			rep.ManualReviewIDs = append(rep.ManualReviewIDs, r.ID)
		}
		delete(actual, r.ID)
	}

	// 后端独有对象：归属不明。残留绝不删除，结论持久化为 ManualReview 记录，
	// 使"待人工处理"跨进程/重启可见，并在规格可知时纳入占用台账防止超卖。
	for id, info := range actual {
		v := Verdict{
			InstanceID:  id,
			RecordState: "(none)",
			ActualState: string(info.Status),
			Outcome:     OutcomeQuarantined,
			Before:      "(none)",
		}
		now := time.Now().UTC()
		rv := &domain.Review{
			Reason:      domain.ReviewOrphanBackend,
			RecordState: "(none)",
			ActualState: string(info.Status),
			RaisedAt:    now,
			SpecKnown:   info.HasSpec,
			Detail:      "backend residue without any record: ownership ambiguous, residue preserved pending manual resolution",
		}
		inst := &domain.Instance{
			ID:        id,
			Name:      id,
			Spec:      info.Spec,
			State:     domain.StateManualReview,
			Backend:   h.Name(),
			CreatedAt: now,
			UpdatedAt: now,
			LastEvent: "quarantined: orphan backend residue",
			Review:    rv,
		}
		if err := st.Put(ctx, inst); err != nil {
			v.After = "quarantined-orphan-persist-failed"
			v.Action = "none"
			v.Detail = rv.Detail + " (persist failed: " + err.Error() + ")"
			logf("[reconcile] ORPHAN id=%s persist FAILED: %v (残留保留，报告隔离)", id, err)
		} else {
			v.After = string(domain.StateManualReview)
			v.Action = "persist-manual-review"
			v.Detail = rv.Detail
			records = append(records, inst)
			rep.ManualReviewIDs = append(rep.ManualReviewIDs, id)
			if !info.HasSpec {
				rep.UnaccountedOrphans = append(rep.UnaccountedOrphans, id)
				logf("[reconcile] ORPHAN id=%s actual=%s -> ManualReview（规格不可读，台账无法计入，收口时需补报规格）", id, info.Status)
			} else {
				logf("[reconcile] ORPHAN id=%s actual=%s -> ManualReview（已持久化，规格=%+v，占用继续计入）", id, info.Status, info.Spec.Resources)
			}
		}
		rep.Orphans = append(rep.Orphans, id)
		rep.Quarantine = append(rep.Quarantine, id)
		rep.Verdicts = append(rep.Verdicts, v)
	}

	// 配额台账权威集合：所有"持有资源"状态的记录
	// （Defined/Running/Stopped/ManualReview；Destroyed 墓碑除外）。
	// ManualReview 继续占用，收口裁决前资源不能被再次分配。
	// 规格不可读的孤儿无法计入，已在 UnaccountedOrphans 显式标出。
	alive := make(map[string]domain.Resources)
	for _, r := range records {
		if !r.State.HoldsResources() {
			continue
		}
		if r.State == domain.StateManualReview && (r.Review == nil || !r.Review.SpecKnown) {
			continue
		}
		alive[r.ID] = r.Spec.Resources
	}
	if rebuildLedger != nil {
		res, err := rebuildLedger(alive)
		if err != nil {
			// 真正的台账损坏（如非正规格）：保持旧台账不动并隔离上报。
			rep.QuotaDetail = "ledger rebuild rejected: " + err.Error()
			logf("[reconcile] QUOTA DRIFT: %s", rep.QuotaDetail)
			rep.Quarantine = append(rep.Quarantine, "(quota-ledger)")
		} else {
			rep.QuotaConverged = true
			rep.QuotaDetail = res.Detail
			rep.QuotaOverCapacity = res.OverCapacity
			if res.OverCapacity {
				// 告警级结论：台账已按权威集合落账且自洽，只是总额超过当前容量。
				// 不隔离、不否决任何命令；新增准入继续由配额池拒绝。
				logf("[reconcile] QUOTA OVER CAPACITY (advisory, ledger consistent): %s", res.Detail)
			} else {
				logf("[reconcile] quota ledger CONVERGED held=%d snapshot=%s", len(alive), res.Detail)
			}
		}
	}

	logf("[reconcile] DONE matched=%d recovered=%d quarantined=%d orphans=%d manualReview=%d quotaConverged=%t overCapacity=%t",
		rep.count(OutcomeMatched), rep.count(OutcomeRecovered), rep.count(OutcomeQuarantined),
		len(rep.Orphans), len(rep.ManualReviewIDs), rep.QuotaConverged, rep.QuotaOverCapacity)
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
	rep *Report,
	logf Logf,
) Verdict {
	switch r.State {
	case domain.StateManualReview:
		// 已持久化的待人工处理：重复对账绝不覆盖、绝不自动动作，只回报当前事实。
		v.Outcome = OutcomeQuarantined
		v.After = string(domain.StateManualReview)
		v.Action = "held-pending-manual-resolution"
		if r.Review != nil {
			v.Detail = "held for manual review since " + r.Review.RaisedAt.Format(time.RFC3339) +
				" reason=" + string(r.Review.Reason) + ": " + r.Review.Detail
		} else {
			v.Detail = "held for manual review (no review detail)"
		}
		if r.Review != nil && !r.Review.SpecKnown {
			rep.UnaccountedOrphans = append(rep.UnaccountedOrphans, r.ID)
		}
		logf("[reconcile] id=%s HELD ManualReview actual=%s（不覆盖、不动作，等待人工收口）", r.ID, v.ActualState)
		return v

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
			// 宿主疑似被重建/外部清理：记录说在跑但承载整体消失，无法判定该重建还是认销毁。
			// 结论持久化为 ManualReview：不自动重建、不删任何残留（本就没有），占用继续保留。
			return holdForReview(ctx, st, r, "absent",
				domain.ReviewRunningMissingBackend,
				"record Running but backend fully absent: host rebuild or external wipe, cannot auto-recreate",
				v, logf)
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

// holdForReview 把一条无法自动判定的记录持久化落定为 ManualReview 冻结态，
// 并在记录上保留判定原因与现场（跨进程/重启可见）。不触碰任何后端承载。
func holdForReview(
	ctx context.Context,
	st store.Store,
	r *domain.Instance,
	actualState string,
	reason domain.ReviewReason,
	detail string,
	v Verdict,
	logf Logf,
) Verdict {
	now := time.Now().UTC()
	from := r.State
	r.Review = &domain.Review{
		Reason:      reason,
		Detail:      detail,
		RecordState: string(from),
		ActualState: actualState,
		RaisedAt:    now,
		SpecKnown:   true,
	}
	r.State = domain.StateManualReview
	r.UpdatedAt = now
	r.LastEvent = "quarantined: " + detail
	if err := st.Put(ctx, r); err != nil {
		v.Outcome = OutcomeQuarantined
		v.After = "quarantined-persist-failed"
		v.Action = "none"
		v.Detail = detail + " (persist failed: " + err.Error() + ")"
		logf("[reconcile] id=%s MANUAL-REVIEW PERSIST FAILED from=%s: %v", r.ID, from, err)
		return v
	}
	v.Outcome = OutcomeQuarantined
	v.After = string(domain.StateManualReview)
	v.Action = "persist-manual-review"
	v.Detail = detail
	logf("[reconcile] id=%s action=persist-manual-review %s -> ManualReview reason=%s（占用继续计入，等待人工收口）",
		r.ID, from, reason)
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
