package orchestrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

// TestOversizedOrphanQueriesSurviveAndDiscardResolves 稳定复现并验证整条链路：
//
//	造出声明规格大于本机容量的后端孤儿残留（且总额超容量）
//	-> 跑一次对账（落盘 ManualReview、台账按事实重建并超容量告警）
//	-> 换一个全新进程：get/list/quota/reviews 全部可用，占用读数与记录自洽
//	-> 无关实例的查询/变更不被牵连
//	-> 真正会超卖的新建仍被拒绝且原因为 insufficient_resource（与其它失败可分）
//	-> 人工 discard 收口成功：残留清除、占用释放、落 Destroyed 墓碑
//	-> 再换新进程复核：记录与占用一致、不算漏不算重。
func TestOversizedOrphanQueriesSurviveAndDiscardResolves(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 4096}
	// 无关的正常实例：它的查询和变更必须始终可用。
	normalSpec := smallSpec(1000, 512, 1024)
	// 超大残留：单实例 CPU 一项就超过整机容量。
	hugeSpec := smallSpec(9000, 512, 1024)

	st0, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	h := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st0, h, cap, testLog{t}.Logf)
	if _, err := o1.Create(ctx, "normal", "normal", normalSpec); err != nil {
		t.Fatal(err)
	}
	// 直接在后端造一个没有任何记录对应的承载残留，且声明规格超过本机容量。
	if err := h.Define(ctx, "huge-orphan", hugeSpec); err != nil {
		t.Fatal(err)
	}

	// === 第一次对账 ===
	rep, err := o1.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatalf("reconcile must not fail on over-capacity rebuild: %v", err)
	}
	if v := verdictOf(rep, "huge-orphan"); v.Outcome != reconcile.OutcomeQuarantined || v.After != "ManualReview" {
		t.Fatalf("huge orphan must be persisted ManualReview, got %+v", v)
	}
	if !rep.QuotaConverged {
		t.Fatal("ledger must converge (be rebuilt) even when over capacity")
	}
	if !rep.QuotaOverCapacity {
		t.Fatalf("report must flag over-capacity as advisory: detail=%s", rep.QuotaDetail)
	}
	for _, q := range rep.Quarantine {
		if q == "(quota-ledger)" {
			t.Fatal("over-capacity must be an advisory, not a quota-ledger quarantine")
		}
	}

	// 对账后本进程读数自洽：used == normal + huge，明确标记超容量与超出量。
	assertSelfConsistent(t, o1, cap,
		domain.Resources{CPUMillis: 10000, MemoryMB: 1024, DiskMB: 2048},
		map[string]domain.Resources{"normal": normalSpec.Resources, "huge-orphan": hugeSpec.Resources})

	// === 换一个全新进程（模拟运维脚本另起一个 vmctl 进程）===
	o2 := freshProcess(t, dir, h, cap) // RestoreFromStore 必须成功，不能再报 inconsistent_state
	snap := o2.ResourceSnapshot()
	if !snap.OverCapacity {
		t.Fatalf("new process must observe over-capacity advisory: %+v", snap)
	}
	if snap.OverBy != (domain.Resources{CPUMillis: 6000, MemoryMB: 0, DiskMB: 0}) {
		t.Fatalf("new process overBy wrong: %+v", snap.OverBy)
	}

	// 查询侧全部可用，且能解释得通。
	got, err := o2.Get(ctx, "huge-orphan")
	if err != nil {
		t.Fatalf("get on oversized held instance must work, got %v", err)
	}
	if got.State != domain.StateManualReview || got.Review == nil ||
		got.Review.Reason != domain.ReviewOrphanBackend || !got.Review.SpecKnown {
		t.Fatalf("held record/review must be readable: %+v", got)
	}
	if list, err := o2.List(ctx); err != nil || len(list) != 2 {
		t.Fatalf("list must work and return 2 records: n=%d err=%v", len(list), err)
	}
	if reviews, err := o2.ManualReviewList(ctx); err != nil || len(reviews) != 1 || reviews[0].ID != "huge-orphan" {
		t.Fatalf("reviews must work and list exactly the orphan: n=%d err=%v", len(reviews), err)
	}
	assertReservations(t, o2, map[string]domain.Resources{
		"normal":      normalSpec.Resources,
		"huge-orphan": hugeSpec.Resources,
	})

	// 常规变更对挂起实例仍按原口径拒绝（manual_review_pending），原因必须可区分。
	if _, err := o2.Start(ctx, "huge-orphan"); !domain.IsKind(err, domain.KindManualReviewPending) {
		t.Fatalf("start on held instance must be manual_review_pending, got %v", err)
	}
	if _, err := o2.Destroy(ctx, "huge-orphan"); !domain.IsKind(err, domain.KindManualReviewPending) {
		t.Fatalf("destroy on held instance must be manual_review_pending, got %v", err)
	}

	// 无关实例不被牵连：查询、start/stop 全程正常。
	if g, err := o2.Get(ctx, "normal"); err != nil || g.State != domain.StateDefined {
		t.Fatalf("unrelated instance get must work: %+v err=%v", g, err)
	}
	if _, err := o2.Start(ctx, "normal"); err != nil {
		t.Fatalf("unrelated instance start must not be blocked: %v", err)
	}
	if _, err := o2.Stop(ctx, "normal"); err != nil {
		t.Fatalf("unrelated instance stop must not be blocked: %v", err)
	}

	// 真正会超卖的新建立刻拒绝，原因是 insufficient_resource，且零副作用、零台账变化。
	before := o2.ResourceSnapshot()
	if _, err := o2.Create(ctx, "newbie", "newbie", smallSpec(1, 1, 1)); !domain.IsKind(err, domain.KindInsufficientResource) {
		t.Fatalf("new admission while over capacity must be insufficient_resource, got %v", err)
	}
	if _, err := o2.Get(ctx, "newbie"); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("rejected create must leave no record, got err=%v", err)
	}
	after := o2.ResourceSnapshot()
	if len(after.Reservations) != len(before.Reservations) || after.Used != before.Used {
		t.Fatalf("rejected admission must be side-effect free: before=%+v after=%+v", before, after)
	}
	// 既有失败原因依旧清楚可分：不存在、非法迁移。
	if _, err := o2.Start(ctx, "missing-id"); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("missing id must stay not_found, got %v", err)
	}
	if _, err := o2.Create(ctx, "normal", "normal-other", smallSpec(2000, 512, 1024)); !domain.IsKind(err, domain.KindAlreadyExists) {
		t.Fatalf("same id different spec must stay already_exists, got %v", err)
	}

	// === 人工 discard 收口必须可用（旧版本正是被 inconsistent_state 挡在门外）===
	disc, err := o2.Resolve(ctx, "huge-orphan", orchestrator.ResolveDiscard, "", nil, "wipe confirmed oversized residue")
	if err != nil {
		t.Fatalf("manual discard must succeed despite over-capacity: %v", err)
	}
	if disc.State != domain.StateDestroyed || disc.Review != nil {
		t.Fatalf("discard must land Destroyed and clear review: %+v", disc)
	}
	if info, _ := h.Inspect(ctx, "huge-orphan"); info.Status != backend.StatusAbsent {
		t.Fatalf("residue must be gone after discard, got %s", info.Status)
	}
	// 占用释放后台账回到容量以内，读数自洽（只剩 normal）。
	snap = o2.ResourceSnapshot()
	if snap.OverCapacity {
		t.Fatalf("over-capacity advisory must clear after discard: %+v", snap)
	}
	if snap.Used != normalSpec.Resources || len(snap.Reservations) != 1 {
		t.Fatalf("only normal must remain reserved: %+v", snap)
	}
	if _, ok := snap.Reservations["huge-orphan"]; ok {
		t.Fatal("discarded instance must not be double-counted in ledger")
	}

	// === 再换第三个进程复核：记录与占用跨进程一致 ===
	o3 := freshProcess(t, dir, h, cap)
	if g, err := o3.Get(ctx, "huge-orphan"); err != nil || g.State != domain.StateDestroyed {
		t.Fatalf("third process must see Destroyed tombstone: %+v err=%v", g, err)
	}
	if reviews, _ := o3.ManualReviewList(ctx); len(reviews) != 0 {
		t.Fatalf("resolved instance must leave review queue: %+v", reviews)
	}
	assertSelfConsistent(t, o3, cap, normalSpec.Resources,
		map[string]domain.Resources{"normal": normalSpec.Resources})
	if _, err := o3.Destroy(ctx, "normal"); err != nil {
		t.Fatalf("unrelated instance lifecycle must stay available: %v", err)
	}
	if u := o3.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("everything destroyed must zero usage: %+v", u)
	}
}

// TestOversizedOrphanAdoptLandsDeterministic 人工确认继续存在（adopt）时：
// 承载收敛到目标态、占用落到与记录一致（不重不漏），超容量仅作为告警保留，
// 直到容量调高或残留被处理；之后 discard 能正常释放。
func TestOversizedOrphanAdoptLandsDeterministic(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 4096}
	hugeSpec := smallSpec(8000, 1024, 2048)

	st0, _ := store.NewFileStore(filepath.Join(dir, "state"))
	h := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st0, h, cap, testLog{t}.Logf)
	if err := h.Define(ctx, "giant", hugeSpec); err != nil {
		t.Fatal(err)
	}
	rep, err := o1.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rep.QuotaOverCapacity {
		t.Fatal("expected over-capacity advisory")
	}

	// 新进程 adopt 到 Stopped（承载被收敛为已定义不运行），占用保持且仍超容量。
	o2 := freshProcess(t, dir, h, cap)
	adopted, err := o2.Resolve(ctx, "giant", orchestrator.ResolveAdopt, domain.StateStopped, nil, "keep, host capacity to be raised")
	if err != nil {
		t.Fatalf("adopt must succeed despite over-capacity: %v", err)
	}
	if adopted.State != domain.StateStopped || adopted.Review != nil {
		t.Fatalf("adopt must land Stopped and clear review: %+v", adopted)
	}
	if info, _ := h.Inspect(ctx, "giant"); info.Status != backend.StatusStopped {
		t.Fatalf("backend must be converged to stopped, got %s", info.Status)
	}
	assertSelfConsistent(t, o2, cap, hugeSpec.Resources,
		map[string]domain.Resources{"giant": hugeSpec.Resources})
	if !o2.ResourceSnapshot().OverCapacity {
		t.Fatal("adopt keeps the factual over-commitment; advisory must remain")
	}

	// 第三进程：确定态 + 占用一致，reviews 队列为空。
	o3 := freshProcess(t, dir, h, cap)
	if g, _ := o3.Get(ctx, "giant"); g.State != domain.StateStopped {
		t.Fatalf("third process must see Stopped, got %s", g.State)
	}
	if reviews, _ := o3.ManualReviewList(ctx); len(reviews) != 0 {
		t.Fatalf("adopted instance must leave review queue: %+v", reviews)
	}
	assertReservations(t, o3, map[string]domain.Resources{"giant": hugeSpec.Resources})

	// 确认清理仍然可用，释放后归零。
	if _, err := o3.Destroy(ctx, "giant"); err != nil {
		t.Fatalf("Stopped instance destroy must work (Stopped -> Destroyed allowed): %v", err)
	}
	if u := o3.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must be zero after destroy: %+v", u)
	}
}

// TestOversizedPersistedRecordRestoresAsAdvisory 覆盖「任何会让重建后总额超容量的
// 已落盘情况」：不经过孤儿，直接存在一条规格过大的已落盘 ManualReview 记录，
// 新进程 RestoreFromStore 也必须成功、查询可用、收口可落地。
func TestOversizedPersistedRecordRestoresAsAdvisory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 2000, MemoryMB: 1024, DiskMB: 2048}
	st := newFileStore(t, dir)
	h := backend.NewSimHandle(backend.FaultHooks{})
	huge := smallSpec(5000, 100, 100)
	seedManualReview(t, st, "disk-giant", huge, domain.ReviewOrphanBackend, "stopped", true)

	o := orchestrator.New(st, h, cap, testLog{t}.Logf)
	if err := o.RestoreFromStore(ctx); err != nil {
		t.Fatalf("restore with over-capacity records must succeed (advisory), got %v", err)
	}
	snap := o.ResourceSnapshot()
	if !snap.OverCapacity || snap.OverBy.CPUMillis != 3000 {
		t.Fatalf("restore must expose over-capacity advisory: %+v", snap)
	}
	if g, err := o.Get(ctx, "disk-giant"); err != nil || g.State != domain.StateManualReview {
		t.Fatalf("query must work after over-capacity restore: %+v err=%v", g, err)
	}
	// 收口可用：discard 后告警消失、归零。
	if _, err := o.Resolve(ctx, "disk-giant", orchestrator.ResolveDiscard, "", nil, ""); err != nil {
		t.Fatalf("discard must work after over-capacity restore: %v", err)
	}
	if after := o.ResourceSnapshot(); after.OverCapacity || after.Used != (domain.Resources{}) {
		t.Fatalf("ledger must be clear after discard: %+v", after)
	}
}

// TestOversizedUnknownSpecOrphanClaimAdopt 超容量场景下，读不出规格的孤儿
// 人工补报一个超大规格也能认领（补登记是事实登记，不走准入），随后读数自洽。
func TestOversizedUnknownSpecOrphanClaimAdopt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 2000, MemoryMB: 1024, DiskMB: 2048}
	st := newFileStore(t, dir)
	h := backend.NewSimHandle(backend.FaultHooks{})
	// 模拟「后端残留目录里没有 spec.json」的孤儿：记录上 spec_known=false。
	seedManualReview(t, st, "ghost-no-spec", domain.Spec{Image: "img"}, domain.ReviewOrphanBackend, "stopped", false)

	o := orchestrator.New(st, h, cap, testLog{t}.Logf)
	if err := o.RestoreFromStore(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, ok := o.ResourceSnapshot().Reservations["ghost-no-spec"]; ok {
		t.Fatal("unknown-spec orphan must not be counted before claim")
	}
	claimed := smallSpec(7000, 2048, 4096) // 远超容量
	adopted, err := o.Resolve(ctx, "ghost-no-spec", orchestrator.ResolveAdopt, domain.StateStopped, &claimed, "claim oversized")
	if err != nil {
		t.Fatalf("claim-adopt must not be gated by capacity: %v", err)
	}
	if adopted.State != domain.StateStopped || adopted.Spec.Resources != claimed.Resources {
		t.Fatalf("adopted state/spec wrong: %+v", adopted)
	}
	snap := o.ResourceSnapshot()
	if !snap.OverCapacity {
		t.Fatalf("claimed oversized usage must be flagged over-capacity: %+v", snap)
	}
	assertReservations(t, o, map[string]domain.Resources{"ghost-no-spec": claimed.Resources})

	// 新进程复核：补报的规格已随记录落盘，重建结果一致。
	o2 := freshProcess(t, dir, h, cap)
	assertSelfConsistent(t, o2, cap, claimed.Resources,
		map[string]domain.Resources{"ghost-no-spec": claimed.Resources})
}

// --- helpers ---

func seedManualReview(t *testing.T, st store.Store, id string, spec domain.Spec, reason domain.ReviewReason, actual string, specKnown bool) {
	t.Helper()
	now := time.Now().UTC()
	inst := &domain.Instance{
		ID: id, Name: id, Spec: spec, State: domain.StateManualReview, Backend: "sim",
		CreatedAt: now, UpdatedAt: now, LastEvent: "quarantined: seeded",
		Review: &domain.Review{
			Reason: reason, RecordState: "(none)", ActualState: actual,
			RaisedAt: now, SpecKnown: specKnown,
			Detail: "seeded over-capacity manual review",
		},
	}
	if err := st.Put(context.Background(), inst); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

func assertSelfConsistent(t *testing.T, o *orchestrator.Orchestrator, cap, wantUsed domain.Resources, wantRes map[string]domain.Resources) {
	t.Helper()
	snap := o.ResourceSnapshot()
	if snap.Used != wantUsed {
		t.Fatalf("used=%+v want %+v (capacity=%+v)", snap.Used, wantUsed, cap)
	}
	// 核心自洽不变量：used 必须逐维等于全部预留之和。
	sum := domain.Resources{}
	for id, r := range snap.Reservations {
		sum = sum.Add(r)
		_ = id
	}
	if sum != snap.Used {
		t.Fatalf("ledger not self-consistent: used=%+v sum(reservations)=%+v", snap.Used, sum)
	}
	assertReservations(t, o, wantRes)
}

func assertReservations(t *testing.T, o *orchestrator.Orchestrator, want map[string]domain.Resources) {
	t.Helper()
	got := o.ResourceSnapshot().Reservations
	if len(got) != len(want) {
		t.Fatalf("reservation count=%d want %d: got=%+v want=%+v", len(got), len(want), got, want)
	}
	for id, r := range want {
		gr, ok := got[id]
		if !ok {
			t.Fatalf("reservation for %q missing: got=%+v", id, got)
		}
		if gr != r {
			t.Fatalf("reservation for %q = %+v want %+v", id, gr, r)
		}
	}
}
