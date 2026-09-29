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

// 本组用例覆盖缺陷场景：对账重建出的占用超过当前宿主容量（典型为无记录的超大
// 后端残留，或任何使重建总额超容量的已落盘情况）。修复前它会让 RestoreFromStore
// 直接报 inconsistent_state，连锁冻结 list/get/quota/reviews 与人工 resolve。
//
// 修复后的口径：
//   - 台账按权威记录集合如实重建，OverCommit 作为可读结论暴露，查询永远可用且自洽；
//   - 人工 adopt/discard 始终可落地，记录与占用同步更新，跨进程一致；
//   - 超容量只拒绝"会新增占用"的准入，错误类别 host_overcommitted 与既有的
//     not_found / illegal_transition / insufficient_resource / manual_review_pending
//     清楚区分；不相关实例的查询与变更不受牵连。

func smallCap() domain.Resources {
	return domain.Resources{CPUMillis: 2000, MemoryMB: 1024, DiskMB: 2048}
}

// assertLedgerMatchesRecords 是跨进程的核心自洽断言：
// 台账预留集合 == 所有持有资源且规格可知的记录；Destroyed 不占资源。
func assertLedgerMatchesRecords(t *testing.T, o *orchestrator.Orchestrator) {
	t.Helper()
	ctx := context.Background()
	list, err := o.List(ctx)
	if err != nil {
		t.Fatalf("list must stay available: %v", err)
	}
	snap := o.ResourceSnapshot()
	want := map[string]domain.Resources{}
	var total domain.Resources
	for _, in := range list {
		if !in.State.HoldsResources() {
			continue
		}
		if in.State == domain.StateManualReview && (in.Review == nil || !in.Review.SpecKnown) {
			continue
		}
		want[in.ID] = in.Spec.Resources
		total = total.Add(in.Spec.Resources)
	}
	if len(snap.Reservations) != len(want) {
		t.Fatalf("ledger/record set mismatch: reservations=%v want=%v", snap.Reservations, want)
	}
	for id, r := range want {
		got, ok := snap.Reservations[id]
		if !ok || got != r {
			t.Fatalf("reservation for %s: got=%+v ok=%v want=%+v", id, got, ok, r)
		}
	}
	if snap.Used != total {
		t.Fatalf("used=%+v must equal sum of record specs=%+v", snap.Used, total)
	}
}

// TestOversizeOrphanDiscardCrossProcess 是需求要求的稳定复现链路：
// 造超大后端残留 -> 对账 -> 换新进程查询可用且自洽 -> 人工 discard 收口 ->
// 记录与占用同步更新（再换进程仍一致，不漏算不重算）。
func TestOversizeOrphanDiscardCrossProcess(t *testing.T) {
	ctx := context.Background()
	cap := smallCap()

	// 显式 state 目录，供多个"新进程"复用同一磁盘，真正模拟跨进程。
	st, stateDir := sharedFileStore(t)
	sim := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, sim, cap, testLog{t}.Logf)

	normal := smallSpec(500, 256, 512)
	if _, err := o1.Create(ctx, "normal", "normal", normal); err != nil {
		t.Fatal(err)
	}
	// 无任何记录对应的后端残留，且它自称的规格在每个维度上都超过整机容量。
	huge := smallSpec(3000, 2048, 4096)
	if err := sim.Define(ctx, "huge", huge); err != nil {
		t.Fatal(err)
	}

	// 1) 对账：孤儿落盘 ManualReview，超容量作为结论暴露而不是错误。
	rep, err := o1.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatalf("reconcile must not fail: %v", err)
	}
	v := verdictOf(rep, "huge")
	if v.Outcome != reconcile.OutcomeQuarantined || v.After != "ManualReview" {
		t.Fatalf("huge orphan must be persisted ManualReview: %+v", v)
	}
	if !rep.QuotaConverged || !rep.CapacityExceeded {
		t.Fatalf("ledger must converge AND flag capacity exceeded: conv=%t exceeded=%t",
			rep.QuotaConverged, rep.CapacityExceeded)
	}
	if len(rep.ExceededDimensions) != 3 {
		t.Fatalf("all three dims must be exceeded: %v", rep.ExceededDimensions)
	}
	for _, q := range rep.Quarantine {
		if q == "(quota-ledger)" {
			t.Fatal("over-capacity must not be reported as a ledger quarantine item")
		}
	}

	// 2) 换新进程：RestoreFromStore 不再失败，查询全部可用、读数自洽。
	o2 := freshProcessShared(t, stateDir, sim, cap)
	if got, err := o2.Get(ctx, "huge"); err != nil || got.State != domain.StateManualReview {
		t.Fatalf("new process must see held huge: state=%s err=%v", gotState(got), err)
	}
	if list, err := o2.List(ctx); err != nil || len(list) != 2 {
		t.Fatalf("list must work and see 2 records: n=%d err=%v", len(list), err)
	}
	if reviews, err := o2.ManualReviewList(ctx); err != nil || len(reviews) != 1 || reviews[0].ID != "huge" {
		t.Fatalf("reviews must list only huge: %+v err=%v", reviews, err)
	}
	snap := o2.ResourceSnapshot()
	if !snap.OverCommit {
		t.Fatal("snapshot must expose OverCommit")
	}
	if snap.Used != (domain.Resources{CPUMillis: 3500, MemoryMB: 2304, DiskMB: 4608}) {
		t.Fatalf("used must count both records honestly: %+v", snap.Used)
	}
	if snap.Available.CPUMillis >= 0 || snap.Available.MemoryMB >= 0 || snap.Available.DiskMB >= 0 {
		t.Fatalf("available must be negative while overcommitted: %+v", snap.Available)
	}
	assertLedgerMatchesRecords(t, o2)

	// 3) 超容量期间：新增准入一律 host_overcommitted，且与其它失败原因清楚区分。
	if _, err := o2.Create(ctx, "tiny", "tiny", smallSpec(1, 1, 1)); !domain.IsKind(err, domain.KindHostOvercommitted) {
		t.Fatalf("create while overcommitted must be host_overcommitted, got %v", err)
	}
	// 对照组：不存在 / 非法迁移 / 待人工处理 仍是各自原因，不被超容量覆盖。
	if _, err := o2.Start(ctx, "missing"); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("not_found must remain distinct: %v", err)
	}
	// normal 处于 Defined，对它 stop 是确定的非法迁移。
	if _, err := o2.Stop(ctx, "normal"); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("stop on Defined must be illegal_transition: %v", err)
	}
	if _, err := o2.Start(ctx, "huge"); !domain.IsKind(err, domain.KindManualReviewPending) {
		t.Fatalf("start on held instance must be manual_review_pending: %v", err)
	}

	// 4) 不相关实例的查询与变更不能被牵连。
	if _, err := o2.Start(ctx, "normal"); err != nil {
		t.Fatalf("unrelated instance start must work during overcommit: %v", err)
	}
	if _, err := o2.Stop(ctx, "normal"); err != nil {
		t.Fatalf("unrelated instance stop must work during overcommit: %v", err)
	}

	// 5) 人工 discard 收口必须可用：清残留、释放占用、落 Destroyed。
	disc, err := o2.Resolve(ctx, "huge", orchestrator.ResolveDiscard, "", nil, "wipe confirmed")
	if err != nil {
		t.Fatalf("manual discard must be possible while overcommitted: %v", err)
	}
	if disc.State != domain.StateDestroyed || disc.Review != nil {
		t.Fatalf("discard must land Destroyed and clear review: %+v", disc)
	}
	if info, _ := sim.Inspect(ctx, "huge"); info.Status != backend.StatusAbsent {
		t.Fatalf("residue must be removed after discard, got %s", info.Status)
	}

	// 6) 再换第三个进程：记录与占用一致，只剩 normal；超容量解除。
	o3 := freshProcessShared(t, stateDir, sim, cap)
	if g, _ := o3.Get(ctx, "huge"); g.State != domain.StateDestroyed {
		t.Fatalf("tombstone must survive: %s", gotState(g))
	}
	if reviews, _ := o3.ManualReviewList(ctx); len(reviews) != 0 {
		t.Fatalf("review queue must be empty: %+v", reviews)
	}
	snap3 := o3.ResourceSnapshot()
	if snap3.OverCommit {
		t.Fatalf("overcommit must clear after discard: %+v", snap3)
	}
	if snap3.Used != normal.Resources {
		t.Fatalf("only normal must remain: %+v", snap3.Used)
	}
	assertLedgerMatchesRecords(t, o3)

	// 7) 超容量解除后准入恢复；普通容量不足仍是 insufficient_resource（不混为 overcommit）。
	if _, err := o3.Create(ctx, "extra", "extra", smallSpec(1000, 512, 1024)); err != nil {
		t.Fatalf("admission must recover after resolution: %v", err)
	}
	if _, err := o3.Create(ctx, "toobig", "toobig", smallSpec(1000, 512, 1024)); !domain.IsKind(err, domain.KindInsufficientResource) {
		t.Fatalf("plain shortage must stay insufficient_resource: %v", err)
	}
	// 残留实例收口后不应在后端留下任何对象。
	if info, _ := sim.Inspect(ctx, "huge"); info.Status != backend.StatusAbsent {
		t.Fatalf("huge backend object must stay absent: %+v", info)
	}
}

// TestOversizeOrphanAdoptKeepsReservation 覆盖 adopt 收口：确认继续存在时恢复为
// 正常状态、占用按规格如实保留（即便仍超容量），随后常规生命周期可继续并最终释放。
func TestOversizeOrphanAdoptKeepsReservation(t *testing.T) {
	ctx := context.Background()
	cap := smallCap()
	st, dir := sharedFileStore(t)
	sim := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, sim, cap, testLog{t}.Logf)

	huge := smallSpec(3000, 2048, 4096)
	if err := sim.Define(ctx, "big", huge); err != nil {
		t.Fatal(err)
	}
	if rep, err := o1.Reconcile(ctx, testLog{t}.Logf); err != nil || !rep.CapacityExceeded {
		t.Fatalf("reconcile: err=%v exceeded=%v", err, rep.CapacityExceeded)
	}

	// 新进程 adopt 到 Stopped：收口必须成功，占用一分不松（事实就是有这么大负载）。
	o2 := freshProcessShared(t, dir, sim, cap)
	adopted, err := o2.Resolve(ctx, "big", orchestrator.ResolveAdopt, domain.StateStopped, nil, "host rebuilt, keep")
	if err != nil {
		t.Fatalf("adopt must be possible while overcommitted: %v", err)
	}
	if adopted.State != domain.StateStopped || adopted.Review != nil {
		t.Fatalf("adopted must be Stopped with review cleared: %+v", adopted)
	}
	if r, ok := o2.ResourceSnapshot().Reservations["big"]; !ok || r != huge.Resources {
		t.Fatalf("reservation must remain after adopt: ok=%v r=%+v", ok, r)
	}
	if info, _ := sim.Inspect(ctx, "big"); info.Status != backend.StatusStopped {
		t.Fatalf("backend must converge to stopped: %s", info.Status)
	}
	// 仍超容量：新增准入继续被拒。
	if _, err := o2.Create(ctx, "x", "x", smallSpec(1, 1, 1)); !domain.IsKind(err, domain.KindHostOvercommitted) {
		t.Fatalf("still overcommitted after adopt: %v", err)
	}

	// 再换进程：Stopped 与占用自洽；常规 destroy 可用并最终把超大占用释放归零。
	o3 := freshProcessShared(t, dir, sim, cap)
	assertLedgerMatchesRecords(t, o3)
	if _, err := o3.Destroy(ctx, "big"); err != nil {
		t.Fatalf("adopted instance must follow normal lifecycle: %v", err)
	}
	if u := o3.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must be zero after destroy: %+v", u)
	}
	assertLedgerMatchesRecords(t, o3)
}

// TestTotalExceedsButEachFits 覆盖"没有任何单个对象超容量、但重建总额超容量"
// 的已落盘情况：同样只能作为结论暴露，不能冻结命令；只在真正超额的维度上标记。
func TestTotalExceedsButEachFits(t *testing.T) {
	ctx := context.Background()
	cap := smallCap() // cpu=2000
	st, dir := sharedFileStore(t)
	sim := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, sim, cap, testLog{t}.Logf)

	// 正常准入一台 1200 CPU（单独看合法）。
	if _, err := o1.Create(ctx, "a", "a", smallSpec(1200, 600, 1200)); err != nil {
		t.Fatal(err)
	}
	// 再来一个孤儿，自称 1200 CPU（单独也放得下），合计 2400 > 2000。
	if err := sim.Define(ctx, "b", smallSpec(1200, 200, 400)); err != nil {
		t.Fatal(err)
	}
	rep, err := o1.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.CapacityExceeded || len(rep.ExceededDimensions) != 1 || rep.ExceededDimensions[0] != "cpu_millis" {
		t.Fatalf("only cpu must be exceeded: %+v", rep.ExceededDimensions)
	}

	// 换进程：查询可用、自洽；mem/disk 还有余量也不能放行新准入（宿主整体已超卖）。
	o2 := freshProcessShared(t, dir, sim, cap)
	if _, err := o2.List(ctx); err != nil {
		t.Fatalf("list must work: %v", err)
	}
	if _, err := o2.Create(ctx, "c", "c", smallSpec(1, 500, 500)); !domain.IsKind(err, domain.KindHostOvercommitted) {
		t.Fatalf("any new admission must be rejected while host overcommitted: %v", err)
	}
	// 收口孤儿 b 后 CPU 回到 1200，超容量解除，准入恢复。
	if _, err := o2.Resolve(ctx, "b", orchestrator.ResolveDiscard, "", nil, ""); err != nil {
		t.Fatalf("discard must work: %v", err)
	}
	o3 := freshProcessShared(t, dir, sim, cap)
	if o3.ResourceSnapshot().OverCommit {
		t.Fatal("overcommit must clear")
	}
	if _, err := o3.Create(ctx, "c", "c", smallSpec(800, 1, 1)); err != nil {
		t.Fatalf("admission must recover: %v", err)
	}
	assertLedgerMatchesRecords(t, o3)
}

// TestOversizeClaimAdoptUnknownSpec 覆盖读不出规格的孤儿在超容量口径下的认领：
// 人工 --claim 补报的规格即"已存在事实"，登记不做容量准入，收口必须落地；
// 跨进程读数与记录一致，随后 discard 释放。
func TestOversizeClaimAdoptUnknownSpec(t *testing.T) {
	ctx := context.Background()
	cap := smallCap()
	st, dir := sharedFileStore(t)
	sim := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, sim, cap, testLog{t}.Logf)

	now := time.Now().UTC()
	if err := st.Put(ctx, &domain.Instance{
		ID: "ghost", Name: "ghost", State: domain.StateManualReview, Backend: "sim",
		CreatedAt: now, UpdatedAt: now, LastEvent: "quarantined: orphan without spec",
		Review: &domain.Review{
			Reason: domain.ReviewOrphanBackend, RecordState: "(none)", ActualState: "stopped",
			RaisedAt: now, SpecKnown: false,
			Detail: "backend residue without readable spec",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := o1.RestoreFromStore(ctx); err != nil {
		t.Fatalf("restore must not fail: %v", err)
	}
	// 规格未知 -> 暂不占用。
	if _, ok := o1.ResourceSnapshot().Reservations["ghost"]; ok {
		t.Fatal("unknown-spec orphan must not be reserved before claim")
	}

	// 补报一个超过容量的规格并认领：作为既存事实登记，收口必须成功。
	claim := smallSpec(3000, 2048, 4096)
	adopted, err := o1.Resolve(ctx, "ghost", orchestrator.ResolveAdopt, domain.StateStopped, &claim, "claimed big")
	if err != nil {
		t.Fatalf("claim-adopt must succeed even if claimed spec exceeds capacity: %v", err)
	}
	if adopted.State != domain.StateStopped {
		t.Fatalf("want Stopped, got %s", adopted.State)
	}
	snap := o1.ResourceSnapshot()
	if !snap.OverCommit || snap.Reservations["ghost"] != claim.Resources {
		t.Fatalf("claimed spec must be registered honestly (over-commit allowed): %+v", snap)
	}

	// 换进程：记录 Stopped + 占用 = 补报规格，自洽。
	o2 := freshProcessShared(t, dir, sim, cap)
	assertLedgerMatchesRecords(t, o2)
	if g, _ := o2.Get(ctx, "ghost"); g.State != domain.StateStopped {
		t.Fatalf("fresh process must see Stopped: %s", gotState(g))
	}
	// 收尾：discard 释放，归零。
	if _, err := o2.Resolve(ctx, "ghost", orchestrator.ResolveDiscard, "", nil, ""); err != nil {
		// 已非 ManualReview，改用常规 destroy。
		if _, err := o2.Destroy(ctx, "ghost"); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	}
	if u := o2.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must end at zero: %+v", u)
	}
}

func gotState(in *domain.Instance) domain.State {
	if in == nil {
		return "<nil>"
	}
	return in.State
}

// sharedFileStore 返回一个落在独立临时目录下的 FileStore，以及该目录根路径，
// 供后续 freshProcessShared 用"全新编排器 + 同一磁盘"模拟换进程。
func sharedFileStore(t *testing.T) (*store.FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	return st, dir
}

// freshProcessShared 等价于 CLI 新起一个进程：同一磁盘上的全新编排器，
// 启动先 RestoreFromStore（超容量不再失败）。
func freshProcessShared(t *testing.T, dir string, h backend.Handle, cap domain.Resources) *orchestrator.Orchestrator {
	t.Helper()
	return freshProcess(t, dir, h, cap)
}
