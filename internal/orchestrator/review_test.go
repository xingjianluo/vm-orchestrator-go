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

// freshProcess 用同一个文件存储与后端目录构造一个"全新进程"的编排器，
// 并在启动时执行 RestoreFromStore（与 CLI 行为一致）。
func freshProcess(t *testing.T, dir string, h backend.Handle, cap domain.Resources) *orchestrator.Orchestrator {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	o := orchestrator.New(st, h, cap, testLog{t}.Logf)
	if err := o.RestoreFromStore(context.Background()); err != nil {
		t.Fatalf("fresh process restore: %v", err)
	}
	return o
}

// TestManualReviewFullFlowAdopt 覆盖：
// 判定挂起 -> 换新进程/重启后仍可见 -> 常规变更被拒且原因可区分 -> 人工 adopt 收口
// -> 实例记录与占用同步更新。
func TestManualReviewFullFlowAdopt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	spec := smallSpec(1000, 512, 1024)

	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	h := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, h, cap, testLog{t}.Logf)

	if _, err := o1.Create(ctx, "db", "db", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Start(ctx, "db"); err != nil {
		t.Fatal(err)
	}
	// 宿主被重建：Running 记录还在，后端承载整体消失。
	if err := h.Undefine(ctx, "db"); err != nil {
		t.Fatal(err)
	}
	rep, err := o1.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if v := verdictOf(rep, "db"); v.Outcome != reconcile.OutcomeQuarantined || v.After != "ManualReview" {
		t.Fatalf("want persisted ManualReview, got %+v", v)
	}
	// 挂起期间占用继续计入。
	if res, ok := o1.ResourceSnapshot().Reservations["db"]; !ok || res != spec.Resources {
		t.Fatalf("held instance must keep reservation: ok=%v res=%+v", ok, res)
	}

	// === 模拟换一个新进程（宿主再重启也一样：记录与后端目录都在磁盘上）===
	o2 := freshProcess(t, dir, h, cap)
	got, err := o2.Get(ctx, "db")
	if err != nil {
		t.Fatalf("new process get: %v", err)
	}
	if got.State != domain.StateManualReview {
		t.Fatalf("new process must still see ManualReview, got %s", got.State)
	}
	if got.Review == nil || got.Review.Reason != domain.ReviewRunningMissingBackend {
		t.Fatalf("review reason must survive restart: %+v", got.Review)
	}
	if got.Review.RecordState != "Running" || got.Review.ActualState != "absent" {
		t.Fatalf("review现场必须保留 record/actual: %+v", got.Review)
	}
	// list 也能看到。
	list, err := o2.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !anyState(list, "db", domain.StateManualReview) {
		t.Fatalf("list must include held instance: %+v", list)
	}
	reviews, err := o2.ManualReviewList(ctx)
	if err != nil || len(reviews) != 1 {
		t.Fatalf("reviews list wrong: n=%d err=%v", len(reviews), err)
	}
	// 新进程台账与事实自洽：挂起实例仍占着。
	if res, ok := o2.ResourceSnapshot().Reservations["db"]; !ok || res != spec.Resources {
		t.Fatalf("new process usage must still count held instance: ok=%v res=%+v", ok, res)
	}

	// === 常规变更一律拒绝，且原因与既有的 not_found/illegal/insufficient 可区分 ===
	for _, op := range []string{"start", "stop", "destroy"} {
		var e error
		switch op {
		case "start":
			_, e = o2.Start(ctx, "db")
		case "stop":
			_, e = o2.Stop(ctx, "db")
		case "destroy":
			_, e = o2.Destroy(ctx, "db")
		}
		if !domain.IsKind(e, domain.KindManualReviewPending) {
			t.Fatalf("%s on held instance must be manual_review_pending, got %v", op, e)
		}
	}
	// 对比：不存在实例仍是 not_found；非法迁移仍是 illegal_transition。
	if _, e := o2.Start(ctx, "nope"); !domain.IsKind(e, domain.KindNotFound) {
		t.Fatalf("missing id must be not_found, got %v", e)
	}
	// 拒绝后状态与后端残留都不能被悄悄改动。
	if g, _ := o2.Get(ctx, "db"); g.State != domain.StateManualReview {
		t.Fatalf("state must remain held after rejected ops, got %s", g.State)
	}

	// === 人工确认它应继续存在：adopt 到 Running（后端按目标态重建承载）===
	adopted, err := o2.Resolve(ctx, "db", orchestrator.ResolveAdopt, domain.StateRunning, nil, "host rebuilt, keep db")
	if err != nil {
		t.Fatalf("resolve adopt: %v", err)
	}
	if adopted.State != domain.StateRunning || adopted.Review != nil {
		t.Fatalf("adopted instance must be Running with review cleared: %+v", adopted)
	}
	info, _ := h.Inspect(ctx, "db")
	if info.Status != backend.StatusRunning {
		t.Fatalf("backend must be running after adopt-to-Running, got %s", info.Status)
	}
	// 占用保持不变（收口不新增也不释放）。
	if res, ok := o2.ResourceSnapshot().Reservations["db"]; !ok || res != spec.Resources {
		t.Fatalf("usage must remain reserved after adopt: ok=%v res=%+v", ok, res)
	}

	// === 再换第三个进程看：确定状态与占用都自洽 ===
	o3 := freshProcess(t, dir, h, cap)
	if g, _ := o3.Get(ctx, "db"); g.State != domain.StateRunning {
		t.Fatalf("third process must see Running, got %s", g.State)
	}
	if res, ok := o3.ResourceSnapshot().Reservations["db"]; !ok || res != spec.Resources {
		t.Fatalf("third process usage mismatch: ok=%v res=%+v", ok, res)
	}
	// 收口后常规生命周期恢复可用：stop/destroy 正常，占用最终释放。
	if _, err := o3.Stop(ctx, "db"); err != nil {
		t.Fatal(err)
	}
	if _, err := o3.Destroy(ctx, "db"); err != nil {
		t.Fatal(err)
	}
	if u := o3.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must be zero after destroy: %+v", u)
	}
}

// TestManualReviewDiscardFrees 覆盖人工 discard 收口：彻底收掉并释放资源，记录与占用同步。
func TestManualReviewDiscardFrees(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	spec := smallSpec(1000, 512, 1024)

	st, _ := store.NewFileStore(filepath.Join(dir, "state"))
	h := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, h, cap, testLog{t}.Logf)
	if _, err := o1.Create(ctx, "cache", "cache", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Start(ctx, "cache"); err != nil {
		t.Fatal(err)
	}
	if err := h.Undefine(ctx, "cache"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Reconcile(ctx, testLog{t}.Logf); err != nil {
		t.Fatal(err)
	}

	// 新进程确认清理。
	o2 := freshProcess(t, dir, h, cap)
	disc, err := o2.Resolve(ctx, "cache", orchestrator.ResolveDiscard, "", nil, "wipe confirmed")
	if err != nil {
		t.Fatalf("resolve discard: %v", err)
	}
	if disc.State != domain.StateDestroyed || disc.Review != nil {
		t.Fatalf("discard must land on Destroyed and clear review: %+v", disc)
	}
	info, _ := h.Inspect(ctx, "cache")
	if info.Status != backend.StatusAbsent {
		t.Fatalf("backend residue must be gone after discard, got %s", info.Status)
	}
	if u := o2.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must be released after discard: %+v", u)
	}
	if _, ok := o2.ResourceSnapshot().Reservations["cache"]; ok {
		t.Fatal("reservation must be gone after discard")
	}

	// 再换进程：Destroyed 墓碑可见、占用为零，且不再出现在 reviews 列表。
	o3 := freshProcess(t, dir, h, cap)
	g, err := o3.Get(ctx, "cache")
	if err != nil || g.State != domain.StateDestroyed {
		t.Fatalf("tombstone must survive, got %+v err=%v", g, err)
	}
	reviews, _ := o3.ManualReviewList(ctx)
	if len(reviews) != 0 {
		t.Fatalf("resolved instance must leave the review queue: %+v", reviews)
	}
	if u := o3.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("fresh process usage must be zero: %+v", u)
	}
	// Destroyed 的 destroy 仍幂等。
	if _, err := o3.Destroy(ctx, "cache"); err != nil {
		t.Fatalf("idempotent destroy on tombstone: %v", err)
	}
}

// TestManualReviewOrphanAdoptWithClaim 覆盖后端孤儿（无记录）落盘挂起后，
// adopt 收口恢复正常、占用计入；以及规格不可读孤儿需要补报的场景。
func TestManualReviewOrphanAdopt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	spec := smallSpec(1000, 512, 1024)

	st, _ := store.NewFileStore(filepath.Join(dir, "state"))
	h := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, h, cap, testLog{t}.Logf)
	if err := h.Define(ctx, "ghost", spec); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(ctx, "ghost"); err != nil {
		t.Fatal(err)
	}
	rep, err := o1.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if v := verdictOf(rep, "ghost"); v.Outcome != reconcile.OutcomeQuarantined {
		t.Fatalf("orphan must quarantine: %+v", v)
	}
	// 孤儿已落盘，且规格可知 -> 已计入占用。
	if res, ok := o1.ResourceSnapshot().Reservations["ghost"]; !ok || res != spec.Resources {
		t.Fatalf("spec-known orphan must count: ok=%v res=%+v", ok, res)
	}

	// 新进程 adopt 成 Stopped（承载会被停掉，占用保留）。
	o2 := freshProcess(t, dir, h, cap)
	adopted, err := o2.Resolve(ctx, "ghost", orchestrator.ResolveAdopt, domain.StateStopped, nil, "belongs to billing team")
	if err != nil {
		t.Fatalf("orphan adopt: %v", err)
	}
	if adopted.State != domain.StateStopped {
		t.Fatalf("want Stopped, got %s", adopted.State)
	}
	info, _ := h.Inspect(ctx, "ghost")
	if info.Status != backend.StatusStopped {
		t.Fatalf("backend must be stopped after adopt-to-Stopped, got %s", info.Status)
	}
	if res, ok := o2.ResourceSnapshot().Reservations["ghost"]; !ok || res != spec.Resources {
		t.Fatalf("usage remains after adopt-to-Stopped: ok=%v res=%+v", ok, res)
	}
}

func verdictOf(rep *reconcile.Report, id string) reconcile.Verdict {
	for _, v := range rep.Verdicts {
		if v.InstanceID == id {
			return v
		}
	}
	return reconcile.Verdict{Outcome: "<missing>"}
}

func anyState(list []*domain.Instance, id string, s domain.State) bool {
	for _, in := range list {
		if in.ID == id && in.State == s {
			return true
		}
	}
	return false
}

// TestResolveUnknownSpecOrphan 直接构造一条"规格不可读孤儿"的 ManualReview 记录，
// 验证 adopt 需要补报规格（补登记占用）、discard 不需要释放预留。
func TestResolveUnknownSpecOrphan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	st := newFileStore(t, dir)
	h := backend.NewSimHandle(backend.FaultHooks{})
	o := orchestrator.New(st, h, cap, testLog{t}.Logf)

	now := time.Now().UTC()
	if err := st.Put(ctx, &domain.Instance{
		ID:        "unk",
		Name:      "unk",
		State:     domain.StateManualReview,
		Backend:   "sim",
		CreatedAt: now, UpdatedAt: now,
		LastEvent: "quarantined: orphan",
		Review: &domain.Review{
			Reason: domain.ReviewOrphanBackend, RecordState: "(none)", ActualState: "stopped",
			RaisedAt: now, SpecKnown: false,
			Detail: "backend residue without readable spec",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := o.RestoreFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	// 规格不可读 -> 不在台账。
	if _, ok := o.ResourceSnapshot().Reservations["unk"]; ok {
		t.Fatal("unknown-spec orphan must not be reserved until claimed")
	}

	// 无 claim 的 adopt 拒绝。
	if _, err := o.Resolve(ctx, "unk", orchestrator.ResolveAdopt, domain.StateStopped, nil, ""); !domain.IsKind(err, domain.KindInvalidArgument) {
		t.Fatalf("adopt without claim must be invalid_argument, got %v", err)
	}
	claim := smallSpec(100, 200, 300)
	adopted, err := o.Resolve(ctx, "unk", orchestrator.ResolveAdopt, domain.StateStopped, &claim, "claimed")
	if err != nil {
		t.Fatalf("claimed adopt: %v", err)
	}
	if adopted.State != domain.StateStopped || adopted.Spec.Resources != claim.Resources {
		t.Fatalf("adopted state/spec wrong: %+v", adopted)
	}
	if res, ok := o.ResourceSnapshot().Reservations["unk"]; !ok || res != claim.Resources {
		t.Fatalf("claimed orphan must now be reserved: ok=%v res=%+v", ok, res)
	}

	// 再造一个同类型的走 discard：无预留但仍应收成 Destroyed 且不报错。
	if err := st.Put(ctx, &domain.Instance{
		ID: "unk2", Name: "unk2", State: domain.StateManualReview, Backend: "sim",
		CreatedAt: now, UpdatedAt: now, LastEvent: "q",
		Review: &domain.Review{Reason: domain.ReviewOrphanBackend, RaisedAt: now, SpecKnown: false},
	}); err != nil {
		t.Fatal(err)
	}
	disc, err := o.Resolve(ctx, "unk2", orchestrator.ResolveDiscard, "", nil, "")
	if err != nil {
		t.Fatalf("discard unknown-spec orphan should not require release: %v", err)
	}
	if disc.State != domain.StateDestroyed {
		t.Fatalf("want Destroyed, got %s", disc.State)
	}
}

// TestResolveRequiresManualReview 非 ManualReview 实例调用收口必须被拒。
func TestResolveRequiresManualReview(t *testing.T) {
	ctx := context.Background()
	o, _ := newHarness(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})
	if _, err := o.Create(ctx, "n", "n", smallSpec(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Resolve(ctx, "n", orchestrator.ResolveAdopt, domain.StateRunning, nil, ""); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("resolve on non-review instance must be illegal_transition, got %v", err)
	}
	if _, err := o.Resolve(ctx, "missing", orchestrator.ResolveDiscard, "", nil, ""); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("resolve missing id must be not_found, got %v", err)
	}
}
