package reconcile_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

func testLogger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Helper()
		t.Logf("[reconcile-seq] "+format, args...)
	}
}

func specOf(cpu, mem, disk int64) domain.Spec {
	return domain.Spec{Image: "img", Resources: domain.Resources{CPUMillis: cpu, MemoryMB: mem, DiskMB: disk}}
}

func writeRecord(t *testing.T, st store.Store, id string, state domain.State, spec domain.Spec) {
	t.Helper()
	now := time.Now().UTC()
	inst := &domain.Instance{
		ID: id, Name: id, Spec: spec, State: state, Backend: "sim",
		CreatedAt: now, UpdatedAt: now, LastEvent: "test-seeded:" + string(state),
	}
	if err := st.Put(context.Background(), inst); err != nil {
		t.Fatalf("seed record %s: %v", id, err)
	}
}

type env struct {
	st store.Store
	h  *backend.SimHandle
	o  *orchestrator.Orchestrator
}

func newEnv(t *testing.T, cap domain.Resources) *env {
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	h := backend.NewSimHandle(backend.FaultHooks{})
	o := orchestrator.New(st, h, cap, testLogger(t))
	return &env{st: st, h: h, o: o}
}

func (e *env) reconcile(t *testing.T) *reconcile.Report {
	t.Helper()
	rep, err := reconcile.Run(context.Background(), e.st, e.h,
		func(alive map[string]domain.Resources) (string, error) {
			snap, err := e.o.Quota().Rebuild(alive)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("used={cpu:%d,mem:%d,disk:%d} reservations=%d",
				snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB, len(snap.Reservations)), nil
		}, testLogger(t))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, v := range rep.Verdicts {
		t.Logf("[verdict] id=%-10s record=%-10s actual=%-10s outcome=%-11s after=%-35s action=%s",
			v.InstanceID, v.RecordState, v.ActualState, v.Outcome, v.After, v.Action)
	}
	t.Logf("[reconcile-conclusion] orphans=%v quarantine=%v converged=%t detail=%s",
		rep.Orphans, rep.Quarantine, rep.QuotaConverged, rep.QuotaDetail)
	return rep
}

func findVerdict(rep *reconcile.Report, id string) reconcile.Verdict {
	for _, v := range rep.Verdicts {
		if v.InstanceID == id {
			return v
		}
	}
	return reconcile.Verdict{Outcome: "<missing>"}
}

// TestReconcileMatched 正常情况下全部 Matched。
func TestReconcileMatched(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})

	if _, err := e.o.Create(ctx, "a", "a", specOf(1000, 512, 1024)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Create(ctx, "b", "b", specOf(1000, 512, 1024)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Start(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Destroy(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	if err := e.o.RestoreFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	rep := e.reconcile(t)
	for _, v := range rep.Verdicts {
		if v.Outcome != reconcile.OutcomeMatched {
			t.Fatalf("id=%s want Matched, got %s (%s)", v.InstanceID, v.Outcome, v.Detail)
		}
	}
	if !rep.QuotaConverged || len(rep.Quarantine) != 0 {
		t.Fatalf("clean world must converge without quarantine: %+v", rep)
	}
	snap := e.o.ResourceSnapshot()
	if snap.Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("ledger must account only alive b: %+v", snap)
	}
}

// TestReconcileGuestCrash Running 记录 + guest 已退出 -> 确定收敛 Stopped。
func TestReconcileGuestCrash(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})
	if _, err := e.o.Create(ctx, "crash", "crash", specOf(1000, 512, 1024)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Start(ctx, "crash"); err != nil {
		t.Fatal(err)
	}
	// 模拟 guest 异常退出：直接在后端停止，记录仍停留在 Running。
	if err := e.h.Stop(ctx, "crash"); err != nil {
		t.Fatal(err)
	}

	rep := e.reconcile(t)
	v := findVerdict(rep, "crash")
	if v.Outcome != reconcile.OutcomeRecovered || v.After != "Stopped" {
		t.Fatalf("crash recovery wrong: %+v", v)
	}
	got, _ := e.st.Get(ctx, "crash")
	if got.State != domain.StateStopped {
		t.Fatalf("record must persist as Stopped, got %s", got.State)
	}
	if !rep.QuotaConverged {
		t.Fatal("quota must remain accounted (Stopped still reserved)")
	}
	snap := e.o.ResourceSnapshot()
	if snap.Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("crashed (Stopped) instance keeps reservation: %+v", snap)
	}
}

// TestReconcileStartWindow Start 副作用生效但 Running 未落盘 -> 采纳为 Running。
func TestReconcileStartWindow(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})
	spec := specOf(1000, 512, 1024)
	if _, err := e.o.Create(ctx, "win", "win", spec); err != nil {
		t.Fatal(err)
	}
	// 绕过编排层直接让后端 guest 跑起来（模拟 Start 成功后、落盘前崩溃）。
	if err := e.h.Start(ctx, "win"); err != nil {
		t.Fatal(err)
	}

	rep := e.reconcile(t)
	v := findVerdict(rep, "win")
	if v.Outcome != reconcile.OutcomeRecovered || v.After != "Running" {
		t.Fatalf("start-window adoption wrong: %+v", v)
	}
	got, _ := e.st.Get(ctx, "win")
	if got.State != domain.StateRunning {
		t.Fatalf("record adopted to Running, got %s", got.State)
	}
}

// TestReconcileHalfDoneDestroy Undefine 已完成而 Destroyed 未落盘 -> 收敛终态并释放配额。
func TestReconcileHalfDoneDestroy(t *testing.T) {
	ctx := context.Background()
	cap := domain.Resources{CPUMillis: 2000, MemoryMB: 1024, DiskMB: 2048}
	e := newEnv(t, cap)
	spec := specOf(1000, 512, 1024)
	if _, err := e.o.Create(ctx, "half", "half", spec); err != nil {
		t.Fatal(err)
	}
	// 重建一个"重启后"的新编排器：先恢复台账（记录仍是 Defined）。
	o2 := orchestrator.New(e.st, e.h, cap, testLogger(t))
	if err := o2.RestoreFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	if u := o2.ResourceSnapshot().Used; u != spec.Resources {
		t.Fatalf("restore should reserve half: %+v", u)
	}
	// 后端定义已不存在（Destroy 的 Undefine 完成后崩溃）。
	if err := e.h.Undefine(ctx, "half"); err != nil {
		t.Fatal(err)
	}

	rep, err := o2.Reconcile(ctx, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	v := findVerdict(rep, "half")
	if v.Outcome != reconcile.OutcomeRecovered || v.After != "Destroyed" {
		t.Fatalf("half-done destroy must converge to Destroyed: %+v", v)
	}
	got, _ := e.st.Get(ctx, "half")
	if got.State != domain.StateDestroyed {
		t.Fatalf("persisted state = %s want Destroyed", got.State)
	}
	if u := o2.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("quota for half-done destroy must be released: %+v", u)
	}
}

// TestReconcileRunningMissingBackend Running 记录但后端整体消失 -> 隔离，不自动重建。
func TestReconcileRunningMissingBackend(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})
	spec := specOf(1000, 512, 1024)
	if _, err := e.o.Create(ctx, "lost", "lost", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Start(ctx, "lost"); err != nil {
		t.Fatal(err)
	}
	// 宿主被重建：后端对象彻底消失。
	if err := e.h.Undefine(ctx, "lost"); err != nil {
		t.Fatal(err)
	}
	// 重新写回 Running 记录（Undefine 本身不动记录，这里记录本就还是 Running）。

	rep := e.reconcile(t)
	v := findVerdict(rep, "lost")
	if v.Outcome != reconcile.OutcomeQuarantined {
		t.Fatalf("ambiguous loss must quarantine, got %s (%s)", v.Outcome, v.Detail)
	}
	got, _ := e.st.Get(ctx, "lost")
	if got.State != domain.StateRunning {
		t.Fatalf("quarantine must NOT overwrite record, got %s", got.State)
	}
	// 被隔离实例不纳入台账重建 -> 其配额被显式摘出等待裁决，避免漂移。
	if _, ok := e.o.ResourceSnapshot().Reservations["lost"]; ok {
		t.Fatal("quarantined instance must be excluded from rebuilt ledger until resolved")
	}
}

// TestReconcileOrphanBackend 后端残留无记录 -> 孤儿隔离，绝不静默删除。
func TestReconcileOrphanBackend(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})
	spec := specOf(1000, 512, 1024)
	if err := e.h.Define(ctx, "orphan", spec); err != nil {
		t.Fatal(err)
	}
	if err := e.h.Start(ctx, "orphan"); err != nil {
		t.Fatal(err)
	}

	rep := e.reconcile(t)
	found := false
	for _, id := range rep.Orphans {
		if id == "orphan" {
			found = true
		}
	}
	if !found {
		t.Fatalf("orphan must be reported: %+v", rep.Orphans)
	}
	info, _ := e.h.Inspect(ctx, "orphan")
	if info.Status != backend.StatusRunning {
		t.Fatalf("orphan residue must be preserved, not killed: %+v", info)
	}
	if rep.QuotaConverged != true {
		t.Fatal("ledger with no alive records should still converge to empty")
	}
}

// TestReconcileTombstoneResidue 墓碑 + 后端残留 -> 幂等清除残留并保持 Destroyed。
func TestReconcileTombstoneResidue(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384})
	spec := specOf(1000, 512, 1024)
	if _, err := e.o.Create(ctx, "z", "z", spec); err != nil {
		t.Fatal(err)
	}
	// 构造异常组合：记录已是 Destroyed，但后端 Define 残留仍在。
	_ = e.h.Undefine(ctx, "z")
	writeRecord(t, e.st, "z", domain.StateDestroyed, spec)
	if err := e.h.Define(ctx, "z", spec); err != nil {
		t.Fatal(err)
	}

	rep := e.reconcile(t)
	v := findVerdict(rep, "z")
	if v.Outcome != reconcile.OutcomeRecovered || v.Action != "undefine-residue" {
		t.Fatalf("tombstone residue recovery wrong: %+v", v)
	}
	info, _ := e.h.Inspect(ctx, "z")
	if info.Status != backend.StatusAbsent {
		t.Fatalf("residue should be removed: %+v", info)
	}
}

// TestReconcileMixedDeterministic 一次对账覆盖全部崩溃窗口组合，结论须逐条可解释。
func TestReconcileMixedDeterministic(t *testing.T) {
	ctx := context.Background()
	cap := domain.Resources{CPUMillis: 10000, MemoryMB: 8192, DiskMB: 20000}
	e := newEnv(t, cap)
	spec := specOf(1000, 512, 1024)

	// 1) 正常 Defined
	if _, err := e.o.Create(ctx, "defined1", "defined1", spec); err != nil {
		t.Fatal(err)
	}
	// 2) Running 记录 + guest 崩溃
	if _, err := e.o.Create(ctx, "crash1", "crash1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Start(ctx, "crash1"); err != nil {
		t.Fatal(err)
	}
	_ = e.h.Stop(ctx, "crash1")
	// 3) Defined 记录 + guest 已在跑（Start 窗口）
	if _, err := e.o.Create(ctx, "startwin1", "startwin1", spec); err != nil {
		t.Fatal(err)
	}
	_ = e.h.Start(ctx, "startwin1")
	// 4) Stopped 记录 + 后端缺失（Destroy 窗口）
	if _, err := e.o.Create(ctx, "halfkill1", "halfkill1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Start(ctx, "halfkill1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Stop(ctx, "halfkill1"); err != nil {
		t.Fatal(err)
	}
	_ = e.h.Undefine(ctx, "halfkill1")
	// 5) Running 记录 + 后端整体消失（不可判定）
	if _, err := e.o.Create(ctx, "ambiguous1", "ambiguous1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := e.o.Start(ctx, "ambiguous1"); err != nil {
		t.Fatal(err)
	}
	_ = e.h.Undefine(ctx, "ambiguous1")
	// 6) 孤儿
	_ = e.h.Define(ctx, "orphan1", spec)

	rep := e.reconcile(t)

	expect := map[string]reconcile.Outcome{
		"defined1":   reconcile.OutcomeMatched,
		"crash1":     reconcile.OutcomeRecovered,
		"startwin1":  reconcile.OutcomeRecovered,
		"halfkill1":  reconcile.OutcomeRecovered,
		"ambiguous1": reconcile.OutcomeQuarantined,
		"orphan1":    reconcile.OutcomeQuarantined,
	}
	for id, want := range expect {
		if got := findVerdict(rep, id).Outcome; got != want {
			t.Errorf("id=%s outcome=%s want %s", id, got, want)
		}
	}
	// 配额守恒：defined1 + crash1 + startwin1 = 3 台存活；
	// halfkill1 收敛 Destroyed（释放）；ambiguous1 隔离（摘出）；orphan1 无记录不占配额。
	snap := e.o.ResourceSnapshot()
	wantUsed := domain.Resources{CPUMillis: 3000, MemoryMB: 1536, DiskMB: 3072}
	if snap.Used != wantUsed {
		t.Fatalf("ledger after mixed reconcile wrong: got %+v want %+v", snap.Used, wantUsed)
	}
	if len(snap.Reservations) != 3 {
		t.Fatalf("reservations = %v", snap.Reservations)
	}
	t.Logf("[snapshot] mixed reconcile settled: used=%+v reservations=%d", snap.Used, len(snap.Reservations))
}
