package reconcile_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

type env struct {
	dir string
	bk  *backend.ProcessBackend
}

func setup(t *testing.T, total model.Resources, fault backend.FaultPoint) (*env, *orchestrator.Manager, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	pb, err := backend.NewProcessBackend(filepath.Join(dir, "backend"))
	if err != nil {
		t.Fatal(err)
	}
	var bk backend.Backend = backend.NewFaultyBackend(pb, fault)
	qp := quota.New(total)
	mgr, err := orchestrator.New(orchestrator.Options{
		Store: st, Backend: bk, Quota: qp,
		Logger: orchestrator.NewSlogLogger(slog.LevelError),
	})
	if err != nil {
		t.Fatal(err)
	}
	// 全新启动也跑一次对账（空记录），模拟服务启动顺序。
	if _, err := reconcile.New(mgr).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.KillAllByDir(filepath.Join(dir, "backend")) })
	return &env{dir: dir, bk: pb}, mgr, st
}

// boot 模拟异常退出后重新启动：空配额池 + 新管理器 + 对账，返回报告。
func (e *env) boot(t *testing.T, total model.Resources) (*orchestrator.Manager, *reconcile.Report) {
	t.Helper()
	st, err := store.New(filepath.Join(e.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	pb, err := backend.NewProcessBackend(filepath.Join(e.dir, "backend"))
	if err != nil {
		t.Fatal(err)
	}
	qp := quota.New(total) // 重启时内存配额已丢失，必须靠对账重建
	mgr, err := orchestrator.New(orchestrator.Options{
		Store: st, Backend: pb, Quota: qp,
		Logger: orchestrator.NewSlogLogger(slog.LevelError),
	})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := reconcile.New(mgr).Run(context.Background())
	if err != nil {
		t.Fatalf("reconcile run: %v", err)
	}
	return mgr, rep
}

func findItem(r *reconcile.Report, id string) *reconcile.Item {
	for i := range r.Items {
		if r.Items[i].ID == id {
			return &r.Items[i]
		}
	}
	return nil
}

func TestReconcileStableStates(t *testing.T) {
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{})

	if _, err := mgr.Create(ctx, "stopped-vm", "stopped-vm", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Create(ctx, "running-vm", "running-vm", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Start(ctx, "running-vm"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Destroy(ctx, "stopped-vm"); err != nil {
		t.Fatal(err)
	}

	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())

	got, err := mgr2.Get(ctx, "running-vm")
	if err != nil || got.State != model.StateRunning {
		t.Fatalf("running-vm must remain Running, got %v (%v)", got, err)
	}
	got, err = mgr2.Get(ctx, "stopped-vm")
	if err != nil || got.State != model.StateDestroyed {
		t.Fatalf("stopped-vm must remain Destroyed, got %v (%v)", got, err)
	}
	// 配额按记录重建：只有 running-vm 占用。
	snaps, qv, _ := mgr2.Snapshot(ctx)
	if qv.Used != spec {
		t.Fatalf("rebuilt quota used=%+v want=%+v", qv.Used, spec)
	}
	t.Logf("[resource snapshot] %d records, used=%+v", len(snaps), qv.Used)
}

func TestReconcileCrashAfterProcessExit(t *testing.T) {
	// Start 成功后进程立即被杀（崩溃），管理器未来得及观测；重启对账应收敛 Stopped。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{StartKillAfter: true})
	if _, err := mgr.Create(ctx, "vm1", "vm1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Start(ctx, "vm1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := mgr.Get(ctx, "vm1"); got.State != model.StateRunning {
		t.Fatalf("precondition: record Running, got %s", got.State)
	}

	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())
	item := findItem(rep, "vm1")
	if item == nil || item.Conclusion != reconcile.ConclusionRecovered ||
		item.Before != "Running" || item.After != "Stopped" {
		t.Fatalf("want Running -> Stopped recovered, got %+v", item)
	}
	got, _ := mgr2.Get(ctx, "vm1")
	if got.State != model.StateStopped || got.Reason == "" {
		t.Fatalf("want Stopped with explanatory reason, got %+v", got)
	}
	// 配额仍保留（Stopped 仍占宿主定义资源）。
	if !qvUsed(mgr2, ctx).Equal(spec) {
		t.Fatalf("quota should remain reserved for Stopped instance")
	}
}

func TestReconcileStartDidNotTakeEffect(t *testing.T) {
	// Start 后端失败：记录停在 Starting、定义存在、无运行实体 -> Stopped。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{FailStart: true})
	if _, err := mgr.Create(ctx, "vm1", "vm1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Start(ctx, "vm1"); err == nil {
		t.Fatal("injected start must fail")
	}
	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())
	item := findItem(rep, "vm1")
	if item == nil || item.Before != "Starting" || item.After != "Stopped" ||
		item.Conclusion != reconcile.ConclusionRecovered {
		t.Fatalf("want Starting -> Stopped recovered, got %+v", item)
	}
	if got, _ := mgr2.Get(ctx, "vm1"); got.State != model.StateStopped {
		t.Fatalf("got %s", got.State)
	}
}

func TestReconcileCrashedBeforeDefine(t *testing.T) {
	// Define 失败留下 Pending 且后端无定义 -> 确定地 Destroyed 并释放配额。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{FailDefine: true})
	if _, err := mgr.Create(ctx, "vm1", "vm1", spec); err == nil {
		t.Fatal("injected define must fail")
	}
	if got, _ := mgr.Get(ctx, "vm1"); got.State != model.StatePending {
		t.Fatalf("precondition Pending, got %s", got.State)
	}
	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())
	item := findItem(rep, "vm1")
	if item == nil || item.After != "Destroyed" || item.Conclusion != reconcile.ConclusionRecovered {
		t.Fatalf("want Pending -> Destroyed recovered, got %+v", item)
	}
	if q := qvUsed(mgr2, ctx); !q.Equal(model.Resources{}) {
		t.Fatalf("quota must be released for destroyed Pending, used=%+v", q)
	}
}

func TestReconcileMissingDefinitionIsMarked(t *testing.T) {
	// 记录 Stopped 但定义文件被外部删除：无法解释，必须标记而非静默重建。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{})
	if _, err := mgr.Create(ctx, "vm1", "vm1", spec); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(e.dir, "backend", "defs", "vm1.json")); err != nil {
		t.Fatal(err)
	}
	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())
	item := findItem(rep, "vm1")
	if item == nil || item.After != "Failed" || item.Conclusion != reconcile.ConclusionMarked {
		t.Fatalf("want marked Failed, got %+v", item)
	}
	got, _ := mgr2.Get(ctx, "vm1")
	if got.State != model.StateFailed || got.Reason == "" {
		t.Fatalf("Failed instance must carry reason, got %+v", got)
	}
	// 常规生命周期操作必须被拒绝（原因可区分）。
	if _, err := mgr2.Start(ctx, "vm1"); !errors.Is(err, model.ErrInconsistent) {
		t.Fatalf("Start on Failed must return ErrInconsistent, got %v", err)
	}
	// 标记为人工介入期间配额不静默释放（避免漂移）。
	if !qvUsed(mgr2, ctx).Equal(spec) {
		t.Fatalf("quota must stay reserved while marked, got %+v", qvUsed(mgr2, ctx))
	}
	// 人工确认后可从 Failed 终结：用干净后端重放清理动作。
	if err := mgr2.Destroy(ctx, "vm1"); err != nil {
		t.Fatalf("destroy from Failed: %v", err)
	}
	if q := qvUsed(mgr2, ctx); !q.Equal(model.Resources{}) {
		t.Fatalf("quota must be released after manual destroy, got %+v", q)
	}
}

func TestReconcileUndefineFailureThenManualRecovery(t *testing.T) {
	// 销毁时 Undefine 失败：后端残留 -> 标记 Failed，不释放配额。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{FailUndefine: true})
	if _, err := mgr.Create(ctx, "vm1", "vm1", spec); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Destroy(ctx, "vm1"); err == nil {
		t.Fatal("injected undefine must fail")
	}
	got, _ := mgr.Get(ctx, "vm1")
	if got.State != model.StateFailed {
		t.Fatalf("precondition Failed, got %s reason=%s", got.State, got.Reason)
	}
	// 重启（故障已解除）：事实仍与 Failed 记录矛盾，不允许自动翻案，维持 Failed。
	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())
	item := findItem(rep, "vm1")
	if item == nil || item.After != "Failed" {
		t.Fatalf("Failed must not auto-flip, got %+v", item)
	}
	// 人工介入：重放 Destroy 完成清理。
	if err := mgr2.Destroy(ctx, "vm1"); err != nil {
		t.Fatalf("manual destroy: %v", err)
	}
	if q := qvUsed(mgr2, ctx); !q.Equal(model.Resources{}) {
		t.Fatalf("quota leaked after manual recovery: %+v", q)
	}
}

func TestReconcileBackendOrphanReported(t *testing.T) {
	// 后端存在定义但状态存储完全无记录：作为残留报告，不自动删除/接管。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{})
	if _, err := mgr.Create(ctx, "known", "known", spec); err != nil {
		t.Fatal(err)
	}
	// 直接通过干净后端造一个无记录残留。
	if err := e.bk.Define(ctx, "orphan", "orphan"); err != nil {
		t.Fatal(err)
	}
	_, rep := e.boot(t, total)
	t.Log(rep.String())
	if len(rep.Orphans) != 1 || rep.Orphans[0] != "orphan" {
		t.Fatalf("want exactly one orphan, got %v", rep.Orphans)
	}
	// 残留不得被自动删除，也不得占用配额。
	if _, err := e.bk.Inspect(ctx, "orphan"); err != nil {
		t.Fatalf("orphan must still exist: %v", err)
	}
}

func TestReconcileDestroyedRecordWithResidue(t *testing.T) {
	// 终态记录与后端残留矛盾：必须暴露为 Failed，而不是静默确认 Destroyed。
	ctx := context.Background()
	total := model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}
	e, mgr, _ := setup(t, total, backend.FaultPoint{})
	if _, err := mgr.Create(ctx, "vm1", "vm1", spec); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Destroy(ctx, "vm1"); err != nil {
		t.Fatal(err)
	}
	// 外部因素重新造出残留。
	if err := e.bk.Define(ctx, "vm1", "vm1"); err != nil {
		t.Fatal(err)
	}
	mgr2, rep := e.boot(t, total)
	t.Log(rep.String())
	item := findItem(rep, "vm1")
	if item == nil || item.Before != "Destroyed" || item.After != "Failed" ||
		item.Conclusion != reconcile.ConclusionMarked {
		t.Fatalf("want Destroyed -> Failed marked, got %+v", item)
	}
	// 残留意味着真实资源仍在，重算配额重新计入，供人工处置。
	if !qvUsed(mgr2, ctx).Equal(spec) {
		t.Fatalf("quota must reflect residual resource, used=%+v", qvUsed(mgr2, ctx))
	}
}

func qvUsed(mgr *orchestrator.Manager, ctx context.Context) model.Resources {
	_, qv, err := mgr.Snapshot(ctx)
	if err != nil {
		panic(err)
	}
	return qv.Used
}
