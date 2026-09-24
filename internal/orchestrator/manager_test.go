package orchestrator_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

type fixture struct {
	dir string
	st  *store.Store
	bk  backend.Backend
	qp  *quota.Pool
	mgr *orchestrator.Manager
}

func newFixture(t *testing.T, total model.Resources, fault backend.FaultPoint) *fixture {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	pb, err := backend.NewProcessBackend(filepath.Join(dir, "backend"))
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	var bk backend.Backend = pb
	bk = backend.NewFaultyBackend(pb, fault)
	qp := quota.New(total)
	mgr, err := orchestrator.New(orchestrator.Options{
		Store: st, Backend: bk, Quota: qp,
		Logger: orchestrator.NewSlogLogger(slog.LevelError),
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	fx := &fixture{dir: dir, st: st, bk: bk, qp: qp, mgr: mgr}
	t.Cleanup(func() { _ = backend.KillAllByDir(filepath.Join(dir, "backend")) })
	return fx
}

// restart 模拟进程异常退出后，使用同一持久化与后端目录重新拉起编排器，并先做对账。
func (f *fixture) restart(t *testing.T) *reconcile.Report {
	t.Helper()
	st, err := store.New(filepath.Join(f.dir, "state.json"))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	pb, err := backend.NewProcessBackend(filepath.Join(f.dir, "backend"))
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	qp := quota.New(model.Resources{CPUMillis: 8000, MemoryMiB: 16384, DiskMiB: 500000})
	mgr, err := orchestrator.New(orchestrator.Options{
		Store: st, Backend: pb, Quota: qp,
		Logger: orchestrator.NewSlogLogger(slog.LevelError),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	f.st, f.bk, f.qp, f.mgr = st, pb, qp, mgr
	rep, err := reconcile.New(mgr).Run(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return rep
}

func TestLifecycleHappyPath(t *testing.T) {
	fx := newFixture(t, model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}, backend.FaultPoint{})
	ctx := context.Background()
	spec := model.Resources{CPUMillis: 1000, MemoryMiB: 1024, DiskMiB: 10000}

	seq := []string{}
	in, err := fx.mgr.Create(ctx, "vm1", "web", spec)
	seq = append(seq, "Create(vm1)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if in.State != model.StateStopped {
		t.Fatalf("after create want Stopped, got %s", in.State)
	}
	if _, err := fx.mgr.Start(ctx, "vm1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	seq = append(seq, "Start(vm1)")
	got, _ := fx.mgr.Get(ctx, "vm1")
	if got.State != model.StateRunning || got.PID == 0 {
		t.Fatalf("after start want Running with pid, got %+v", got)
	}
	if _, err := fx.mgr.Stop(ctx, "vm1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	seq = append(seq, "Stop(vm1)")
	if err := fx.mgr.Destroy(ctx, "vm1"); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	seq = append(seq, "Destroy(vm1)")

	if used := fx.qp.Used(); used != (model.Resources{}) {
		t.Fatalf("quota drift after destroy: used=%+v", used)
	}
	list, _ := fx.bk.List(ctx)
	if len(list) != 0 {
		t.Fatalf("backend residue: %v", list)
	}
	snaps, qv, _ := fx.mgr.Snapshot(ctx)
	t.Logf("[operation sequence] %v", seq)
	t.Logf("[resource snapshot] total=%+v used=%+v allocations=%d", qv.Total, qv.Used, len(snaps))
	for id, in := range snaps {
		t.Logf("  instance %s state=%s", id, in.State)
	}
}

func TestIdempotentRepeatedCalls(t *testing.T) {
	fx := newFixture(t, model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}, backend.FaultPoint{})
	ctx := context.Background()
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}

	if _, err := fx.mgr.Create(ctx, "vm1", "web", spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 重复 Create 同 ID：返回 ErrAlreadyExists 且不重复占用。
	_, err := fx.mgr.Create(ctx, "vm1", "web", spec)
	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("repeat create want ErrAlreadyExists, got %v", err)
	}
	// 同 Name 不同 ID 也拒绝。
	_, err = fx.mgr.Create(ctx, "vm2", "web", spec)
	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("dup name want ErrAlreadyExists, got %v", err)
	}
	if used := fx.qp.Used(); used != spec {
		t.Fatalf("dup create caused quota drift: used=%+v want=%+v", used, spec)
	}
	// 重复 Start / Stop 幂等成功。
	if _, err := fx.mgr.Start(ctx, "vm1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := fx.mgr.Start(ctx, "vm1"); err != nil {
		t.Fatalf("repeat start must be idempotent: %v", err)
	}
	if _, err := fx.mgr.Stop(ctx, "vm1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, err := fx.mgr.Stop(ctx, "vm1"); err != nil {
		t.Fatalf("repeat stop must be idempotent: %v", err)
	}
	// Destroy 不存在的实例幂等成功；重复 Destroy 同样幂等。
	if err := fx.mgr.Destroy(ctx, "ghost"); err != nil {
		t.Fatalf("destroy ghost: %v", err)
	}
	if err := fx.mgr.Destroy(ctx, "vm1"); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if err := fx.mgr.Destroy(ctx, "vm1"); err != nil {
		t.Fatalf("repeat destroy must be idempotent: %v", err)
	}
	t.Logf("[operation sequence] Create, Create(dup), Create(dup-name), Start x2, Stop x2, Destroy x2")
	t.Logf("[resource snapshot] used=%+v (want zero)", fx.qp.Used())
}

func TestInvalidTransitionsAreDistinguishable(t *testing.T) {
	fx := newFixture(t, model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}, backend.FaultPoint{})
	ctx := context.Background()
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}

	if _, err := fx.mgr.Create(ctx, "vm1", "web", spec); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Stopped -> Stop 是幂等成功；Stopped -> Destroy 合法。Running -> Destroy 非法。
	if _, err := fx.mgr.Start(ctx, "vm1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	err := fx.mgr.Destroy(ctx, "vm1")
	var te *model.TransitionError
	if !errors.As(err, &te) || !errors.Is(err, model.ErrInvalidTransition) {
		t.Fatalf("destroy running want TransitionError/ErrInvalidTransition, got %v", err)
	}
	if te.From != model.StateRunning {
		t.Fatalf("transition error context From=%s", te.From)
	}
	// 不存在的实例给出另一种错误。
	_, err = fx.mgr.Start(ctx, "nope")
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("start missing want ErrNotFound, got %v", err)
	}
	t.Logf("[operation sequence] Start, Destroy(while Running rejected: %s), Start(missing rejected)", te.Error())
}

func TestQuotaRejectBeforeSideEffects(t *testing.T) {
	total := model.Resources{CPUMillis: 1000, MemoryMiB: 1024, DiskMiB: 1000}
	fx := newFixture(t, total, backend.FaultPoint{})
	ctx := context.Background()

	// 仅 CPU 超额，应给出可定位到 cpuMillis 的原因。
	_, err := fx.mgr.Create(ctx, "big", "big", model.Resources{CPUMillis: 1001, MemoryMiB: 1, DiskMiB: 1})
	var qe *model.QuotaError
	if !errors.As(err, &qe) || !errors.Is(err, model.ErrQuotaExceeded) || qe.Resource != "cpuMillis" {
		t.Fatalf("want cpuMillis QuotaError, got %v", err)
	}
	// 被拒绝的创建不得产生任何后端副作用（无定义、无残留）。
	list, _ := fx.bk.List(ctx)
	if len(list) != 0 {
		t.Fatalf("quota rejection must precede backend side effects, found: %v", list)
	}
	if used := fx.qp.Used(); used != (model.Resources{}) {
		t.Fatalf("rejected create leaked quota: %+v", used)
	}
	// 合法占用成功；第二个申请超内存被拒，第一个占用不受影响。
	first := model.Resources{CPUMillis: 600, MemoryMiB: 512, DiskMiB: 500}
	if _, err := fx.mgr.Create(ctx, "vm1", "vm1", first); err != nil {
		t.Fatalf("create vm1: %v", err)
	}
	_, err = fx.mgr.Create(ctx, "vm2", "vm2", model.Resources{CPUMillis: 100, MemoryMiB: 600, DiskMiB: 100})
	if !errors.As(err, &qe) || qe.Resource != "memoryMiB" {
		t.Fatalf("want memoryMiB QuotaError, got %v", err)
	}
	if used := fx.qp.Used(); used != first {
		t.Fatalf("quota changed after rejected create: %+v", used)
	}
	snaps, qv, _ := fx.mgr.Snapshot(ctx)
	t.Logf("[operation sequence] Create over-cpu(rejected), Create vm1(ok), Create over-mem(rejected)")
	t.Logf("[resource snapshot] total=%+v used=%+v instances=%v", qv.Total, qv.Used, mapsKeys(snaps))
}

func mapsKeys(m map[string]*model.Instance) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
