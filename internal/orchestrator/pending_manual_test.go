package orchestrator_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

// newHarnessOnDir 在既有状态目录上构造一个"全新进程"的编排器
// （进程内模拟后端可独立重置，用来模拟宿主/进程重启）。
func newHarnessOnDir(t *testing.T, dir string, h backend.Handle, capacity domain.Resources) *orchestrator.Orchestrator {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	o, err := orchestrator.New(st, h, capacity, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// seedPendingRunningMissing 构造"记录 Running、后端整体消失"的分歧并由对账判定挂起。
func seedPendingRunningMissing(t *testing.T, capacity domain.Resources, id string) (string, *backend.SimHandle, domain.Spec) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	h := backend.NewSimHandle(backend.FaultHooks{})
	spec := smallSpec(1000, 512, 1024)
	o, err := orchestrator.New(st, h, capacity, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Create(ctx, id, id, spec); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Start(ctx, id); err != nil {
		t.Fatal(err)
	}
	// 宿主疑似重建：承载事实彻底消失。
	if err := h.Undefine(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Reconcile(ctx, testLog{t}.Logf); err != nil {
		t.Fatal(err)
	}
	return dir, h, spec
}

// TestPendingManualFullFlowKeep 走通：判定挂起 -> 换新进程仍可见 -> 常规变更被拒
// （原因可区分）-> 人工 keep 收口到 Running -> 记录与占用同步更新。
func TestPendingManualFullFlowKeep(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	id := "lostkeep"
	dir, _, spec := seedPendingRunningMissing(t, capacity, id)

	// —— 全新进程（新后端句柄模拟重启；承载已 absent）——
	h2 := backend.NewSimHandle(backend.FaultHooks{})
	o2 := newHarnessOnDir(t, dir, h2, capacity)

	// 换新进程后，不经对账也能看到挂起结论与原因（结论已持久化）。
	got, err := o2.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StatePendingManual || !got.IsPending() {
		t.Fatalf("fresh process must see PendingManual, got %s", got.State)
	}
	if got.Pending == nil || got.Pending.Rule != "running-missing-backend" ||
		got.Pending.RecordState != "Running" || got.Pending.ActualState != "absent" ||
		got.Pending.Reason == "" {
		t.Fatalf("pending verdict must survive restart with reason: %+v", got.Pending)
	}
	// list 同样带得出挂起实例。
	list, err := o2.List(ctx)
	if err != nil || len(list) != 1 || !list[0].IsPending() {
		t.Fatalf("list must surface pending instance: n=%d err=%v", len(list), err)
	}
	// 占用：挂起期间继续冻结计入。
	if r, ok := o2.ResourceSnapshot().Reservations[id]; !ok || r != spec.Resources {
		t.Fatalf("fresh process usage must include frozen pending reservation: ok=%v r=%+v", ok, r)
	}
	// 宿主再"重启"一次（第三个进程），结论依旧。
	o3 := newHarnessOnDir(t, dir, h2, capacity)
	g3, _ := o3.Get(ctx, id)
	if g3.State != domain.StatePendingManual {
		t.Fatalf("second restart must still see PendingManual, got %s", g3.State)
	}

	// 常规变更一律拒绝，且原因必须是 pending_manual，
	// 可与 not_found / illegal_transition / insufficient_resource 明确区分。
	checkReject := func(fn func() error) {
		t.Helper()
		err := fn()
		if !domain.IsKind(err, domain.KindPendingManual) {
			t.Fatalf("want pending_manual, got %v", err)
		}
		for _, other := range []domain.ErrorKind{
			domain.KindNotFound, domain.KindIllegalTransition, domain.KindInsufficientResource,
		} {
			if domain.IsKind(err, other) {
				t.Fatalf("pending_manual must be distinguishable from %s", other)
			}
		}
	}
	checkReject(func() error { _, e := o3.Start(ctx, id); return e })
	checkReject(func() error { _, e := o3.Stop(ctx, id); return e })
	checkReject(func() error { _, e := o3.Destroy(ctx, id); return e })
	checkReject(func() error { _, e := o3.Create(ctx, id, id, spec); return e })
	// 拒绝不得悄悄改变状态或清理承载。
	gmid, _ := o3.Get(ctx, id)
	if gmid.State != domain.StatePendingManual {
		t.Fatalf("rejected ops must not overwrite pending: %s", gmid.State)
	}

	// 人工确认它应继续存在：承载已 absent，收口 keep 应按规格自动重建定义并拉起，
	// 落定 Running、清空挂起结论。
	inst, err := o3.Resolve(ctx, orchestrator.ResolveRequest{
		ID: id, Decision: orchestrator.ResolveKeep, Target: domain.StateRunning, Note: "ops-confirmed",
	})
	if err != nil {
		t.Fatalf("resolve keep: %v", err)
	}
	if inst.State != domain.StateRunning || inst.Pending != nil {
		t.Fatalf("after keep resolve want Running with pending cleared: %+v", inst)
	}
	info, _ := h2.Inspect(ctx, id)
	if info.Status != backend.StatusRunning {
		t.Fatalf("keep->Running must (re)define and bring backend up, got %s", info.Status)
	}
	// 收口后再跑一轮对账：必须稳定为 Matched，不会又被当成半完成销毁收掉。
	rep, err := o3.Reconcile(ctx, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	for _, vd := range rep.Verdicts {
		if vd.InstanceID == id && vd.Outcome != reconcile.OutcomeMatched {
			t.Fatalf("post-resolution reconcile must be Matched, got %s (%s)", vd.Outcome, vd.Detail)
		}
	}
	// 收口后：恢复为正常实例，可正常 stop；占用仍在（继续持有资源）。
	if _, err := o3.Stop(ctx, id); err != nil {
		t.Fatalf("resolved instance must accept normal stop: %v", err)
	}
	snap := o3.ResourceSnapshot()
	if snap.Used != spec.Resources {
		t.Fatalf("kept instance must keep occupying resources: got %+v want %+v", snap.Used, spec.Resources)
	}
	// 重启后仍是确定的正常状态，不再挂起。
	o4 := newHarnessOnDir(t, dir, h2, capacity)
	g4, _ := o4.Get(ctx, id)
	if g4.State != domain.StateStopped || g4.Pending != nil {
		t.Fatalf("resolution must persist as normal state across restart: %s pending=%v", g4.State, g4.Pending)
	}
	if u := o4.ResourceSnapshot().Used; u != spec.Resources {
		t.Fatalf("fresh process after keep must still count reservation: %+v", u)
	}
}

// TestPendingManualFullFlowDestroy 走通：挂起 -> 人工 destroy 收口 -> 后端清空、
// 配额释放、落墓碑，且换新进程读数一致。
func TestPendingManualFullFlowDestroy(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	id := "lostkill"
	dir, _, _ := seedPendingRunningMissing(t, capacity, id)

	h2 := backend.NewSimHandle(backend.FaultHooks{})
	o2 := newHarnessOnDir(t, dir, h2, capacity)
	if _, err := o2.Get(ctx, id); err != nil {
		t.Fatal(err)
	}

	inst, err := o2.Resolve(ctx, orchestrator.ResolveRequest{
		ID: id, Decision: orchestrator.ResolveDestroy, Note: "ops-confirmed-wipe",
	})
	if err != nil {
		t.Fatalf("resolve destroy: %v", err)
	}
	if inst.State != domain.StateDestroyed || inst.Pending != nil {
		t.Fatalf("after destroy resolve want tombstone with pending cleared: %+v", inst)
	}
	info, _ := h2.Inspect(ctx, id)
	if info.Status != backend.StatusAbsent {
		t.Fatalf("destroy resolution must remove backend residue: %+v", info)
	}
	if _, ok := o2.ResourceSnapshot().Reservations[id]; ok {
		t.Fatal("destroy resolution must release quota reservation")
	}
	if u := o2.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must be zero after resolution destroy: %+v", u)
	}
	// 换新进程：墓碑仍在、占用为零、后端无残留。
	o3 := newHarnessOnDir(t, dir, h2, capacity)
	g3, _ := o3.Get(ctx, id)
	if g3.State != domain.StateDestroyed || g3.Pending != nil {
		t.Fatalf("resolution must persist as Destroyed: %s pending=%v", g3.State, g3.Pending)
	}
	if u := o3.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("fresh process must see zero usage: %+v", u)
	}
	// 已收口（墓碑）的实例再次 resolve 必须被明确拒绝。
	if _, err := o3.Resolve(ctx, orchestrator.ResolveRequest{
		ID: id, Decision: orchestrator.ResolveDestroy,
	}); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("resolve on non-pending must be illegal_transition, got %v", err)
	}
}

// TestResolveValidation 收口参数校验：缺决策/多决策/keep 无合法目标都拒绝。
func TestResolveValidation(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 8000, MemoryMB: 4096, DiskMB: 16384}
	dir, _, _ := seedPendingRunningMissing(t, capacity, "v")
	h := backend.NewSimHandle(backend.FaultHooks{})
	o := newHarnessOnDir(t, dir, h, capacity)

	if _, err := o.Resolve(ctx, orchestrator.ResolveRequest{ID: "v"}); !domain.IsKind(err, domain.KindInvalidArgument) {
		t.Fatalf("missing decision want invalid_argument, got %v", err)
	}
	if _, err := o.Resolve(ctx, orchestrator.ResolveRequest{ID: "x", Decision: "weird"}); !domain.IsKind(err, domain.KindInvalidArgument) {
		t.Fatalf("bad decision want invalid_argument, got %v", err)
	}
	if _, err := o.Resolve(ctx, orchestrator.ResolveRequest{
		ID: "v", Decision: orchestrator.ResolveKeep, Target: domain.StateDestroyed,
	}); !domain.IsKind(err, domain.KindInvalidArgument) {
		t.Fatalf("keep target Destroyed want invalid_argument, got %v", err)
	}
	// 对正常实例收口：illegal_transition。
	if _, err := o.Create(ctx, "ok", "ok", smallSpec(100, 100, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Resolve(ctx, orchestrator.ResolveRequest{
		ID: "ok", Decision: orchestrator.ResolveDestroy,
	}); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("resolve on normal instance want illegal_transition, got %v", err)
	}
}
