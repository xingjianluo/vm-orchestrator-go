package orchestrator_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
	"path/filepath"
)

type testLog struct{ t *testing.T }

func (l testLog) Logf(format string, args ...any) {
	l.t.Logf("[op-seq] "+format, args...)
}

func newHarness(t *testing.T, capacity domain.Resources) (*orchestrator.Orchestrator, *backend.SimHandle) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	h := backend.NewSimHandle(backend.FaultHooks{})
	o, err := orchestrator.New(st, h, capacity, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}
	return o, h
}

func smallSpec(cpu, mem, disk int64) domain.Spec {
	return domain.Spec{
		Image:     "img",
		Resources: domain.Resources{CPUMillis: cpu, MemoryMB: mem, DiskMB: disk},
	}
}

// TestLifecycleHappyPath 验证完整合法迁移链与每一步的配额守恒。
func TestLifecycleHappyPath(t *testing.T) {
	ctx := context.Background()
	o, _ := newHarness(t, domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})

	inst, err := o.Create(ctx, "vm1", "vm1", smallSpec(1000, 512, 1024))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if inst.State != domain.StateDefined {
		t.Fatalf("want Defined, got %s", inst.State)
	}
	snap := o.ResourceSnapshot()
	if snap.Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("quota after create wrong: %+v", snap)
	}
	t.Logf("[snapshot] after create: %+v", snap)

	if _, err := o.Start(ctx, "vm1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := o.Stop(ctx, "vm1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	got, err := o.Get(ctx, "vm1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StateStopped {
		t.Fatalf("want Stopped, got %s", got.State)
	}

	if _, err := o.Destroy(ctx, "vm1"); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	snap = o.ResourceSnapshot()
	if snap.Used != (domain.Resources{}) {
		t.Fatalf("quota not released after destroy: %+v", snap)
	}
	if len(snap.Reservations) != 0 {
		t.Fatalf("reservations leaked: %+v", snap.Reservations)
	}
	t.Logf("[snapshot] after destroy: used=%+v reservations=%d", snap.Used, len(snap.Reservations))

	got, err = o.Get(ctx, "vm1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StateDestroyed {
		t.Fatalf("want tombstone Destroyed, got %s", got.State)
	}
}

// TestIdempotentAndIllegalTransitions 验证幂等空操作与非法迁移的可区分原因。
func TestIdempotentAndIllegalTransitions(t *testing.T) {
	ctx := context.Background()
	o, _ := newHarness(t, domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})

	create := func(id string) {
		if _, err := o.Create(ctx, id, id, smallSpec(1000, 512, 1024)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	create("vm")

	// 同规格重复创建：幂等。
	if _, err := o.Create(ctx, "vm", "vm", smallSpec(1000, 512, 1024)); err != nil {
		t.Fatalf("duplicate create same spec should be idempotent: %v", err)
	}
	// 不同规格重复创建：already_exists。
	_, err := o.Create(ctx, "vm", "vm", smallSpec(2000, 512, 1024))
	if !domain.IsKind(err, domain.KindAlreadyExists) {
		t.Fatalf("want already_exists, got %v", err)
	}
	if o.ResourceSnapshot().Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("rejected duplicate must not move quota: %+v", o.ResourceSnapshot())
	}

	// Defined -> Stop 非法。
	if _, err := o.Stop(ctx, "vm"); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("Defined stop want illegal_transition, got %v", err)
	}
	// Defined -> Destroy 合法。
	create("vm2")
	if _, err := o.Destroy(ctx, "vm2"); err != nil {
		t.Fatalf("Defined destroy should be legal: %v", err)
	}

	create("vm3")
	if _, err := o.Start(ctx, "vm3"); err != nil {
		t.Fatal(err)
	}
	// Running -> Start 幂等。
	if _, err := o.Start(ctx, "vm3"); err != nil {
		t.Fatalf("Running start idempotent: %v", err)
	}
	// Running -> Destroy 非法（必须先停止）。
	if _, err := o.Destroy(ctx, "vm3"); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("Running destroy want illegal_transition, got %v", err)
	}
	// 状态必须保持 Running，配额不动。
	got, _ := o.Get(ctx, "vm3")
	if got.State != domain.StateRunning {
		t.Fatalf("state overwritten after illegal op: %s", got.State)
	}

	// Stopped 重复 Stop 幂等；Destroyed 重复 Destroy 幂等。
	if _, err := o.Stop(ctx, "vm3"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Stop(ctx, "vm3"); err != nil {
		t.Fatalf("double stop should be idempotent: %v", err)
	}
	if _, err := o.Destroy(ctx, "vm3"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Destroy(ctx, "vm3"); err != nil {
		t.Fatalf("double destroy should be idempotent: %v", err)
	}

	// 不存在的 id：not_found。
	if _, err := o.Start(ctx, "ghost"); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("want not_found, got %v", err)
	}

	// 终态再迁移：非法。
	if _, err := o.Start(ctx, "vm3"); !domain.IsKind(err, domain.KindIllegalTransition) {
		t.Fatalf("Destroyed start want illegal_transition, got %v", err)
	}
}

// TestQuotaAdmissionBeforeSideEffect 验证资源不足在副作用前拒绝，配额零漂移。
func TestQuotaAdmissionBeforeSideEffect(t *testing.T) {
	ctx := context.Background()
	o, h := newHarness(t, domain.Resources{CPUMillis: 2000, MemoryMB: 1024, DiskMB: 2048})

	// 单项超额。
	_, err := o.Create(ctx, "big", "big", smallSpec(3000, 512, 1024))
	if !domain.IsKind(err, domain.KindInsufficientResource) {
		t.Fatalf("want insufficient_resource, got %v", err)
	}
	var oe *domain.OpError
	if errors.As(err, &oe); oe.Resource != "cpu_millis" {
		t.Fatalf("want cpu_millis dimension, got %q (%v)", oe.Resource, err)
	}
	if info, _ := h.Inspect(ctx, "big"); info.Status != backend.StatusAbsent {
		t.Fatalf("rejected create must leave no backend object: %+v", info)
	}
	if _, err := o.Get(ctx, "big"); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("rejected create must leave no record: %v", err)
	}
	if u := o.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("rejected create must not reserve quota: %+v", u)
	}

	// 填满后再申请：剩余维度不足。
	if _, err := o.Create(ctx, "a", "a", smallSpec(1500, 700, 1500)); err != nil {
		t.Fatal(err)
	}
	_, err = o.Create(ctx, "b", "b", smallSpec(500, 700, 500))
	if !domain.IsKind(err, domain.KindInsufficientResource) {
		t.Fatalf("want insufficient on mem, got %v", err)
	}
	errors.As(err, &oe)
	if oe.Resource != "memory_mb" {
		t.Fatalf("want memory_mb, got %q", oe.Resource)
	}
	t.Logf("[snapshot] after rejections used=%+v reservations=%v",
		o.ResourceSnapshot().Used, o.ResourceSnapshot().Reservations)

	// a 销毁后配额完全释放，b 可准入。
	if _, err := o.Destroy(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Create(ctx, "b", "b", smallSpec(500, 700, 500)); err != nil {
		t.Fatalf("b should be admitted after release: %v", err)
	}
}

// TestBackendFailureRollsBackQuota 验证后端 Define 失败时配额回滚、无记录残留。
func TestBackendFailureRollsBackQuota(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, _ := store.NewFileStore(filepath.Join(dir, "state"))
	var failMu sync.Mutex
	failedOnce := map[string]bool{}
	h := backend.NewSimHandle(backend.FaultHooks{
		BeforeDefine: func(id string) error {
			failMu.Lock()
			defer failMu.Unlock()
			if id == "failme" && !failedOnce[id] {
				failedOnce[id] = true
				return errors.New("simulated define fault")
			}
			return nil
		},
	})
	o, err := orchestrator.New(st, h, domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192}, testLog{t}.Logf)
	if err != nil {
		t.Fatal(err)
	}

	_, err = o.Create(ctx, "failme", "failme", smallSpec(1000, 512, 1024))
	if !domain.IsKind(err, domain.KindBackendFailure) {
		t.Fatalf("want backend_failure, got %v", err)
	}
	if u := o.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("quota must rollback after backend failure: %+v", u)
	}
	if _, err := o.Get(ctx, "failme"); !domain.IsKind(err, domain.KindNotFound) {
		t.Fatalf("no record may persist after failed create: %v", err)
	}
	// 回滚后同 id 可立即重试成功。
	if _, err := o.Create(ctx, "failme", "failme", smallSpec(1000, 512, 1024)); err != nil {
		t.Fatalf("retry after rollback should succeed: %v", err)
	}
}

// TestConcurrentCreateSameID 验证并发重复创建恰好成功一次，无重复实例与重复预留。
func TestConcurrentCreateSameID(t *testing.T) {
	ctx := context.Background()
	o, _ := newHarness(t, domain.Resources{CPUMillis: 10000, MemoryMB: 10240, DiskMB: 20480})

	const n = 40
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = o.Create(ctx, "race", "race", smallSpec(1000, 512, 1024))
		}(i)
	}
	wg.Wait()

	var ok, conflict int
	for _, e := range errs {
		switch {
		case e == nil:
			ok++
		case domain.IsKind(e, domain.KindAlreadyExists):
			conflict++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}
	// 同规格的确定性 ID 创建天然幂等：40 个并发请求全部成功返回同一实例，
	// 但底层只能有一条记录、一份预留。冲突仅可能出现在规格不同的场景（另测）。
	if ok != n || conflict != 0 {
		t.Fatalf("same-spec concurrent creates must all be idempotent-ok, got ok=%d conflict=%d", ok, conflict)
	}
	snap := o.ResourceSnapshot()
	if len(snap.Reservations) != 1 || snap.Reservations["race"] == (domain.Resources{}) {
		t.Fatalf("exactly one reservation expected: %+v", snap.Reservations)
	}
	t.Logf("[snapshot] concurrent create settled: ok=%d conflict=%d reservations=%d used=%+v",
		ok, conflict, len(snap.Reservations), snap.Used)

	// 记录也只有一条。
	list, err := o.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("want exactly one record, got %d: %v", len(list), list)
	}
}

// TestConcurrentMixedOperationsConservation 混合并发操作下资源与状态守恒。
func TestConcurrentMixedOperationsConservation(t *testing.T) {
	ctx := context.Background()
	o, _ := newHarness(t, domain.Resources{CPUMillis: 20000, MemoryMB: 16384, DiskMB: 32768})

	const ids = 12
	for i := 0; i < ids; i++ {
		id := "vm" + string(rune('a'+i))
		if _, err := o.Create(ctx, id, id, smallSpec(1000, 512, 1024)); err != nil {
			t.Fatalf("seed create %s: %v", id, err)
		}
	}

	var wg sync.WaitGroup
	action := func(id string, fn func() error) {
		defer wg.Done()
		if err := fn(); err != nil {
			// 非法迁移/后端错误在此压测中允许，但必须是可区分类别。
			if !domain.IsKind(err, domain.KindIllegalTransition) &&
				!domain.IsKind(err, domain.KindBackendFailure) &&
				!strings.Contains(err.Error(), "quota") {
				t.Errorf("unexpected error on %s: %v", id, err)
			}
		}
	}
	for round := 0; round < 4; round++ {
		for i := 0; i < ids; i++ {
			id := "vm" + string(rune('a'+i))
			wg.Add(3)
			go action(id, func() error { _, e := o.Start(ctx, id); return e })
			go action(id, func() error { _, e := o.Stop(ctx, id); return e })
			go action(id, func() error { _, e := o.Get(ctx, id); return e })
		}
		wg.Wait()
	}

	// 波动结束后串行收敛：每个实例先 Stop（幂等）再 Destroy，确保终态。
	for i := 0; i < ids; i++ {
		id := "vm" + string(rune('a'+i))
		if _, err := o.Stop(ctx, id); err != nil && !domain.IsKind(err, domain.KindIllegalTransition) {
			t.Fatalf("settle stop %s: %v", id, err)
		}
		if _, err := o.Destroy(ctx, id); err != nil {
			t.Fatalf("settle destroy %s: %v", id, err)
		}
	}
	// 最终：全部销毁，配额归零。
	for i := 0; i < ids; i++ {
		id := "vm" + string(rune('a'+i))
		got, err := o.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if got.State != domain.StateDestroyed {
			t.Fatalf("id=%s final state=%s want Destroyed", id, got.State)
		}
	}
	snap := o.ResourceSnapshot()
	if snap.Used != (domain.Resources{}) || len(snap.Reservations) != 0 {
		t.Fatalf("quota conservation violated after all destroys: %+v", snap)
	}
	t.Logf("[snapshot] final: used=%+v reservations=%d (conserved)", snap.Used, len(snap.Reservations))
}
