package orchestrator_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

func mustNewOrch(st store.Store, h backend.Handle, cap domain.Resources, t *testing.T) (*orchestrator.Orchestrator, error) {
	t.Helper()
	return orchestrator.New(st, h, cap, testLog{t}.Logf)
}

// TestDistinctInstancesDoNotBlock 不同实例的变更彼此并行推进、不互相排队：
// 实例 blockvm 卡在后端 Start 上时，另一个实例 freevm 的 Start 仍能完成。
func TestDistinctInstancesDoNotBlock(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 20000, MemoryMB: 16384, DiskMB: 32768}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	hooks := backend.FaultHooks{
		BeforeStart: func(id string) error {
			if id == "blockvm" {
				entered <- struct{}{}
				<-release // 阻塞直到测试放行
			}
			return nil
		},
	}
	h := backend.NewSimHandle(hooks)
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	o, err := newOrch(st, h, capacity, t)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"blockvm", "freevm"} {
		if _, err := o.Create(ctx, id, id, smallSpec(1000, 512, 1024)); err != nil {
			t.Fatal(err)
		}
	}

	blockDone := make(chan struct{})
	go func() {
		_, _ = o.Start(ctx, "blockvm")
		close(blockDone)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("blockvm start never entered backend")
	}

	// blockvm 仍卡在后端：freevm 必须能独立推进完成，不排队等待。
	freeDone := make(chan error, 1)
	go func() {
		_, e := o.Start(ctx, "freevm")
		freeDone <- e
	}()
	select {
	case e := <-freeDone:
		if e != nil {
			t.Fatalf("freevm must progress while blockvm is stalled: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("freevm was blocked behind blockvm (distinct instances must not queue)")
	}

	close(release)
	<-blockDone

	g, _ := o.Get(ctx, "freevm")
	if g.State != domain.StateRunning {
		t.Fatalf("freevm want Running, got %s", g.State)
	}
}

func newOrch(st store.Store, h backend.Handle, cap domain.Resources, t *testing.T) (*orchestrator.Orchestrator, error) {
	t.Helper()
	return mustNewOrch(st, h, cap, t)
}

// TestSameInstanceConcurrentOpsSerialize 同一实例的并发 start/stop 有确定先后、
// 不互相覆盖；结束后状态确定，资源恰好一份。
func TestSameInstanceConcurrentOpsSerialize(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192}
	o, _ := newHarness(t, capacity)
	if _, err := o.Create(ctx, "solo", "solo", smallSpec(1000, 512, 1024)); err != nil {
		t.Fatal(err)
	}

	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = o.Start(ctx, "solo") }()
		go func() { defer wg.Done(); _, _ = o.Stop(ctx, "solo") }()
	}
	wg.Wait()

	got, err := o.Get(ctx, "solo")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StateRunning && got.State != domain.StateStopped {
		t.Fatalf("final state must be determinate, got %s", got.State)
	}
	snap := o.ResourceSnapshot()
	if len(snap.Reservations) != 1 {
		t.Fatalf("exactly one reservation required, got %v", snap.Reservations)
	}
	if snap.Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("reservation must be exactly one spec: %+v", snap.Used)
	}
}

// TestManyInstancesConcurrentConservation 多实例并发 start/stop/destroy 混合压测：
// 彼此不阻塞，全程资源不重复占用也不泄漏；最终全部销毁后占用归零。
func TestManyInstancesConcurrentConservation(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 20000, MemoryMB: 16384, DiskMB: 32768}
	o, _ := newHarness(t, capacity)

	const n = 16
	spec := smallSpec(1000, 512, 1024)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("vm%02d", i)
		if _, err := o.Create(ctx, id, id, spec); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	// 并发变更期间，任何时刻占用都不得超过实际持有资源的实例集合：
	// used 必须等于 n*spec（本压测不创建/销毁），reservations 恰好 n。
	stop := make(chan struct{})
	var watchErr error
	var watchWg sync.WaitGroup
	watchWg.Add(1)
	go func() {
		defer watchWg.Done()
		want := domain.Resources{CPUMillis: int64(n) * 1000, MemoryMB: int64(n) * 512, DiskMB: int64(n) * 1024}
		for {
			select {
			case <-stop:
				return
			default:
				s := o.ResourceSnapshot()
				if s.Used != want || len(s.Reservations) != n {
					watchErr = fmt.Errorf("invariant violated: used=%+v reservations=%d", s.Used, len(s.Reservations))
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	act := func(id string, fn func() error) {
		defer wg.Done()
		if e := fn(); e != nil && !domain.IsKind(e, domain.KindIllegalTransition) {
			t.Errorf("unexpected error on %s: %v", id, e)
		}
	}
	for round := 0; round < 5; round++ {
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("vm%02d", i)
			wg.Add(3)
			go act(id, func() error { _, e := o.Start(ctx, id); return e })
			go act(id, func() error { _, e := o.Stop(ctx, id); return e })
			go act(id, func() error { _, e := o.Get(ctx, id); return e })
		}
	}
	wg.Wait()
	close(stop)
	watchWg.Wait()
	if watchErr != nil {
		t.Fatal(watchErr)
	}

	// 全部串行收敛销毁：最终状态 Destroyed、占用归零、无重复释放。
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("vm%02d", i)
		if _, err := o.Stop(ctx, id); err != nil && !domain.IsKind(err, domain.KindIllegalTransition) {
			t.Fatalf("settle stop %s: %v", id, err)
		}
		// 并发 destroy 同一批互不相同 id 也可并行：这里串行断言终态守恒即可。
		if _, err := o.Destroy(ctx, id); err != nil {
			t.Fatalf("settle destroy %s: %v", id, err)
		}
	}
	if u := o.ResourceSnapshot(); u.Used != (domain.Resources{}) || len(u.Reservations) != 0 {
		t.Fatalf("resources leaked after concurrent churn: %+v", u)
	}
}

// TestCrossProcessUsageConsistency 任何新进程查询占用都与真实持有资源的记录集合一致：
// 覆盖 Defined/Running/Stopped/Destroyed 与 PendingManual 混合场景，重启前与重启后。
func TestCrossProcessUsageConsistency(t *testing.T) {
	ctx := context.Background()
	capacity := domain.Resources{CPUMillis: 20000, MemoryMB: 16384, DiskMB: 32768}
	spec := smallSpec(1000, 512, 1024)

	dir := t.TempDir()
	st1, _ := store.NewFileStore(filepath.Join(dir, "state"))
	h1 := backend.NewSimHandle(backend.FaultHooks{})
	o1, err := mustNewOrch(st1, h1, capacity, t)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"d", "r", "s", "x", "p"} {
		if _, err := o1.Create(ctx, id, id, spec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o1.Start(ctx, "r"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Start(ctx, "s"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Stop(ctx, "s"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Destroy(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	// p 进入挂起（Running 但后端消失）。
	if _, err := o1.Start(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if err := h1.Undefine(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Reconcile(ctx, testLog{t}.Logf); err != nil {
		t.Fatal(err)
	}

	// 持有者集合：d(Defined) r(Running) s(Stopped) p(PendingManual) = 4 份；x 已销毁。
	wantUsed := domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 4096}
	if u := o1.ResourceSnapshot().Used; u != wantUsed {
		t.Fatalf("before restart usage wrong: %+v", u)
	}

	// 模拟全新进程：新后端句柄 + 新编排器（构造时自动按记录重建台账，无需先操作）。
	h2 := backend.NewSimHandle(backend.FaultHooks{})
	o2 := newHarnessOnDir(t, dir, h2, capacity)
	if u := o2.ResourceSnapshot().Used; u != wantUsed {
		t.Fatalf("fresh process must report identical usage: got %+v want %+v", u, wantUsed)
	}
	if r, ok := o2.ResourceSnapshot().Reservations["p"]; !ok || r != spec.Resources {
		t.Fatalf("pending instance must be counted by fresh process: ok=%v r=%+v", ok, r)
	}
	if _, ok := o2.ResourceSnapshot().Reservations["x"]; ok {
		t.Fatal("destroyed instance must not be counted")
	}
	// 第三个进程，结论与读数仍一致。
	o3 := newHarnessOnDir(t, dir, h2, capacity)
	if u := o3.ResourceSnapshot().Used; u != wantUsed {
		t.Fatalf("second fresh process mismatch: %+v", u)
	}
	pg, _ := o3.Get(ctx, "p")
	if pg.State != domain.StatePendingManual || pg.Pending == nil {
		t.Fatalf("pending verdict must survive restart: %+v", pg)
	}
}
