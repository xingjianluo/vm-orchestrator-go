package orchestrator_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

// TestDifferentInstancesDoNotBlock 用一个会阻塞的后端钩子实证：
// 一个实例的慢操作不会让其他互不相关实例的操作排队等待。
func TestDifferentInstancesDoNotBlock(t *testing.T) {
	ctx := context.Background()
	cap := domain.Resources{CPUMillis: 20000, MemoryMB: 16384, DiskMB: 32768}
	o, h := newHarness(t, cap)

	// 三个实例都先建到 Defined。
	for _, id := range []string{"slow", "fast1", "fast2"} {
		if _, err := o.Create(ctx, id, id, smallSpec(1000, 512, 1024)); err != nil {
			t.Fatal(err)
		}
	}

	release := make(chan struct{})
	slowEntered := make(chan struct{})
	h.SetHooks(backend.FaultHooks{
		// "slow" 的 Start 在后端处阻塞，直到测试放行。
		BeforeStart: func(id string) error {
			if id == "slow" {
				close(slowEntered)
				<-release
			}
			return nil
		},
	})

	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		if _, err := o.Start(ctx, "slow"); err != nil {
			t.Errorf("slow start: %v", err)
		}
	}()

	// 等 slow 确认已经卡在临界区里。
	select {
	case <-slowEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow op never entered backend")
	}

	// 在 slow 被卡住期间，另外两个互不相关实例的操作必须能立即推进完成。
	doneFast := make(chan error, 2)
	go func() { _, e := o.Start(ctx, "fast1"); doneFast <- e }()
	go func() { _, e := o.Start(ctx, "fast2"); doneFast <- e }()
	deadline := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case e := <-doneFast:
			if e != nil {
				t.Fatalf("unrelated instance op must not queue behind slow one: %v", e)
			}
		case <-deadline:
			t.Fatal("unrelated instance ops were blocked by slow instance (global lock regression)")
		}
	}

	// slow 仍未结束（它还在等放行），证明 fast 不是因为锁全放才完成的。
	select {
	case <-slowDone:
		t.Fatal("slow op should still be blocked until released")
	default:
	}
	close(release)
	<-slowDone

	// Get 这类读操作也不应被变更互相阻塞（顺带验证读路径）。
	for _, id := range []string{"slow", "fast1", "fast2"} {
		g, err := o.Get(ctx, id)
		if err != nil || g.State != domain.StateRunning {
			t.Fatalf("id=%s want Running, got %+v err=%v", id, g, err)
		}
	}
}

// TestSameInstanceOpsSerialize 同一实例上的并发 start/stop 必须严格串行、状态不互相覆盖：
// 任意时刻后端事实与记录必须自洽，最终状态唯一确定，配额守恒。
func TestSameInstanceOpsSerialize(t *testing.T) {
	ctx := context.Background()
	o, _ := newHarness(t, domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})
	if _, err := o.Create(ctx, "solo", "solo", smallSpec(1000, 512, 1024)); err != nil {
		t.Fatal(err)
	}

	const n = 24
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	start := func() {
		defer wg.Done()
		_, e := o.Start(ctx, "solo")
		errs <- e
	}
	stop := func() {
		defer wg.Done()
		_, e := o.Stop(ctx, "solo")
		errs <- e
	}
	for i := 0; i < n; i++ {
		wg.Add(2)
		go start()
		go stop()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil && !domain.IsKind(e, domain.KindIllegalTransition) {
			t.Fatalf("only idempotent/illegal outcomes expected, got %v", e)
		}
	}

	// 收敛到确定终态：串行 stop -> destroy，占用归零，没有半成品。
	if _, err := o.Stop(ctx, "solo"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Destroy(ctx, "solo"); err != nil {
		t.Fatal(err)
	}
	if u := o.ResourceSnapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("usage must be zero: %+v", u)
	}
	g, _ := o.Get(ctx, "solo")
	if g.State != domain.StateDestroyed {
		t.Fatalf("want Destroyed, got %s", g.State)
	}
}

// TestManyInstancesConcurrentConservation 多实例并发混合变更互不阻塞，
// 资源不重复占用也不泄漏，状态各自合法。
func TestManyInstancesConcurrentConservation(t *testing.T) {
	ctx := context.Background()
	const n = 20
	cap := domain.Resources{CPUMillis: int64(n) * 1000, MemoryMB: int64(n) * 512, DiskMB: int64(n) * 1024}
	o, h := newHarness(t, cap)

	for i := 0; i < n; i++ {
		id := instanceID(i)
		if _, err := o.Create(ctx, id, id, smallSpec(1000, 512, 1024)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	// 给后端加一点抖动，制造真正的交错而不是瞬时完成。
	var mu sync.Mutex
	h.SetHooks(backend.FaultHooks{
		BeforeStart: func(string) error {
			mu.Lock()
			defer mu.Unlock()
			time.Sleep(time.Millisecond)
			return nil
		},
		BeforeStop: func(string) error {
			mu.Lock()
			defer mu.Unlock()
			time.Sleep(time.Millisecond)
			return nil
		},
	})

	var wg sync.WaitGroup
	for round := 0; round < 3; round++ {
		for i := 0; i < n; i++ {
			id := instanceID(i)
			wg.Add(2)
			go func() { defer wg.Done(); _, e := o.Start(ctx, id); errsAcceptable(t, e) }()
			go func() { defer wg.Done(); _, e := o.Stop(ctx, id); errsAcceptable(t, e) }()
		}
		wg.Wait()
	}

	// 守恒：每条记录的预留要么在（非 Destroyed），要么不在（Destroyed）。
	snap := o.ResourceSnapshot()
	list, err := o.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held := 0
	for _, in := range list {
		_, reserved := snap.Reservations[in.ID]
		if in.State.HoldsResources() != reserved {
			t.Fatalf("id=%s state=%s but reservation presence=%t (ledger/record mismatch)",
				in.ID, in.State, reserved)
		}
		if reserved {
			held++
		}
	}
	if len(snap.Reservations) != held {
		t.Fatalf("reservation count=%d but held records=%d", len(snap.Reservations), held)
	}

	// 全部串行销毁后占用必须归零。
	for i := 0; i < n; i++ {
		id := instanceID(i)
		if _, err := o.Stop(ctx, id); err != nil && !domain.IsKind(err, domain.KindIllegalTransition) {
			t.Fatalf("settle stop %s: %v", id, err)
		}
		if _, err := o.Destroy(ctx, id); err != nil {
			t.Fatalf("settle destroy %s: %v", id, err)
		}
	}
	if u := o.ResourceSnapshot().Used; u != (domain.Resources{}) || len(o.ResourceSnapshot().Reservations) != 0 {
		t.Fatalf("final conservation violated: %+v", o.ResourceSnapshot())
	}
}

// TestCrossProcessUsageConsistency 新起进程（只做 Restore，不做 reconcile）
// 看到的占用必须与真正持有资源的记录集合一致，包括 Running/Stopped/ManualReview。
func TestCrossProcessUsageConsistency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cap := domain.Resources{CPUMillis: 20000, MemoryMB: 16384, DiskMB: 32768}
	spec := smallSpec(1000, 512, 1024)

	st := newFileStore(t, dir)
	h := backend.NewSimHandle(backend.FaultHooks{})
	o1 := orchestrator.New(st, h, cap, testLog{t}.Logf)
	for _, id := range []string{"run1", "stop1", "def1"} {
		if _, err := o1.Create(ctx, id, id, spec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o1.Start(ctx, "run1"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Start(ctx, "stop1"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Stop(ctx, "stop1"); err != nil {
		t.Fatal(err)
	}
	// 再造一个 ManualReview（Running + 后端消失）。
	if _, err := o1.Create(ctx, "held1", "held1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Start(ctx, "held1"); err != nil {
		t.Fatal(err)
	}
	if err := h.Undefine(ctx, "held1"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Reconcile(ctx, testLog{t}.Logf); err != nil {
		t.Fatal(err)
	}
	// 再来一个已销毁的，验证不占。
	if _, err := o1.Create(ctx, "dead1", "dead1", spec); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Destroy(ctx, "dead1"); err != nil {
		t.Fatal(err)
	}

	wantUsed := domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 4096} // run1/stop1/def1/held1

	// 全新进程，只 RestoreFromStore（不触碰后端），占用就必须与事实集合一致。
	o2 := freshProcess(t, dir, h, cap)
	snap := o2.ResourceSnapshot()
	if snap.Used != wantUsed {
		t.Fatalf("fresh process usage = %+v want %+v", snap.Used, wantUsed)
	}
	for _, id := range []string{"run1", "stop1", "def1", "held1"} {
		if _, ok := snap.Reservations[id]; !ok {
			t.Fatalf("fresh process must account %s", id)
		}
	}
	if _, ok := snap.Reservations["dead1"]; ok {
		t.Fatal("Destroyed must not be accounted by fresh process")
	}

	// 再连续起两个新进程，读数必须逐次一致（不会漂移/漏算）。
	o3 := freshProcess(t, dir, h, cap)
	o4 := freshProcess(t, dir, h, cap)
	if o3.ResourceSnapshot().Used != wantUsed || o4.ResourceSnapshot().Used != wantUsed {
		t.Fatalf("repeated fresh-process reads must be stable: %+v %+v",
			o3.ResourceSnapshot().Used, o4.ResourceSnapshot().Used)
	}
}

func errsAcceptable(t *testing.T, e error) {
	t.Helper()
	if e == nil {
		return
	}
	if domain.IsKind(e, domain.KindIllegalTransition) || domain.IsKind(e, domain.KindBackendFailure) {
		return
	}
	// 兜底：其他错误也算测试失败（含意外错误）。
	if errors.Is(e, context.Canceled) {
		return
	}
	t.Errorf("unexpected error: %v", e)
}

func instanceID(i int) string {
	return "vm" + string(rune('a'+i))
}

func newFileStore(t *testing.T, dir string) *store.FileStore {
	t.Helper()
	st, err := store.NewFileStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}
