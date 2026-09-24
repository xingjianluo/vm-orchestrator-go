package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
)

// TestConcurrentDuplicateCreate 验证并发重复创建同一实例：
// 恰好一个成功，不产生重复实例，配额只占用一份。
func TestConcurrentDuplicateCreate(t *testing.T) {
	fx := newFixture(t, model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}, backend.FaultPoint{})
	ctx := context.Background()
	spec := model.Resources{CPUMillis: 500, MemoryMiB: 512, DiskMiB: 1000}

	const n = 32
	var wg sync.WaitGroup
	var ok, dup int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := fx.mgr.Create(ctx, "vmX", "vmX", spec)
			switch {
			case err == nil:
				atomic.AddInt64(&ok, 1)
			case errors.Is(err, model.ErrAlreadyExists):
				atomic.AddInt64(&dup, 1)
			default:
				t.Errorf("unexpected create error: %v", err)
			}
		}()
	}
	wg.Wait()

	if ok != 1 || dup != n-1 {
		t.Fatalf("want exactly 1 success and %d duplicates, got ok=%d dup=%d", n-1, ok, dup)
	}
	if used := fx.qp.Used(); used != spec {
		t.Fatalf("quota drift under concurrent create: used=%+v want=%+v", used, spec)
	}
	list, _ := fx.bk.List(ctx)
	if len(list) != 1 || list[0] != "vmX" {
		t.Fatalf("backend must have exactly one definition, got %v", list)
	}
	t.Logf("[operation sequence] %d x Create(vmX) concurrently -> ok=%d duplicate=%d", n, ok, dup)
	t.Logf("[resource snapshot] used=%+v backendDefinitions=%v", fx.qp.Used(), list)
}

// TestConcurrentQuotaContention 验证配额争抢下的守恒：
// 成功分配之和恰好等于已用量且永不超过总量；拒绝全部可区分为配额原因。
func TestConcurrentQuotaContention(t *testing.T) {
	total := model.Resources{CPUMillis: 2000, MemoryMiB: 2048, DiskMiB: 20000}
	fx := newFixture(t, total, backend.FaultPoint{})
	ctx := context.Background()
	need := model.Resources{CPUMillis: 300, MemoryMiB: 300, DiskMiB: 3000}
	const n = 24

	var wg sync.WaitGroup
	var ok, rejected int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		id := fmt.Sprintf("vm-%02d", i)
		go func() {
			defer wg.Done()
			_, err := fx.mgr.Create(ctx, id, id, need)
			switch {
			case err == nil:
				atomic.AddInt64(&ok, 1)
			case errors.Is(err, model.ErrQuotaExceeded):
				atomic.AddInt64(&rejected, 1)
			default:
				t.Errorf("unexpected error for %s: %v", id, err)
			}
		}()
	}
	wg.Wait()

	wantInstances := total.CPUMillis / need.CPUMillis // 2000/300 = 6
	if int(ok) != wantInstances {
		t.Fatalf("want %d admits, got %d (rejected=%d)", wantInstances, ok, rejected)
	}
	if int(ok+rejected) != n {
		t.Fatalf("every request must be admitted or rejected, ok=%d rejected=%d", ok, rejected)
	}
	used := fx.qp.Used()
	wantUsed := model.Resources{
		CPUMillis: int(ok) * need.CPUMillis,
		MemoryMiB: int(ok) * need.MemoryMiB,
		DiskMiB:   int(ok) * need.DiskMiB,
	}
	if used != wantUsed {
		t.Fatalf("quota not conserved: used=%+v want=%+v", used, wantUsed)
	}
	if used.CPUMillis > total.CPUMillis || used.MemoryMiB > total.MemoryMiB || used.DiskMiB > total.DiskMiB {
		t.Fatalf("quota over-allocated: used=%+v total=%+v", used, total)
	}
	list, _ := fx.bk.List(ctx)
	if len(list) != int(ok) {
		t.Fatalf("backend definitions (%d) must match admitted count (%d)", len(list), ok)
	}
	t.Logf("[operation sequence] %d x Create concurrently, quota cpu=%d -> admitted=%d rejected=%d",
		n, total.CPUMillis, ok, rejected)
	t.Logf("[resource snapshot] total=%+v used=%+v", total, used)
}

// TestConcurrentStartStop 验证运行态竞态：任意交错后，记录状态必须与后端事实一致。
func TestConcurrentStartStop(t *testing.T) {
	fx := newFixture(t, model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}, backend.FaultPoint{})
	ctx := context.Background()
	if _, err := fx.mgr.Create(ctx, "vm1", "vm1", model.Resources{CPUMillis: 100, MemoryMiB: 128, DiskMiB: 100}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const rounds = 40
	var wg sync.WaitGroup
	var starts, stops, rejected int64
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := fx.mgr.Start(ctx, "vm1"); err != nil {
				if errors.Is(err, model.ErrInvalidTransition) {
					atomic.AddInt64(&rejected, 1)
					return
				}
				t.Errorf("start: %v", err)
			} else {
				atomic.AddInt64(&starts, 1)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := fx.mgr.Stop(ctx, "vm1"); err != nil {
				if errors.Is(err, model.ErrInvalidTransition) {
					atomic.AddInt64(&rejected, 1)
					return
				}
				t.Errorf("stop: %v", err)
			} else {
				atomic.AddInt64(&stops, 1)
			}
		}()
	}
	wg.Wait()

	got, _ := fx.mgr.Get(ctx, "vm1")
	info, err := fx.bk.Inspect(ctx, "vm1")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	switch got.State {
	case model.StateRunning:
		if info.Status != backend.StatusRunning {
			t.Fatalf("record Running but backend=%s (pid=%d)", info.Status, info.PID)
		}
	case model.StateStopped:
		if info.Status != backend.StatusDefined {
			t.Fatalf("record Stopped but backend=%s", info.Status)
		}
	default:
		t.Fatalf("unexpected terminal state after race: %s reason=%s", got.State, got.Reason)
	}
	if used := fx.qp.Used(); used != got.Spec {
		t.Fatalf("quota must remain single allocation, used=%+v", used)
	}
	t.Logf("[operation sequence] %d Start/Stop pairs interleaved: startOK=%d stopOK=%d rejected=%d",
		rounds, starts, stops, rejected)
	t.Logf("[resource snapshot] final state=%s backend=%s used=%+v", got.State, info.Status, fx.qp.Used())
}

// TestConcurrentStartDestroy 验证运行/销毁竞态不会泄漏：
// 最终要么仍存在(Running/Stopped 且后端+配额一致)，要么彻底消失(Destroyed 且无残留)。
func TestConcurrentStartDestroy(t *testing.T) {
	fx := newFixture(t, model.Resources{CPUMillis: 4000, MemoryMiB: 8192, DiskMiB: 100000}, backend.FaultPoint{})
	ctx := context.Background()
	spec := model.Resources{CPUMillis: 100, MemoryMiB: 128, DiskMiB: 100}
	if _, err := fx.mgr.Create(ctx, "vm1", "vm1", spec); err != nil {
		t.Fatalf("create: %v", err)
	}

	const rounds = 30
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = fx.mgr.Start(ctx, "vm1")
		}()
		go func() {
			defer wg.Done()
			// 直接销毁只在 Stopped 合法；先尝试停止以制造更多交错。
			_ = fx.mgr.Destroy(ctx, "vm1")
		}()
	}
	wg.Wait()

	got, err := fx.mgr.Get(ctx, "vm1")
	switch {
	case errors.Is(err, model.ErrNotFound):
		t.Fatalf("destroyed record must still exist as Destroyed")
	case err != nil:
		t.Fatalf("get: %v", err)
	}
	list, _ := fx.bk.List(ctx)
	switch got.State {
	case model.StateDestroyed:
		if len(list) != 0 {
			t.Fatalf("Destroyed but backend residue: %v", list)
		}
		if fx.qp.Has("vm1") {
			t.Fatalf("Destroyed but quota still allocated")
		}
	case model.StateRunning, model.StateStopped:
		if len(list) != 1 {
			t.Fatalf("alive instance must have backend definition, got %v", list)
		}
		if !fx.qp.Has("vm1") {
			t.Fatalf("alive instance lost its quota")
		}
	default:
		t.Fatalf("unexpected state after start/destroy race: %s (%s)", got.State, got.Reason)
	}
	t.Logf("[operation sequence] %d Start/Destroy pairs interleaved -> final=%s", rounds, got.State)
	t.Logf("[resource snapshot] used=%+v backendDefinitions=%v", fx.qp.Used(), list)
}
