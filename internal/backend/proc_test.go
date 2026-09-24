//go:build !windows

package backend_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

func newProc(t *testing.T) (*backend.ProcHandle, string) {
	t.Helper()
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep not available")
	}
	dir := filepath.Join(t.TempDir(), "procs")
	h, err := backend.NewProcHandle(backend.ProcConfig{Dir: dir, StopGrace: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return h, dir
}

func TestProcBackendRealProcessLifecycle(t *testing.T) {
	ctx := context.Background()
	h, dir := newProc(t)
	spec := domain.Spec{Image: "img", Resources: domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}}

	if err := h.Define(ctx, "p1", spec); err != nil {
		t.Fatal(err)
	}
	if info, _ := h.Inspect(ctx, "p1"); info.Status != backend.StatusStopped {
		t.Fatalf("defined inspect = %s want stopped", info.Status)
	}
	if err := h.Start(ctx, "p1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	info, _ := h.Inspect(ctx, "p1")
	if info.Status != backend.StatusRunning || info.Pid <= 0 {
		t.Fatalf("running inspect wrong: %+v", info)
	}
	t.Logf("[proc] p1 running pid=%d", info.Pid)

	// 全新句柄模拟"编排器重启"：没有任何内存 session，仅凭磁盘 pid 探活。
	hRestart, err := backend.NewProcHandle(backend.ProcConfig{Dir: dir, StopGrace: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	info2, _ := hRestart.Inspect(ctx, "p1")
	if info2.Status != backend.StatusRunning || info2.Pid != info.Pid {
		t.Fatalf("post-restart inspect should detect live guest: %+v (orig pid=%d)", info2, info.Pid)
	}
	t.Logf("[proc] restarted orchestrator observes live pid=%d", info2.Pid)

	if err := hRestart.Stop(ctx, "p1"); err != nil {
		t.Fatalf("stop across restart: %v", err)
	}
	if info, _ := h.Inspect(ctx, "p1"); info.Status != backend.StatusStopped {
		t.Fatalf("after stop inspect = %s", info.Status)
	}
	if err := hRestart.Undefine(ctx, "p1"); err != nil {
		t.Fatalf("undefine: %v", err)
	}
	if info, _ := h.Inspect(ctx, "p1"); info.Status != backend.StatusAbsent {
		t.Fatalf("after undefine inspect = %s want absent", info.Status)
	}
}

func TestProcBackendDetectsExternalKillAsCrash(t *testing.T) {
	ctx := context.Background()
	h, _ := newProc(t)
	spec := domain.Spec{Image: "img", Resources: domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}}
	if err := h.Define(ctx, "p2", spec); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(ctx, "p2"); err != nil {
		t.Fatal(err)
	}
	info, _ := h.Inspect(ctx, "p2")
	if info.Status != backend.StatusRunning {
		t.Fatalf("want running: %+v", info)
	}

	// 外部 SIGKILL：模拟宿主/内核强杀，完全绕过编排层 Stop。
	if err := syscall.Kill(info.Pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var status backend.Status
	for time.Now().Before(deadline) {
		got, _ := h.Inspect(ctx, "p2")
		status = got.Status
		if status == backend.StatusStopped {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if status != backend.StatusStopped {
		t.Fatalf("external kill must be observed as stopped (crash), got %s", status)
	}
	t.Logf("[proc] external SIGKILL on pid=%d observed as stopped/crash", info.Pid)

	// 崩溃后 Stop 必须幂等：清理 stale pid 而不报错。
	if err := h.Stop(ctx, "p2"); err != nil {
		t.Fatalf("stop after crash must be idempotent: %v", err)
	}
	if info, _ := h.Inspect(ctx, "p2"); info.Status != backend.StatusStopped {
		t.Fatalf("idempotent post-crash stop keeps stopped: %+v", info)
	}
}

// TestProcBackendWithRealGuest 用本仓库 vmguest 作为承载程序验证 SIGTERM 优雅退出。
func TestProcBackendWithRealGuest(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain unavailable")
	}
	guestBin := filepath.Join(t.TempDir(), "vmguest")
	build := exec.Command("go", "build", "-o", guestBin, "../../cmd/vmguest")
	if out, err := build.CombinedOutput(); err != nil {
		t.Skipf("cannot build vmguest: %v %s", err, out)
	}

	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "procs")
	h, err := backend.NewProcHandle(backend.ProcConfig{Dir: dir, GuestBin: guestBin, StopGrace: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	spec := domain.Spec{Image: "img", Resources: domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}}
	if err := h.Define(ctx, "g1", spec); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(ctx, "g1"); err != nil {
		t.Fatalf("guest start: %v", err)
	}
	if info, _ := h.Inspect(ctx, "g1"); info.Status != backend.StatusRunning {
		t.Fatalf("guest not running: %+v", info)
	}
	// SIGTERM 优雅停止（StopGrace 内应退出）。
	done := make(chan error, 1)
	go func() { done <- h.Stop(ctx, "g1") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("guest graceful stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("guest did not exit gracefully within timeout")
	}
	if info, _ := h.Inspect(ctx, "g1"); info.Status != backend.StatusStopped {
		t.Fatalf("guest state after graceful stop: %+v", info)
	}
}
