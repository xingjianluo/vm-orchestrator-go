package backend_test

import (
	"context"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
)

func TestProcessBackendIdempotencyAndCleanup(t *testing.T) {
	dir := t.TempDir()
	b, err := backend.NewProcessBackend(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.KillAllByDir(dir) })
	ctx := context.Background()

	if err := b.Define(ctx, "v", "v"); err != nil {
		t.Fatal(err)
	}
	// Define 幂等。
	if err := b.Define(ctx, "v", "v"); err != nil {
		t.Fatalf("define must be idempotent: %v", err)
	}
	pid, err := b.Start(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	// 重复 Start 返回同一存活 PID。
	pid2, err := b.Start(ctx, "v")
	if err != nil || pid2 != pid {
		t.Fatalf("restart idempotency: pid=%d pid2=%d err=%v", pid, pid2, err)
	}
	if info, _ := b.Inspect(ctx, "v"); info.Status != backend.StatusRunning {
		t.Fatalf("want running, got %s", info.Status)
	}
	if err := b.Stop(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	// Stop 幂等，Stopped 后为 defined。
	if err := b.Stop(ctx, "v"); err != nil {
		t.Fatalf("stop must be idempotent: %v", err)
	}
	if info, _ := b.Inspect(ctx, "v"); info.Status != backend.StatusDefined {
		t.Fatalf("want defined after stop, got %s", info.Status)
	}
	if err := b.Undefine(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	if err := b.Undefine(ctx, "v"); err != nil {
		t.Fatalf("undefine must be idempotent: %v", err)
	}
	if info, _ := b.Inspect(ctx, "v"); info.Status != backend.StatusGone {
		t.Fatalf("want gone, got %s", info.Status)
	}
	ids, _ := b.List(ctx)
	if len(ids) != 0 {
		t.Fatalf("backend list must be empty: %v", ids)
	}
}
