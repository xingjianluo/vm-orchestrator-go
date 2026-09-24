// vm-demo 演示一次完整编排：构造管理器 -> 重启对账 -> 创建/启动/停止/销毁。
// 用法: vm-demo [dataDir]（默认 ./.vm-demo-data）
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

func main() {
	dataDir := "./.vm-demo-data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}
	ctx := context.Background()

	st, err := store.New(filepath.Join(dataDir, "state.json"))
	must(err)
	bk, err := backend.NewProcessBackend(filepath.Join(dataDir, "backend"))
	must(err)
	pool := quota.New(model.Resources{CPUMillis: 8000, MemoryMiB: 16384, DiskMiB: 500000})
	mgr, err := orchestrator.New(orchestrator.Options{
		Store: st, Backend: bk, Quota: pool,
		Logger: orchestrator.NewSlogLogger(slog.LevelInfo),
	})
	must(err)

	// 重启对账：先收敛，再对外提供服务。
	rep, err := reconcile.New(mgr).Run(ctx)
	must(err)
	fmt.Println(rep.String())

	in, err := mgr.Create(ctx, "vm-demo-1", "demo", model.Resources{CPUMillis: 1000, MemoryMiB: 1024, DiskMiB: 20480})
	if err != nil {
		fmt.Println("create:", err)
	} else {
		fmt.Println("created:", in.ID, in.State)
	}
	if _, err := mgr.Start(ctx, "vm-demo-1"); err != nil {
		fmt.Println("start:", err)
	}
	if _, err := mgr.Stop(ctx, "vm-demo-1"); err != nil {
		fmt.Println("stop:", err)
	}
	if err := mgr.Destroy(ctx, "vm-demo-1"); err != nil {
		fmt.Println("destroy:", err)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
