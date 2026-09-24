package backend

import (
	"context"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// Status 是后端观测到的实际承载状态。
type Status string

const (
	StatusAbsent  Status = "absent"
	StatusStopped Status = "stopped"
	StatusRunning Status = "running"
)

// Info 是一次后端观测结果。
type Info struct {
	ID     string
	Status Status
	Pid    int
	Detail string
}

// Handle 抽象本地承载能力（模拟虚拟化或进程级实例）。
type Handle interface {
	Name() string
	Define(ctx context.Context, id string, spec domain.Spec) error
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Undefine(ctx context.Context, id string) error
	Inspect(ctx context.Context, id string) (Info, error)
	List(ctx context.Context) ([]Info, error)
}
