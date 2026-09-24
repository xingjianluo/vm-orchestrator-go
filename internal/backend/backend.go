// Package backend 抽象本地虚拟化/进程模拟承载能力。
// orchestrator 只依赖该接口，便于替换真实 hypervisor 或注入故障。
package backend

import "context"

// InstanceStatus 是后端观测到的实例事实状态。
type InstanceStatus string

const (
	StatusDefined InstanceStatus = "defined"
	StatusRunning InstanceStatus = "running"
	StatusGone    InstanceStatus = "gone"
	StatusUnknown InstanceStatus = "unknown"
)

// RuntimeInfo 是后端运行事实（PID、启动时间等）。
type RuntimeInfo struct {
	Status  InstanceStatus
	PID     int
	Started int64 // unix 秒，用于识别 PID 复用；0 表示不可得
}

// Backend 是实例承载后端。
type Backend interface {
	// Define 创建实例定义（幂等：已定义视为成功）。
	Define(ctx context.Context, id, name string) error
	// Start 拉起实例运行实体，返回其 PID。
	Start(ctx context.Context, id string) (pid int, err error)
	// Stop 终止运行实体（幂等：已退出视为成功）。
	Stop(ctx context.Context, id string) error
	// Undefine 删除实例定义（幂等：不存在视为成功）。
	Undefine(ctx context.Context, id string) error
	// Inspect 查询后端事实。
	Inspect(ctx context.Context, id string) (RuntimeInfo, error)
	// List 返回后端存在定义的全部实例 ID（用于对账发现后端残留）。
	List(ctx context.Context) ([]string, error)
	// Name 返回后端名称，用于日志与对账报告。
	Name() string
}
