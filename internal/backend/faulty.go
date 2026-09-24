package backend

import (
	"context"
	"fmt"
)

// FaultPoint 描述对操作注入的故障。
type FaultPoint struct {
	FailDefine bool
	FailStart  bool
	FailStop   bool
	// FailUndefine：Undefine 失败（模拟销毁时后端残留）。
	FailUndefine bool
	// StartKillAfter：Start 成功后立刻杀死子进程，模拟启动后即崩溃。
	StartKillAfter bool
	// FailIDs 非空时，仅对其中的实例 ID 注入故障；为空则对所有实例生效。
	FailIDs map[string]bool
}

func (f FaultPoint) hit(id string) bool {
	if len(f.FailIDs) == 0 {
		return true
	}
	return f.FailIDs[id]
}

// FaultyBackend 在委托后端之上注入故障，供异常恢复测试使用。
type FaultyBackend struct {
	inner Backend
	fault FaultPoint
}

// NewFaultyBackend 包装一个后端并注入故障点。
func NewFaultyBackend(inner Backend, fault FaultPoint) *FaultyBackend {
	return &FaultyBackend{inner: inner, fault: fault}
}

func (b *FaultyBackend) Name() string { return "faulty(" + b.inner.Name() + ")" }

func (b *FaultyBackend) Define(ctx context.Context, id, name string) error {
	if b.fault.FailDefine && b.fault.hit(id) {
		return fmt.Errorf("%w: injected define failure", errInjected)
	}
	return b.inner.Define(ctx, id, name)
}

func (b *FaultyBackend) Start(ctx context.Context, id string) (int, error) {
	if b.fault.FailStart && b.fault.hit(id) {
		return 0, fmt.Errorf("%w: injected start failure", errInjected)
	}
	pid, err := b.inner.Start(ctx, id)
	if err != nil {
		return 0, err
	}
	if b.fault.StartKillAfter && b.fault.hit(id) {
		if pb, ok := b.inner.(*ProcessBackend); ok {
			pb.KillAndReap(pid)
		} else {
			_ = killGroup(pid)
		}
		return pid, nil
	}
	return pid, nil
}

func (b *FaultyBackend) Stop(ctx context.Context, id string) error {
	if b.fault.FailStop && b.fault.hit(id) {
		return fmt.Errorf("%w: injected stop failure", errInjected)
	}
	return b.inner.Stop(ctx, id)
}

func (b *FaultyBackend) Undefine(ctx context.Context, id string) error {
	if b.fault.FailUndefine && b.fault.hit(id) {
		return fmt.Errorf("%w: injected undefine failure", errInjected)
	}
	return b.inner.Undefine(ctx, id)
}

func (b *FaultyBackend) Inspect(ctx context.Context, id string) (RuntimeInfo, error) {
	return b.inner.Inspect(ctx, id)
}

func (b *FaultyBackend) List(ctx context.Context) ([]string, error) {
	return b.inner.List(ctx)
}
