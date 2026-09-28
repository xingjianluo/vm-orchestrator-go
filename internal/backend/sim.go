package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// FaultHooks 允许测试在后端动作之前注入失败，用于模拟"副作用已发生但返回错误"等边界。
type FaultHooks struct {
	BeforeDefine   func(id string) error
	BeforeStart    func(id string) error
	BeforeStop     func(id string) error
	BeforeUndefine func(id string) error
}

// simEntry 是模拟后端中单个实例的承载记录。
type simEntry struct {
	status Status
	spec   domain.Spec
}

// SimHandle 是进程内的本地虚拟化模拟后端。
// 它自身只管理"承载事实"，不关心编排层状态机，
// 因此可以模拟重启后"记录丢失但承载残留"或反之的对账场景。
type SimHandle struct {
	mu      sync.Mutex
	entries map[string]*simEntry
	hooksMu sync.RWMutex
	hooks   FaultHooks
}

func NewSimHandle(hooks FaultHooks) *SimHandle {
	return &SimHandle{
		entries: make(map[string]*simEntry),
		hooks:   hooks,
	}
}

// SetHooks 替换故障钩子（主要供测试动态注入阻塞/故障），与钩子触发并发安全。
func (h *SimHandle) SetHooks(hooks FaultHooks) {
	h.hooksMu.Lock()
	defer h.hooksMu.Unlock()
	h.hooks = hooks
}

func (h *SimHandle) fireDefine(id string) error {
	h.hooksMu.RLock()
	f := h.hooks.BeforeDefine
	h.hooksMu.RUnlock()
	if f != nil {
		return f(id)
	}
	return nil
}

func (h *SimHandle) fireStart(id string) error {
	h.hooksMu.RLock()
	f := h.hooks.BeforeStart
	h.hooksMu.RUnlock()
	if f != nil {
		return f(id)
	}
	return nil
}

func (h *SimHandle) fireStop(id string) error {
	h.hooksMu.RLock()
	f := h.hooks.BeforeStop
	h.hooksMu.RUnlock()
	if f != nil {
		return f(id)
	}
	return nil
}

func (h *SimHandle) fireUndefine(id string) error {
	h.hooksMu.RLock()
	f := h.hooks.BeforeUndefine
	h.hooksMu.RUnlock()
	if f != nil {
		return f(id)
	}
	return nil
}

func (h *SimHandle) Name() string { return "sim" }

func (h *SimHandle) Define(ctx context.Context, id string, spec domain.Spec) error {
	if err := h.fireDefine(id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.entries[id]; ok && e.status != StatusAbsent {
		return fmt.Errorf("sim: %s already defined (status=%s)", id, e.status)
	}
	h.entries[id] = &simEntry{status: StatusStopped, spec: spec}
	return nil
}

func (h *SimHandle) Start(ctx context.Context, id string) error {
	if err := h.fireStart(id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.entries[id]
	if !ok {
		return fmt.Errorf("sim: %s not defined", id)
	}
	e.status = StatusRunning
	return nil
}

func (h *SimHandle) Stop(ctx context.Context, id string) error {
	if err := h.fireStop(id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.entries[id]
	if !ok {
		return fmt.Errorf("sim: %s not defined", id)
	}
	if e.status == StatusRunning {
		e.status = StatusStopped
	}
	return nil
}

func (h *SimHandle) Undefine(ctx context.Context, id string) error {
	if err := h.fireUndefine(id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.entries, id)
	return nil
}

// Inspect 对不存在的实例返回 StatusAbsent 而非错误，便于对账层做笛卡尔比对。
func (h *SimHandle) Inspect(ctx context.Context, id string) (Info, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.entries[id]
	if !ok {
		return Info{ID: id, Status: StatusAbsent}, nil
	}
	return Info{ID: id, Status: e.status, Detail: "simulated", Spec: e.spec, HasSpec: true}, nil
}

func (h *SimHandle) List(ctx context.Context) ([]Info, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Info, 0, len(h.entries))
	for id, e := range h.entries {
		out = append(out, Info{ID: id, Status: e.status, Detail: "simulated", Spec: e.spec, HasSpec: true})
	}
	return out, nil
}
