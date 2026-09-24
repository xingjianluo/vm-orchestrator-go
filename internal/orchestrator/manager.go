// Package orchestrator 在 backend 与 store 之上实现实例生命周期编排：
// 互斥串行化所有变更、配额准入先于副作用、状态迁移合法校验、
// 状态先改内存再原子落盘，保证并发安全与状态可解释。
//
// 关键不变量：
//  1. 任意时刻只有一个变更操作在执行（mu 串行化），杜绝并发覆盖。
//  2. 配额在后端副作用之前完成准入与占用；失败路径必释放，占用不漂移。
//  3. 每个操作的中间态与结果态均先持久化再推进后端，崩溃后可按记录对账。
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

// Options 是 Manager 的构造参数。
type Options struct {
	Store   *store.Store
	Backend backend.Backend
	Quota   *quota.Pool
	Logger  Logger
}

// Logger 是编排层使用的最小日志接口。
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Manager 编排实例生命周期。所有变更操作在同一把互斥锁内完成。
type Manager struct {
	mu      sync.Mutex
	st      *store.Store
	bk      backend.Backend
	qp      *quota.Pool
	log     Logger
	snap    *store.Snapshot
	nameIdx map[string]string // name -> id
}

// New 创建管理器并载入已有快照。
// 注意：配额池不会从快照自动恢复（避免崩溃前记录与实际不一致时的漂移），
// 应由启动方在调用 reconcile 之后使用 RebuildQuota 重算。
func New(opts Options) (*Manager, error) {
	if opts.Store == nil || opts.Backend == nil || opts.Quota == nil {
		return nil, errors.New("orchestrator: store, backend and quota are required")
	}
	log := opts.Logger
	if log == nil {
		log = nopLogger{}
	}
	snap, err := opts.Store.Load(context.Background())
	if err != nil {
		return nil, fmt.Errorf("orchestrator init: %w", err)
	}
	nameIdx := make(map[string]string, len(snap.Instances))
	for id, in := range snap.Instances {
		if in == nil {
			continue
		}
		nameIdx[in.Name] = id
	}
	return &Manager{
		st:      opts.Store,
		bk:      opts.Backend,
		qp:      opts.Quota,
		log:     log,
		snap:    snap,
		nameIdx: nameIdx,
	}, nil
}

// persistLocked 在持锁状态下原子落盘当前内存快照。
func (m *Manager) persistLocked(ctx context.Context) error {
	m.snap.Seq++
	m.snap.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := m.st.Save(ctx, m.snap); err != nil {
		return fmt.Errorf("persist state: %w", err)
	}
	return nil
}

// transitionLocked 校验并执行一次状态迁移，同时更新簿记字段。
func (m *Manager) transitionLocked(in *model.Instance, to model.State, reason string) error {
	if !model.CanTransition(in.State, to) {
		return &model.TransitionError{ID: in.ID, From: in.State}
	}
	in.State = to
	in.Reason = reason
	in.UpdatedAt = time.Now().UTC()
	in.Generation++
	return nil
}

// Create 创建实例：重复校验 -> 配额准入/占用 -> 记录 Pending 落盘 -> 后端 Define。
func (m *Manager) Create(ctx context.Context, id, name string, spec model.Resources) (*model.Instance, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: id and name must be non-empty", model.ErrInvalidArgument)
	}
	if spec.CPUMillis < 0 || spec.MemoryMiB < 0 || spec.DiskMiB < 0 {
		return nil, fmt.Errorf("%w: resources must be non-negative", model.ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 幂等：相同 ID 已存在 -> 不重复占用/不重复定义。
	// 对停留在 Pending 的上次失败/崩溃创建，在此重试 Define 并尝试收敛到 Stopped。
	if existing, ok := m.snap.Instances[id]; ok {
		if existing.State == model.StatePending {
			info, ierr := m.bk.Inspect(ctx, id)
			switch {
			case ierr == nil && info.Status == backend.StatusDefined:
				if err := m.transitionLocked(existing, model.StateStopped, "define confirmed on create retry"); err == nil {
					_ = m.persistLocked(ctx)
				}
			case ierr == nil && info.Status == backend.StatusGone:
				if derr := m.bk.Define(ctx, id, name); derr == nil {
					if err := m.transitionLocked(existing, model.StateStopped, ""); err == nil {
						_ = m.persistLocked(ctx)
					}
				}
			}
			return existing.Clone(), nil
		}
		m.log.Info("create idempotent hit", "id", id, "state", existing.State)
		return existing.Clone(), model.ErrAlreadyExists
	}
	// 同名拒绝，防止别名产生重复实例。
	if otherID, ok := m.nameIdx[name]; ok {
		return nil, fmt.Errorf("%w: name %q already used by instance %s", model.ErrAlreadyExists, name, otherID)
	}

	// 配额准入发生在任何后端副作用之前。
	if err := m.qp.Allocate(id, spec); err != nil {
		m.log.Info("create rejected by quota", "id", id, "err", err.Error())
		return nil, err
	}

	now := time.Now().UTC()
	in := &model.Instance{
		ID:        id,
		Name:      name,
		Spec:      spec,
		State:     model.StatePending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.snap.Instances[id] = in
	m.nameIdx[name] = id
	// 先把“占用配额 + Pending”落盘，再产生后端副作用。
	if err := m.persistLocked(ctx); err != nil {
		_ = m.qp.Release(id)
		delete(m.snap.Instances, id)
		delete(m.nameIdx, name)
		return nil, err
	}
	m.log.Info("allocated quota and persisted Pending", "id", id, "spec",
		fmt.Sprintf("cpu=%d mem=%d disk=%d", spec.CPUMillis, spec.MemoryMiB, spec.DiskMiB))

	if err := m.bk.Define(ctx, id, name); err != nil {
		// Define 失败：记录留在 Pending 并标注原因（占用保留），
		// 由后续重试 Create 或重启对账收敛；不静默删除已落盘事实。
		_ = m.transitionLocked(in, model.StatePending, "define failed: "+err.Error())
		_ = m.persistLocked(ctx)
		m.log.Error("backend define failed", "id", id, "err", err.Error())
		return nil, fmt.Errorf("%w: %v", model.ErrBackendFailed, err)
	}

	if err := m.transitionLocked(in, model.StateStopped, ""); err != nil {
		return nil, err
	}
	if err := m.persistLocked(ctx); err != nil {
		return nil, err
	}
	m.log.Info("instance created", "id", id, "state", model.StateStopped)
	return in.Clone(), nil
}

// Start 启动实例：合法迁移 -> 记录 Starting 落盘 -> 后端 Start -> Running 落盘。
func (m *Manager) Start(ctx context.Context, id string) (*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	in, err := m.lockedGet(id)
	if err != nil {
		return nil, err
	}
	switch in.State {
	case model.StateRunning, model.StateStarting:
		// 幂等：重复启动直接返回当前状态。
		m.log.Info("start idempotent hit", "id", id, "state", in.State)
		return in.Clone(), nil
	case model.StateFailed:
		return nil, fmt.Errorf("%w: instance %s is inconsistent: %s", model.ErrInconsistent, id, in.Reason)
	case model.StateStopped:
		// 唯一合法的启动入口。
	default:
		return nil, &model.TransitionError{ID: id, From: in.State, Op: model.OpStart}
	}

	if err := m.transitionLocked(in, model.StateStarting, ""); err != nil {
		return nil, err
	}
	if err := m.persistLocked(ctx); err != nil {
		return nil, err
	}

	pid, err := m.bk.Start(ctx, id)
	if err != nil {
		// 后端失败：事实不确定，保留 Starting + 原因，交由重试/对账判定。
		_ = m.transitionLocked(in, model.StateStarting, "start failed: "+err.Error())
		_ = m.persistLocked(ctx)
		m.log.Error("backend start failed", "id", id, "err", err.Error())
		return nil, fmt.Errorf("%w: %v", model.ErrBackendFailed, err)
	}
	in.PID = pid

	if err := m.transitionLocked(in, model.StateRunning, ""); err != nil {
		return nil, err
	}
	if err := m.persistLocked(ctx); err != nil {
		return nil, err
	}
	m.log.Info("instance started", "id", id, "pid", pid)
	return in.Clone(), nil
}

// Stop 停止实例：合法迁移 -> 记录 Stopping 落盘 -> 后端 Stop -> Stopped 落盘。
func (m *Manager) Stop(ctx context.Context, id string) (*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	in, err := m.lockedGet(id)
	if err != nil {
		return nil, err
	}
	switch in.State {
	case model.StateStopped, model.StateStopping:
		m.log.Info("stop idempotent hit", "id", id, "state", in.State)
		return in.Clone(), nil
	case model.StateFailed:
		return nil, fmt.Errorf("%w: instance %s is inconsistent: %s", model.ErrInconsistent, id, in.Reason)
	case model.StateRunning, model.StateStarting:
		// 合法入口（Starting 时崩溃/失败可用 Stop 兜底清理）。
	default:
		return nil, &model.TransitionError{ID: id, From: in.State, Op: model.OpStop}
	}

	if err := m.transitionLocked(in, model.StateStopping, ""); err != nil {
		return nil, err
	}
	if err := m.persistLocked(ctx); err != nil {
		return nil, err
	}

	if err := m.bk.Stop(ctx, id); err != nil {
		_ = m.transitionLocked(in, model.StateStopping, "stop failed: "+err.Error())
		_ = m.persistLocked(ctx)
		m.log.Error("backend stop failed", "id", id, "err", err.Error())
		return nil, fmt.Errorf("%w: %v", model.ErrBackendFailed, err)
	}
	in.PID = 0

	if err := m.transitionLocked(in, model.StateStopped, ""); err != nil {
		return nil, err
	}
	if err := m.persistLocked(ctx); err != nil {
		return nil, err
	}
	m.log.Info("instance stopped", "id", id)
	return in.Clone(), nil
}

// Destroy 销毁实例：Stopped/Failed 入口 -> Destroying 落盘 -> 后端清理 -> 释放配额 -> Destroyed。
func (m *Manager) Destroy(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	in, err := m.lockedGet(id)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			// 销毁不存在的实例：幂等成功。
			m.log.Info("destroy idempotent hit", "id", id, "exists", false)
			return nil
		}
		return err
	}
	if in.State == model.StateDestroyed {
		m.log.Info("destroy idempotent hit", "id", id, "state", model.StateDestroyed)
		return nil
	}
	switch in.State {
	case model.StateStopped, model.StateFailed, model.StatePending:
		// 允许从 Failed/Pending 人工/自动终结。
	case model.StateRunning, model.StateStarting, model.StateStopping:
		// 运行态必须先 Stop，拒绝直接销毁（避免泄漏进程）。
		return &model.TransitionError{ID: id, From: in.State, Op: model.OpDestroy}
	default:
		return &model.TransitionError{ID: id, From: in.State, Op: model.OpDestroy}
	}

	if err := m.transitionLocked(in, model.StateDestroying, ""); err != nil {
		return err
	}
	if err := m.persistLocked(ctx); err != nil {
		return err
	}

	// 先尽力停止可能存在的运行实体，再删除定义。
	if err := m.bk.Stop(ctx, id); err != nil {
		_ = m.markFailedLocked(ctx, in, "destroy: stop phase failed: "+err.Error())
		return fmt.Errorf("%w: %v", model.ErrBackendFailed, err)
	}
	if err := m.bk.Undefine(ctx, id); err != nil {
		// 后端残留：无法安全释放配额（可能仍被占用），标记人工介入，绝不静默释放。
		_ = m.markFailedLocked(ctx, in, "destroy: undefine failed, backend residue: "+err.Error())
		m.log.Error("backend undefine failed, instance marked inconsistent", "id", id, "err", err.Error())
		return fmt.Errorf("%w: %v", model.ErrBackendFailed, err)
	}

	// 后端事实已清除，释放配额（配额必须与实际状态守恒）。
	if m.qp.Has(id) {
		if err := m.qp.Release(id); err != nil {
			_ = m.markFailedLocked(ctx, in, "destroy: quota release failed: "+err.Error())
			return err
		}
	}
	in.PID = 0
	if err := m.transitionLocked(in, model.StateDestroyed, ""); err != nil {
		return err
	}
	if err := m.persistLocked(ctx); err != nil {
		return nil
	}
	delete(m.nameIdx, in.Name)
	m.log.Info("instance destroyed", "id", id)
	return nil
}

// markFailedLocked 把实例标记为人工介入状态并落盘（仅在持锁时调用）。
func (m *Manager) markFailedLocked(ctx context.Context, in *model.Instance, reason string) error {
	if err := m.transitionLocked(in, model.StateFailed, reason); err != nil {
		return err
	}
	return m.persistLocked(ctx)
}

// lockedGet 返回实例指针（仅在持锁时调用，调用方不得在解锁后保留修改）。
func (m *Manager) lockedGet(id string) (*model.Instance, error) {
	in, ok := m.snap.Instances[id]
	if !ok {
		return nil, fmt.Errorf("%w: instance %s", model.ErrNotFound, id)
	}
	return in, nil
}

// Get 返回实例记录拷贝。
func (m *Manager) Get(ctx context.Context, id string) (*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, err := m.lockedGet(id)
	if err != nil {
		return nil, err
	}
	return in.Clone(), nil
}

// List 返回全部实例记录拷贝。
func (m *Manager) List(ctx context.Context) ([]*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*model.Instance, 0, len(m.snap.Instances))
	for _, in := range m.snap.Instances {
		out = append(out, in.Clone())
	}
	return out, nil
}

// QuotaView 是配额池的不可变观测视图。
type QuotaView struct {
	Total model.Resources
	Used  model.Resources
}

// Snapshot 返回实例记录拷贝与配额用量视图。
func (m *Manager) Snapshot(ctx context.Context) (map[string]*model.Instance, QuotaView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*model.Instance, len(m.snap.Instances))
	for k, v := range m.snap.Instances {
		out[k] = v.Clone()
	}
	return out, QuotaView{Total: m.qp.Total(), Used: m.qp.Used()}, nil
}

// Persist 将当前内存状态强制落盘。
func (m *Manager) Persist(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persistLocked(ctx)
}
