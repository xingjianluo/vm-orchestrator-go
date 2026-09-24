package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
	"github.com/xingjianluo/vm-orchestrator-go/internal/reconcile"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

// Logger 是最小日志接口（*log.Logger 等均可直接满足）。
type Logger interface {
	Logf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Logf(format string, args ...any) {}

// Orchestrator 是实例生命周期编排器。
// 并发模型：mu 串行化所有"读记录-判定-改后端-持久化"的迁移过程，
// 保证重复/并发请求不会产生重复实例或状态互相覆盖。
// 配额池内部自带锁，编排器在 mu 内调用，锁序固定为 orchestrator.mu -> 下层锁。
type Orchestrator struct {
	mu      sync.Mutex
	store   store.Store
	backend backend.Handle
	quota   *quota.Pool
	logf    func(string, ...any)
}

func New(st store.Store, h backend.Handle, capacity domain.Resources, logf func(string, ...any)) *Orchestrator {
	if logf == nil {
		var nop nopLogger
		logf = nop.Logf
	}
	return &Orchestrator{
		store:   st,
		backend: h,
		quota:   quota.NewPool(capacity),
		logf:    logf,
	}
}

func (o *Orchestrator) log(format string, args ...any) {
	o.logf("[orchestrator] "+format, args...)
}

// Create 创建实例：准入检查先于任何后端副作用；后端失败则回滚配额，记录不落盘。
// 对相同 id+相同规格的重复请求幂等返回已存在记录。
func (o *Orchestrator) Create(ctx context.Context, id, name string, spec domain.Spec) (*domain.Instance, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.NewInvalidArgumentError("create", "empty instance id")
	}
	if spec.Resources.NonPositive() {
		return nil, domain.NewInvalidArgumentError("create",
			"resource amounts must all be positive")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if existing, err := o.store.Get(ctx, id); err == nil {
		if existing.Spec != spec {
			return nil, domain.NewAlreadyExistsError("create", id)
		}
		o.log("Create id=%s idempotent-hit state=%s (重复创建被幂等吸收)", id, existing.State)
		return existing.Clone(), nil
	} else if !domain.IsKind(err, domain.KindNotFound) {
		return nil, err
	}

	if err := o.quota.Reserve("create", id, spec.Resources); err != nil {
		o.log("Create id=%s rejected: %v (准入拒绝，无任何副作用)", id, err)
		return nil, err
	}
	snap := o.quota.Snapshot()
	o.log("Create id=%s quota reserved cpu=%d/%d mem=%d/%d disk=%d/%d",
		id, snap.Used.CPUMillis, snap.Capacity.CPUMillis,
		snap.Used.MemoryMB, snap.Capacity.MemoryMB,
		snap.Used.DiskMB, snap.Capacity.DiskMB)

	if err := o.backend.Define(ctx, id, spec); err != nil {
		// 副作用失败：必须回滚刚做的预留，杜绝占用漂移。
		if _, rerr := o.quota.Release(id); rerr != nil {
			o.log("Create id=%s CRITICAL: backend Define failed (%v) AND quota rollback failed: %v", id, err, rerr)
		}
		return nil, domain.NewBackendError("create", id, err)
	}

	now := time.Now().UTC()
	inst := &domain.Instance{
		ID:        id,
		Name:      name,
		Spec:      spec,
		State:     domain.StateDefined,
		Backend:   o.backend.Name(),
		CreatedAt: now,
		UpdatedAt: now,
		LastEvent: "created",
	}
	if err := o.store.Put(ctx, inst); err != nil {
		// 记录落盘失败：尽最大努力撤销已产生的后端与配额副作用。
		_ = o.backend.Undefine(ctx, id)
		_, _ = o.quota.Release(id)
		return nil, fmt.Errorf("create %s: persist: %w", id, err)
	}
	o.log("Create id=%s -> Defined", id)
	return inst.Clone(), nil
}

// Start 将 Defined/Stopped 迁移到 Running；Running 请求幂等返回。
func (o *Orchestrator) Start(ctx context.Context, id string) (*domain.Instance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State == domain.StateRunning {
		o.log("Start id=%s idempotent-hit (already Running)", id)
		return inst.Clone(), nil
	}
	if inst.State != domain.StateDefined && inst.State != domain.StateStopped {
		return nil, domain.NewIllegalTransitionError("start", id, inst.State, domain.StateRunning,
			"only Defined/Stopped can start")
	}

	if err := o.backend.Start(ctx, id); err != nil {
		// 状态不前进：记录保持原态，交由后续 Stop/对账判定真实承载情况。
		o.log("Start id=%s backend failed from=%s: %v (state unchanged=%s)", id, inst.State, err, inst.State)
		return nil, domain.NewBackendError("start", id, err)
	}

	from := inst.State
	inst.State = domain.StateRunning
	inst.UpdatedAt = time.Now().UTC()
	inst.LastEvent = "started"
	if err := o.store.Put(ctx, inst); err != nil {
		return nil, fmt.Errorf("start %s: persist: %w", id, err)
	}
	o.log("Start id=%s: %s -> Running", id, from)
	return inst.Clone(), nil
}

// Stop 将 Running 迁移到 Stopped；Stopped 请求幂等返回。
func (o *Orchestrator) Stop(ctx context.Context, id string) (*domain.Instance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State == domain.StateStopped {
		o.log("Stop id=%s idempotent-hit (already Stopped)", id)
		return inst.Clone(), nil
	}
	if inst.State != domain.StateRunning {
		return nil, domain.NewIllegalTransitionError("stop", id, inst.State, domain.StateStopped,
			"only Running can stop; Defined instances carry no running guest")
	}

	if err := o.backend.Stop(ctx, id); err != nil {
		o.log("Stop id=%s backend failed: %v (state unchanged=Running)", id, err)
		return nil, domain.NewBackendError("stop", id, err)
	}

	inst.State = domain.StateStopped
	inst.UpdatedAt = time.Now().UTC()
	inst.LastEvent = "stopped"
	if err := o.store.Put(ctx, inst); err != nil {
		return nil, fmt.Errorf("stop %s: persist: %w", id, err)
	}
	o.log("Stop id=%s: Running -> Stopped", id)
	return inst.Clone(), nil
}

// Destroy 销毁 Defined/Stopped 实例：先后端 Undefine，再释放配额，最后持久化墓碑。
// Running 必须先 Stop；Destroyed 请求幂等返回墓碑。
func (o *Orchestrator) Destroy(ctx context.Context, id string) (*domain.Instance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State == domain.StateDestroyed {
		o.log("Destroy id=%s idempotent-hit (already Destroyed)", id)
		return inst.Clone(), nil
	}
	if inst.State == domain.StateRunning {
		return nil, domain.NewIllegalTransitionError("destroy", id, inst.State, domain.StateDestroyed,
			"must stop before destroy")
	}

	if err := o.backend.Undefine(ctx, id); err != nil {
		o.log("Destroy id=%s backend undefine failed: %v (quota retained, state unchanged=%s)",
			id, err, inst.State)
		return nil, domain.NewBackendError("destroy", id, err)
	}
	if _, err := o.quota.Release(id); err != nil {
		// 后端已清除但配额释放失败：这是明确的占用漂移，不能静默，返回不一致错误。
		o.log("Destroy id=%s CRITICAL: backend undefined but quota release failed: %v", id, err)
		return nil, domain.NewInconsistentStateError("destroy", id,
			"backend removed but quota reservation missing: "+err.Error())
	}

	inst.State = domain.StateDestroyed
	inst.UpdatedAt = time.Now().UTC()
	inst.LastEvent = "destroyed (tombstone retained)"
	if err := o.store.Put(ctx, inst); err != nil {
		return nil, fmt.Errorf("destroy %s: persist tombstone: %w", id, err)
	}
	snap := o.quota.Snapshot()
	o.log("Destroy id=%s -> Destroyed; quota now used cpu=%d mem=%d disk=%d reservations=%d",
		id, snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB, len(snap.Reservations))
	return inst.Clone(), nil
}

func (o *Orchestrator) Get(ctx context.Context, id string) (*domain.Instance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.store.Get(ctx, id)
}

func (o *Orchestrator) List(ctx context.Context) ([]*domain.Instance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	loaded, err := o.store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*domain.Instance, 0, len(loaded))
	for _, in := range loaded {
		out = append(out, in.Clone())
	}
	return out, nil
}

func (o *Orchestrator) ResourceSnapshot() quota.Snapshot {
	return o.quota.Snapshot()
}

// Backend 与 Quota/Store 仅供对账器使用。
func (o *Orchestrator) Backend() backend.Handle { return o.backend }
func (o *Orchestrator) Quota() *quota.Pool      { return o.quota }
func (o *Orchestrator) Store() store.Store      { return o.store }

// Reconcile 在重启后执行对账：以存储记录与后端事实对账，并重建配额台账。
func (o *Orchestrator) Reconcile(ctx context.Context, logf func(string, ...any)) (*reconcile.Report, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return reconcile.Run(ctx, o.store, o.backend, func(alive map[string]domain.Resources) (string, error) {
		snap, err := o.quota.Rebuild(alive)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("capacity={cpu:%d,mem:%d,disk:%d} used={cpu:%d,mem:%d,disk:%d} reservations=%d",
			snap.Capacity.CPUMillis, snap.Capacity.MemoryMB, snap.Capacity.DiskMB,
			snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB,
			len(snap.Reservations)), nil
	}, logf)
}

// RestoreFromStore 在进程启动时根据持久化记录重建内存配额台账。
// 它不触碰后端事实（那是 reconcile 的职责），只保证台账与记录一致；
// 台账重建超出宿主容量时明确报错，而不是截断或静默。
func (o *Orchestrator) RestoreFromStore(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	loaded, err := o.store.List(ctx)
	if err != nil {
		return fmt.Errorf("restore: list: %w", err)
	}
	want := make(map[string]domain.Resources)
	for _, in := range loaded {
		if in.State == domain.StateDestroyed {
			continue
		}
		want[in.ID] = in.Spec.Resources
	}
	snap, err := o.quota.Rebuild(want)
	if err != nil {
		return domain.NewInconsistentStateError("restore", "", err.Error())
	}
	o.log("Restore: rebuilt quota ledger from %d non-destroyed records; used cpu=%d mem=%d disk=%d",
		len(want), snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB)
	return nil
}
