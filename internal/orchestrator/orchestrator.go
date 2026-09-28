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
//
// 并发模型：
//   - mutate 持有 RLock（允许不同实例并行），Reconcile/Restore 持有 Lock
//     （全局排他，与任何变更互斥）；
//   - 同一实例的迁移再由 idLocks[id] 串行化，保证同实例并发请求有确定先后、
//     不会互相覆盖；不同实例之间不排队；
//   - 锁序固定为 gate(RLock) -> idMu -> 下层（quota/store/backend）锁，
//     Reconcile 只取 gate 的写锁，因此不会与 per-id 锁形成环。
//
// 配额池内部自带锁。任何新构造的 Orchestrator 都会先按持久化记录重建内存台账，
// 因此"另起一个进程"查询占用时，读数与真正持有资源的记录集合一致。
type Orchestrator struct {
	// gate 是全局屏障：变更取读锁（可并发），对账/恢复取写锁（排他）。
	gate    sync.RWMutex
	idMu    sync.Mutex
	idLocks map[string]*sync.Mutex
	store   store.Store
	backend backend.Handle
	quota   *quota.Pool
	logf    func(string, ...any)
}

// lockInstance 先取全局读屏障，再取该实例专属锁。
// 调用方必须 defer 其返回的解锁函数；不得在持有期间嵌套获取其他实例的锁。
func (o *Orchestrator) lockInstance(id string) func() {
	o.gate.RLock()
	o.idMu.Lock()
	m, ok := o.idLocks[id]
	if !ok {
		m = &sync.Mutex{}
		o.idLocks[id] = m
	}
	o.idMu.Unlock()
	m.Lock()
	return func() {
		m.Unlock()
		o.gate.RUnlock()
	}
}

// New 创建编排器，并立即依据持久化记录重建内存配额台账（模拟进程启动）。
// 这样任何新进程在第一次查询前，占用就与"所有非 Destroyed 记录"一致，
// 不会出现实例明明占着、新进程却显示为零的情况。
// 若重建结果超过宿主容量，返回 inconsistent_state 错误而非截断。
func New(st store.Store, h backend.Handle, capacity domain.Resources, logf func(string, ...any)) (*Orchestrator, error) {
	if logf == nil {
		var nop nopLogger
		logf = nop.Logf
	}
	o := &Orchestrator{
		idLocks: make(map[string]*sync.Mutex),
		store:   st,
		backend: h,
		quota:   quota.NewPool(capacity),
		logf:    logf,
	}
	if err := o.rebuildQuotaFromStore(context.Background()); err != nil {
		return nil, err
	}
	return o, nil
}

// rebuildQuotaFromStore 以所有非 Destroyed（含 PendingManual）记录为权威集合重建台账。
// 调用方不得持有 gate 写锁以外的状态；本方法自行加 gate 写锁。
func (o *Orchestrator) rebuildQuotaFromStore(ctx context.Context) error {
	o.gate.Lock()
	defer o.gate.Unlock()

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
	o.log("[restore] rebuilt quota ledger from %d non-destroyed records; used cpu=%d mem=%d disk=%d reservations=%d",
		len(want), snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB, len(snap.Reservations))
	return nil
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

	unlock := o.lockInstance(id)
	defer unlock()

	if existing, err := o.store.Get(ctx, id); err == nil {
		if existing.IsPending() {
			return nil, domain.NewPendingManualError("create", id,
				"instance is pending manual resolution; use resolve --keep/--destroy: "+existing.Pending.Reason)
		}
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
	unlock := o.lockInstance(id)
	defer unlock()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.IsPending() {
		return nil, domain.NewPendingManualError("start", id,
			"instance is pending manual resolution; use resolve --keep/--destroy: "+inst.Pending.Reason)
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
	unlock := o.lockInstance(id)
	defer unlock()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.IsPending() {
		return nil, domain.NewPendingManualError("stop", id,
			"instance is pending manual resolution; use resolve --keep/--destroy: "+inst.Pending.Reason)
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
// PendingManual 实例不允许走常规销毁，必须由人工 Resolve 收口。
func (o *Orchestrator) Destroy(ctx context.Context, id string) (*domain.Instance, error) {
	unlock := o.lockInstance(id)
	defer unlock()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.IsPending() {
		return nil, domain.NewPendingManualError("destroy", id,
			"instance is pending manual resolution; use resolve --destroy after human confirmation: "+inst.Pending.Reason)
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
	o.gate.RLock()
	defer o.gate.RUnlock()
	return o.store.Get(ctx, id)
}

func (o *Orchestrator) List(ctx context.Context) ([]*domain.Instance, error) {
	o.gate.RLock()
	defer o.gate.RUnlock()
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

// ResourceSnapshot 返回配额台账的守恒快照。台账在构造时按持久化记录重建，
// 并随每次迁移/对账更新，因此任意新进程任意时刻查询都自洽。
func (o *Orchestrator) ResourceSnapshot() quota.Snapshot {
	return o.quota.Snapshot()
}

// Backend 与 Quota/Store 仅供对账器使用。
func (o *Orchestrator) Backend() backend.Handle { return o.backend }
func (o *Orchestrator) Quota() *quota.Pool      { return o.quota }
func (o *Orchestrator) Store() store.Store      { return o.store }

// ResolveDecision 是人工收口的两种确定结论。
type ResolveDecision string

const (
	// ResolveKeep 人工确认该实例应继续存在：落定到指定正常状态，配额继续占用。
	ResolveKeep ResolveDecision = "keep"
	// ResolveDestroy 人工确认该实例应被清理：彻底 Undefine、释放配额、落为墓碑。
	ResolveDestroy ResolveDecision = "destroy"
)

// ResolveRequest 是一次人工收口请求。
type ResolveRequest struct {
	ID       string
	Decision ResolveDecision
	// Target 仅在 Decision=keep 时使用，指定落定到的正常状态：
	// StateRunning / StateStopped / StateDefined。
	Target domain.State
	Note   string
}

// Resolve 对处于 PendingManual 的实例执行显式人工收口。
// 它是唯一能把实例带出挂起状态的入口；收口后实例记录与配额占用同步更新。
func (o *Orchestrator) Resolve(ctx context.Context, req ResolveRequest) (*domain.Instance, error) {
	if strings.TrimSpace(req.ID) == "" {
		return nil, domain.NewInvalidArgumentError("resolve", "empty instance id")
	}
	switch req.Decision {
	case ResolveKeep:
		// 允许落定到任何一个"仍持有资源"的正常状态。
		if req.Target != domain.StateRunning &&
			req.Target != domain.StateStopped &&
			req.Target != domain.StateDefined {
			return nil, domain.NewInvalidArgumentError("resolve",
				"keep requires target Running/Stopped/Defined, got "+string(req.Target))
		}
	case ResolveDestroy:
	default:
		return nil, domain.NewInvalidArgumentError("resolve",
			"decision must be keep or destroy, got "+string(req.Decision))
	}

	unlock := o.lockInstance(req.ID)
	defer unlock()

	inst, err := o.store.Get(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	if !inst.IsPending() {
		if inst.State == domain.StateDestroyed {
			return nil, domain.NewIllegalTransitionError("resolve", req.ID, inst.State, inst.State,
				"resolve only applies to PendingManual instances; this one is already Destroyed")
		}
		return nil, domain.NewIllegalTransitionError("resolve", req.ID, inst.State, inst.State,
			"resolve only applies to PendingManual instances; instance is not pending")
	}
	pendingReason := inst.Pending.Reason
	pendingRule := inst.Pending.Rule

	switch req.Decision {
	case ResolveDestroy:
		return o.resolveDestroy(ctx, inst, pendingRule, pendingReason, req.Note)
	case ResolveKeep:
		return o.resolveKeep(ctx, inst, req.Target, pendingRule, pendingReason, req.Note)
	}
	return nil, domain.NewInvalidArgumentError("resolve", "unreachable")
}

// resolveDestroy 人工确认清理：幂等 Undefine 后端残留 -> 释放配额 -> 落墓碑并清空挂起结论。
func (o *Orchestrator) resolveDestroy(ctx context.Context, inst *domain.Instance, rule, reason, note string) (*domain.Instance, error) {
	id := inst.ID
	if err := o.backend.Undefine(ctx, id); err != nil {
		o.log("Resolve destroy id=%s backend undefine failed: %v (stays PendingManual)", id, err)
		return nil, domain.NewBackendError("resolve", id, err)
	}
	if _, err := o.quota.Release(id); err != nil {
		// 后端已清但台账缺记录：明确的占用漂移，保持挂起并报错，绝不静默。
		o.log("Resolve destroy id=%s CRITICAL quota release failed: %v (stays PendingManual)", id, err)
		return nil, domain.NewInconsistentStateError("resolve", id,
			"backend removed but quota reservation missing: "+err.Error())
	}

	inst.State = domain.StateDestroyed
	inst.UpdatedAt = time.Now().UTC()
	inst.Pending = nil
	inst.LastEvent = "resolved: human-confirmed destroy (was pending rule=" + rule + ")"
	if note != "" {
		inst.LastEvent += " note=" + note
	}
	if err := o.store.Put(ctx, inst); err != nil {
		// 墓碑落盘失败：已释放的配额无法回滚为占用，明确报错而不是装作成功。
		return nil, fmt.Errorf("resolve destroy %s: persist tombstone: %w", id, err)
	}
	snap := o.quota.Snapshot()
	o.log("Resolve id=%s -> Destroyed (human-confirmed; rule=%s reason=%s); used cpu=%d mem=%d disk=%d reservations=%d",
		id, rule, reason, snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB, len(snap.Reservations))
	return inst.Clone(), nil
}

// resolveKeep 人工确认保留：确保承载存在并对齐到目标状态 -> 落定正常状态、
// 清空挂起结论，配额继续占用。
func (o *Orchestrator) resolveKeep(ctx context.Context, inst *domain.Instance, target domain.State, rule, reason, note string) (*domain.Instance, error) {
	id := inst.ID
	// 人工确认它应继续存在：若承载定义已缺失（如 running-missing），按规格重建定义，
	// 否则会落成"正常状态 + 后端 absent"，下轮对账又被当成半完成销毁而收掉。
	info, ierr := o.backend.Inspect(ctx, id)
	if ierr != nil {
		return nil, domain.NewBackendError("resolve", id, ierr)
	}
	if info.Status == backend.StatusAbsent {
		if err := o.backend.Define(ctx, id, inst.Spec); err != nil {
			o.log("Resolve keep id=%s backend define failed: %v (stays PendingManual)", id, err)
			return nil, domain.NewBackendError("resolve", id, err)
		}
	}
	// 把实际承载对齐到人工指定的目标。
	switch target {
	case domain.StateRunning:
		if err := o.backend.Start(ctx, id); err != nil {
			o.log("Resolve keep id=%s backend start failed: %v (stays PendingManual)", id, err)
			return nil, domain.NewBackendError("resolve", id, err)
		}
	case domain.StateStopped:
		if err := o.backend.Stop(ctx, id); err != nil {
			o.log("Resolve keep id=%s backend stop failed: %v (stays PendingManual)", id, err)
			return nil, domain.NewBackendError("resolve", id, err)
		}
	case domain.StateDefined:
		// Defined 语义为"已定义未运行"：确保 guest 不在跑，但不删除承载定义。
		if err := o.backend.Stop(ctx, id); err != nil {
			o.log("Resolve keep id=%s backend stop failed: %v (stays PendingManual)", id, err)
			return nil, domain.NewBackendError("resolve", id, err)
		}
	}

	inst.State = target
	inst.UpdatedAt = time.Now().UTC()
	inst.Pending = nil
	inst.LastEvent = "resolved: human-confirmed keep -> " + string(target) + " (was pending rule=" + rule + ")"
	if note != "" {
		inst.LastEvent += " note=" + note
	}
	if err := o.store.Put(ctx, inst); err != nil {
		return nil, fmt.Errorf("resolve keep %s: persist: %w", id, err)
	}
	snap := o.quota.Snapshot()
	o.log("Resolve id=%s -> %s (human-confirmed; rule=%s reason=%s); quota retained reservations=%d used cpu=%d",
		id, target, rule, reason, len(snap.Reservations), snap.Used.CPUMillis)
	return inst.Clone(), nil
}

// Reconcile 在重启后执行对账：以存储记录与后端事实对账，并重建配额台账。
// 它持有全局写屏障：进行期间任何实例的变更请求都会等待，避免与自动收敛互相覆盖。
func (o *Orchestrator) Reconcile(ctx context.Context, logf func(string, ...any)) (*reconcile.Report, error) {
	o.gate.Lock()
	defer o.gate.Unlock()
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

// RestoreFromStore 依据持久化记录重建内存配额台账。
// New 已在构造时自动执行本恢复；保留该方法以便显式再同步（重复执行幂等安全）。
func (o *Orchestrator) RestoreFromStore(ctx context.Context) error {
	return o.rebuildQuotaFromStore(ctx)
}
