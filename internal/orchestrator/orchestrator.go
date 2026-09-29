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
// 并发模型（不同实例并行、同一实例串行）：
//   - keyLocks 为每个实例 id 提供一把互斥锁：同一实例上的 create/start/stop/destroy/
//     resolve 严格按到达顺序串行，读记录→判定迁移→调后端→持久化是一个不可分割的临界区，
//     因此同实例并发请求绝不会互相覆盖；
//   - 不同实例持有各自的锁，互不排队（后端/存储/配额池自身也支持并发）；
//   - adminMu 是"全表"操作（RestoreFromStore/Reconcile，需重建台账或扫描全部记录）
//     与"单实例"变更之间的屏障：变更持 RLock（彼此可并行），全表操作持 Lock（独占）。
//     会退化成全局排队的唯一场景：进程启动后的 restore / 显式 reconcile 执行期间，
//     因为它们要重算全量台账，必须等在途变更全部结束并阻塞新变更。
//   - 锁序固定为 adminMu -> keyLock -> 下层（store/backend/quota）锁，无环路。
type Orchestrator struct {
	adminMu  sync.RWMutex
	keyLocks *keyLocks
	store    store.Store
	backend  backend.Handle
	quota    *quota.Pool
	logf     func(string, ...any)
}

// keyLocks 是按实例 id 取锁的锁表。
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newKeyLocks() *keyLocks {
	return &keyLocks{locks: make(map[string]*sync.Mutex)}
}

// lock 返回 id 对应的实例锁（不自动加锁，调用方自行 Lock/Unlock）。
// 锁表条目在生命周期内保留：id 集合有界（每条记录一个），避免加锁计数的复杂度。
func (k *keyLocks) lock(id string) *sync.Mutex {
	k.mu.Lock()
	defer k.mu.Unlock()
	l, ok := k.locks[id]
	if !ok {
		l = &sync.Mutex{}
		k.locks[id] = l
	}
	return l
}

func New(st store.Store, h backend.Handle, capacity domain.Resources, logf func(string, ...any)) *Orchestrator {
	if logf == nil {
		var nop nopLogger
		logf = nop.Logf
	}
	return &Orchestrator{
		keyLocks: newKeyLocks(),
		store:    st,
		backend:  h,
		quota:    quota.NewPool(capacity),
		logf:     logf,
	}
}

func (o *Orchestrator) log(format string, args ...any) {
	o.logf("[orchestrator] "+format, args...)
}

// beginInstanceOp 进入单实例变更临界区：先取读侧管理屏障（允许与其他实例变更并行，
// 但与 restore/reconcile 互斥），再取该实例自己的串行锁。返回退出函数。
func (o *Orchestrator) beginInstanceOp(id string) func() {
	o.adminMu.RLock()
	l := o.keyLocks.lock(id)
	l.Lock()
	return func() {
		l.Unlock()
		o.adminMu.RUnlock()
	}
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

	end := o.beginInstanceOp(id)
	defer end()

	if existing, err := o.store.Get(ctx, id); err == nil {
		if existing.State == domain.StateManualReview {
			return nil, domain.NewManualReviewPendingError("create", id, existing.Review)
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
	end := o.beginInstanceOp(id)
	defer end()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State == domain.StateManualReview {
		return nil, domain.NewManualReviewPendingError("start", id, inst.Review)
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
	end := o.beginInstanceOp(id)
	defer end()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State == domain.StateManualReview {
		return nil, domain.NewManualReviewPendingError("stop", id, inst.Review)
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
// ManualReview 实例拒绝常规销毁：必须由人工 ResolveDiscard 显式收口。
func (o *Orchestrator) Destroy(ctx context.Context, id string) (*domain.Instance, error) {
	end := o.beginInstanceOp(id)
	defer end()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State == domain.StateManualReview {
		return nil, domain.NewManualReviewPendingError("destroy", id, inst.Review)
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
	// 读操作走存储快照（单条原子写），并取读侧屏障以免与 restore/reconcile 的全表动作交错；
	// 不与其他实例的变更互斥，同一实例也只会读到原子的旧/新版本，不会读到半写。
	o.adminMu.RLock()
	defer o.adminMu.RUnlock()
	return o.store.Get(ctx, id)
}

func (o *Orchestrator) List(ctx context.Context) ([]*domain.Instance, error) {
	o.adminMu.RLock()
	defer o.adminMu.RUnlock()
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

// ResourceSnapshot 返回当前配额台账快照。台账是内存权威，与持久化记录的一致性由
// 进程启动时的 RestoreFromStore（新进程必做）与每次对账后的重建保证。
// 当宿主被重建出超容量占用时，快照上 OverCommit=true、Available 可能为负：
// 这是可读、自洽的对账结论（见 reconcile.Report.CapacityExceeded），不是内部错误。
func (o *Orchestrator) ResourceSnapshot() quota.Snapshot {
	return o.quota.Snapshot()
}

// Backend 与 Quota/Store 仅供对账器使用。
func (o *Orchestrator) Backend() backend.Handle { return o.backend }
func (o *Orchestrator) Quota() *quota.Pool      { return o.quota }
func (o *Orchestrator) Store() store.Store      { return o.store }

// ResolveDecision 是人工对"待人工处理"实例的收口结论。
type ResolveDecision string

const (
	// ResolveAdopt 确认实例应继续存在：恢复为一个确定的正常状态并继续占用资源。
	ResolveAdopt ResolveDecision = "adopt"
	// ResolveDiscard 确认实例应被清理：彻底收掉（Undefine）并释放资源，落为 Destroyed 墓碑。
	ResolveDiscard ResolveDecision = "discard"
)

// ManualReviewList 返回当前处于 ManualReview 冻结态的实例（含判定原因），供运维收口时浏览。
func (o *Orchestrator) ManualReviewList(ctx context.Context) ([]*domain.Instance, error) {
	o.adminMu.RLock()
	defer o.adminMu.RUnlock()
	loaded, err := o.store.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []*domain.Instance
	for _, in := range loaded {
		if in.State == domain.StateManualReview {
			out = append(out, in.Clone())
		}
	}
	return out, nil
}

// Resolve 是"待人工处理"实例的唯一收口入口。
//   - decision=adopt：确认实例应继续存在，落定到 target（Defined/Running/Stopped）。
//     收口前保证承载与目标态一致：target=Running 时确保后端已定义并启动；
//     target=Defined/Stopped 时确保后端已定义且不运行。ManualReview 期间占用一直保留，
//     因此这里不发生新的配额预留。只有"无记录且后端读不出规格"的孤儿需要 operator 用
//     providedSpec 补报规格（此时才补登记台账）。
//   - decision=discard：确认实例应被清理：幂等 Undefine 后端残留、释放占用、落为 Destroyed。
//
// note 记录人工收口说明（写入 LastEvent，便于审计）。
func (o *Orchestrator) Resolve(ctx context.Context, id string, decision ResolveDecision, target domain.State, providedSpec *domain.Spec, note string) (*domain.Instance, error) {
	end := o.beginInstanceOp(id)
	defer end()

	inst, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if inst.State != domain.StateManualReview {
		return nil, domain.NewIllegalTransitionError("resolve", id, inst.State, target,
			"resolve only applies to ManualReview instances")
	}

	switch decision {
	case ResolveAdopt:
		return o.resolveAdopt(ctx, inst, target, providedSpec, note)
	case ResolveDiscard:
		return o.resolveDiscard(ctx, inst, note)
	default:
		return nil, domain.NewInvalidArgumentError("resolve", "decision must be adopt or discard, got: "+string(decision))
	}
}

// resolveAdopt 把待人工处理实例落定为一个确定的正常状态，资源继续占用。
func (o *Orchestrator) resolveAdopt(ctx context.Context, inst *domain.Instance, target domain.State, providedSpec *domain.Spec, note string) (*domain.Instance, error) {
	if target != domain.StateDefined && target != domain.StateRunning && target != domain.StateStopped {
		return nil, domain.NewInvalidArgumentError("resolve.adopt",
			"target must be one of Defined/Running/Stopped, got: "+string(target))
	}

	// 规格未知的孤儿（无记录且后端无 spec.json）：必须由人工补报规格，此时才把占用登记进台账。
	needRegister := inst.Review != nil && !inst.Review.SpecKnown
	if needRegister {
		if providedSpec == nil || providedSpec.Resources.NonPositive() {
			return nil, domain.NewInvalidArgumentError("resolve.adopt",
				"orphan residue has no readable spec; provide resource amounts to claim")
		}
		inst.Spec = *providedSpec
		if inst.Backend == "" {
			inst.Backend = o.backend.Name()
		}
	}

	// 收敛承载事实到目标态：仅在后端对象缺失时 Define（Define 本身不保证幂等）。
	info, ierr := o.backend.Inspect(ctx, inst.ID)
	if ierr != nil {
		return nil, domain.NewBackendError("resolve.adopt", inst.ID, ierr)
	}
	if info.Status == backend.StatusAbsent {
		if err := o.backend.Define(ctx, inst.ID, inst.Spec); err != nil {
			o.log("Resolve adopt id=%s backend define failed: %v (held ManualReview unchanged)", inst.ID, err)
			return nil, domain.NewBackendError("resolve.adopt", inst.ID, err)
		}
	}
	switch target {
	case domain.StateRunning:
		if err := o.backend.Start(ctx, inst.ID); err != nil {
			o.log("Resolve adopt id=%s backend start failed: %v (held ManualReview unchanged)", inst.ID, err)
			return nil, domain.NewBackendError("resolve.adopt", inst.ID, err)
		}
	case domain.StateDefined, domain.StateStopped:
		if err := o.backend.Stop(ctx, inst.ID); err != nil {
			o.log("Resolve adopt id=%s backend stop failed: %v (held ManualReview unchanged)", inst.ID, err)
			return nil, domain.NewBackendError("resolve.adopt", inst.ID, err)
		}
	}

	if needRegister {
		// 补登记占用（此前无法计入台账）。这是对"已存在事实"的人工裁决登记，
		// 不做容量准入（即便宿主已 over-commit 也要让收口落地）；先登记再落盘，
		// 失败则整体不落定，保持 ManualReview。
		if err := o.quota.Register("resolve.adopt", inst.ID, inst.Spec.Resources); err != nil {
			return nil, err
		}
	}

	from := inst.State
	reason := ""
	if inst.Review != nil {
		reason = string(inst.Review.Reason)
	}
	inst.State = target
	inst.Review = nil
	inst.UpdatedAt = time.Now().UTC()
	inst.LastEvent = fmt.Sprintf("manual-resolved adopt->%s (was %s%s)%s",
		target, from, reasonSuffix(reason), noteSuffix(note))
	if err := o.store.Put(ctx, inst); err != nil {
		return nil, fmt.Errorf("resolve adopt %s: persist: %w", inst.ID, err)
	}
	o.log("Resolve id=%s adopt -> %s（人工确认继续存在，占用保留%s）", inst.ID, target, registerSuffix(needRegister))
	return inst.Clone(), nil
}

// resolveDiscard 确认清理：幂等 Undefine 残留、释放占用、落为 Destroyed 墓碑。
func (o *Orchestrator) resolveDiscard(ctx context.Context, inst *domain.Instance, note string) (*domain.Instance, error) {
	id := inst.ID
	if err := o.backend.Undefine(ctx, id); err != nil {
		o.log("Resolve discard id=%s backend undefine failed: %v (held ManualReview unchanged)", id, err)
		return nil, domain.NewBackendError("resolve.discard", id, err)
	}

	// 规格可知的挂起实例在台账里一直占着，需要释放；规格未知的孤儿从未登记，Release 会报漂移。
	released := true
	if _, err := o.quota.Release(id); err != nil {
		if inst.Review == nil || inst.Review.SpecKnown {
			o.log("Resolve discard id=%s CRITICAL: quota release failed: %v", id, err)
			return nil, domain.NewInconsistentStateError("resolve.discard", id,
				"backend removed but quota release failed: "+err.Error())
		}
		// 规格未知的孤儿本就未计入台账，无预留可释放属于预期。
		released = false
	}

	reason := ""
	if inst.Review != nil {
		reason = string(inst.Review.Reason)
	}
	inst.State = domain.StateDestroyed
	inst.Review = nil
	inst.UpdatedAt = time.Now().UTC()
	inst.LastEvent = fmt.Sprintf("manual-resolved discard->Destroyed (was %s%s)%s",
		domain.StateManualReview, reasonSuffix(reason), noteSuffix(note))
	if err := o.store.Put(ctx, inst); err != nil {
		return nil, fmt.Errorf("resolve discard %s: persist tombstone: %w", id, err)
	}
	o.log("Resolve id=%s discard -> Destroyed（人工确认清理，quotaReleased=%t）", id, released)
	return inst.Clone(), nil
}

func reasonSuffix(reason string) string {
	if reason == "" {
		return ""
	}
	return ", reason=" + reason
}

func noteSuffix(note string) string {
	if strings.TrimSpace(note) == "" {
		return ""
	}
	return " note=" + note
}

func registerSuffix(registered bool) string {
	if registered {
		return "，孤儿补报规格已补登记"
	}
	return ""
}

// Reconcile 在重启后执行对账：以存储记录与后端事实对账，并重建配额台账。
// 这是全表动作：持有独占管理屏障，期间任何实例变更都会等待（排队只发生在对账窗口）。
func (o *Orchestrator) Reconcile(ctx context.Context, logf func(string, ...any)) (*reconcile.Report, error) {
	o.adminMu.Lock()
	defer o.adminMu.Unlock()
	return reconcile.Run(ctx, o.store, o.backend, func(alive map[string]domain.Resources) (quota.Snapshot, string, error) {
		snap, err := o.quota.Rebuild(alive)
		if err != nil {
			return quota.Snapshot{}, "", err
		}
		detail := fmt.Sprintf("capacity={cpu:%d,mem:%d,disk:%d} used={cpu:%d,mem:%d,disk:%d} reservations=%d",
			snap.Capacity.CPUMillis, snap.Capacity.MemoryMB, snap.Capacity.DiskMB,
			snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB,
			len(snap.Reservations))
		if snap.OverCommit {
			detail += " OVER-COMMIT exceeded=" + fmt.Sprintf("%v", snap.Exceeded)
		}
		return snap, detail, nil
	}, logf)
}

// RestoreFromStore 在进程启动时根据持久化记录重建内存配额台账。
// 它不触碰后端事实（那是 reconcile 的职责），只保证台账与记录一致。
//
// 台账始终按权威集合如实重建：即使重建占用超过宿主容量（例如对账刚落盘的超大
// 残留），本方法也不报错——超容量通过 ResourceSnapshot().OverCommit 暴露为
// 告警/准入条件，绝不阻断查询或人工收口。只有记录本身含非法（非正）规格时才报错。
//
// 这是"新进程看到的占用与事实一致"的保证：任何新进程在查询占用前必须先执行本方法
// （CLI 每个子命令默认执行；作为库嵌入时同样应在启动时调用一次）。
// 计入口径：所有持有资源的记录（Defined/Running/Stopped/ManualReview）；Destroyed 除外；
// ManualReview 且规格不可读的孤儿无法计入（其记录上 review.spec_known=false）。
func (o *Orchestrator) RestoreFromStore(ctx context.Context) error {
	o.adminMu.Lock()
	defer o.adminMu.Unlock()

	loaded, err := o.store.List(ctx)
	if err != nil {
		return fmt.Errorf("restore: list: %w", err)
	}
	want := make(map[string]domain.Resources)
	for _, in := range loaded {
		if !in.State.HoldsResources() {
			continue
		}
		if in.State == domain.StateManualReview && (in.Review == nil || !in.Review.SpecKnown) {
			continue
		}
		want[in.ID] = in.Spec.Resources
	}
	snap, err := o.quota.Rebuild(want)
	if err != nil {
		return domain.NewInconsistentStateError("restore", "", err.Error())
	}
	if snap.OverCommit {
		// 只告警、不失败：超容量是对账结论，不能一票否决本进程的任何命令。
		o.log("Restore: rebuilt ledger OVER-COMMIT exceeded=%v used cpu=%d mem=%d disk=%d capacity cpu=%d mem=%d disk=%d（人工收口前拒绝新增准入）",
			snap.Exceeded, snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB,
			snap.Capacity.CPUMillis, snap.Capacity.MemoryMB, snap.Capacity.DiskMB)
	} else {
		o.log("Restore: rebuilt quota ledger from %d resource-holding records; used cpu=%d mem=%d disk=%d",
			len(want), snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB)
	}
	return nil
}
