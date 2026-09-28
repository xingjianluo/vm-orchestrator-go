package domain

import "time"

// State 是实例生命周期中的确定状态。
type State string

const (
	// StateDefined 已创建：配额已预留、后端已定义，但未运行。
	StateDefined State = "Defined"
	// StateRunning 运行中。
	StateRunning State = "Running"
	// StateStopped 已停止（正常停止或崩溃后对账收敛）。
	StateStopped State = "Stopped"
	// StateManualReview 待人工处理：对账发现记录与承载事实无法自动判定，
	// 结论已持久化，拒绝一切常规变更，只能经人工显式收口（adopt/discard）。
	StateManualReview State = "ManualReview"
	// StateDestroyed 终态：后端已清除、配额已释放，仅保留墓碑记录。
	StateDestroyed State = "Destroyed"
)

// Resources 描述一份资源量（CPU 用毫核表示，内存与磁盘用 MB）。
type Resources struct {
	CPUMillis int64 `json:"cpu_millis"`
	MemoryMB  int64 `json:"memory_mb"`
	DiskMB    int64 `json:"disk_mb"`
}

// Add 返回 r+x（不可变累加）。
func (r Resources) Add(x Resources) Resources {
	return Resources{
		CPUMillis: r.CPUMillis + x.CPUMillis,
		MemoryMB:  r.MemoryMB + x.MemoryMB,
		DiskMB:    r.DiskMB + x.DiskMB,
	}
}

// Sub 返回 r-x，调用方需保证各项不小于 x。
func (r Resources) Sub(x Resources) Resources {
	return Resources{
		CPUMillis: r.CPUMillis - x.CPUMillis,
		MemoryMB:  r.MemoryMB - x.MemoryMB,
		DiskMB:    r.DiskMB - x.DiskMB,
	}
}

// AnyExceeds 报告 r 的任一项是否超过 cap。
func (r Resources) AnyExceeds(cap Resources) bool {
	return r.CPUMillis > cap.CPUMillis ||
		r.MemoryMB > cap.MemoryMB ||
		r.DiskMB > cap.DiskMB
}

// NonPositive 报告资源项是否存在非法的非正值。
func (r Resources) NonPositive() bool {
	return r.CPUMillis <= 0 || r.MemoryMB <= 0 || r.DiskMB <= 0
}

// Spec 是实例创建时的不可变规格。
type Spec struct {
	Resources Resources `json:"resources"`
	Image     string    `json:"image"`
}

// ReviewReason 是对账判定为"待人工处理"的原因类别。
type ReviewReason string

const (
	// ReviewRunningMissingBackend 记录 Running 但后端承载整体消失，无法判定是否该重建。
	ReviewRunningMissingBackend ReviewReason = "running_missing_backend"
	// ReviewOrphanBackend 后端独有对象（无记录），归属不明，残留保留待裁决。
	ReviewOrphanBackend ReviewReason = "orphan_backend"
)

// Review 是"待人工处理"结论的持久化留痕（随实例记录一起落盘，跨进程/宿主重启可见）。
type Review struct {
	// Reason 是判定原因类别（可程序化分支）。
	Reason ReviewReason `json:"reason"`
	// Detail 是判定时记录与后端事实的人类可读说明。
	Detail string `json:"detail"`
	// RecordState 是判定发生时的记录状态（ManualReview 之前的状态；孤儿为 "(none)"）。
	RecordState string `json:"record_state"`
	// ActualState 是判定发生时的后端实际承载状态。
	ActualState string `json:"actual_state"`
	// RaisedAt 是判定时间。
	RaisedAt time.Time `json:"raised_at"`
	// SpecKnown 表示规格可知（由记录或后端 spec.json 获得），此时占用按规格计入台账；
	// false 仅出现在无记录且后端无法读出规格的孤儿上，其占用无法计入台账，
	// 需人工在收口时补报规格。
	SpecKnown bool `json:"spec_known"`
}

// Instance 是持久化的实例记录。
type Instance struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Spec      Spec      `json:"spec"`
	State     State     `json:"state"`
	Backend   string    `json:"backend"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	LastEvent string    `json:"last_event"`
	// Review 非空当且仅当 State == ManualReview，携带判定原因与现场信息。
	Review *Review `json:"review,omitempty"`
}

// Clone 返回深拷贝，避免调用方在锁外修改内部记录。
func (i *Instance) Clone() *Instance {
	if i == nil {
		return nil
	}
	cp := *i
	return &cp
}

// Valid 判断状态值是否合法。
func (s State) Valid() bool {
	switch s {
	case StateDefined, StateRunning, StateStopped, StateManualReview, StateDestroyed:
		return true
	default:
		return false
	}
}

// HoldsResources 判断处于该状态的记录是否应计入宿主资源占用（重启台账重建的权威口径）。
// 口径：Defined/Running/Stopped 持有规格预留；ManualReview 也继续计入占用——
// 其承载归属未裁决，资源不能被再次分配（防止超卖），人工收口到 Destroyed 后才释放；
// Destroyed 墓碑不占资源。
func (s State) HoldsResources() bool {
	switch s {
	case StateDefined, StateRunning, StateStopped, StateManualReview:
		return true
	default:
		return false
	}
}

// transitionReason 返回从当前状态迁移到 to 是否合法；
// from == to 时由编排层解释为幂等空操作，这里判为非法以区分两条路径。
// ManualReview 不参与任何常规状态机迁移（只能由编排层人工收口动作落定）。
func (s State) transitionReason(to State) string {
	switch s {
	case StateDefined:
		if to == StateRunning || to == StateDestroyed {
			return ""
		}
	case StateRunning:
		if to == StateStopped {
			return ""
		}
	case StateStopped:
		if to == StateRunning || to == StateDestroyed {
			return ""
		}
	case StateManualReview:
		// 冻结态：start/stop/destroy 一律拒绝，收口只能通过专用动作。
	case StateDestroyed:
		// 终态：不允许再迁移。
	}
	return "no transition rule from " + string(s) + " to " + string(to)
}

// CanTransition 判断迁移是否合法（不含同态幂等）。
func (s State) CanTransition(to State) bool {
	return s.transitionReason(to) == ""
}
