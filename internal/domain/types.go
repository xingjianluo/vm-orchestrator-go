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
	// StateDestroyed 终态：后端已清除、配额已释放，仅保留墓碑记录。
	StateDestroyed State = "Destroyed"
	// StatePendingManual 待人工处理：重启对账发现记录与承载事实无法唯一判定，
	// 系统把结论（原因/规则/当时事实）持久化挂起。该状态下：
	//   - 一切常规生命周期动作（create/start/stop/destroy）一律拒绝，
	//     错误类别为 pending_manual，与其他原因可程序化区分；
	//   - 对账不会再自动收敛或覆盖它，也不会清理其后端承载残留；
	//   - 只有显式人工收口 Resolve（keep/destroy）才能离开该状态；
	//   - 配额口径：挂起期间继续按规格计入占用（冻结保留），收口销毁时才释放。
	StatePendingManual State = "PendingManual"
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

// PendingInfo 记录"待人工处理"的持久化结论。
// 只要 Instance.Pending 非 nil，该实例即处于挂起等待人工收口状态。
type PendingInfo struct {
	// Reason 是人类可读的判定原因（必须随记录持久化并在查询时带回）。
	Reason string `json:"reason"`
	// Rule 是触发挂起的确定性判定规则码（如 running-missing-backend）。
	Rule string `json:"rule"`
	// RecordState 是挂起那一刻记录上的状态（审计用，便于人工判断）。
	RecordState string `json:"record_state"`
	// ActualState 是挂起那一刻观测到的后端承载事实（absent/stopped/running）。
	ActualState string `json:"actual_state"`
	// DetectedAt 是判定时间。
	DetectedAt time.Time `json:"detected_at"`
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
	// Pending 非 nil 表示该实例处于待人工处理状态（State 也必为 PendingManual）。
	// 收口（Resolve）完成后必须清空，使记录回到正常状态机或终态。
	Pending *PendingInfo `json:"pending,omitempty"`
}

// Clone 返回深拷贝，避免调用方在锁外修改内部记录。
func (i *Instance) Clone() *Instance {
	if i == nil {
		return nil
	}
	cp := *i
	if i.Pending != nil {
		p := *i.Pending
		cp.Pending = &p
	}
	return &cp
}

// IsPending 报告实例是否处于待人工处理状态。
func (i *Instance) IsPending() bool {
	return i != nil && i.State == StatePendingManual && i.Pending != nil
}

// Valid 判断状态值是否合法。
func (s State) Valid() bool {
	switch s {
	case StateDefined, StateRunning, StateStopped, StateDestroyed, StatePendingManual:
		return true
	default:
		return false
	}
}

// transitionReason 返回从当前状态迁移到 to 是否合法；
// from == to 时由编排层解释为幂等空操作，这里判为非法以区分两条路径。
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
	case StateDestroyed:
		// 终态：不允许再迁移。
	}
	return "no transition rule from " + string(s) + " to " + string(to)
}

// CanTransition 判断迁移是否合法（不含同态幂等）。
func (s State) CanTransition(to State) bool {
	return s.transitionReason(to) == ""
}
