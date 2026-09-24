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
	case StateDefined, StateRunning, StateStopped, StateDestroyed:
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
