// Package model 定义虚拟机编排系统的核心领域模型：
// 实例状态机、宿主资源描述、实例记录以及可区分的错误类型。
package model

import "time"

// State 表示实例的生命周期状态。
type State string

const (
	// StatePending：创建中，后端 Define 的结果尚未确认。
	StatePending State = "Pending"
	// StateStopped：已定义未运行。
	StateStopped State = "Stopped"
	// StateStarting：Start 已发起、运行事实尚未确认。
	StateStarting State = "Starting"
	// StateRunning：运行中。
	StateRunning State = "Running"
	// StateStopping：Stop 已发起、停止事实尚未确认。
	StateStopping State = "Stopping"
	// StateDestroying：Destroy 已发起、删除事实尚未确认。
	StateDestroying State = "Destroying"
	// StateFailed：状态不一致，需要人工介入（见 Reason）。
	StateFailed State = "Failed"
	// StateDestroyed：已销毁，终态。
	StateDestroyed State = "Destroyed"
)

// Resources 描述实例对宿主资源的申请量。
type Resources struct {
	CPUMillis int `json:"cpuMillis"` // 千分之一核
	MemoryMiB int `json:"memoryMiB"` // MiB
	DiskMiB   int `json:"diskMiB"`   // MiB
}

// Add 返回 r + other。
func (r Resources) Add(other Resources) Resources {
	return Resources{
		CPUMillis: r.CPUMillis + other.CPUMillis,
		MemoryMiB: r.MemoryMiB + other.MemoryMiB,
		DiskMiB:   r.DiskMiB + other.DiskMiB,
	}
}

// Equal 判断两份资源描述是否完全相等。
func (r Resources) Equal(other Resources) bool {
	return r == other
}

// Sub 返回 r - other（允许负数，由调用方做约束校验）。
func (r Resources) Sub(other Resources) Resources {
	return Resources{
		CPUMillis: r.CPUMillis - other.CPUMillis,
		MemoryMiB: r.MemoryMiB - other.MemoryMiB,
		DiskMiB:   r.DiskMiB - other.DiskMiB,
	}
}

// Instance 是一个虚拟机实例的持久化记录。
type Instance struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Spec       Resources `json:"spec"`
	State      State     `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	PID        int       `json:"pid,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	Generation int64     `json:"generation"`
}

// Clone 返回实例的深拷贝。
func (in *Instance) Clone() *Instance {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

// transitions 定义显式允许的状态迁移（包含中间态自迁移与对账兜底迁移）。
var transitions = map[State]map[State]bool{
	StatePending: {
		StatePending: true, StateStopped: true, StateFailed: true,
		StateDestroying: true, StateDestroyed: true,
	},
	StateStopped: {
		StateStopped: true, StateStarting: true, StateFailed: true,
		StateDestroying: true, StateDestroyed: true,
	},
	StateStarting: {
		StateStarting: true, StateRunning: true, StateStopped: true, StateFailed: true,
	},
	StateRunning: {
		StateRunning: true, StateStopping: true, StateStopped: true, StateFailed: true,
	},
	StateStopping: {
		StateStopping: true, StateStopped: true, StateRunning: true, StateFailed: true,
	},
	StateDestroying: {
		StateDestroying: true, StateDestroyed: true, StateRunning: true, StateFailed: true,
	},
	StateFailed: {
		StateFailed: true, StateDestroying: true,
	},
	StateDestroyed: {
		StateDestroyed: true, StateFailed: true, // 仅对账：终态与后端残留矛盾时暴露为人工介入
	},
}

// CanTransition 判断从当前状态迁移到目标状态是否合法。
func CanTransition(from, to State) bool {
	return transitions[from][to]
}

// OpType 标识一次操作的种类，用于日志与对账报告。
type OpType string

const (
	OpCreate  OpType = "Create"
	OpStart   OpType = "Start"
	OpStop    OpType = "Stop"
	OpDestroy OpType = "Destroy"
)
