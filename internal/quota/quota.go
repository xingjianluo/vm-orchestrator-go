package quota

import (
	"fmt"
	"sync"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// Snapshot 是某一时刻配额池的守恒快照：available = capacity - used。
type Snapshot struct {
	Capacity     domain.Resources            `json:"capacity"`
	Used         domain.Resources            `json:"used"`
	Available    domain.Resources            `json:"available"`
	Reservations map[string]domain.Resources `json:"reservations"`
}

// Pool 是宿主资源配额池。
// 不变量：
//  1. used == 所有 reservations 之和；
//  2. used 各维度永不超过 capacity；
//  3. Reserve 成功之前不产生任何后端副作用（由编排层保证调用顺序）；
//  4. Release 必须对应一次 Reserve，重复释放返回错误以暴露占用漂移。
type Pool struct {
	mu           sync.Mutex
	capacity     domain.Resources
	used         domain.Resources
	reservations map[string]domain.Resources
}

func NewPool(capacity domain.Resources) *Pool {
	return &Pool{
		capacity:     capacity,
		reservations: make(map[string]domain.Resources),
	}
}

// Reserve 为实例 id 预留资源；容量不足或重复预留时返回带类别的错误，状态不被修改。
func (p *Pool) Reserve(op string, id string, r domain.Resources) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if r.NonPositive() {
		return domain.NewInvalidArgumentError(op,
			fmt.Sprintf("resource amounts must be positive: cpu=%d mem=%d disk=%d",
				r.CPUMillis, r.MemoryMB, r.DiskMB))
	}
	if _, exists := p.reservations[id]; exists {
		return domain.NewAlreadyExistsError(op+"/quota", id)
	}
	if name, avail, req, short := p.shortDimension(r); short {
		return domain.NewInsufficientResourceError(op, id, name, req, avail)
	}

	p.reservations[id] = r
	p.used = p.used.Add(r)
	return nil
}

// Release 释放实例 id 的预留；无台账记录时返回错误（重复释放 = 占用漂移信号）。
func (p *Pool) Release(id string) (domain.Resources, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.reservations[id]
	if !ok {
		return domain.Resources{}, fmt.Errorf("quota: release without reservation for %q (double release or drift)", id)
	}
	delete(p.reservations, id)
	p.used = p.used.Sub(r)
	return r, nil
}

// HasReservation 报告实例是否仍持有预留。
func (p *Pool) HasReservation(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.reservations[id]
	return ok
}

// Snapshot 返回当前配额的不可变拷贝。
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	res := make(map[string]domain.Resources, len(p.reservations))
	for k, v := range p.reservations {
		res[k] = v
	}
	return Snapshot{
		Capacity:     p.capacity,
		Used:         p.used,
		Available:    p.capacity.Sub(p.used),
		Reservations: res,
	}
}

// Rebuild 在重启/对账后依据权威实例集合重建台账。
// 若重建结果超过宿主容量，返回错误且不修改当前台账（由对账层标记隔离）。
func (p *Pool) Rebuild(want map[string]domain.Resources) (Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := domain.Resources{}
	for id, r := range want {
		if r.NonPositive() {
			return Snapshot{}, fmt.Errorf("quota rebuild: invalid reservation for %q", id)
		}
		total = total.Add(r)
	}
	if total.AnyExceeds(p.capacity) {
		return Snapshot{}, fmt.Errorf("quota rebuild: rebuilt usage %+v exceeds capacity %+v", total, p.capacity)
	}

	next := make(map[string]domain.Resources, len(want))
	for k, v := range want {
		next[k] = v
	}
	p.reservations = next
	p.used = total
	return Snapshot{
		Capacity:     p.capacity,
		Used:         p.used,
		Available:    p.capacity.Sub(p.used),
		Reservations: next,
	}, nil
}

// shortDimension 返回首个不足的维度名、可用量、请求后增量需求。
// avail 语义为 capacity - used（剩余可分配量）。
func (p *Pool) shortDimension(r domain.Resources) (name string, avail, requested int64, short bool) {
	rem := p.capacity.Sub(p.used)
	switch {
	case r.CPUMillis > rem.CPUMillis:
		return "cpu_millis", rem.CPUMillis, r.CPUMillis, true
	case r.MemoryMB > rem.MemoryMB:
		return "memory_mb", rem.MemoryMB, r.MemoryMB, true
	case r.DiskMB > rem.DiskMB:
		return "disk_mb", rem.DiskMB, r.DiskMB, true
	default:
		return "", 0, 0, false
	}
}
