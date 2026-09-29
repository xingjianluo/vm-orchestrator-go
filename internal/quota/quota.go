package quota

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// ErrUsageExceedsCapacity 是「重建后的占用总额超过当前宿主容量」的告警级结论。
//
// 它只可能由 Rebuild（重启恢复 / 对账重建）返回，语义与普通错误不同：
//   - 台账**已经**按权威记录集合重建完成（used == Σ 预留），系统内部并不矛盾，
//     占用读数完全自洽、可解释；
//   - 超容量是一个对账结论/告警（例如出现了规格大于本机容量的后端残留），
//     **不能**变成一票否决所有命令的开关：查询照常给出结果，人工收口照常可用；
//   - 真正会造成超卖的新增准入（Reserve）仍然拒绝，返回 insufficient_resource。
//
// 调用方应当用 errors.Is 识别它并作为告警暴露，而不是按内部不一致处理。
var ErrUsageExceedsCapacity = errors.New("quota: rebuilt usage exceeds host capacity (advisory: ledger consistent, admission stays closed)")

// Snapshot 是某一时刻配额池的守恒快照：available = capacity - used。
//
// 当 used 超过 capacity（只可能由重建带入，例如超大规格的对账残留）时：
// OverCapacity=true、OverBy 给出逐维度超出量、Available 对应维度为负值。
// 这不是内部不一致：used 仍精确等于全部预留之和，负的 Available 只是把
// 「当前事实占用已超过本机配置容量」如实显示出来。
type Snapshot struct {
	Capacity     domain.Resources            `json:"capacity"`
	Used         domain.Resources            `json:"used"`
	Available    domain.Resources            `json:"available"`
	OverCapacity bool                        `json:"over_capacity"`
	OverBy       domain.Resources            `json:"over_by"`
	Reservations map[string]domain.Resources `json:"reservations"`
}

// Pool 是宿主资源配额池。
// 不变量：
//  1. used == 所有 reservations 之和（台账永远自洽，哪怕 used 已超过 capacity）；
//  2. 在线准入路径（Reserve）绝不制造超卖：任何维度 used+r>capacity 都拒绝；
//     used 超过 capacity 只可能由 Rebuild（重启恢复/对账重建）按落盘事实带入，
//     此时照常重建台账并以 ErrUsageExceedsCapacity 作为告警返回；
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

// Register 用于人工收口（resolve adopt --claim）把一条此前无法计入的占用
// （无记录且后端读不出规格的孤儿）**按人工确认的事实规格**登记进台账。
// 它不走容量准入：此时占用是对既成事实的补登记，容量是否已超由 Snapshot 如实暴露；
// 重复登记同一 id 返回错误（漂移信号），由编排层处理。
func (p *Pool) Register(op string, id string, r domain.Resources) error {
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
	p.reservations[id] = r
	p.used = p.used.Add(r)
	return nil
}

// HasReservation 报告实例是否仍持有预留。
func (p *Pool) HasReservation(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.reservations[id]
	return ok
}

// overBy 返回 used 相对 capacity 在各维度上的超出量（未超出的维度为 0）。
func overBy(used, capacity domain.Resources) domain.Resources {
	return domain.Resources{
		CPUMillis: max64(0, used.CPUMillis-capacity.CPUMillis),
		MemoryMB:  max64(0, used.MemoryMB-capacity.MemoryMB),
		DiskMB:    max64(0, used.DiskMB-capacity.DiskMB),
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Snapshot 返回当前配额的不可变拷贝。
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	res := make(map[string]domain.Resources, len(p.reservations))
	for k, v := range p.reservations {
		res[k] = v
	}
	ob := overBy(p.used, p.capacity)
	return Snapshot{
		Capacity:     p.capacity,
		Used:         p.used,
		Available:    p.capacity.Sub(p.used),
		OverCapacity: ob != (domain.Resources{}),
		OverBy:       ob,
		Reservations: res,
	}
}

// Rebuild 在重启/对账后依据权威实例集合重建台账。
//
// 无论总额是否超过容量，台账都会**先按权威集合重建完成**（used==Σwant），
// 以保证「换任何一个进程查询，占用都与落盘事实一致」。随后：
//   - 总额未超容量：返回 (snapshot, nil)；
//   - 总额超过容量：返回 (snapshot, ErrUsageExceedsCapacity)。这是告警级对账结论，
//     不是内部不一致——台账自洽，查询与人工收口不受影响，新增准入继续按容量拒绝；
//   - want 中存在非法（非正）规格：返回错误且不修改台账（这才是真正的数据损坏）。
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

	next := make(map[string]domain.Resources, len(want))
	for k, v := range want {
		next[k] = v
	}
	// 关键修正：超容量也要落账。权威集合是「持有资源的记录」，台账必须等于它，
	// 否则查询侧会与记录长期矛盾、人工收口也无法在台账上落地。
	p.reservations = next
	p.used = total

	ob := overBy(total, p.capacity)
	snap := Snapshot{
		Capacity:     p.capacity,
		Used:         p.used,
		Available:    p.capacity.Sub(p.used),
		OverCapacity: ob != (domain.Resources{}),
		OverBy:       ob,
		Reservations: next,
	}
	if snap.OverCapacity {
		return snap, fmt.Errorf("%w: rebuilt usage %+v exceeds capacity %+v overBy %+v",
			ErrUsageExceedsCapacity, total, p.capacity, ob)
	}
	return snap, nil
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
