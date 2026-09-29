package quota

import (
	"fmt"
	"sync"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// Snapshot 是某一时刻配额池的守恒快照：available = capacity - used。
// 宿主被对账重建出"占用超过容量"的事实时，used 如实反映记录集合之和，
// available 相应可能为负，OverCommit=true 并在 Exceeded 列出超出维度——
// 这是一种对账结论/告警，不影响台账自洽，只用来拒绝新增占用的准入。
type Snapshot struct {
	Capacity     domain.Resources            `json:"capacity"`
	Used         domain.Resources            `json:"used"`
	Available    domain.Resources            `json:"available"`
	Reservations map[string]domain.Resources `json:"reservations"`
	// OverCommit 表示 used 任一维度已超过 capacity（宿主事实超卖）。
	OverCommit bool `json:"over_commit"`
	// Exceeded 列出 used 超过 capacity 的维度名（cpu_millis/memory_mb/disk_millis）。
	Exceeded []string `json:"exceeded,omitempty"`
}

// Pool 是宿主资源配额池。
// 不变量：
//  1. used == 所有 reservations 之和（台账永远自洽，即使宿主事实已超容量）；
//  2. 准入（Reserve）不允许把 used 推过 capacity：容量正常不足时返回
//     insufficient_resource；宿主已经 over-commit（used > capacity，只能由
//     Rebuild 对既有事实重建产生）时返回 host_overcommitted，两者明确可分；
//  3. Reserve 成功之前不产生任何后端副作用（由编排层保证调用顺序）；
//  4. Release 必须对应一次 Reserve，重复释放返回错误以暴露占用漂移。
//
// 注意：超过容量本身不是"内部不一致"。对账/重启可能重建出超过当前容量的占用
// （例如后端残留声明了超大规格），此时台账必须如实落地并标记 OverCommit，
// 由人工收口消化；不能让它把查询与收口一并锁死。
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
	// 宿主已被既有事实推到超容量（通常来自对账重建的超大残留）：
	// 这是一种"准入策略"拒绝，用独立类别暴露，不能伪装成内部不一致或普通不足。
	if name, avail, req, over := p.overCommitDimension(r); over {
		return domain.NewHostOvercommittedError(op, id, name, req, avail)
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

// Register 登记一份"已存在事实"的占用（不做容量准入）。
// 唯一调用场景：人工 resolve adopt 认领一个后端读不出规格、此前无法计入台账的
// 孤儿——人工确认本身就是对既有事实的裁决，规格即占用口径，即使因此 over-commit
// 也必须让收口落地；真正的新增准入走 Reserve，仍会被容量/超卖检查拒绝。
// 已有预留时返回错误（登记只能发生一次）。
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

// Snapshot 返回当前配额的不可变拷贝。
func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

// snapshotLocked 构造快照（调用方须持有 p.mu）。
func (p *Pool) snapshotLocked() Snapshot {
	res := make(map[string]domain.Resources, len(p.reservations))
	for k, v := range p.reservations {
		res[k] = v
	}
	avail := p.capacity.Sub(p.used)
	exceeded := exceededDimensions(p.used, p.capacity)
	return Snapshot{
		Capacity:     p.capacity,
		Used:         p.used,
		Available:    avail,
		Reservations: res,
		OverCommit:   len(exceeded) > 0,
		Exceeded:     exceeded,
	}
}

// Rebuild 在重启/对账后依据权威实例集合重建台账。
// 台账始终按权威集合如实落地（used == Σ want），保证"记录集合 == 台账"自洽；
// 若重建结果超过宿主容量，不报错、不截断，而是在返回快照里以 OverCommit 暴露，
// 由对账层作为结论/告警上报——超容量不是内部不一致，不能一票否决查询与人工收口。
// 仅当权威集合本身含有非法（非正）规格时才返回错误且不修改台账。
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
	p.reservations = next
	p.used = total
	return p.snapshotLocked(), nil
}

// exceededDimensions 返回 used 超过 capacity 的维度名列表（保持固定顺序）。
func exceededDimensions(used, cap domain.Resources) []string {
	var out []string
	if used.CPUMillis > cap.CPUMillis {
		out = append(out, "cpu_millis")
	}
	if used.MemoryMB > cap.MemoryMB {
		out = append(out, "memory_mb")
	}
	if used.DiskMB > cap.DiskMB {
		out = append(out, "disk_mb")
	}
	return out
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

// overCommitDimension 在"宿主当前占用已超容量"时返回首个超卖维度：
// avail 为负说明即便不新增请求也已超出；此时任何新增准入一律拒绝。
func (p *Pool) overCommitDimension(r domain.Resources) (name string, avail, requested int64, over bool) {
	rem := p.capacity.Sub(p.used)
	switch {
	case rem.CPUMillis < 0:
		return "cpu_millis", rem.CPUMillis, r.CPUMillis, true
	case rem.MemoryMB < 0:
		return "memory_mb", rem.MemoryMB, r.MemoryMB, true
	case rem.DiskMB < 0:
		return "disk_mb", rem.DiskMB, r.DiskMB, true
	default:
		return "", 0, 0, false
	}
}
