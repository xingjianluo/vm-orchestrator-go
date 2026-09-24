// Package quota 维护宿主资源总量、已占用量与分配计数，
// 提供准入判定与守恒保证（占用/释放必须成对）。
// 本类型自身不加锁；并发安全由上层 orchestrator 统一保证。
package quota

import (
	"fmt"

	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
)

// Pool 是宿主资源配额池。
type Pool struct {
	total     model.Resources
	used      model.Resources
	allocated map[string]model.Resources
}

// New 创建配额池。
func New(total model.Resources) *Pool {
	return &Pool{
		total:     total,
		allocated: make(map[string]model.Resources),
	}
}

// Total 返回宿主总量。
func (p *Pool) Total() model.Resources { return p.total }

// Used 返回当前已占用量。
func (p *Pool) Used() model.Resources { return p.used }

// Available 返回剩余可分配量。
func (p *Pool) Available() model.Resources { return p.total.Sub(p.used) }

// Admit 判定某资源申请能否被接纳（不产生副作用）。
func (p *Pool) Admit(need model.Resources) error {
	avail := p.Available()
	switch {
	case need.CPUMillis > avail.CPUMillis:
		return &model.QuotaError{Requested: need, Available: avail, Resource: "cpuMillis"}
	case need.MemoryMiB > avail.MemoryMiB:
		return &model.QuotaError{Requested: need, Available: avail, Resource: "memoryMiB"}
	case need.DiskMiB > avail.DiskMiB:
		return &model.QuotaError{Requested: need, Available: avail, Resource: "diskMiB"}
	}
	return nil
}

// Allocate 为实例占用配额；超额或重复占用均返回错误且不产生副作用。
func (p *Pool) Allocate(id string, need model.Resources) error {
	if _, ok := p.allocated[id]; ok {
		return fmt.Errorf("%w: quota already allocated for instance %s", model.ErrAlreadyExists, id)
	}
	if err := p.Admit(need); err != nil {
		return err
	}
	p.allocated[id] = need
	p.used = p.used.Add(need)
	return nil
}

// Release 释放实例占用的配额；未占用时返回错误（暴露配对错误，防止静默漂移）。
func (p *Pool) Release(id string) error {
	need, ok := p.allocated[id]
	if !ok {
		return fmt.Errorf("quota release without allocation for instance %s (possible drift)", id)
	}
	p.used = p.used.Sub(need)
	delete(p.allocated, id)
	return nil
}

// Has 返回某实例是否占用配额。
func (p *Pool) Has(id string) bool {
	_, ok := p.allocated[id]
	return ok
}

// Allocations 返回当前占用明细（拷贝）。
func (p *Pool) Allocations() map[string]model.Resources {
	out := make(map[string]model.Resources, len(p.allocated))
	for k, v := range p.allocated {
		out[k] = v
	}
	return out
}
