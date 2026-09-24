package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
)

// ConvergeResult 是单实例状态收敛的结果。
type ConvergeResult struct {
	Before  model.State
	After   model.State
	Changed bool
	Reason  string
}

// Backend 返回底层后端（对账时用于事实探测与收敛动作）。
func (m *Manager) Backend() backend.Backend { return m.bk }

// ConvergeState 在持锁下把实例收敛到目标状态（必须是合法对账迁移），
// 落盘；目标为 Destroyed 时同步释放配额（若占用）并移除名字索引。
// 该方法仅供重启对账在单线程下调用。
func (m *Manager) ConvergeState(ctx context.Context, id string, target model.State, reason string) (ConvergeResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	in, err := m.lockedGet(id)
	if err != nil {
		return ConvergeResult{}, err
	}
	before := in.State
	res := ConvergeResult{Before: before, After: before}
	if before == target {
		if reason != "" && in.Reason != reason {
			in.Reason = reason
			in.UpdatedAt = time.Now().UTC()
			in.Generation++
			if err := m.persistLocked(ctx); err != nil {
				return res, err
			}
			res.Reason = reason
		}
		return res, nil
	}
	if !model.CanTransition(before, target) {
		return res, &model.TransitionError{ID: id, From: before}
	}
	in.State = target
	in.Reason = reason
	in.UpdatedAt = time.Now().UTC()
	in.Generation++
	if target == model.StateDestroyed {
		in.PID = 0
	}
	if err := m.persistLocked(ctx); err != nil {
		return res, err
	}
	if target == model.StateDestroyed {
		if m.qp.Has(id) {
			if err := m.qp.Release(id); err != nil {
				return res, err
			}
		}
		delete(m.nameIdx, in.Name)
	}
	res.After = target
	res.Changed = true
	res.Reason = reason
	return res, nil
}

// InstanceIDs 返回全部记录 ID（持锁拷贝）。
func (m *Manager) InstanceIDs(ctx context.Context) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.snap.Instances))
	for id := range m.snap.Instances {
		ids = append(ids, id)
	}
	return ids
}

// RecordState 返回某记录的状态与原因副本；不存在时 ok=false。
func (m *Manager) RecordState(ctx context.Context, id string) (model.State, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.snap.Instances[id]
	if !ok {
		return "", "", false
	}
	return in.State, in.Reason, true
}

// RebuildQuota 以持久化记录为唯一基准重算配额：
// 所有非 Destroyed 记录占用配额，Destroyed 不占用。
// 返回重算前后的已用量；若记录总量超过宿主总量，返回错误且不替换配额池。
func (m *Manager) RebuildQuota(ctx context.Context) (model.Resources, model.Resources, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	before := m.qp.Used()
	fresh := newPoolForRebuild(m.qp.Total())
	for id, in := range m.snap.Instances {
		if in == nil || in.State == model.StateDestroyed {
			continue
		}
		if err := fresh.Allocate(id, in.Spec); err != nil {
			return before, m.qp.Used(), fmt.Errorf("rebuild quota for %s: %w", id, err)
		}
	}
	m.qp = fresh
	return before, fresh.Used(), nil
}
