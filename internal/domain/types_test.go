package domain

import "testing"

func TestStateTransitionRules(t *testing.T) {
	legal := [][2]State{
		{StateDefined, StateRunning},
		{StateDefined, StateDestroyed},
		{StateRunning, StateStopped},
		{StateStopped, StateRunning},
		{StateStopped, StateDestroyed},
	}
	for _, p := range legal {
		if !p[0].CanTransition(p[1]) {
			t.Errorf("%s -> %s must be legal", p[0], p[1])
		}
	}
	illegal := [][2]State{
		{StateDefined, StateStopped},
		{StateRunning, StateDefined},
		{StateRunning, StateDestroyed},
		{StateStopped, StateDefined},
		{StateDestroyed, StateRunning},
		{StateDestroyed, StateStopped},
		{State("Bogus"), StateRunning},
		{StateRunning, StateRunning}, // 同态由编排层解释为幂等，不算迁移规则
	}
	// ManualReview 是冻结态：没有任何常规迁移规则（只能经人工收口落定）。
	for _, to := range []State{StateDefined, StateRunning, StateStopped, StateDestroyed, StateManualReview} {
		illegal = append(illegal, [2]State{StateManualReview, to})
	}
	// 常规状态也不允许直接迁进/迁出 ManualReview。
	for _, from := range []State{StateDefined, StateRunning, StateStopped, StateDestroyed} {
		illegal = append(illegal, [2]State{from, StateManualReview})
	}
	for _, p := range illegal {
		if p[0].CanTransition(p[1]) {
			t.Errorf("%s -> %s must be illegal", p[0], p[1])
		}
	}
	for _, s := range []State{StateDefined, StateRunning, StateStopped, StateManualReview, StateDestroyed} {
		if !s.Valid() {
			t.Errorf("%s must be valid", s)
		}
	}
	if State("nope").Valid() {
		t.Error("unknown state must be invalid")
	}

	// 占用口径：ManualReview 与三个存活态都占用；Destroyed 不占用。
	for _, s := range []State{StateDefined, StateRunning, StateStopped, StateManualReview} {
		if !s.HoldsResources() {
			t.Errorf("%s must count toward usage while unresolved", s)
		}
	}
	if StateDestroyed.HoldsResources() {
		t.Error("Destroyed must not hold resources")
	}
}

func TestResourceArithmetic(t *testing.T) {
	a := Resources{CPUMillis: 1, MemoryMB: 2, DiskMB: 3}
	b := Resources{CPUMillis: 4, MemoryMB: 5, DiskMB: 6}
	if got := a.Add(b); got != (Resources{5, 7, 9}) {
		t.Fatalf("add: %+v", got)
	}
	if got := b.Sub(a); got != (Resources{3, 3, 3}) {
		t.Fatalf("sub: %+v", got)
	}
	if a.NonPositive() {
		t.Fatal("positive resources must not be non-positive")
	}
	if !(Resources{0, 1, 1}).NonPositive() {
		t.Fatal("zero cpu must be non-positive")
	}
	if (Resources{1, 1, 1}).AnyExceeds(Resources{2, 2, 2}) {
		t.Fatal("1 <= 2 must not exceed")
	}
	if !(Resources{3, 1, 1}).AnyExceeds(Resources{2, 2, 2}) {
		t.Fatal("cpu exceed must be detected")
	}
}
