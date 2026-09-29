package quota_test

import (
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
)

func TestReserveReleaseConservation(t *testing.T) {
	p := quota.NewPool(domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})
	r := domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}

	if err := p.Reserve("op", "a", r); err != nil {
		t.Fatal(err)
	}
	if err := p.Reserve("op", "a", r); err == nil {
		t.Fatal("double reservation must fail")
	}
	if _, err := p.Release("a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := p.Release("a"); err == nil {
		t.Fatal("double release must be reported (drift signal), not silently ignored")
	}
	if u := p.Snapshot().Used; u != (domain.Resources{}) {
		t.Fatalf("used must return to zero, got %+v", u)
	}
}

func TestRebuild(t *testing.T) {
	p := quota.NewPool(domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})
	_, err := p.Rebuild(map[string]domain.Resources{
		"x": {CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024},
		"y": {CPUMillis: 2000, MemoryMB: 1024, DiskMB: 2048},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap := p.Snapshot()
	if snap.Used != (domain.Resources{CPUMillis: 3000, MemoryMB: 1536, DiskMB: 3072}) || len(snap.Reservations) != 2 {
		t.Fatalf("rebuild wrong: %+v", snap)
	}
	// 超过容量不再是重建错误：台账如实落地并以 OverCommit 暴露结论，
	// 保证记录集合与占用始终自洽、查询与人工收口不会被锁死。
	snap2, err := p.Rebuild(map[string]domain.Resources{
		"z": {CPUMillis: 5000, MemoryMB: 1, DiskMB: 1},
	})
	if err != nil {
		t.Fatalf("rebuild beyond capacity must not fail: %v", err)
	}
	if !snap2.OverCommit || snap2.Used.CPUMillis != 5000 || snap2.Available.CPUMillis != -1000 {
		t.Fatalf("over-capacity rebuild must land and be flagged: %+v", snap2)
	}
	if got := p.Snapshot(); got.Used.CPUMillis != 5000 || !got.OverCommit {
		t.Fatalf("ledger must reflect over-capacity reality: %+v", got)
	}
	// 已超卖：任何新增准入都以 host_overcommitted 拒绝（区别于普通不足）。
	err = p.Reserve("create", "n", domain.Resources{CPUMillis: 1, MemoryMB: 1, DiskMB: 1})
	if !domain.IsKind(err, domain.KindHostOvercommitted) {
		t.Fatalf("new admission while overcommitted must be host_overcommitted, got %v", err)
	}
	// 台账恢复到容量以内后，普通容量不足仍按 insufficient_resource 区分。
	if _, err := p.Rebuild(map[string]domain.Resources{
		"x": {CPUMillis: 3000, MemoryMB: 1, DiskMB: 1},
	}); err != nil {
		t.Fatal(err)
	}
	err = p.Reserve("create", "n2", domain.Resources{CPUMillis: 2000, MemoryMB: 1, DiskMB: 1})
	if !domain.IsKind(err, domain.KindInsufficientResource) {
		t.Fatalf("normal shortage must remain insufficient_resource, got %v", err)
	}
	// 非正规格仍属非法，重建拒绝且不改动台账。
	bad := p.Snapshot().Used
	if _, err := p.Rebuild(map[string]domain.Resources{"bad": {CPUMillis: 0, MemoryMB: 1, DiskMB: 1}}); err == nil {
		t.Fatal("rebuild with non-positive spec must fail")
	}
	if u := p.Snapshot().Used; u != bad {
		t.Fatalf("rejected rebuild must not mutate ledger: %+v", u)
	}
}
