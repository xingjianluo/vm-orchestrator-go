package quota_test

import (
	"errors"
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
	if snap.OverCapacity {
		t.Fatalf("within-capacity rebuild must not be flagged: %+v", snap)
	}
}

// TestRebuildBeyondCapacityIsAdvisory 超容量重建是告警级结论而非内部错误：
// 台账仍必须按权威集合落账（used==Σ预留、读数自洽），并显式暴露 OverCapacity/OverBy；
// 同时在线准入仍然拒绝新预留（不会因为台账已超而放行任何新占用）。
func TestRebuildBeyondCapacityIsAdvisory(t *testing.T) {
	p := quota.NewPool(domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})
	want := map[string]domain.Resources{
		"x": {CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024},
		// 单实例声明规格就超过整机容量：模拟对账发现的超大后端残留。
		"z": {CPUMillis: 5000, MemoryMB: 1, DiskMB: 1},
	}
	snap, err := p.Rebuild(want)
	if !errors.Is(err, quota.ErrUsageExceedsCapacity) {
		t.Fatalf("over-capacity rebuild must return ErrUsageExceedsCapacity, got %v", err)
	}
	// 台账必须已经按权威集合重建，而不是保留旧值或清空。
	if got := p.Snapshot(); got.Used != (domain.Resources{CPUMillis: 6000, MemoryMB: 513, DiskMB: 1025}) ||
		len(got.Reservations) != 2 {
		t.Fatalf("ledger must reflect authoritative records despite over-capacity: %+v", got)
	}
	if !snap.OverCapacity {
		t.Fatalf("snapshot must flag OverCapacity: %+v", snap)
	}
	if snap.OverBy != (domain.Resources{CPUMillis: 2000, MemoryMB: 0, DiskMB: 0}) {
		t.Fatalf("OverBy wrong: %+v", snap.OverBy)
	}
	if snap.Available != (domain.Resources{CPUMillis: -2000, MemoryMB: 1535, DiskMB: 7167}) {
		t.Fatalf("negative available must expose the excess honestly: %+v", snap.Available)
	}
	// 在线准入继续关闭：任何新预留都以 insufficient_resource 拒绝，且不改动台账。
	if err := p.Reserve("create", "new", domain.Resources{CPUMillis: 1, MemoryMB: 1, DiskMB: 1}); err == nil {
		t.Fatal("admission must stay closed while usage exceeds capacity")
	} else if !domain.IsKind(err, domain.KindInsufficientResource) {
		t.Fatalf("admission rejection must be insufficient_resource, got %v", err)
	}
	if _, ok := p.Snapshot().Reservations["new"]; ok {
		t.Fatal("rejected reserve must not register a reservation")
	}
	// 收口感知：释放掉超大残留后，台账立即回到容量以内，告警消失。
	if _, err := p.Release("z"); err != nil {
		t.Fatalf("manual discard of oversized residue must release: %v", err)
	}
	if after := p.Snapshot(); after.OverCapacity || after.Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("after releasing oversized residue ledger must be healthy: %+v", after)
	}
}

// TestRebuildInvalidSpecRejected 真正的数据损坏（非正规格）必须报错且不改动台账，
// 与超容量告警严格区分。
func TestRebuildInvalidSpecRejected(t *testing.T) {
	p := quota.NewPool(domain.Resources{CPUMillis: 4000, MemoryMB: 2048, DiskMB: 8192})
	if _, err := p.Rebuild(map[string]domain.Resources{
		"x": {CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Rebuild(map[string]domain.Resources{
		"bad": {CPUMillis: 0, MemoryMB: 1, DiskMB: 1},
	}); err == nil {
		t.Fatal("invalid spec must be rejected")
	}
	if got := p.Snapshot(); got.Used != (domain.Resources{CPUMillis: 1000, MemoryMB: 512, DiskMB: 1024}) {
		t.Fatalf("invalid rebuild must not mutate ledger: %+v", got)
	}
}
