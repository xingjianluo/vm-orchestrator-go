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
	if _, err := p.Rebuild(map[string]domain.Resources{"z": {CPUMillis: 5000, MemoryMB: 1, DiskMB: 1}}); err == nil {
		t.Fatal("rebuild beyond capacity must be rejected without mutating state")
	}
	if u := p.Snapshot().Used; u != (domain.Resources{CPUMillis: 3000, MemoryMB: 1536, DiskMB: 3072}) {
		t.Fatalf("rejected rebuild must not mutate ledger: %+v", u)
	}
}
