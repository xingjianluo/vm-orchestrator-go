package quota_test

import (
	"errors"
	"testing"

	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
)

func TestAllocateReleaseConservation(t *testing.T) {
	p := quota.New(model.Resources{CPUMillis: 100, MemoryMiB: 100, DiskMiB: 100})
	a := model.Resources{CPUMillis: 40, MemoryMiB: 30, DiskMiB: 20}

	if err := p.Allocate("a", a); err != nil {
		t.Fatal(err)
	}
	// 重复占用拒绝且不改变用量。
	if err := p.Allocate("a", a); !errors.Is(err, model.ErrAlreadyExists) {
		t.Fatalf("dup allocate want ErrAlreadyExists, got %v", err)
	}
	if p.Used() != a {
		t.Fatalf("dup allocate changed used: %+v", p.Used())
	}
	// 超额拒绝且不产生占用。
	err := p.Allocate("b", model.Resources{CPUMillis: 70, MemoryMiB: 1, DiskMiB: 1})
	var qe *model.QuotaError
	if !errors.As(err, &qe) || qe.Resource != "cpuMillis" {
		t.Fatalf("want cpuMillis quota error, got %v", err)
	}
	if _, ok := p.Allocations()["b"]; ok {
		t.Fatal("rejected allocation must not be recorded")
	}
	// 释放未占用必须报错，暴露配对错误。
	if err := p.Release("ghost"); err == nil {
		t.Fatal("release without allocation must error")
	}
	if err := p.Release("a"); err != nil {
		t.Fatal(err)
	}
	if p.Used() != (model.Resources{}) || len(p.Allocations()) != 0 {
		t.Fatalf("pool not reset: used=%+v allocs=%+v", p.Used(), p.Allocations())
	}
	t.Logf("[resource snapshot] total=%+v used=%+v available=%+v", p.Total(), p.Used(), p.Available())
}
