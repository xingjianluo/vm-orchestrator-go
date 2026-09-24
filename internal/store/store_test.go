package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	path := t.TempDir() + "/nested/state.json"
	st, err := store.New(path)
	if err != nil {
		t.Fatal(err)
	}
	snap := &store.Snapshot{
		Version:   1,
		Seq:       7,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Instances: map[string]*model.Instance{
			"vm1": {ID: "vm1", Name: "n", State: model.StateRunning,
				Spec: model.Resources{CPUMillis: 1, MemoryMiB: 2, DiskMiB: 3}},
		},
	}
	if err := st.Save(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	st2, err := store.New(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st2.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 7 || got.Instances["vm1"].State != model.StateRunning {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestLoadMissingReturnsEmpty(t *testing.T) {
	st, _ := store.New(t.TempDir() + "/state.json")
	snap, err := st.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Instances) != 0 {
		t.Fatal("missing file must yield empty snapshot")
	}
}
