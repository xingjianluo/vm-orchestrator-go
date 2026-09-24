package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// Store 是实例记录的持久化接口。
type Store interface {
	Put(ctx context.Context, inst *domain.Instance) error
	Get(ctx context.Context, id string) (*domain.Instance, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*domain.Instance, error)
}

// FileStore 以每实例一个 JSON 文件持久化（<dir>/<id>.json）。
// 写入通过"临时文件 + fsync + rename"保证单条记录的原子性，
// 并发写入同一 id 由互斥锁串行化，避免读到半写文件。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("filestore: empty directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("filestore: mkdir %s: %w", dir, err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func validateID(id string) error {
	if id == "" {
		return errors.New("filestore: empty instance id")
	}
	if strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return fmt.Errorf("filestore: unsafe instance id %q", id)
	}
	return nil
}

func (s *FileStore) Put(ctx context.Context, inst *domain.Instance) error {
	if inst == nil {
		return errors.New("filestore: nil instance")
	}
	if err := validateID(inst.ID); err != nil {
		return err
	}
	data, err := json.MarshalIndent(inst, "", "  ")
	if err != nil {
		return fmt.Errorf("filestore: marshal %s: %w", inst.ID, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	final := s.path(inst.ID)
	tmp, err := os.CreateTemp(s.dir, ".tmp-"+inst.ID+"-*.json")
	if err != nil {
		return fmt.Errorf("filestore: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("filestore: write %s: %w", inst.ID, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return fmt.Errorf("filestore: fsync %s: %w", inst.ID, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("filestore: close %s: %w", inst.ID, err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return fmt.Errorf("filestore: rename %s: %w", inst.ID, err)
	}
	return nil
}

func (s *FileStore) Get(ctx context.Context, id string) (*domain.Instance, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, domain.NewNotFoundError("store.get", id)
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: read %s: %w", id, err)
	}
	var inst domain.Instance
	if err := json.Unmarshal(data, &inst); err != nil {
		return nil, fmt.Errorf("filestore: corrupt record %s: %w", id, err)
	}
	return &inst, nil
}

// Delete 物理删除记录；不存在视为幂等成功（墓碑记录保留与否由编排层决定）。
func (s *FileStore) Delete(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	err := os.Remove(s.path(id))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("filestore: delete %s: %w", id, err)
	}
	return nil
}

func (s *FileStore) List(ctx context.Context) ([]*domain.Instance, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: list %s: %w", s.dir, err)
	}
	var out []*domain.Instance
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		inst, err := s.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, nil
}
