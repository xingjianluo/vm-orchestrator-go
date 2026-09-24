// Package store 提供实例记录的持久化：
// 单文件 JSON + 临时文件原子替换 + fsync，保证重启后记录完整可读。
// 本类型自身不加锁；写入串行化由上层 orchestrator 保证。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
)

// Snapshot 是持久化的完整状态快照。
type Snapshot struct {
	Version   int                        `json:"version"`
	Seq       int64                      `json:"seq"`
	Instances map[string]*model.Instance `json:"instances"`
	UpdatedAt string                     `json:"updatedAt"`
}

// Store 是快照存储。
type Store struct {
	path string
}

// New 创建/打开存储文件（不存在时初始化空快照，不立即写盘）。
func New(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &Store{path: path}, nil
}

// Load 读取最新快照；文件不存在时返回空快照。
func (s *Store) Load(ctx context.Context) (*Snapshot, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &Snapshot{Version: 1, Instances: make(map[string]*model.Instance)}, nil
		}
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	snap := &Snapshot{}
	if err := json.Unmarshal(data, snap); err != nil {
		return nil, fmt.Errorf("corrupt snapshot %s: %w", s.path, err)
	}
	if snap.Instances == nil {
		snap.Instances = make(map[string]*model.Instance)
	}
	return snap, nil
}

// Save 原子写入快照（写临时文件 -> fsync -> rename -> fsync 目录）。
func (s *Store) Save(ctx context.Context, snap *Snapshot) error {
	if snap == nil {
		return errors.New("nil snapshot")
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("rename snapshot: %w", err)
	}
	// fsync 目录，保证 rename 持久化。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
