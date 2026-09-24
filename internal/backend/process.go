package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// sleepSeconds 是承载进程的运行时长（约 10 年），仅用于占位，Stop 时会被强杀。
const sleepSeconds = 315360000

type defRecord struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"createdAt"`
}

type pidRecord struct {
	PID     int   `json:"pid"`
	Started int64 `json:"started"`
}

// ProcessBackend 以宿主机进程承载实例：
// Define 写入定义标记，Start 启动长睡眠子进程，Stop 发信号，Undefine 清理。
type handle struct {
	cmd *exec.Cmd
	ch  chan struct{} // 进程被回收后关闭（单一 Wait 调用者）
}

type ProcessBackend struct {
	rootDir string
	mu      sync.Mutex
	cmds    map[int]*handle // pid -> 运行句柄
}

// NewProcessBackend 创建进程模拟后端，数据落在 rootDir 下。
func NewProcessBackend(rootDir string) (*ProcessBackend, error) {
	if rootDir == "" {
		return nil, errors.New("backend root dir is empty")
	}
	for _, sub := range []string{"defs", "run"} {
		if err := os.MkdirAll(filepath.Join(rootDir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("init backend dir: %w", err)
		}
	}
	return &ProcessBackend{rootDir: rootDir, cmds: make(map[int]*handle)}, nil
}

func (b *ProcessBackend) Name() string { return "process" }

func (b *ProcessBackend) defPath(id string) string {
	return filepath.Join(b.rootDir, "defs", id+".json")
}
func (b *ProcessBackend) pidPath(id string) string { return filepath.Join(b.rootDir, "run", id+".pid") }

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Define 创建实例定义（幂等：已定义视为成功）。
func (b *ProcessBackend) Define(ctx context.Context, id, name string) error {
	rec := defRecord{ID: id, Name: name, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := writeJSON(b.defPath(id), rec); err != nil {
		return fmt.Errorf("backend define: %w", err)
	}
	return nil
}

// readLivePID 读取 pid 文件并校验进程存活与启动时间（防 PID 复用）。
// 返回 (pid, started, exists, alive)。
func (b *ProcessBackend) readLivePID(id string) (int, int64, bool, bool) {
	data, err := os.ReadFile(b.pidPath(id))
	if err != nil {
		return 0, 0, false, false
	}
	var rec pidRecord
	if err := json.Unmarshal(data, &rec); err != nil || rec.PID <= 0 {
		return 0, 0, true, false
	}
	if !processAlive(rec.PID) {
		return rec.PID, rec.Started, true, false
	}
	if rec.Started > 0 {
		if st := processStartUnix(rec.PID); st != 0 && st != rec.Started {
			// PID 被复用：文件里的进程已不是原实例。
			return rec.PID, rec.Started, true, false
		}
	}
	return rec.PID, rec.Started, true, true
}

// Start 拉起实例运行实体，返回其 PID。
func (b *ProcessBackend) Start(ctx context.Context, id string) (int, error) {
	if _, err := os.Stat(b.defPath(id)); err != nil {
		return 0, fmt.Errorf("%w: instance %s not defined", errBackendNotDefined, id)
	}
	if pid, _, _, alive := b.readLivePID(id); alive {
		return pid, nil // 幂等：已在运行
	}
	cmd := exec.CommandContext(ctx, "sleep", strconv.Itoa(sleepSeconds))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("backend start: %w", err)
	}
	pid := cmd.Process.Pid
	rec := pidRecord{PID: pid, Started: processStartUnix(pid)}
	if err := writeJSON(b.pidPath(id), rec); err != nil {
		_ = killGroup(pid)
		_ = cmd.Wait()
		return 0, fmt.Errorf("backend start (pidfile): %w", err)
	}
	h := &handle{cmd: cmd, ch: make(chan struct{})}
	b.mu.Lock()
	b.cmds[pid] = h
	b.mu.Unlock()
	// 单一回收者：保证杀死子进程后不残留僵尸；父进程退出后子进程由 init 接管。
	// 其他路径只等待该 channel，绝不并发调用 cmd.Wait()。
	go func() {
		_ = cmd.Wait()
		close(h.ch)
		b.mu.Lock()
		if b.cmds[pid] == h {
			delete(b.cmds, pid)
		}
		b.mu.Unlock()
	}()
	return pid, nil
}

// KillAndReap 同步杀死由本后端启动的进程并等待回收，消除僵尸窗口。
// 用于故障注入：精确模拟“进程已退出且已回收”的崩溃事实。
func (b *ProcessBackend) KillAndReap(pid int) {
	b.mu.Lock()
	h := b.cmds[pid]
	b.mu.Unlock()
	_ = killGroup(pid)
	if h != nil {
		<-h.ch
	}
}

// Stop 终止运行实体（幂等：已退出视为成功）。
func (b *ProcessBackend) Stop(ctx context.Context, id string) error {
	pid, _, _, alive := b.readLivePID(id)
	if alive {
		if err := killGroup(pid); err != nil {
			return fmt.Errorf("backend stop: %w", err)
		}
	}
	if err := os.Remove(b.pidPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("backend stop (cleanup pidfile): %w", err)
	}
	return nil
}

// Undefine 删除实例定义（幂等：不存在视为成功）。
func (b *ProcessBackend) Undefine(ctx context.Context, id string) error {
	if err := os.Remove(b.defPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("backend undefine: %w", err)
	}
	if err := os.Remove(b.pidPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("backend undefine (pidfile): %w", err)
	}
	return nil
}

// Inspect 查询后端事实。
func (b *ProcessBackend) Inspect(ctx context.Context, id string) (RuntimeInfo, error) {
	_, defErr := os.Stat(b.defPath(id))
	defined := defErr == nil
	pid, started, pidFileExists, alive := b.readLivePID(id)
	switch {
	case defined && alive:
		return RuntimeInfo{Status: StatusRunning, PID: pid, Started: started}, nil
	case defined:
		return RuntimeInfo{Status: StatusDefined, PID: pid}, nil
	case alive || pidFileExists:
		// 无定义但存在运行实体/陈旧 pid 文件：无法解释，交由上层标记。
		return RuntimeInfo{Status: StatusUnknown, PID: pid}, nil
	default:
		return RuntimeInfo{Status: StatusGone}, nil
	}
}

// List 返回后端存在定义的全部实例 ID。
func (b *ProcessBackend) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(b.rootDir, "defs"))
	if err != nil {
		return nil, fmt.Errorf("backend list: %w", err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		ids = append(ids, e.Name()[:len(e.Name())-len(".json")])
	}
	return ids, nil
}

// KillAllByDir 强制清理指定后端目录中仍可能存活的承载进程。
// 仅供测试在夹具拆卸时兜底，保证异常路径不留孤儿进程。
func KillAllByDir(rootDir string) error {
	entries, err := os.ReadDir(filepath.Join(rootDir, "run"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".pid" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(rootDir, "run", e.Name()))
		if err != nil {
			continue
		}
		var rec pidRecord
		if json.Unmarshal(data, &rec) == nil && rec.PID > 0 {
			_ = killGroup(rec.PID)
		}
	}
	return nil
}
