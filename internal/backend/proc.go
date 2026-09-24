//go:build !windows

package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
)

// ProcConfig 配置进程级后端。
type ProcConfig struct {
	// Dir 存放每个实例的承载目录（spec.json / pid 文件）。
	Dir string
	// GuestBin 是承载实例的 guest 可执行文件；为空时退化为 "sleep" 类占位进程。
	GuestBin  string
	GuestArgs []string
	// StopGrace 优雅停机等待时长，默认 5s。
	StopGrace time.Duration
}

type procSession struct {
	cmd    *exec.Cmd
	doneCh chan struct{}
}

// ProcHandle 以真实子进程承载"虚拟机"实例：
//   - Define 在磁盘上建立承载目录与 spec.json；
//   - Start 拉起 guest 子进程并写 pid 文件；
//   - Inspect 只信事实：目录与 pid + 信号探活，因此能识别异常崩溃与重启残留。
type ProcHandle struct {
	cfg  ProcConfig
	mu   sync.Mutex
	live map[string]*procSession
}

func NewProcHandle(cfg ProcConfig) (*ProcHandle, error) {
	if strings.TrimSpace(cfg.Dir) == "" {
		return nil, fmt.Errorf("proc backend: empty dir")
	}
	if cfg.StopGrace == 0 {
		cfg.StopGrace = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("proc backend: mkdir: %w", err)
	}
	return &ProcHandle{cfg: cfg, live: make(map[string]*procSession)}, nil
}

func (h *ProcHandle) Name() string { return "proc" }

func (h *ProcHandle) instDir(id string) string { return filepath.Join(h.cfg.Dir, id) }

func (h *ProcHandle) writeJSON(path string, v any) error {
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

func (h *ProcHandle) Define(ctx context.Context, id string, spec domain.Spec) error {
	if err := h.validateID(id); err != nil {
		return err
	}
	dir := h.instDir(id)
	if _, err := os.Stat(filepath.Join(dir, "spec.json")); err == nil {
		return fmt.Errorf("proc: %s already defined", id)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("proc: define %s: %w", id, err)
	}
	if err := h.writeJSON(filepath.Join(dir, "spec.json"), spec); err != nil {
		_ = os.RemoveAll(dir)
		return fmt.Errorf("proc: define %s: %w", id, err)
	}
	return nil
}

func (h *ProcHandle) Start(ctx context.Context, id string) error {
	if err := h.validateID(id); err != nil {
		return err
	}
	dir := h.instDir(id)
	if _, err := os.Stat(filepath.Join(dir, "spec.json")); err != nil {
		return fmt.Errorf("proc: %s not defined: %w", id, err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if pid, ok := h.readPidLocked(id); ok && processAlive(pid) {
		return fmt.Errorf("proc: %s already running (pid=%d)", id, pid)
	}

	bin, args := h.guestCommand(id)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("proc: start %s: %w", id, err)
	}
	sess := &procSession{cmd: cmd, doneCh: make(chan struct{})}
	h.live[id] = sess
	go func() {
		_ = cmd.Wait()
		close(sess.doneCh)
	}()

	if err := h.writeJSON(filepath.Join(dir, "pid"), map[string]int{"pid": cmd.Process.Pid}); err != nil {
		_ = cmd.Process.Kill()
		delete(h.live, id)
		return fmt.Errorf("proc: write pid %s: %w", id, err)
	}
	return nil
}

func (h *ProcHandle) Stop(ctx context.Context, id string) error {
	h.mu.Lock()
	pid, ok := h.readPidLocked(id)
	sess := h.live[id]
	h.mu.Unlock()
	if !ok {
		return nil // 无 pid 文件：本就未运行，停止幂等。
	}
	if !processAlive(pid) {
		_ = os.Remove(filepath.Join(h.instDir(id), "pid"))
		return nil
	}

	grace := h.cfg.StopGrace
	if sess != nil {
		if err := sess.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return fmt.Errorf("proc: signal term %s: %w", id, err)
		}
	} else {
		if err := signalProcess(pid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("proc: signal term %s: %w", id, err)
		}
	}

	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if processAlive(pid) {
		if err := signalProcess(pid, syscall.SIGKILL); err != nil {
			return fmt.Errorf("proc: signal kill %s: %w", id, err)
		}
		for i := 0; i < 20 && processAlive(pid); i++ {
			time.Sleep(50 * time.Millisecond)
		}
	}

	h.mu.Lock()
	delete(h.live, id)
	h.mu.Unlock()
	if err := os.Remove(filepath.Join(h.instDir(id), "pid")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("proc: remove pid %s: %w", id, err)
	}
	return nil
}

func (h *ProcHandle) Undefine(ctx context.Context, id string) error {
	_ = h.Stop(ctx, id)
	if err := os.RemoveAll(h.instDir(id)); err != nil {
		return fmt.Errorf("proc: undefine %s: %w", id, err)
	}
	h.mu.Lock()
	delete(h.live, id)
	h.mu.Unlock()
	return nil
}

func (h *ProcHandle) Inspect(ctx context.Context, id string) (Info, error) {
	if err := h.validateID(id); err != nil {
		return Info{}, err
	}
	dir := h.instDir(id)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return Info{ID: id, Status: StatusAbsent}, nil
	}
	h.mu.Lock()
	pid, hasPid := h.readPidLocked(id)
	sess, hasSess := h.live[id]
	h.mu.Unlock()
	if !hasPid {
		return Info{ID: id, Status: StatusStopped, Detail: "defined, not running"}, nil
	}
	if processAlive(pid) {
		return Info{ID: id, Status: StatusRunning, Pid: pid, Detail: "alive"}, nil
	}
	if hasSess {
		select {
		case <-sess.doneCh:
		default:
		}
	}
	return Info{ID: id, Status: StatusStopped, Pid: pid, Detail: "stale pid file: guest exited abnormally (crash or kill -9)"}, nil
}

func (h *ProcHandle) List(ctx context.Context) ([]Info, error) {
	entries, err := os.ReadDir(h.cfg.Dir)
	if err != nil {
		return nil, err
	}
	var out []Info
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := h.Inspect(ctx, e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

func (h *ProcHandle) validateID(id string) error {
	if id == "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return fmt.Errorf("proc: unsafe instance id %q", id)
	}
	return nil
}

func (h *ProcHandle) guestCommand(id string) (string, []string) {
	if h.cfg.GuestBin != "" {
		args := make([]string, 0, len(h.cfg.GuestArgs)+2)
		args = append(args, h.cfg.GuestArgs...)
		args = append(args, "--id", id)
		return h.cfg.GuestBin, args
	}
	return "sleep", []string{"86400"}
}

// readPidLocked 读取 pid 文件；损坏或缺失返回 false。
func (h *ProcHandle) readPidLocked(id string) (int, bool) {
	data, err := os.ReadFile(filepath.Join(h.instDir(id), "pid"))
	if err != nil {
		return 0, false
	}
	var v struct {
		Pid int `json:"pid"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return 0, false
	}
	if v.Pid <= 0 {
		return 0, false
	}
	return v.Pid, true
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return signalProcess(pid, syscall.Signal(0)) == nil
}

func signalProcess(pid int, sig syscall.Signal) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := p.Signal(sig); err != nil {
		return err
	}
	return nil
}
