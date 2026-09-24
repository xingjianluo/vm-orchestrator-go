//go:build darwin

package backend

import "syscall"

// processAlive 判断 pid 是否存活（信号 0 探测；EPERM 视为存活）。
// Darwin 无 /proc，僵尸窗口在 Wait goroutine 下极短；测试以 Linux 为准。
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	if pe, ok := err.(syscall.Errno); ok && pe == syscall.EPERM {
		return true
	}
	return false
}

func processStartUnix(pid int) int64 { return 0 }

// killGroup 向以 pid 为组长的整个进程组发 SIGKILL。
func killGroup(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
