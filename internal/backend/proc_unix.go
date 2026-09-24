//go:build linux

package backend

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// procStat 解析 /proc/<pid>/stat：返回 (state 单字符, starttime 时钟滴答数, ok)。
// comm 字段(第 2 列)可能包含空格与括号，因此以最后一个 ')' 为界切分。
// ')' 之后的字段依次为 state(第3列) ... starttime(第22列)，
// 因此 state 下标 0、starttime 下标 19。
func procStat(pid int) (byte, int64, bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, false
	}
	s := string(data)
	rp := strings.LastIndexByte(s, ')')
	if rp < 0 {
		return 0, 0, false
	}
	rest := strings.Fields(s[rp+1:])
	const (
		stateIdx     = 0  // 第 3 列
		starttimeIdx = 19 // 第 22 列
	)
	if len(rest) <= starttimeIdx || len(rest[stateIdx]) == 0 {
		return 0, 0, false
	}
	ticks, err := strconv.ParseInt(rest[starttimeIdx], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return rest[stateIdx][0], ticks, true
}

// processAlive 判断 pid 是否存活。
// 僵尸进程(Z)在 kill(0) 看来仍“存在”，但已不是运行中的承载实体，
// 通过 /proc/<pid>/stat 显式排除，避免崩溃退出被误判为运行中。
func processAlive(pid int) bool {
	if state, _, ok := procStat(pid); ok {
		return state != 'Z'
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	if pe, ok := err.(syscall.Errno); ok && pe == syscall.EPERM {
		return true
	}
	return false
}

// processStartUnix 返回进程启动时钟滴答数，仅用于同一 PID 前后对比识别 PID 复用。
func processStartUnix(pid int) int64 {
	if _, ticks, ok := procStat(pid); ok {
		return ticks
	}
	return 0
}

// killGroup 向以 pid 为组长的整个进程组发 SIGKILL；组不存在视为成功。
func killGroup(pid int) error {
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
