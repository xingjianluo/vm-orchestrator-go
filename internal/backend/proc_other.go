//go:build !(linux || darwin)

package backend

// 非 Unix 平台的降级实现：不做精确的进程存活/进程组支持，
// 仅保证工程可编译；生产请使用 Linux 宿主。

func processAlive(pid int) bool      { return false }
func processStartUnix(pid int) int64 { return 0 }
func killGroup(pid int) error        { return nil }
