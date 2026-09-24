// Command vmguest 是进程级后端承载的"客户机"进程：
// 启动后常驻，收到 SIGTERM/SIGINT 优雅退出；被 SIGKILL 杀死时表现为异常崩溃，
// 供编排器重启后的对账逻辑识别（pid 残留但探活失败）。
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	id := flag.String("id", "", "instance id")
	flag.Parse()

	log.SetPrefix(fmt.Sprintf("[vmguest id=%s pid=%d] ", *id, os.Getpid()))
	log.Printf("boot: guest process starting, image workload simulated")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()

	for {
		select {
		case sig := <-sigCh:
			log.Printf("shutdown: received %s, exiting cleanly", sig)
			return
		case <-tick.C:
			log.Printf("heartbeat: still running")
		}
	}
}
