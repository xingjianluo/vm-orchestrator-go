// Command vmctl 是本地虚拟机生命周期编排的命令行入口。
//
// 状态默认保存在 --state-dir（记录）与 --backend-dir（进程承载目录）。
// 每个变更子命令在启动时都会先执行一次存储恢复 + 对账，
// 因此即使宿主/进程异常重启，操作前也能看到确定的对账结论。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/xingjianluo/vm-orchestrator-go/internal/backend"
	"github.com/xingjianluo/vm-orchestrator-go/internal/domain"
	"github.com/xingjianluo/vm-orchestrator-go/internal/orchestrator"
	"github.com/xingjianluo/vm-orchestrator-go/internal/store"
)

type globalFlags struct {
	stateDir   string
	backendDir string
	useSim     bool
	cpu        int64
	mem        int64
	disk       int64
	capCPU     int64
	capMem     int64
	capDisk    int64
	image      string
	json       bool
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := reorderFlags(os.Args[2:])

	g := &globalFlags{}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.StringVar(&g.stateDir, "state-dir", "./var/state", "directory for instance records")
	fs.StringVar(&g.backendDir, "backend-dir", "./var/run", "directory for process backend payloads")
	fs.BoolVar(&g.useSim, "sim", false, "use in-process simulated backend instead of real child processes")
	fs.Int64Var(&g.cpu, "cpu", 1000, "instance cpu millicores")
	fs.Int64Var(&g.mem, "mem", 512, "instance memory MB")
	fs.Int64Var(&g.disk, "disk", 1024, "instance disk MB")
	fs.Int64Var(&g.capCPU, "capacity-cpu", 8000, "host cpu millicores capacity")
	fs.Int64Var(&g.capMem, "capacity-mem", 4096, "host memory MB capacity")
	fs.Int64Var(&g.capDisk, "capacity-disk", 16384, "host disk MB capacity")
	fs.StringVar(&g.image, "image", "ubuntu-24.04", "instance image")
	fs.BoolVar(&g.json, "json", false, "emit machine-readable JSON for list/quota/reconcile")
	_ = fs.Parse(args)
	rest := fs.Args()

	ctx := context.Background()
	if err := run(ctx, cmd, rest, g); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		var oe *domain.OpError
		if errors.As(err, &oe) {
			fmt.Fprintf(os.Stderr, "kind=%s\n", oe.Kind)
			os.Exit(exitCodeFor(oe.Kind))
		}
		os.Exit(1)
	}
}

// reorderFlags 将所有 -flag/--flag 及其取值移到位置参数之前，
// 从而允许 `vmctl create web --cpu 1000` 这种标志在位置参数之后的写法
// （标准库 flag 包遇到第一个位置参数即停止解析）。
func reorderFlags(argv []string) []string {
	boolFlags := map[string]bool{"json": true, "sim": true}
	flags := make([]string, 0, len(argv))
	pos := make([]string, 0, len(argv))
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		if strings.HasPrefix(a, "-") {
			key := strings.TrimLeft(a, "-")
			// 支持 --k=v 形式：整段视为一个 token。
			flags = append(flags, a)
			if !strings.Contains(key, "=") && !boolFlags[strings.SplitN(key, ".", 2)[0]] {
				if i+1 < len(argv) {
					i++
					flags = append(flags, argv[i])
				}
			}
		} else {
			pos = append(pos, a)
		}
	}
	return append(flags, pos...)
}

func exitCodeFor(k domain.ErrorKind) int {
	switch k {
	case domain.KindNotFound:
		return 3
	case domain.KindAlreadyExists:
		return 4
	case domain.KindIllegalTransition:
		return 5
	case domain.KindInsufficientResource:
		return 6
	case domain.KindBackendFailure:
		return 7
	case domain.KindInconsistentState:
		return 8
	default:
		return 1
	}
}

func run(ctx context.Context, cmd string, args []string, g *globalFlags) error {
	st, err := store.NewFileStore(filepath.Join(g.stateDir, "instances"))
	if err != nil {
		return err
	}
	h, err := newBackend(g)
	if err != nil {
		return err
	}
	o := orchestrator.New(st, h, domain.Resources{CPUMillis: g.capCPU, MemoryMB: g.capMem, DiskMB: g.capDisk},
		log.Printf)

	switch cmd {
	case "create":
		if len(args) < 1 {
			return errors.New("usage: vmctl create <id> [--image ...] [--cpu ...] ...")
		}
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		id := args[0]
		inst, err := o.Create(ctx, id, id, domain.Spec{
			Image:     g.image,
			Resources: domain.Resources{CPUMillis: g.cpu, MemoryMB: g.mem, DiskMB: g.disk},
		})
		if err != nil {
			return err
		}
		printInstance(inst, true)
		return nil

	case "start", "stop", "destroy":
		if len(args) < 1 {
			return fmt.Errorf("usage: vmctl %s <id>", cmd)
		}
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		id := args[0]
		var inst *domain.Instance
		switch cmd {
		case "start":
			inst, err = o.Start(ctx, id)
		case "stop":
			inst, err = o.Stop(ctx, id)
		case "destroy":
			inst, err = o.Destroy(ctx, id)
		}
		if err != nil {
			return err
		}
		printInstance(inst, true)
		return nil

	case "get":
		if len(args) < 1 {
			return errors.New("usage: vmctl get <id>")
		}
		inst, err := o.Get(ctx, args[0])
		if err != nil {
			return err
		}
		printInstance(inst, g.json)
		return nil

	case "list":
		insts, err := o.List(ctx)
		if err != nil {
			return err
		}
		if g.json {
			return emitJSON(insts)
		}
		fmt.Printf("%-20s %-10s %-12s %s\n", "ID", "STATE", "BACKEND", "LAST_EVENT")
		for _, in := range insts {
			fmt.Printf("%-20s %-10s %-12s %s\n", in.ID, in.State, in.Backend, in.LastEvent)
		}
		return nil

	case "quota":
		snap := o.ResourceSnapshot()
		if g.json {
			return emitJSON(snap)
		}
		fmt.Printf("capacity cpu=%d mem=%dMB disk=%dMB\n", snap.Capacity.CPUMillis, snap.Capacity.MemoryMB, snap.Capacity.DiskMB)
		fmt.Printf("used     cpu=%d mem=%dMB disk=%dMB\n", snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB)
		fmt.Printf("avail    cpu=%d mem=%dMB disk=%dMB\n", snap.Available.CPUMillis, snap.Available.MemoryMB, snap.Available.DiskMB)
		fmt.Printf("reservations=%d\n", len(snap.Reservations))
		for id, r := range snap.Reservations {
			fmt.Printf("  - %s cpu=%d mem=%d disk=%d\n", id, r.CPUMillis, r.MemoryMB, r.DiskMB)
		}
		return nil

	case "reconcile":
		if err := o.RestoreFromStore(ctx); err != nil {
			log.Printf("restore warning: %v (继续执行对账)", err)
		}
		rep, err := o.Reconcile(ctx, log.Printf)
		if err != nil {
			return err
		}
		if g.json {
			return emitJSON(rep)
		}
		fmt.Println("== reconcile verdicts ==")
		for _, v := range rep.Verdicts {
			fmt.Printf("- %-16s record=%-10s actual=%-10s outcome=%-11s after=%s\n      action=%s detail=%s\n",
				v.InstanceID, v.RecordState, v.ActualState, v.Outcome, v.After, v.Action, v.Detail)
		}
		fmt.Printf("orphans=%v quarantine=%v quotaConverged=%t\n  %s\n",
			rep.Orphans, rep.Quarantine, rep.QuotaConverged, rep.QuotaDetail)
		return nil

	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func newBackend(g *globalFlags) (backend.Handle, error) {
	if g.useSim {
		return backend.NewSimHandle(backend.FaultHooks{}), nil
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	// vmguest 与 vmctl 同目录；若不存在则回退到 sleep 占位。
	guestPath := filepath.Join(filepath.Dir(exe), "vmguest")
	if _, err := os.Stat(guestPath); err != nil {
		guestPath = ""
	}
	return backend.NewProcHandle(backend.ProcConfig{
		Dir:      g.backendDir,
		GuestBin: guestPath,
	})
}

func printInstance(in *domain.Instance, asJSON bool) {
	if asJSON {
		if err := emitJSON(in); err == nil {
			return
		}
	}
	fmt.Printf("instance %s: state=%s backend=%s image=%s cpu=%d mem=%dMB disk=%dMB\n  updated=%s event=%s\n",
		in.ID, in.State, in.Backend, in.Spec.Image,
		in.Spec.Resources.CPUMillis, in.Spec.Resources.MemoryMB, in.Spec.Resources.DiskMB,
		in.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"), in.LastEvent)
}

func emitJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func usage() {
	fmt.Fprint(os.Stderr, `vmctl - local VM lifecycle orchestrator

usage: vmctl <command> [flags] [args]

commands:
  create <id>   create instance (admission first, then backend define)
  start <id>    Defined/Stopped -> Running (Running is idempotent)
  stop <id>     Running -> Stopped (Stopped is idempotent)
  destroy <id>  Defined/Stopped -> Destroyed (frees quota; tombstone kept)
  get <id>      show one instance record
  list          list instance records
  quota         show host quota snapshot and per-instance reservations
  reconcile     run restart reconciliation and print verdicts

flags (common):
  --state-dir, --backend-dir, --sim
  --cpu, --mem, --disk (create)
  --capacity-cpu, --capacity-mem, --capacity-disk
  --json (machine-readable output)
`)
}
