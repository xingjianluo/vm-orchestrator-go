// Command vmctl 是本地虚拟机生命周期编排的命令行入口。
//
// 状态默认保存在 --state-dir（记录）与 --backend-dir（进程承载目录）。
// 每个子命令在启动时都会先执行一次 RestoreFromStore 重建内存配额台账，
// 因此无论新起多少个进程、什么时候查询，占用都与持久化记录一致；
// 即使重建占用超过当前容量（如对账落盘的超大残留），查询/resolve 也照常工作，
// 超容量只在 quota/reconcile 输出中告警并把新增 create 拒绝为 host_overcommitted。
// 变更/对账命令还会按需执行对账，把无法自动判定的偏差持久化为 ManualReview，
// 由 resolve adopt/discard 人工收口。
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
	target     string
	note       string
	claim      bool
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
	fs.BoolVar(&g.json, "json", false, "emit machine-readable JSON for list/quota/reconcile/reviews")
	fs.StringVar(&g.target, "to", "Running", "resolve adopt target state: Defined|Running|Stopped")
	fs.StringVar(&g.note, "note", "", "operator note recorded on manual resolution")
	fs.BoolVar(&g.claim, "claim", false, "adopt an orphan whose spec is unreadable: register quota using --cpu/--mem/--disk")
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
	boolFlags := map[string]bool{"json": true, "sim": true, "claim": true}
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
	case domain.KindManualReviewPending:
		return 9
	case domain.KindHostOvercommitted:
		// 宿主占用已超容量（如对账重建出超大残留）：仅拒绝新增占用的准入，
		// 与资源不足、待人工处理、内部不一致等原因明确区分。
		return 10
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
		// 新进程也必须先按持久化记录重建台账，保证"什么时候查、换哪个进程"占用都与事实一致。
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		inst, err := o.Get(ctx, args[0])
		if err != nil {
			return err
		}
		printInstance(inst, g.json)
		return nil

	case "list":
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		insts, err := o.List(ctx)
		if err != nil {
			return err
		}
		if g.json {
			return emitJSON(insts)
		}
		fmt.Printf("%-20s %-13s %-12s %s\n", "ID", "STATE", "BACKEND", "LAST_EVENT")
		for _, in := range insts {
			fmt.Printf("%-20s %-13s %-12s %s\n", in.ID, in.State, in.Backend, in.LastEvent)
			if in.State == domain.StateManualReview && in.Review != nil {
				fmt.Printf("%-20s ** MANUAL REVIEW ** reason=%s actual=%s detail=%s\n",
					"", in.Review.Reason, in.Review.ActualState, in.Review.Detail)
			}
		}
		return nil

	case "reviews":
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		insts, err := o.ManualReviewList(ctx)
		if err != nil {
			return err
		}
		if g.json {
			return emitJSON(insts)
		}
		if len(insts) == 0 {
			fmt.Println("no instances pending manual review")
			return nil
		}
		for _, in := range insts {
			fmt.Printf("- %-16s reason=%-24s record=%-10s actual=%-8s raised=%s\n",
				in.ID, in.Review.Reason, in.Review.RecordState, in.Review.ActualState,
				in.Review.RaisedAt.Format("2006-01-02T15:04:05Z07:00"))
			fmt.Printf("    %s\n", in.Review.Detail)
		}
		return nil

	case "resolve":
		// vmctl resolve <adopt|discard> <id> [--to Defined|Running|Stopped] [--note ...]
		// 孤儿残留读不出规格时，adopt 还需 --cpu/--mem/--disk 补报。
		if len(args) < 2 {
			return errors.New("usage: vmctl resolve <adopt|discard> <id> [--to ...] [--note ...]")
		}
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		decision, id := args[0], args[1]
		var target domain.State
		switch domain.State(g.target) {
		case domain.StateDefined, domain.StateRunning, domain.StateStopped:
			target = domain.State(g.target)
		default:
			return fmt.Errorf("invalid --to %q (want Defined|Running|Stopped)", g.target)
		}
		var provided *domain.Spec
		if g.claim {
			s := domain.Spec{Image: g.image, Resources: domain.Resources{CPUMillis: g.cpu, MemoryMB: g.mem, DiskMB: g.disk}}
			provided = &s
		}
		var inst *domain.Instance
		switch orchestrator.ResolveDecision(decision) {
		case orchestrator.ResolveAdopt:
			inst, err = o.Resolve(ctx, id, orchestrator.ResolveAdopt, target, provided, g.note)
		case orchestrator.ResolveDiscard:
			inst, err = o.Resolve(ctx, id, orchestrator.ResolveDiscard, target, nil, g.note)
		default:
			return fmt.Errorf("unknown resolve decision %q (want adopt|discard)", decision)
		}
		if err != nil {
			return err
		}
		printInstance(inst, g.json)
		return nil

	case "quota":
		if err := o.RestoreFromStore(ctx); err != nil {
			return err
		}
		snap := o.ResourceSnapshot()
		if g.json {
			return emitJSON(snap)
		}
		fmt.Printf("capacity cpu=%d mem=%dMB disk=%dMB\n", snap.Capacity.CPUMillis, snap.Capacity.MemoryMB, snap.Capacity.DiskMB)
		fmt.Printf("used     cpu=%d mem=%dMB disk=%dMB\n", snap.Used.CPUMillis, snap.Used.MemoryMB, snap.Used.DiskMB)
		fmt.Printf("avail    cpu=%d mem=%dMB disk=%dMB\n", snap.Available.CPUMillis, snap.Available.MemoryMB, snap.Available.DiskMB)
		if snap.OverCommit {
			// 超容量是可读、自洽的对账结论：列表/查询/收口照常工作，
			// 只有会新增占用的 create 会被 host_overcommitted（退出码 10）拒绝。
			fmt.Fprintf(os.Stderr, "WARNING: host OVER-COMMIT exceeded=%v (rebuilt usage exceeds capacity; resolve ManualReview instances via resolve adopt/discard)\n",
				snap.Exceeded)
		}
		fmt.Printf("reservations=%d\n", len(snap.Reservations))
		for id, r := range snap.Reservations {
			fmt.Printf("  - %s cpu=%d mem=%d disk=%d\n", id, r.CPUMillis, r.MemoryMB, r.DiskMB)
		}
		return nil

	case "reconcile":
		if err := o.RestoreFromStore(ctx); err != nil {
			// 超容量已不再是 restore 失败原因（只告警）；到这里说明记录本身非法，
			// 仍继续执行对账以便暴露/收敛，并把原始问题留在日志里。
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
		fmt.Printf("orphans=%v quarantine=%v quotaConverged=%t capacityExceeded=%t exceeded=%v\n  %s\n",
			rep.Orphans, rep.Quarantine, rep.QuotaConverged, rep.CapacityExceeded, rep.ExceededDimensions, rep.QuotaDetail)
		if rep.CapacityExceeded {
			// 超容量只是对账结论，不是失败：本命令仍以 0 退出。
			fmt.Fprintf(os.Stderr, "WARNING: rebuilt usage exceeds host capacity on %v; queries and resolve stay available, new create is rejected (exit 10) until resolved\n",
				rep.ExceededDimensions)
		}
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
	if in.State == domain.StateManualReview && in.Review != nil {
		fmt.Printf("  *** MANUAL REVIEW PENDING ***\n  reason=%s record=%s actual=%s specKnown=%t raised=%s\n  detail=%s\n",
			in.Review.Reason, in.Review.RecordState, in.Review.ActualState,
			in.Review.SpecKnown, in.Review.RaisedAt.Format("2006-01-02T15:04:05Z07:00"),
			in.Review.Detail)
	}
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
  resolve <adopt|discard> <id>
                close a ManualReview instance: adopt to --to state (resource
                stays reserved; use --claim with --cpu/--mem/--disk for orphans
                whose spec.json is unreadable) or discard (undefine + free)
  reviews       list instances pending manual review (with reason)
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
