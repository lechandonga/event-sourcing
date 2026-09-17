// 演示：本地事件溯源的开户 -> 并发写入冲突 -> 历史事件升级 -> 投影增量/重建/恢复。
// 所有数据只落在内存（去掉 --mem 即落本地文件），不依赖任何外部服务。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/lechandonga/event-sourcing/bank"
	"github.com/lechandonga/event-sourcing/eventstore"
	"github.com/lechandonga/event-sourcing/projection"
	"github.com/lechandonga/event-sourcing/snapshot"
)

func main() {
	mem := flag.Bool("mem", true, "use in-memory storage (false: local files under ./.demo-data)")
	flag.Parse()

	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var store eventstore.Store
	var cps projection.CheckpointStore
	var fls projection.FailureStore
	var snaps snapshot.Store

	if *mem {
		store = eventstore.NewMemoryStore()
		cps = projection.NewMemoryCheckpointStore()
		fls = projection.NewMemoryFailureStore()
		snaps = snapshot.NewMemoryStore()
	} else {
		dir := filepath.Join(".", ".demo-data")
		fs, err := eventstore.OpenFileStore(ctx, filepath.Join(dir, "events.log"))
		must(err)
		store = fs
		cps = projection.NewFileCheckpointStore(filepath.Join(dir, "checkpoints"))
		fls = projection.NewFileFailureStore(filepath.Join(dir, "failures"))
		snaps = snapshot.NewFileStore(filepath.Join(dir, "snapshots"))
	}
	defer store.Close(ctx)

	registry := bank.NewRegistry()

	// 1) 写侧：开户与并存入 500.00（分）。
	acc, err := bank.OpenAccount("acct-1", "Ada Lovelace", 500_00, "USD")
	must(err)
	repoInst := bank.NewAccountRepository(bank.AccountRepositoryConfig{
		Store: store, Registry: registry, Snapshots: snaps, Threshold: 10,
	})
	ver, err := bank.SaveAccount(ctx, repoInst, acc)
	must(err)
	fmt.Printf("opened account, stream version=%d\n", ver)

	// 2) 乐观并发冲突：两个基于同一版本的写入，只有一个生效。
	a1, err := bank.LoadAccount(ctx, repoInst, "acct-1")
	must(err)
	a2, err := bank.LoadAccount(ctx, repoInst, "acct-1")
	must(err)
	must(a1.Deposit(100_00, "paycheck"))
	_, err = bank.SaveAccount(ctx, repoInst, a1)
	must(err)
	must(a2.Withdraw(50_00, "rent"))
	_, err = bank.SaveAccount(ctx, repoInst, a2)
	if err != nil {
		var ce *eventstore.ConflictError
		if errors.As(err, &ce) {
			fmt.Printf("expected conflict rejected: reason=%s expected=%d actual=%d\n",
				ce.Reason, ce.Expected, ce.Actual)
		} else {
			must(err)
		}
	}
	fresh, err := bank.LoadAccount(ctx, repoInst, "acct-1")
	must(err)
	fmt.Printf("balance after conflict = %d cents (deposit kept, stale withdrawal rejected)\n",
		fresh.BalanceCents())

	// 3) 直接以 v1 旧结构写入一个历史事件（元 -> 升级时换算为分）。
	old := bank.MoneyV1{Amount: 3}.MarshalV1() // 3 元
	_, err = store.Append(ctx,
		eventstore.StreamID{Type: bank.AccountType, ID: "acct-1"},
		eventstore.ExpectedVersion(fresh.Version()),
		[]eventstore.UncommittedEvent{{Type: "bank.MoneyDeposited", Data: old}})
	must(err)

	// 4) 投影：增量处理并展示跨版本升级结果。
	model := bank.NewSummaryReadModel()
	proj, err := projection.New(projection.Config{
		Name: "account-summary", Store: store, Model: model, Registry: registry,
		Checkpoints: cps, Failures: fls, Logger: log,
	})
	must(err)
	must(proj.CatchUp(ctx))
	row := model.Get("acct-1")
	fmt.Printf("projection: balance=%d deposits=%d (v1-schema deposits=%d)\n",
		row.BalanceCents, row.DepositCount, row.DepositCountV1)

	// 5) 在线重建（不阻塞写入）：重建期间追加事件，重建后必须收敛一致。
	done := make(chan struct{})
	go func() {
		defer close(done)
		// 并发写入采用 CAS 重试：版本冲突是预期行为，重新加载、重放命令后再存，
		// 20 笔存款最终都会精确生效一次。
		for i := 0; i < 20; i++ {
			for attempt := 0; ; attempt++ {
				a, lerr := bank.LoadAccount(ctx, repoInst, "acct-1")
				if lerr != nil {
					return
				}
				if lerr = a.Deposit(1, "rebuild concurrent"); lerr != nil {
					return
				}
				if _, lerr = bank.SaveAccount(ctx, repoInst, a); lerr != nil {
					if errors.As(lerr, new(*eventstore.ConflictError)) && attempt < 100 {
						continue
					}
					return
				}
				break
			}
		}
	}()
	must(proj.Rebuild(ctx))
	<-done
	must(proj.CatchUp(ctx))
	cp, err := proj.Checkpoint(ctx)
	must(err)
	lastSeq, err := store.LastSequence(ctx)
	must(err)
	fmt.Printf("rebuilt projection caught up: checkpoint=%d store=%d deposits=%d balance=%d\n",
		cp.Sequence, lastSeq, model.Get("acct-1").DepositCount, model.Get("acct-1").BalanceCents)

	// 6) 快照恢复演示。
	snapAcc, err := bank.LoadAccount(ctx, repoInst, "acct-1")
	must(err)
	fmt.Printf("reloaded via snapshot+replay: version=%d balance=%d\n",
		snapAcc.Version(), snapAcc.BalanceCents())
	fmt.Println("demo OK")
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo error:", err)
		os.Exit(1)
	}
}
