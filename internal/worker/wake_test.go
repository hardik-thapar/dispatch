package worker

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// With instant jobs and a LONG poll interval, throughput must not be limited by the poll interval:
// a freed slot has to wake the claim loop at once.
func TestRun_FreedSlotWakesTheClaimLoop(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := handlers.NewRegistry()
	reg.Register("fast", func(ctx context.Context, job queue.Job) error { return nil })

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'tenant-a', 'fast', 5, 'k' || g FROM generate_series(1, 100) g`)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}

	w := newTestWorker(pool, reg)
	w.pollInterval = time.Second // long on purpose

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx, 4) }()

	start := time.Now()
	for {
		stats, err := queue.GetStats(context.Background(), pool, "")
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.Succeeded == 100 {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("only %d of 100 jobs finished in 10s: the claim loop is sleeping through its poll interval", stats.Succeeded)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("100 instant jobs took %v, want well under 5s", elapsed)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}
