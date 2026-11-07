package worker

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// On shutdown, Run must stop claiming new jobs but let running jobs finish.
func TestRun_ShutdownFinishesInFlightJobs(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := handlers.NewRegistry()
	reg.Register("slow", func(ctx context.Context, job queue.Job) error {
		time.Sleep(500 * time.Millisecond) // takes a while, ignores ctx
		return nil
	})

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'tenant-a', 'slow', 5, 'k' || g FROM generate_series(1, 5) g`)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}

	w := newTestWorker(pool, reg)
	w.pollInterval = 20 * time.Millisecond

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx, 3) }()

	// wait until 3 jobs are running
	deadline := time.Now().Add(3 * time.Second)
	for {
		stats, err := queue.GetStats(context.Background(), pool, "")
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.Running == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("jobs never started running, stats = %+v", stats)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel() // shutdown signal

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}

	stats, err := queue.GetStats(context.Background(), pool, "")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Succeeded != 3 {
		t.Errorf("succeeded = %d, want 3 (running jobs must finish and be recorded)", stats.Succeeded)
	}
	if stats.Queued != 2 {
		t.Errorf("queued = %d, want 2 (no new jobs may be claimed after shutdown)", stats.Queued)
	}
}

func TestRun_RejectsBadConcurrency(t *testing.T) {
	pool := testutil.NewTestPool(t)
	w := newTestWorker(pool, handlers.NewRegistry())
	if err := w.Run(context.Background(), 0); err == nil {
		t.Error("Run with concurrency 0 returned nil, want an error")
	}
}
