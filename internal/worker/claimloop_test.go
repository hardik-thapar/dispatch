package worker

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// The claim loop must never claim more jobs than there are free slots.
func TestClaimLoop_ClaimsOnlyFreeSlots(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'tenant-a', 'work', 5, 'k' || g FROM generate_series(1, 5) g`)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}

	w := newTestWorker(pool, handlers.NewRegistry())
	w.pollInterval = 20 * time.Millisecond

	jobs := make(chan queue.Job, 3)
	finished := make(chan struct{})
	go func() {
		w.claimLoop(ctx, 3, jobs) // 3 slots
		close(finished)
	}()

	// Nobody takes jobs from the channel, so all 3 slots stay busy.
	// The loop must claim exactly 3 of the 5 jobs and then wait.
	time.Sleep(500 * time.Millisecond)
	stats, err := queue.GetStats(ctx, pool, "")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Running != 3 || stats.Queued != 2 {
		t.Errorf("after first claim: running = %d, queued = %d, want 3 and 2", stats.Running, stats.Queued)
	}
	if len(jobs) != 3 {
		t.Errorf("jobs in channel = %d, want 3", len(jobs))
	}

	// One job finishes: one slot is free, so exactly one more job is claimed.
	<-jobs
	w.inFlight.Add(-1)
	time.Sleep(500 * time.Millisecond)
	stats, err = queue.GetStats(ctx, pool, "")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Running != 4 || stats.Queued != 1 {
		t.Errorf("after a slot freed: running = %d, queued = %d, want 4 and 1", stats.Running, stats.Queued)
	}

	// Cancelling the context must stop the loop.
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("claimLoop did not stop after the context was cancelled")
	}
}
