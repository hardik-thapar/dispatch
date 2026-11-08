package worker

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// A job with a 2 second lease must still be leased after 3 seconds,
// because the heartbeat keeps extending the lease.
func TestHeartbeatLoop_KeepsLeaseAlive(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 VALUES ('tenant-a', 'x', 5, 'k1')`)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	jobs, err := queue.Claim(ctx, pool, "w1", 1, 2*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(jobs), err)
	}

	w := newTestWorker(pool, handlers.NewRegistry()) // its id is "w1"
	w.lease = 2 * time.Second

	done := make(chan struct{})
	go func() {
		w.heartbeatLoop(ctx, 200*time.Millisecond)
		close(done)
	}()

	time.Sleep(3 * time.Second) // longer than the 2 second lease

	var stillLeased bool
	err = pool.QueryRow(ctx,
		`SELECT lease_end > now() FROM jobs WHERE id = $1`, jobs[0].ID).Scan(&stillLeased)
	if err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if !stillLeased {
		t.Error("the lease has expired after 3 seconds, want it kept alive by the heartbeat")
	}

	cancel() // the loop must stop when its context is cancelled
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not stop after the context was cancelled")
	}
}