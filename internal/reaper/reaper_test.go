package reaper

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestRun_RecoversExpiredJobs(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// a running job whose worker died: its lease ended 5 seconds ago
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key, status, attempts, lease_owner, lease_end)
		 VALUES ('tenant-a', 'x', 5, 'k1', 'running', 1, 'w-dead', now() - interval '5 seconds')`)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}

	done := make(chan struct{})
	go func() {
		Run(ctx, pool, 50*time.Millisecond)
		close(done)
	}()

	// wait until the reaper has put the job back in the queue
	deadline := time.Now().Add(3 * time.Second)
	for {
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE idempotency_key = 'k1'`).Scan(&status); err != nil {
			t.Fatalf("read status: %v", err)
		}
		if status == "queued" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job is still %q after 3 seconds, want queued", status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel() // the loop must stop when its context is cancelled
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reaper.Run did not stop after the context was cancelled")
	}
}
