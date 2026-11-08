package worker

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// A job that runs longer than its lease must still be leased when it ends,
// because Run sends heartbeats while the job is running.
func TestRun_HeartbeatKeepsLongJobLeased(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leased := make(chan bool, 1)
	reg := handlers.NewRegistry()
	reg.Register("long", func(ctx context.Context, job queue.Job) error {
		time.Sleep(2500 * time.Millisecond) // longer than the 1 second lease
		var stillLeased bool
		err := pool.QueryRow(ctx,
			`SELECT lease_end > now() FROM jobs WHERE id = $1`, job.ID).Scan(&stillLeased)
		leased <- stillLeased
		return err
	})

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 VALUES ('tenant-a', 'long', 5, 'k1')`)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}

	w := newTestWorker(pool, reg)
	w.lease = time.Second
	w.pollInterval = 20 * time.Millisecond

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx, 1) }()

	select {
	case stillLeased := <-leased:
		if !stillLeased {
			t.Error("the lease expired while the job was running, want Run's heartbeat to keep it alive")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("the job never finished")
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}
