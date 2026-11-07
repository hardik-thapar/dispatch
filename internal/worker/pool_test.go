package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// runPool must run every job it is given, with at most `concurrency` jobs at the same time.
func TestRunPool_RunsAllJobsWithLimitedConcurrency(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	var mu sync.Mutex
	running, maxRunning := 0, 0

	reg := handlers.NewRegistry()
	reg.Register("work", func(ctx context.Context, job queue.Job) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		mu.Unlock()

		time.Sleep(200 * time.Millisecond) // pretend to work

		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})

	// six due jobs, all claimed up front
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'tenant-a', 'work', 5, 'k' || g FROM generate_series(1, 6) g`)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}
	claimed, err := queue.Claim(ctx, pool, "w1", 6, 30*time.Second)
	if err != nil || len(claimed) != 6 {
		t.Fatalf("claim: jobs = %d, err = %v", len(claimed), err)
	}

	jobs := make(chan queue.Job, len(claimed))
	for _, j := range claimed {
		jobs <- j
	}
	close(jobs) // no more jobs: the pool should finish and return

	newTestWorker(pool, reg).runPool(ctx, 2, jobs)

	stats, err := queue.GetStats(ctx, pool, "")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Succeeded != 6 {
		t.Errorf("succeeded = %d, want 6", stats.Succeeded)
	}
	if maxRunning != 2 {
		t.Errorf("max jobs running at once = %d, want exactly 2", maxRunning)
	}
}
