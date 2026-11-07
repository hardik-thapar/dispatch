package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// A handler that hangs must be stopped by the per-job timeout,
// and the job must be recorded as a failure that says "timed out".
func TestRunJob_TimeoutStopsHungHandler(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("hang", func(ctx context.Context, job queue.Job) error {
		<-ctx.Done() // wait until the timeout cancels us
		return ctx.Err()
	})
	job := claimOne(t, pool, "hang", 5)
	job.MaxRunTime = 1 // seconds: shorten the limit for this test

	start := time.Now()
	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("runJob took %v, want about 1s (the timeout)", elapsed)
	}
	got, err := queue.Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "queued" {
		t.Errorf("status = %q, want queued (a timeout counts as a failure, attempts are left)", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "timed out") {
		t.Errorf("last_error = %v, want it to mention \"timed out\"", got.LastError)
	}
}
