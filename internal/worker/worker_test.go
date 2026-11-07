package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/retry"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// claimOne inserts one due job of the given type and claims it as worker w1.
func claimOne(t *testing.T, pool *pgxpool.Pool, jobType string, maxAttempts int) queue.Job {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 VALUES ('tenant-a', $1, $2, 'k1')`, jobType, maxAttempts)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	jobs, err := queue.Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(jobs), err)
	}
	return jobs[0]
}

func newTestWorker(pool *pgxpool.Pool, reg *handlers.Registry) *Worker {
	return New(pool, reg, retry.Backoff{Base: time.Second, Max: time.Minute}, "w1")
}

func TestRunJob_Success(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("ok", func(ctx context.Context, job queue.Job) error { return nil })
	job := claimOne(t, pool, "ok", 5)

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	got, err := queue.Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "succeeded" {
		t.Errorf("status = %q, want succeeded", got.Status)
	}
}

func TestRunJob_HandlerErrorRetries(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("bad", func(ctx context.Context, job queue.Job) error { return errors.New("boom") })
	job := claimOne(t, pool, "bad", 5)

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	got, err := queue.Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "queued" {
		t.Errorf("status = %q, want queued (attempts are left)", got.Status)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("last_error = %v, want boom", got.LastError)
	}
	// attempt 1 with Base 1s: the wait must be between 0.5s and 1s
	if d := got.RunAt.Sub(got.UpdatedAt); d < 400*time.Millisecond || d > 1100*time.Millisecond {
		t.Errorf("run_at is %v after updated_at, want between 0.5s and 1s", d)
	}
}

func TestRunJob_LastAttemptGoesDead(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("bad", func(ctx context.Context, job queue.Job) error { return errors.New("boom") })
	job := claimOne(t, pool, "bad", 1) // only one attempt allowed

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	got, err := queue.Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "dead" {
		t.Errorf("status = %q, want dead", got.Status)
	}
}

func TestRunJob_UnknownTypeFails(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry() // nothing registered
	job := claimOne(t, pool, "mystery", 5)

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	got, err := queue.Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "queued" {
		t.Errorf("status = %q, want queued", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no handler") {
		t.Errorf("last_error = %v, want it to mention \"no handler\"", got.LastError)
	}
}

// Being fenced out is normal: runJob must return nil and change nothing.
func TestRunJob_FencedOutIsNotAnError(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("ok", func(ctx context.Context, job queue.Job) error { return nil })
	job := claimOne(t, pool, "ok", 5)

	// the job is taken away from this worker (as the reaper would do)
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'queued', lease_owner = NULL, lease_end = NULL WHERE id = $1`, job.ID)
	if err != nil {
		t.Fatalf("simulate lost lease: %v", err)
	}

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob returned an error for a fenced-out result: %v", err)
	}

	got, err := queue.Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "queued" {
		t.Errorf("status = %q, want queued (the late result must be ignored)", got.Status)
	}
}
