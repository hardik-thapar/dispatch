package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// attemptOf reads the history row of one attempt.
func attemptOf(t *testing.T, pool *pgxpool.Pool, jobID string, attempt int) (workerID string, result, errText *string, finished bool) {
	t.Helper()
	var finishedAt *time.Time
	err := pool.QueryRow(context.Background(),
		`SELECT worker_id, result, error, finished_at
		 FROM job_attempts WHERE job_id = $1 AND attempt_number = $2`, jobID, attempt).
		Scan(&workerID, &result, &errText, &finishedAt)
	if err != nil {
		t.Fatalf("read attempt %d of job %s: %v", attempt, jobID, err)
	}
	return workerID, result, errText, finishedAt != nil
}

func TestAttemptHistory_Succeeded(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("ok", func(ctx context.Context, job queue.Job) error { return nil })
	job := claimOne(t, pool, "ok", 5)

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	worker, result, errText, finished := attemptOf(t, pool, job.ID, 1)
	if worker != "w1" || !finished || result == nil || *result != "succeeded" || errText != nil {
		t.Errorf("attempt = worker %q, result %v, error %v, finished %v; want w1, succeeded, no error, finished",
			worker, result, errText, finished)
	}
}

func TestAttemptHistory_Failed(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("bad", func(ctx context.Context, job queue.Job) error { return errors.New("boom") })
	job := claimOne(t, pool, "bad", 5)

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	_, result, errText, finished := attemptOf(t, pool, job.ID, 1)
	if !finished || result == nil || *result != "failed" || errText == nil || *errText != "boom" {
		t.Errorf("attempt = result %v, error %v, finished %v; want failed, boom, finished", result, errText, finished)
	}
}

func TestAttemptHistory_TimedOut(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("hang", func(ctx context.Context, job queue.Job) error {
		<-ctx.Done()
		return ctx.Err()
	})
	job := claimOne(t, pool, "hang", 5)
	job.MaxRunTime = 1

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	_, result, _, finished := attemptOf(t, pool, job.ID, 1)
	if !finished || result == nil || *result != "timed_out" {
		t.Errorf("attempt = result %v, finished %v; want timed_out, finished", result, finished)
	}
}

func TestAttemptHistory_LeaseLost(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	reg := handlers.NewRegistry()
	reg.Register("ok", func(ctx context.Context, job queue.Job) error { return nil })
	job := claimOne(t, pool, "ok", 5)

	// the job is taken away from this worker before it reports
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'queued', lease_owner = NULL, lease_end = NULL WHERE id = $1`, job.ID)
	if err != nil {
		t.Fatalf("simulate lost lease: %v", err)
	}

	if err := newTestWorker(pool, reg).runJob(ctx, job); err != nil {
		t.Fatalf("runJob: %v", err)
	}

	_, result, _, finished := attemptOf(t, pool, job.ID, 1)
	if !finished || result == nil || *result != "lease_lost" {
		t.Errorf("attempt = result %v, finished %v; want lease_lost, finished", result, finished)
	}
}
