package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestReplay(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	id := "44444444-4444-4444-4444-444444444444"

	// a job that died after 5 of 5 attempts
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, tenant_id, type, max_attempts, idempotency_key,
		                   status, attempts, last_error, finished_at)
		 VALUES ($1, 'tenant-a', 'x', 5, 'k1', 'dead', 5, 'boom', now())`, id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// wrong tenant: not found
	if _, err := Replay(ctx, pool, "tenant-b", id); !errors.Is(err, ErrNotFound) {
		t.Errorf("wrong tenant: err = %v, want ErrNotFound", err)
	}

	// right tenant: back in the queue with 5 fresh attempts
	job, err := Replay(ctx, pool, "tenant-a", id)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if job.Status != "queued" {
		t.Errorf("status = %q, want queued", job.Status)
	}
	if job.Attempts != 5 {
		t.Errorf("attempts = %d, want 5 (must not be reset)", job.Attempts)
	}
	if job.MaxAttempts != 10 {
		t.Errorf("max_attempts = %d, want 10 (attempts + 5)", job.MaxAttempts)
	}
	if job.FinishedAt != nil {
		t.Errorf("finished_at = %v, want nil", job.FinishedAt)
	}
	if job.LastError == nil || *job.LastError != "boom" {
		t.Errorf("last_error = %v, want boom (kept for debugging)", job.LastError)
	}

	// it can be claimed again, as attempt 6
	claimed, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(claimed), err)
	}
	if claimed[0].Attempts != 6 {
		t.Errorf("attempts after claim = %d, want 6", claimed[0].Attempts)
	}

	// replaying a job that is not dead is refused
	if _, err := Replay(ctx, pool, "tenant-a", id); !errors.Is(err, ErrNotReplayable) {
		t.Errorf("replay of a running job: err = %v, want ErrNotReplayable", err)
	}
}
