package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestCancel(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	id := "22222222-2222-2222-2222-222222222222"

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, tenant_id, type, max_attempts, idempotency_key)
		 VALUES ($1, 'tenant-a', 'x', 5, 'k1')`, id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// wrong tenant: not found, and the job stays queued
	_, err = Cancel(ctx, pool, "tenant-b", id)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("wrong tenant: err = %v, want ErrNotFound", err)
	}

	// right tenant: cancelled
	job, err := Cancel(ctx, pool, "tenant-a", id)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if job.Status != "cancelled" {
		t.Errorf("status = %q, want cancelled", job.Status)
	}
	if job.FinishedAt == nil {
		t.Error("finished_at is nil, want a time")
	}

	// second cancel: the job is no longer queued
	_, err = Cancel(ctx, pool, "tenant-a", id)
	if !errors.Is(err, ErrNotCancellable) {
		t.Errorf("second cancel: err = %v, want ErrNotCancellable", err)
	}
}
