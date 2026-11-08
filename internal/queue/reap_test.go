package queue

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

type reapRow struct {
	status     string
	attempts   int
	leaseOwner *string
	leaseEnd   *time.Time
	finishedAt *time.Time
	lastError  *string
}

func rowByKey(t *testing.T, pool *pgxpool.Pool, key string) reapRow {
	t.Helper()
	var r reapRow
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts, lease_owner, lease_end, finished_at, last_error
		 FROM jobs WHERE idempotency_key = $1`, key).
		Scan(&r.status, &r.attempts, &r.leaseOwner, &r.leaseEnd, &r.finishedAt, &r.lastError)
	if err != nil {
		t.Fatalf("read job %s: %v", key, err)
	}
	return r
}

func TestReapExpired(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// three running jobs:
	//   expired-retry: lease expired, attempt 1 of 5  -> must go back to the queue
	//   expired-dead:  lease expired, attempt 5 of 5  -> must become dead
	//   still-leased:  lease valid for another minute -> must be left alone
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key, status, attempts, lease_owner, lease_end)
		 VALUES
		 ('tenant-a', 'x', 5, 'expired-retry', 'running', 1, 'w-dead',  now() - interval '5 seconds'),
		 ('tenant-a', 'x', 5, 'expired-dead',  'running', 5, 'w-dead',  now() - interval '5 seconds'),
		 ('tenant-a', 'x', 5, 'still-leased',  'running', 1, 'w-alive', now() + interval '60 seconds')`)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}

	n, err := ReapExpired(ctx, pool)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 2 {
		t.Errorf("reaped %d jobs, want 2", n)
	}

	retry := rowByKey(t, pool, "expired-retry")
	if retry.status != "queued" {
		t.Errorf("expired-retry status = %q, want queued", retry.status)
	}
	if retry.leaseOwner != nil || retry.leaseEnd != nil {
		t.Errorf("expired-retry lease should be cleared, got owner = %v, end = %v", retry.leaseOwner, retry.leaseEnd)
	}
	if retry.finishedAt != nil {
		t.Errorf("expired-retry finished_at = %v, want nil", retry.finishedAt)
	}
	if retry.lastError == nil || !strings.Contains(*retry.lastError, "lease expired") {
		t.Errorf("expired-retry last_error = %v, want it to mention \"lease expired\"", retry.lastError)
	}
	if retry.attempts != 1 {
		t.Errorf("expired-retry attempts = %d, want 1 (reaping must not change it)", retry.attempts)
	}

	dead := rowByKey(t, pool, "expired-dead")
	if dead.status != "dead" {
		t.Errorf("expired-dead status = %q, want dead", dead.status)
	}
	if dead.finishedAt == nil {
		t.Error("expired-dead finished_at is nil, want a time")
	}

	alive := rowByKey(t, pool, "still-leased")
	if alive.status != "running" || alive.leaseOwner == nil || *alive.leaseOwner != "w-alive" {
		t.Errorf("still-leased = status %q, owner %v; want it untouched (running, w-alive)", alive.status, alive.leaseOwner)
	}

	// running the reaper again finds nothing new
	n, err = ReapExpired(ctx, pool)
	if err != nil {
		t.Fatalf("second reap: %v", err)
	}
	if n != 0 {
		t.Errorf("second reap: reaped %d jobs, want 0", n)
	}

	// the recovered job can be claimed again, as attempt 2
	claimed, err := Claim(ctx, pool, "w2", 5, 30*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v, want exactly the recovered job", len(claimed), err)
	}
	if claimed[0].IdempotencyKey != "expired-retry" || claimed[0].Attempts != 2 {
		t.Errorf("claimed %s attempt %d, want expired-retry attempt 2", claimed[0].IdempotencyKey, claimed[0].Attempts)
	}
}
