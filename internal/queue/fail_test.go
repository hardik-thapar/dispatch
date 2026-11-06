package queue

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestFail_RetriesWhenAttemptsLeft(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	insertTestJob(t, pool, "k1", 0, "0 seconds") // max_attempts = 5

	jobs, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(jobs), err)
	}
	job := jobs[0] // attempt 1 of 5

	ok, err := Fail(ctx, pool, job.ID, job.Attempts, "boom", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("fail: ok = %v, err = %v, want true and nil", ok, err)
	}

	got, err := Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "queued" {
		t.Errorf("status = %q, want queued (it should retry)", got.Status)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("last_error = %v, want boom", got.LastError)
	}
	if d := got.RunAt.Sub(got.UpdatedAt); d < 9*time.Second || d > 11*time.Second {
		t.Errorf("run_at is %v after updated_at, want about 10s", d)
	}
	if got.LeaseOwner != nil || got.LeaseEnd != nil {
		t.Errorf("lease should be cleared, got owner = %v, end = %v", got.LeaseOwner, got.LeaseEnd)
	}
	if got.FinishedAt != nil {
		t.Errorf("finished_at = %v, want nil (job is not finished)", got.FinishedAt)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (Fail must not change it)", got.Attempts)
	}
}

func TestFail_DeadWhenNoAttemptsLeft(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	insertTestJob(t, pool, "k1", 0, "0 seconds")
	// allow only one attempt
	if _, err := pool.Exec(ctx, `UPDATE jobs SET max_attempts = 1`); err != nil {
		t.Fatalf("set max_attempts: %v", err)
	}

	jobs, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(jobs), err)
	}
	job := jobs[0] // attempt 1 of 1

	ok, err := Fail(ctx, pool, job.ID, job.Attempts, "boom", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("fail: ok = %v, err = %v, want true and nil", ok, err)
	}

	got, err := Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "dead" {
		t.Errorf("status = %q, want dead", got.Status)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("last_error = %v, want boom", got.LastError)
	}
	if got.FinishedAt == nil {
		t.Error("finished_at is nil, want a time")
	}
	if got.LeaseOwner != nil || got.LeaseEnd != nil {
		t.Errorf("lease should be cleared, got owner = %v, end = %v", got.LeaseOwner, got.LeaseEnd)
	}
}

func TestFail_FencingRejectsWrongAttempt(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	insertTestJob(t, pool, "k1", 0, "0 seconds")

	jobs, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(jobs), err)
	}

	// a worker that holds a different attempt number is fenced out
	ok, err := Fail(ctx, pool, jobs[0].ID, 99, "boom", time.Second)
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if ok {
		t.Error("fail with wrong attempt returned true, want false")
	}

	got, err := Get(ctx, pool, "tenant-a", jobs[0].ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "running" {
		t.Errorf("status = %q, want running (nothing should change)", got.Status)
	}
}