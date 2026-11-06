package queue

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestSucceed(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	insertTestJob(t, pool, "k1", 0, "0 seconds")

	jobs, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: jobs = %d, err = %v", len(jobs), err)
	}
	job := jobs[0]

	ok, err := Succeed(ctx, pool, job.ID, job.Attempts)
	if err != nil {
		t.Fatalf("succeed: %v", err)
	}
	if !ok {
		t.Fatal("succeed returned false, want true")
	}

	got, err := Get(ctx, pool, "tenant-a", job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "succeeded" {
		t.Errorf("status = %q, want succeeded", got.Status)
	}
	if got.FinishedAt == nil {
		t.Error("finished_at is nil, want a time")
	}
	if got.LeaseOwner != nil || got.LeaseEnd != nil {
		t.Errorf("lease should be cleared, got owner = %v, end = %v", got.LeaseOwner, got.LeaseEnd)
	}

	// calling it again must do nothing: the job is no longer running
	ok, err = Succeed(ctx, pool, job.ID, job.Attempts)
	if err != nil {
		t.Fatalf("second succeed: %v", err)
	}
	if ok {
		t.Error("second succeed returned true, want false")
	}
}

// Fencing: a slow worker that lost its job must not be able to finish it.
func TestSucceed_FencingRejectsLateWorker(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	insertTestJob(t, pool, "k1", 0, "0 seconds")

	// w1 claims the job: this is attempt 1
	first, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("w1 claim: jobs = %d, err = %v", len(first), err)
	}
	id := first[0].ID

	// w1 stalls and loses its lease: put the job back, as the reaper will do later
	_, err = pool.Exec(ctx,
		`UPDATE jobs SET status = 'queued', lease_owner = NULL, lease_end = NULL WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("simulate lost lease: %v", err)
	}

	// w2 claims the same job: this is attempt 2
	second, err := Claim(ctx, pool, "w2", 1, 30*time.Second)
	if err != nil || len(second) != 1 {
		t.Fatalf("w2 claim: jobs = %d, err = %v", len(second), err)
	}
	if second[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", second[0].Attempts)
	}

	// the slow w1 wakes up and reports success for attempt 1
	ok, err := Succeed(ctx, pool, id, 1)
	if err != nil {
		t.Fatalf("late succeed: %v", err)
	}
	if ok {
		t.Error("late worker's succeed returned true, want false (fenced out)")
	}
	got, err := Get(ctx, pool, "tenant-a", id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != "running" || got.LeaseOwner == nil || *got.LeaseOwner != "w2" {
		t.Errorf("job should still belong to w2, got status = %q, owner = %v", got.Status, got.LeaseOwner)
	}

	// w2 finishes attempt 2: this one is accepted
	ok, err = Succeed(ctx, pool, id, 2)
	if err != nil || !ok {
		t.Errorf("w2 succeed: ok = %v, err = %v, want true and nil", ok, err)
	}
}
