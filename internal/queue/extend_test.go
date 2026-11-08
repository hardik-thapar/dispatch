package queue

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestExtendLeases(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	for _, k := range []string{"k1", "k2", "k3"} {
		insertTestJob(t, pool, k, 0, "0 seconds")
	}

	// w1 holds two jobs, w2 holds one. All leases are 5 seconds.
	mine, err := Claim(ctx, pool, "w1", 2, 5*time.Second)
	if err != nil || len(mine) != 2 {
		t.Fatalf("w1 claim: jobs = %d, err = %v", len(mine), err)
	}
	other, err := Claim(ctx, pool, "w2", 1, 5*time.Second)
	if err != nil || len(other) != 1 {
		t.Fatalf("w2 claim: jobs = %d, err = %v", len(other), err)
	}

	// one of w1's jobs finishes, so w1 holds only one running job now
	if ok, err := Succeed(ctx, pool, mine[0].ID, mine[0].Attempts); err != nil || !ok {
		t.Fatalf("succeed: ok = %v, err = %v", ok, err)
	}

	extended, err := ExtendLeases(ctx, pool, "w1", 60*time.Second)
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if len(extended) != 1 {
		t.Fatalf("extended %d leases, want 1 (only w1's running job)", len(extended))
	}
	if extended[0].ID != mine[1].ID || extended[0].Attempt != mine[1].Attempts {
		t.Errorf("extended = %+v, want job %s attempt %d", extended[0], mine[1].ID, mine[1].Attempts)
	}

	// w1's running job now has about 60 seconds left
	got, err := Get(ctx, pool, "tenant-a", mine[1].ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.LeaseEnd == nil {
		t.Fatal("lease end is nil")
	}
	if d := got.LeaseEnd.Sub(got.UpdatedAt); d < 59*time.Second || d > 61*time.Second {
		t.Errorf("w1 lease length = %v, want about 60s", d)
	}

	// w2's job is untouched: still about 5 seconds
	got, err = Get(ctx, pool, "tenant-a", other[0].ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if d := got.LeaseEnd.Sub(got.UpdatedAt); d < 4*time.Second || d > 6*time.Second {
		t.Errorf("w2 lease length = %v, want about 5s (must not change)", d)
	}

	// a worker with no running jobs extends nothing
	none, err := ExtendLeases(ctx, pool, "nobody", 60*time.Second)
	if err != nil {
		t.Fatalf("extend nobody: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("extended %d leases for a worker with no jobs, want 0", len(none))
	}
}
