package queue

import (
	"context"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestGetStats(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// tenant-a: 2 queued, 1 running, 1 dead. tenant-b: 1 queued.
	for _, row := range []struct{ tenant, key, status string }{
		{"tenant-a", "k1", "queued"},
		{"tenant-a", "k2", "queued"},
		{"tenant-a", "k3", "running"},
		{"tenant-a", "k4", "dead"},
		{"tenant-b", "k5", "queued"},
	} {
		_, err := pool.Exec(ctx,
			`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key, status)
			 VALUES ($1, 'x', 5, $2, $3)`, row.tenant, row.key, row.status)
		if err != nil {
			t.Fatalf("insert %s: %v", row.key, err)
		}
	}

	a, err := GetStats(ctx, pool, "tenant-a")
	if err != nil {
		t.Fatalf("stats tenant-a: %v", err)
	}
	want := Stats{Queued: 2, Running: 1, Dead: 1}
	if a != want {
		t.Errorf("tenant-a stats = %+v, want %+v", a, want)
	}

	all, err := GetStats(ctx, pool, "")
	if err != nil {
		t.Fatalf("stats all: %v", err)
	}
	if all.Queued != 3 {
		t.Errorf("all tenants queued = %d, want 3", all.Queued)
	}

	empty, err := GetStats(ctx, pool, "nobody")
	if err != nil {
		t.Fatalf("stats nobody: %v", err)
	}
	if empty != (Stats{}) {
		t.Errorf("unknown tenant stats = %+v, want all zeros", empty)
	}
}
