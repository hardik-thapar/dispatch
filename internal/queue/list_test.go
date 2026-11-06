package queue

import (
	"context"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestList(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	// three jobs for tenant-a, one for tenant-b
	for _, row := range []struct{ tenant, key string }{
		{"tenant-a", "k1"}, {"tenant-a", "k2"}, {"tenant-a", "k3"}, {"tenant-b", "k4"},
	} {
		_, err := pool.Exec(ctx,
			`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
			 VALUES ($1, 'x', 5, $2)`, row.tenant, row.key)
		if err != nil {
			t.Fatalf("insert %s: %v", row.key, err)
		}
	}
	// make one of tenant-a's jobs cancelled
	_, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'cancelled' WHERE tenant_id = 'tenant-a' AND idempotency_key = 'k1'`)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	tests := []struct {
		name   string
		tenant string
		status string
		limit  int
		want   int
	}{
		{"all of tenant-a", "tenant-a", "", 0, 3},
		{"only queued of tenant-a", "tenant-a", "queued", 0, 2},
		{"only cancelled of tenant-a", "tenant-a", "cancelled", 0, 1},
		{"tenant-b is separate", "tenant-b", "", 0, 1},
		{"limit works", "tenant-a", "", 1, 1},
		{"unknown tenant is empty", "nobody", "", 0, 0},
	}
	for _, tc := range tests {
		jobs, err := List(ctx, pool, tc.tenant, tc.status, tc.limit)
		if err != nil {
			t.Errorf("%s: err = %v", tc.name, err)
			continue
		}
		if len(jobs) != tc.want {
			t.Errorf("%s: got %d jobs, want %d", tc.name, len(jobs), tc.want)
		}
	}
}
