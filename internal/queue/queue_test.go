package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestGet(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	id := "11111111-1111-1111-1111-111111111111"

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, tenant_id, type, max_attempts, idempotency_key)
		 VALUES ($1, 'tenant-a', 'x', 5, 'k1')`, id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// 1. right tenant: found
	job, err := Get(ctx, pool, "tenant-a", id)
	if err != nil {
		t.Fatalf("Get with right tenant: err = %v, want nil", err)
	}
	if job.ID != id {
		t.Errorf("job.ID = %q, want %q", job.ID, id)
	}

	// 2. wrong tenant: ErrNotFound
	_, err = Get(ctx, pool, "tenant-b", id)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get with wrong tenant: err = %v, want ErrNotFound", err)
	}
}
