package queue

import (
	"context"
	"strings"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestReapExpired_ClosesOpenAttemptRow(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	deadID := "66666666-6666-6666-6666-666666666666"
	aliveID := "77777777-7777-7777-7777-777777777777"

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, tenant_id, type, max_attempts, idempotency_key, status, attempts, lease_owner, lease_end)
		 VALUES
		 ($1, 'tenant-a', 'x', 5, 'dead-worker',  'running', 1, 'w-dead',  now() - interval '5 seconds'),
		 ($2, 'tenant-a', 'x', 5, 'alive-worker', 'running', 1, 'w-alive', now() + interval '60 seconds')`,
		deadID, aliveID)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}
	// both attempts were started and neither has finished
	for _, a := range []struct{ id, worker string }{{deadID, "w-dead"}, {aliveID, "w-alive"}} {
		if err := StartAttempt(ctx, pool, a.id, 1, a.worker); err != nil {
			t.Fatalf("start attempt: %v", err)
		}
	}

	n, err := ReapExpired(ctx, pool)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Errorf("reaped %d jobs, want 1", n)
	}

	// the crashed attempt is closed as lease_lost
	dead := attemptsOf(t, pool, deadID)
	if len(dead) != 1 || dead[0].finishedAt == nil || dead[0].result == nil || *dead[0].result != "lease_lost" {
		t.Fatalf("crashed attempt = %+v, want finished with result lease_lost", dead)
	}
	if dead[0].errText == nil || !strings.Contains(*dead[0].errText, "lease expired") {
		t.Errorf("crashed attempt error = %v, want it to mention \"lease expired\"", dead[0].errText)
	}

	// the healthy worker's attempt is left open
	alive := attemptsOf(t, pool, aliveID)
	if len(alive) != 1 || alive[0].finishedAt != nil || alive[0].result != nil {
		t.Errorf("healthy attempt = %+v, want it left open", alive)
	}
}
