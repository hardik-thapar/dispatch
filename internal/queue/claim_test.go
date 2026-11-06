package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// insertTestJob adds one queued job.
// runIn is a Postgres interval text: "0 seconds" means due now, "1 hour" means due later.
func insertTestJob(t *testing.T, pool *pgxpool.Pool, key string, priority int, runIn string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key, priority, run_at)
		 VALUES ('tenant-a', 'x', 5, $1, $2, now() + $3::interval)`,
		key, priority, runIn)
	if err != nil {
		t.Fatalf("insert job %s: %v", key, err)
	}
}

func TestClaim_Basic(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	for _, k := range []string{"k1", "k2", "k3"} {
		insertTestJob(t, pool, k, 0, "0 seconds")
	}

	jobs, err := Claim(ctx, pool, "w1", 2, 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("claimed %d jobs, want 2", len(jobs))
	}
	for _, j := range jobs {
		if j.Status != "running" {
			t.Errorf("status = %q, want running", j.Status)
		}
		if j.Attempts != 1 {
			t.Errorf("attempts = %d, want 1", j.Attempts)
		}
		if j.LeaseOwner == nil || *j.LeaseOwner != "w1" {
			t.Errorf("lease owner = %v, want w1", j.LeaseOwner)
		}
		if j.LeaseEnd == nil {
			t.Error("lease end is nil")
		} else if d := j.LeaseEnd.Sub(j.UpdatedAt); d < 29*time.Second || d > 31*time.Second {
			t.Errorf("lease length = %v, want about 30s", d)
		}
	}

	s, err := GetStats(ctx, pool, "")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if s.Queued != 1 || s.Running != 2 {
		t.Errorf("stats = %+v, want 1 queued and 2 running", s)
	}
}

func TestClaim_SkipsFutureJobs(t *testing.T) {
	pool := testutil.NewTestPool(t)
	insertTestJob(t, pool, "later", 0, "1 hour")

	jobs, err := Claim(context.Background(), pool, "w1", 5, 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("claimed %d jobs, want 0 (job is not due yet)", len(jobs))
	}
}

func TestClaim_PriorityOrder(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	insertTestJob(t, pool, "p5", 5, "0 seconds")
	insertTestJob(t, pool, "p1", 1, "0 seconds")
	insertTestJob(t, pool, "p3", 3, "0 seconds")

	want := []int{1, 3, 5} // smaller number runs first
	for i, wantPriority := range want {
		jobs, err := Claim(ctx, pool, "w1", 1, 30*time.Second)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim %d: jobs = %d, err = %v", i, len(jobs), err)
		}
		if jobs[0].Priority != wantPriority {
			t.Errorf("claim %d: priority = %d, want %d", i, jobs[0].Priority, wantPriority)
		}
	}
}

// The proof that SKIP LOCKED works: 10 workers claim at the same time
// and no job is ever handed out twice.
func TestClaim_ConcurrentNoDoubleClaim(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	const total = 100

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'tenant-a', 'x', 5, 'k' || g FROM generate_series(1, $1::int) g`, total)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}

	var mu sync.Mutex
	seen := map[string]int{} // job id -> how many times it was claimed
	var wg sync.WaitGroup

	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			workerID := fmt.Sprintf("w%d", n)
			for {
				jobs, err := Claim(ctx, pool, workerID, 3, 30*time.Second)
				if err != nil {
					t.Errorf("%s claim: %v", workerID, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != total {
		t.Errorf("claimed %d different jobs, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("job %s was claimed %d times, want 1", id, count)
		}
	}
}
