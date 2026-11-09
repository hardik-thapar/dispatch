package queue

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

type attemptRow struct {
	workerID   string
	finishedAt *time.Time
	result     *string
	errText    *string
}

func attemptsOf(t *testing.T, pool *pgxpool.Pool, jobID string) []attemptRow {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT worker_id, finished_at, result, error
		 FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number`, jobID)
	if err != nil {
		t.Fatalf("query attempts: %v", err)
	}
	defer rows.Close()

	var out []attemptRow
	for rows.Next() {
		var r attemptRow
		if err := rows.Scan(&r.workerID, &r.finishedAt, &r.result, &r.errText); err != nil {
			t.Fatalf("scan attempt: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("attempts rows: %v", err)
	}
	return out
}

func TestAttempts(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()
	id := "55555555-5555-5555-5555-555555555555"

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (id, tenant_id, type, max_attempts, idempotency_key)
		 VALUES ($1, 'tenant-a', 'x', 5, 'k1')`, id)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}

	// start: one open row
	if err := StartAttempt(ctx, pool, id, 1, "w1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	rows := attemptsOf(t, pool, id)
	if len(rows) != 1 || rows[0].workerID != "w1" || rows[0].finishedAt != nil || rows[0].result != nil {
		t.Fatalf("after start: rows = %+v, want one open row for w1", rows)
	}

	// starting the same attempt again adds nothing
	if err := StartAttempt(ctx, pool, id, 1, "w1"); err != nil {
		t.Fatalf("second start: %v", err)
	}
	if n := len(attemptsOf(t, pool, id)); n != 1 {
		t.Errorf("after second start: %d rows, want 1", n)
	}

	// finish: result, error and end time are saved
	if err := FinishAttempt(ctx, pool, id, 1, "failed", "boom"); err != nil {
		t.Fatalf("finish: %v", err)
	}
	r := attemptsOf(t, pool, id)[0]
	if r.finishedAt == nil || r.result == nil || *r.result != "failed" || r.errText == nil || *r.errText != "boom" {
		t.Errorf("after finish: row = %+v, want finished, failed, boom", r)
	}

	// a finished row is never changed again
	if err := FinishAttempt(ctx, pool, id, 1, "succeeded", ""); err != nil {
		t.Fatalf("second finish: %v", err)
	}
	if r := attemptsOf(t, pool, id)[0]; *r.result != "failed" {
		t.Errorf("result after second finish = %q, want failed (must not change)", *r.result)
	}

	// a successful attempt has no error text
	if err := StartAttempt(ctx, pool, id, 2, "w2"); err != nil {
		t.Fatalf("start 2: %v", err)
	}
	if err := FinishAttempt(ctx, pool, id, 2, "succeeded", ""); err != nil {
		t.Fatalf("finish 2: %v", err)
	}
	if r := attemptsOf(t, pool, id)[1]; r.errText != nil {
		t.Errorf("error text = %q, want NULL for a success", *r.errText)
	}

	// a result outside the allowed list is rejected by the database
	if err := StartAttempt(ctx, pool, id, 3, "w1"); err != nil {
		t.Fatalf("start 3: %v", err)
	}
	if err := FinishAttempt(ctx, pool, id, 3, "banana", ""); err == nil {
		t.Error("finish with result banana returned nil, want an error")
	}
}
