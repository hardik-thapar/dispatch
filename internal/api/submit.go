package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hardik-thapar/dispatch/internal/queue"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SubmitInput struct {
	TenantID       string
	Type           string
	Payload        []byte
	Priority       int
	RunAt          time.Time
	MaxAttempts    int
	IdempotencyKey string
}

var ErrKeyConflict = errors.New("key already exists")

func Submit(ctx context.Context, pool *pgxpool.Pool, input SubmitInput) (queue.Job, bool, error) {

	if input.TenantID == "" || input.Type == "" || input.IdempotencyKey == "" {
		return queue.Job{}, false, fmt.Errorf("invalid Input")
	}
	if input.MaxAttempts == 0 {
		input.MaxAttempts = 5
	}
	var runAt any //nil means null
	if !input.RunAt.IsZero() {
		runAt = input.RunAt
	}
	if len(input.Payload) == 0 {
		input.Payload = []byte("{}")
	}


	sql := `INSERT INTO jobs(tenant_id, type, payload, priority, run_at, max_attempts, idempotency_key)
	VALUES ($1,$2,$3,$4,coalesce($5::timestamptz, now()),$6,$7)
	ON CONFLICT(tenant_id, idempotency_key) DO NOTHING
	RETURNING id, tenant_id, type, payload, status, priority, run_at, attempts,
          max_attempts, last_error, max_run_time, lease_owner, lease_end,
          idempotency_key, created_at, updated_at, finished_at`

	job := queue.Job{}
	err := pool.QueryRow(ctx, sql, input.TenantID, input.Type, input.Payload, input.Priority, runAt, input.MaxAttempts, input.IdempotencyKey).Scan(&job.ID, &job.TenantID, &job.Type, &job.Payload, &job.Status, &job.Priority, &job.RunAt, &job.Attempts, &job.MaxAttempts, &job.LastError, &job.MaxRunTime, &job.LeaseOwner, &job.LeaseEnd, &job.IdempotencyKey, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		selectSQL := `SELECT id, tenant_id, type, payload, status, priority, run_at, attempts,
		max_attempts, last_error, max_run_time, lease_owner, lease_end,
		idempotency_key, created_at, updated_at, finished_at,
		payload = $3::jsonb AS same_payload
		FROM jobs
		WHERE tenant_id = $1 AND idempotency_key = $2`

		var existing queue.Job
		var samePayload bool

		err = pool.QueryRow(ctx, selectSQL, input.TenantID, input.IdempotencyKey, input.Payload).Scan(
			&existing.ID, &existing.TenantID, &existing.Type, &existing.Payload, &existing.Status, &existing.Priority, &existing.RunAt, &existing.Attempts, &existing.MaxAttempts, &existing.LastError, &existing.MaxRunTime, &existing.LeaseOwner, &existing.LeaseEnd, &existing.IdempotencyKey, &existing.CreatedAt, &existing.UpdatedAt, &existing.FinishedAt, &samePayload)
		if err != nil {
			return queue.Job{}, false, fmt.Errorf("read existing job: %w", err)
		}

		if !samePayload {
			return queue.Job{}, false, ErrKeyConflict
		}
		return existing, false, nil
	}
	if err != nil {
		return queue.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	return job, true, nil

}
