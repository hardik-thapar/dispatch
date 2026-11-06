package queue

import (
	"fmt"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const jobColumns = `id, tenant_id, type, payload, status, priority, run_at, attempts,
	max_attempts, last_error, max_run_time, lease_owner, lease_end,
	idempotency_key, created_at, updated_at, finished_at`

var ErrNotFound = errors.New("job not found")

func scanJob(row pgx.Row) (Job, error) {
	job := Job{}
	err := row.Scan(&job.ID, &job.TenantID, &job.Type, &job.Payload, &job.Status, &job.Priority, &job.RunAt, &job.Attempts, &job.MaxAttempts, &job.LastError, &job.MaxRunTime, &job.LeaseOwner, &job.LeaseEnd, &job.IdempotencyKey, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt)

	if err != nil {return Job{}, fmt.Errorf("scan error: %w", err)}
	
	return job, nil
}

func Get(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (Job, error) {
	row := pool.QueryRow(ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = $1 AND tenant_id = $2",
		id, tenantID)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}
