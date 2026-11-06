package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotCancellable means the job exists but is not in the queued state.
var ErrNotCancellable = errors.New("job is not queued, cannot cancel")

// Cancel stops a job that has not started yet.
// It takes a context, the pool, the tenant ID and the job ID.
// It returns the cancelled job, ErrNotFound if there is no such job for
// that tenant, or ErrNotCancellable if the job is not queued.
func Cancel(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (Job, error) {
	row := pool.QueryRow(ctx,
		`UPDATE jobs
		SET status = 'cancelled', finished_at = now(), updated_at = now()
		WHERE id = $1 AND tenant_id = $2 AND status = 'queued'
		RETURNING `+jobColumns, id, tenantID)

	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("cancel job: %w", err)
	}

	// The UPDATE changed nothing. Find out why.
	if _, getErr := Get(ctx, pool, tenantID, id); getErr != nil {
		return Job{}, getErr // ErrNotFound, or a real error
	}
	return Job{}, ErrNotCancellable
}
