package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// replayAttempts is how many fresh attempts a replayed job gets.
const replayAttempts = 5

// ErrNotReplayable means the job exists but is not dead.
var ErrNotReplayable = errors.New("job is not dead, cannot replay")

// Replay puts a dead job back in the queue so it runs again.
// It takes a context, the pool, the tenant ID and the job ID.
// attempts is NOT reset (the attempt history needs unique numbers);
// instead max_attempts is raised so the job gets replayAttempts more tries.
// It returns the updated job, ErrNotFound, or ErrNotReplayable.
func Replay(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (Job, error) {
	row := pool.QueryRow(ctx,
		`UPDATE jobs
		 SET status = 'queued',
		     max_attempts = attempts + $3,
		     run_at = now(),
		     finished_at = NULL,
		     lease_owner = NULL,
		     lease_end = NULL,
		     updated_at = now()
		 WHERE id = $1 AND tenant_id = $2 AND status = 'dead'
		 RETURNING `+jobColumns,
		id, tenantID, replayAttempts)

	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("replay job: %w", err)
	}

	// The UPDATE changed nothing. Find out why.
	if _, getErr := Get(ctx, pool, tenantID, id); getErr != nil {
		return Job{}, getErr // ErrNotFound, or a real error
	}
	return Job{}, ErrNotReplayable
}
