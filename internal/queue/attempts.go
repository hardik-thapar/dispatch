package queue

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartAttempt records that a worker began one try of a job.
// It takes a context, the pool, the job ID, the attempt number and the worker's name.
// Calling it twice for the same attempt is harmless: the second call does nothing.
func StartAttempt(ctx context.Context, pool *pgxpool.Pool, jobID string, attempt int, workerID string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO job_attempts (job_id, attempt_number, worker_id)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (job_id, attempt_number) DO NOTHING`,
		jobID, attempt, workerID)
	if err != nil {
		return fmt.Errorf("start attempt: %w", err)
	}
	return nil
}

// FinishAttempt records how one try ended.
// result must be "succeeded", "failed", "timed_out" or "lease_lost".
// errMsg is the error text (empty means no error).
// A row that is already finished is never changed.
func FinishAttempt(ctx context.Context, pool *pgxpool.Pool, jobID string, attempt int, result, errMsg string) error {
	_, err := pool.Exec(ctx,
		`UPDATE job_attempts
		 SET finished_at = now(), result = $3, error = NULLIF($4, '')
		 WHERE job_id = $1 AND attempt_number = $2 AND finished_at IS NULL`,
		jobID, attempt, result, errMsg)
	if err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}
	return nil
}
