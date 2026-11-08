package queue

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ReapExpired(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	sql := `WITH reaped AS (
		UPDATE jobs SET
			status = CASE
				WHEN attempts >= max_attempts THEN 'dead'
				ELSE 'queued' END,
			finished_at = CASE
				WHEN attempts >= max_attempts THEN now()
				ELSE NULL END,
			last_error = 'lease expired: worker presumed dead',
			lease_owner = NULL, lease_end = NULL, updated_at = now()
		WHERE status = 'running' AND lease_end < now()
		RETURNING id, attempts
	),
	closed AS (
		UPDATE job_attempts a
		SET finished_at = now(),
			result = 'lease_lost',
			error = 'lease expired: worker presumed dead'
		FROM reaped r
		WHERE a.job_id = r.id
		  AND a.attempt_number = r.attempts
		  AND a.finished_at IS NULL
	)
	SELECT count(*) FROM reaped`

	var n int
	if err := pool.QueryRow(ctx, sql).Scan(&n); err != nil {
		return 0, fmt.Errorf("reap expired jobs: %w", err)
	}
	return n, nil
}
