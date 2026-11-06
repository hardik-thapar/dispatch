package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Fail(ctx context.Context, pool *pgxpool.Pool, id string, attempt int, errMsg string, retryAfter time.Duration) (bool, error) {
	sql := `
	UPDATE jobs SET 
	status = CASE
		WHEN attempts >= max_attempts THEN 'dead'
		ELSE 'queued' END,
	run_at = CASE
		WHEN attempts >= max_attempts THEN run_at
		ELSE now()+make_interval(secs => $4) END,
	finished_at = CASE
		WHEN attempts >= max_attempts THEN now()
		ELSE NULL END,
	last_error = $3, lease_owner = NULL, lease_end = NULL, updated_at = now()
	WHERE id = $1 AND status = 'running' AND attempts = $2
	`
	tag, err := pool.Exec(ctx, sql, id, attempt, errMsg, retryAfter.Seconds())
	if err != nil {
		return false, fmt.Errorf("fail job: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
