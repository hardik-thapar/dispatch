package queue

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ReapExpired(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	sql := `UPDATE jobs SET 
	status = CASE
	WHEN attempts >= max_attempts THEN 'dead'
	ELSE 'queued' END,
	finished_at = CASE
	WHEN attempts >= max_attempts THEN now()
	ELSE NULL END,
	last_error = 'lease expired: worker presumed dead',
	lease_owner = NULL, lease_end = NULL, updated_at = now()
	WHERE status = 'running' AND lease_end < now()`
	tag, err := pool.Exec(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("reap expired jobs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
