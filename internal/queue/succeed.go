package queue

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Succeed(ctx context.Context, pool *pgxpool.Pool, id string, attempt int) (bool, error) {
	sql := `update jobs
	set status = 'succeeded', finished_at = now(), updated_at = now(),
    lease_owner = null, lease_end = null
	where id = $1 and status = 'running' and attempts = $2`

	tag, err := pool.Exec(ctx, sql, id, attempt)
	if err != nil {
		return false, fmt.Errorf("succeed job: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
