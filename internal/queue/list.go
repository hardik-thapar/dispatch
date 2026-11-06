package queue

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// List returns a tenant's jobs, newest first.
// It takes a context, the pool, the tenant ID, an optional status filter
// (empty string means any status) and a limit (0 or less means 50, max 200).
// It returns the jobs (an empty slice if there are none), or an error.
func List(ctx context.Context, pool *pgxpool.Pool, tenantID, status string, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	rows, err := pool.Query(ctx,
		`SELECT `+jobColumns+`
		 FROM jobs
		 WHERE tenant_id = $1 AND ($2::text = '' OR status = $2)
		 ORDER BY created_at DESC
		 LIMIT $3`,
		tenantID, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}
