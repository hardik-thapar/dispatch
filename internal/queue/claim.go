package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Claim(ctx context.Context, pool *pgxpool.Pool, workerID string, n int, lease time.Duration) ([]Job, error) {
	sql := `update jobs
	set status='running', lease_owner=$1, lease_end = now()+make_interval(secs=>$3), attempts = attempts+1, updated_at = now()
	where id in (
	select id from jobs
	where status='queued' and run_at <= now()
	order by priority asc, run_at asc
	limit $2
	for update skip locked)
	returning ` + jobColumns

	rows, err := pool.Query(ctx, sql, workerID, n, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("claim jobs: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	return jobs, nil
}
