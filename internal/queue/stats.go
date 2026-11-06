package queue

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Stats holds how many jobs are in each state.
type Stats struct {
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Dead      int `json:"dead"`
	Cancelled int `json:"cancelled"`
}

// GetStats counts jobs by status.
// It takes a context, the pool and a tenant ID (empty string means all tenants).
// It returns the counts, or an error.
func GetStats(ctx context.Context, pool *pgxpool.Pool, tenantID string) (Stats, error) {
	rows, err := pool.Query(ctx,
		`SELECT status, count(*)
		 FROM jobs
		 WHERE ($1::text = '' OR tenant_id = $1)
		 GROUP BY status`,
		tenantID)
	if err != nil {
		return Stats{}, fmt.Errorf("get stats: %w", err)
	}
	defer rows.Close()

	var s Stats
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return Stats{}, fmt.Errorf("scan stats: %w", err)
		}
		switch status {
		case "queued":
			s.Queued = n
		case "running":
			s.Running = n
		case "succeeded":
			s.Succeeded = n
		case "dead":
			s.Dead = n
		case "cancelled":
			s.Cancelled = n
		}
	}
	if err := rows.Err(); err != nil {
		return Stats{}, fmt.Errorf("get stats: %w", err)
	}
	return s, nil
}
