package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HeldLease struct {
	ID      string
	Attempt int
}

func ExtendLeases(ctx context.Context, pool *pgxpool.Pool, workerID string, lease time.Duration) ([]HeldLease, error) {
	sql := `
	UPDATE jobs
	SET lease_end = now()+make_interval(secs=>$2), updated_at = now()
	WHERE lease_owner = $1 AND status = 'running'
	RETURNING id, attempts`
	rows, err := pool.Query(ctx, sql, workerID, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("extend leases for worker %s: %v", workerID, err)
	}
	defer rows.Close()

	leases := []HeldLease{}
	for rows.Next() {
		var lease HeldLease
		err := rows.Scan(&lease.ID, &lease.Attempt)
		if err != nil {
			return nil, fmt.Errorf("extend leases for worker %s: %v", workerID, err)
		}
		leases = append(leases, lease)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("extend leases for worker %s: %v", workerID, err)
	}
	return leases, nil
}
