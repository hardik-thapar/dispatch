package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool opens a connection pool to Postgres and checks it works.
// It takes a context and a connection URL.
// It returns the pool, or an error if the URL is bad or the database is down.
func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}

	// Pool size is a deliberate choice. DB_MAX_CONNS overrides the default (benchmarks use it).
	cfg.MaxConns = 20
	if v := os.Getenv("DB_MAX_CONNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("DB_MAX_CONNS must be a number of at least 1, got %q", v)
		}
		cfg.MaxConns = int32(n)
	}
	cfg.MinConns = 1

	// Safety timeouts, set on every connection (guide decision 14).
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"                    // ms
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10000" // ms

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return pool, nil
}
