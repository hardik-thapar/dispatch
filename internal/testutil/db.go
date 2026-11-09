package testutil

import (
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // lets database/sql talk to Postgres (goose needs it)
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// NewTestPool starts a fresh Postgres 16 container, runs the migrations,
// and returns a connection pool to it.
// It takes the test object t. It returns the pool.
// The container and pool are removed automatically when the test ends.
func NewTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("dispatch"),
		postgres.WithUsername("dispatch"),
		postgres.WithPassword("dispatch"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })

	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	runMigrations(t, url)

	pool, err := db.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// runMigrations applies every goose "Up" migration to the database at url.
func runMigrations(t *testing.T, url string) {
	t.Helper()

	sqlDB, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	defer sqlDB.Close()

	// Find the migrations folder relative to THIS file, so it works from any package.
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")

	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("goose dialect: %v", err)
	}
	if err := goose.Up(sqlDB, dir); err != nil {
		t.Fatalf("goose up: %v", err)
	}
}
