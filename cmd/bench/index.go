package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// indexVariant is one way of indexing the jobs table.
type indexVariant struct {
	name  string
	setup []string // SQL that puts this variant in place
	index string   // name of its index, to report the size ("" for none)
}

// runIndex measures the claim query on a big table of finished jobs, with different indexes.
func runIndex(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("index", flag.ExitOnError)
	rows := fs.Int("rows", 1000000, "total rows in the jobs table")
	queued := fs.Int("queued", 1000, "how many of them are queued (the rest are succeeded)")
	runs := fs.Int("runs", 30, "measured runs per variant (the median is reported)")
	fs.Parse(args)
	if *queued >= *rows {
		log.Fatalf("-queued must be smaller than -rows")
	}

	pool := connect(ctx)
	defer pool.Close()

	// Setup statements can take longer than the pool's 5 second statement_timeout,
	// so they run on one connection with the timeout switched off.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire connection: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET statement_timeout = 0`); err != nil {
		log.Fatalf("set statement_timeout: %v", err)
	}
	defer conn.Exec(ctx, `RESET statement_timeout`)

	// Always leave the database with the real index in place.
	defer func() {
		for _, q := range []string{
			`DROP INDEX IF EXISTS idx_bench_full`,
			`CREATE INDEX IF NOT EXISTS idx_jobs_queued ON jobs (priority ASC, run_at ASC) WHERE status = 'queued'`,
		} {
			if _, err := conn.Exec(ctx, q); err != nil {
				log.Printf("WARNING: could not restore the index (%s): %v", q, err)
			}
		}
	}()

	resetTables(ctx, pool)
	log.Printf("inserting %d rows (this takes a while)...", *rows)
	_, err = conn.Exec(ctx, `
		INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key, status, attempts, finished_at)
		SELECT 'bench', 'noop', 3, 'i' || g,
		       CASE WHEN g <= $2 THEN 'queued' ELSE 'succeeded' END,
		       CASE WHEN g <= $2 THEN 0 ELSE 1 END,
		       CASE WHEN g <= $2 THEN NULL ELSE now() END
		FROM generate_series(1, $1::int) g`, *rows, *queued)
	if err != nil {
		log.Fatalf("insert rows: %v", err)
	}

	variants := []indexVariant{
		{"none", []string{
			`DROP INDEX IF EXISTS idx_jobs_queued`,
			`DROP INDEX IF EXISTS idx_bench_full`,
		}, ""},
		{"full index (status, priority, run_at)", []string{
			`DROP INDEX IF EXISTS idx_jobs_queued`,
			`CREATE INDEX idx_bench_full ON jobs (status, priority, run_at)`,
		}, "idx_bench_full"},
		{"partial index (queued rows only)", []string{
			`DROP INDEX IF EXISTS idx_bench_full`,
			`CREATE INDEX idx_jobs_queued ON jobs (priority ASC, run_at ASC) WHERE status = 'queued'`,
		}, "idx_jobs_queued"},
	}

	printEnvironment(ctx, pool)
	fmt.Printf("Index effect on the claim query: %d rows in jobs, %d queued, %d succeeded\n", *rows, *queued, *rows-*queued)
	fmt.Printf("EXPLAIN ANALYZE of the claim's SELECT (ORDER BY priority, run_at LIMIT 1 FOR UPDATE SKIP LOCKED),\n")
	fmt.Printf("median execution time of %d runs after 3 warm-up runs\n\n", *runs)
	fmt.Printf("%-40s %-44s %10s %11s\n", "index", "plan", "median ms", "index size")

	for _, v := range variants {
		for _, q := range v.setup {
			if _, err := conn.Exec(ctx, q); err != nil {
				log.Fatalf("%s: %v", q, err)
			}
		}
		if _, err := conn.Exec(ctx, `VACUUM (ANALYZE) jobs`); err != nil {
			log.Fatalf("vacuum analyze: %v", err)
		}

		var times []float64
		plan := ""
		for i := 0; i < 3+*runs; i++ {
			p, ms, err := explainClaim(ctx, pool)
			if err != nil {
				log.Fatalf("%s: %v", v.name, err)
			}
			plan = p
			if i >= 3 { // the first three are warm-up
				times = append(times, ms)
			}
		}
		sort.Float64s(times)

		size := "-"
		if v.index != "" {
			var bytes int64
			if err := pool.QueryRow(ctx, `SELECT pg_relation_size(to_regclass($1::text))`, v.index).Scan(&bytes); err != nil {
				log.Fatalf("index size: %v", err)
			}
			size = formatBytes(bytes)
		}
		fmt.Printf("%-40s %-44s %10.3f %11s\n", v.name, plan, median(times), size)
	}

	fmt.Println()
	fmt.Println("The claim's WHERE and ORDER BY match the partial index, so it reads only queued rows.")
	fmt.Println("The real index (partial) has been put back.")
}

// explainClaim runs EXPLAIN ANALYZE of the claim's SELECT and returns the scan used and the execution time.
// It runs in a transaction that is rolled back, because FOR UPDATE would lock the row it picks.
func explainClaim(ctx context.Context, pool *pgxpool.Pool) (string, float64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", 0, err
	}
	defer tx.Rollback(ctx)

	var raw []byte
	err = tx.QueryRow(ctx, `EXPLAIN (ANALYZE, FORMAT JSON)
		SELECT id FROM jobs
		WHERE status = 'queued' AND run_at <= now()
		ORDER BY priority ASC, run_at ASC
		LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&raw)
	if err != nil {
		return "", 0, fmt.Errorf("explain: %w", err)
	}

	var out []map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || len(out) == 0 {
		return "", 0, fmt.Errorf("could not read the explain output: %v", err)
	}
	plan, _ := out[0]["Plan"].(map[string]any)
	execMS, _ := out[0]["Execution Time"].(float64)
	return scanNode(plan), execMS, nil
}

// scanNode finds the scan that reads the table in a plan tree, for example "Index Scan using idx_jobs_queued".
func scanNode(plan map[string]any) string {
	t, _ := plan["Node Type"].(string)
	if strings.Contains(t, "Scan") {
		if idx, ok := plan["Index Name"].(string); ok {
			return t + " using " + idx
		}
		return t
	}
	children, _ := plan["Plans"].([]any)
	for _, c := range children {
		if m, ok := c.(map[string]any); ok {
			if s := scanNode(m); s != "" {
				return s
			}
		}
	}
	return ""
}

// formatBytes prints a size as KB or MB.
func formatBytes(n int64) string {
	if n < 1<<20 {
		return fmt.Sprintf("%.0f KB", float64(n)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
}
