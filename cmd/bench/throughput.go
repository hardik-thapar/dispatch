package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runThroughput measures jobs per second for different numbers of worker processes.
func runThroughput(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("throughput", flag.ExitOnError)
	jobs := fs.Int("jobs", 20000, "number of noop jobs per run")
	reps := fs.Int("reps", 3, "runs per worker count (the median is reported)")
	list := fs.String("workers", "1,2,4,8,16,32", "worker process counts, separated by commas")
	concurrency := fs.Int("concurrency", 4, "job slots in each worker process")
	bin := fs.String("worker-bin", "./bin/worker", "path to the worker binary")
	fs.Parse(args)

	counts, err := parseCounts(*list)
	if err != nil {
		log.Fatalf("-workers: %v", err)
	}
	if _, err := os.Stat(*bin); err != nil {
		log.Fatalf("worker binary not found at %s (build it: go build -o bin/worker ./cmd/worker)", *bin)
	}
	refuseIfWorkersRunning(*bin)

	pool := connect(ctx)
	defer pool.Close()

	printEnvironment(ctx, pool)
	fmt.Printf("Throughput of noop jobs: %d jobs per run, median of %d runs\n\n", *jobs, *reps)
	fmt.Printf("%-8s %6s %10s %8s %8s %9s\n", "workers", "slots", "jobs/s", "min", "max", "speedup")

	var baseline float64
	for _, workers := range counts {
		var rates []float64
		for rep := 1; rep <= *reps; rep++ {
			rate, err := throughputRun(ctx, pool, *bin, workers, *concurrency, *jobs)
			if err != nil {
				log.Fatalf("workers=%d run %d: %v", workers, rep, err)
			}
			fmt.Fprintf(os.Stderr, "workers=%d run %d/%d: %.0f jobs/s\n", workers, rep, *reps, rate)
			rates = append(rates, rate)
		}
		sort.Float64s(rates)
		median := rates[len(rates)/2]
		if baseline == 0 {
			baseline = median
		}
		fmt.Printf("%-8d %6d %10.0f %8.0f %8.0f %8.2fx\n",
			workers, workers**concurrency, median, rates[0], rates[len(rates)-1], median/baseline)
	}

	fmt.Println()
	fmt.Println("Each worker is a separate process with its own job slots.")
	fmt.Println("Jobs are inserted in bulk before the workers start, so this measures")
	fmt.Println("claiming, running and recording jobs, not submitting them.")
}

// throughputRun does one run and returns the jobs per second.
func throughputRun(ctx context.Context, pool *pgxpool.Pool, bin string, workers, concurrency, jobs int) (float64, error) {
	resetTables(ctx, pool)

	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'bench', 'noop', 3, 'b' || g FROM generate_series(1, $1::int) g`, jobs)
	if err != nil {
		return 0, fmt.Errorf("insert jobs: %w", err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE jobs`); err != nil { // fresh statistics for the planner
		return 0, fmt.Errorf("analyze: %w", err)
	}

	set := startWorkers(bin, workers, []string{
		"DATABASE_URL=" + databaseURL(),
		"WORKER_CONCURRENCY=" + strconv.Itoa(concurrency),
		"DB_MAX_CONNS=8", // keeps 32 workers inside Postgres's max_connections
	})
	defer set.stop()

	// wait until every job has succeeded
	start := time.Now()
	for {
		var done int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'succeeded'`).Scan(&done); err != nil {
			return 0, fmt.Errorf("count succeeded: %w", err)
		}
		if done == jobs {
			break
		}
		if time.Since(start) > 10*time.Minute {
			return 0, fmt.Errorf("only %d of %d jobs finished in 10 minutes", done, jobs)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Measure with the database clock: first attempt started to last attempt finished.
	// This leaves out the time the worker processes need to start.
	var n int
	var seconds float64
	err = pool.QueryRow(ctx,
		`SELECT count(*), extract(epoch FROM max(finished_at) - min(started_at))::float8
		 FROM job_attempts WHERE result = 'succeeded'`).Scan(&n, &seconds)
	if err != nil {
		return 0, fmt.Errorf("measure: %w", err)
	}
	if n != jobs {
		return 0, fmt.Errorf("%d attempts succeeded, want %d (some jobs ran more than once?)", n, jobs)
	}
	if seconds <= 0 {
		return 0, fmt.Errorf("measured a duration of %v seconds", seconds)
	}
	return float64(n) / seconds, nil
}

// parseCounts turns "1,2,4" into []int{1, 2, 4}.
func parseCounts(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("%q is not a number of at least 1", part)
		}
		out = append(out, n)
	}
	return out, nil
}
