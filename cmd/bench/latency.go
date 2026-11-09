package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/api"
)

// latencyResult is the outcome of one run.
type latencyResult struct {
	achieved           float64 // jobs per second that were really submitted
	p50, p95, p99, max float64 // queue wait in milliseconds
}

// runLatency measures how long jobs wait before starting, with jobs arriving at a steady rate.
func runLatency(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("latency", flag.ExitOnError)
	rates := fs.String("rates", "500,1000,2000,3000", "arrival rates in jobs per second, separated by commas")
	workers := fs.Int("workers", 4, "number of worker processes")
	concurrency := fs.Int("concurrency", 4, "job slots in each worker process")
	duration := fs.Duration("duration", 15*time.Second, "how long jobs keep arriving in each run")
	poll := fs.Duration("poll", 200*time.Millisecond, "worker poll interval when the queue is empty")
	reps := fs.Int("reps", 3, "runs per rate (the median of each number is reported)")
	bin := fs.String("worker-bin", "./bin/worker", "path to the worker binary")
	fs.Parse(args)

	rateList, err := parseCounts(*rates)
	if err != nil {
		log.Fatalf("-rates: %v", err)
	}
	if _, err := os.Stat(*bin); err != nil {
		log.Fatalf("worker binary not found at %s (build it: go build -o bin/worker ./cmd/worker)", *bin)
	}
	refuseIfWorkersRunning(*bin)

	pool := connect(ctx)
	defer pool.Close()

	printEnvironment(ctx, pool)
	fmt.Printf("Queue wait latency: noop jobs arriving at a steady rate\n")
	fmt.Printf("%d workers (%d slots), poll interval %v, %v of arrivals per run, median of %d runs\n",
		*workers, *workers**concurrency, *poll, *duration, *reps)
	fmt.Printf("Wait = time from the job being created to its first attempt starting.\n")
	fmt.Printf("The first 2 seconds of each run are left out (warm-up).\n\n")
	fmt.Printf("%8s %9s %8s %8s %8s %8s\n", "rate/s", "achieved", "p50 ms", "p95 ms", "p99 ms", "max ms")

	for _, rate := range rateList {
		var runs []latencyResult
		for rep := 1; rep <= *reps; rep++ {
			r, err := latencyRun(ctx, pool, *bin, *workers, *concurrency, rate, *duration, *poll)
			if err != nil {
				log.Fatalf("rate=%d run %d: %v", rate, rep, err)
			}
			fmt.Fprintf(os.Stderr, "rate=%d run %d/%d: p50 %.1f ms, p99 %.1f ms\n", rate, rep, *reps, r.p50, r.p99)
			runs = append(runs, r)
		}
		fmt.Printf("%8d %9.0f %8.1f %8.1f %8.1f %8.1f\n", rate,
			medianOf(runs, func(r latencyResult) float64 { return r.achieved }),
			medianOf(runs, func(r latencyResult) float64 { return r.p50 }),
			medianOf(runs, func(r latencyResult) float64 { return r.p95 }),
			medianOf(runs, func(r latencyResult) float64 { return r.p99 }),
			medianOf(runs, func(r latencyResult) float64 { return r.max }))
	}

	fmt.Println()
	fmt.Println("Jobs are submitted one by one through the same code the API uses.")
	fmt.Println("When the queue is empty, a worker sleeps for the poll interval, so a")
	fmt.Println("new job can wait for that sleep to end. A shorter poll interval lowers")
	fmt.Println("the wait and raises the number of queries sent to the database.")
}

// latencyRun does one run at one arrival rate.
func latencyRun(ctx context.Context, pool *pgxpool.Pool, bin string, workers, concurrency, rate int, duration, poll time.Duration) (latencyResult, error) {
	resetTables(ctx, pool)

	set := startWorkers(bin, workers, []string{
		"DATABASE_URL=" + databaseURL(),
		"WORKER_CONCURRENCY=" + strconv.Itoa(concurrency),
		"WORKER_POLL_INTERVAL=" + poll.String(),
		"DB_MAX_CONNS=8",
	})
	defer set.stop()
	time.Sleep(time.Second) // let the workers start and begin polling

	submitted := submitAtRate(ctx, pool, rate, duration)

	// wait until every submitted job has succeeded
	waitStart := time.Now()
	for {
		var done int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'succeeded'`).Scan(&done); err != nil {
			return latencyResult{}, fmt.Errorf("count succeeded: %w", err)
		}
		if done == submitted {
			break
		}
		if time.Since(waitStart) > 2*time.Minute {
			return latencyResult{}, fmt.Errorf("only %d of %d jobs finished within 2 minutes after submitting stopped", done, submitted)
		}
		time.Sleep(250 * time.Millisecond)
	}

	var n int
	var p50, p95, p99, mx float64
	err := pool.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(percentile_cont(0.50) WITHIN GROUP (ORDER BY w), 0)::float8,
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY w), 0)::float8,
		       coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY w), 0)::float8,
		       coalesce(max(w), 0)::float8
		FROM (
		    SELECT (extract(epoch FROM (a.started_at - j.created_at)) * 1000)::float8 AS w
		    FROM job_attempts a JOIN jobs j ON j.id = a.job_id
		    WHERE a.attempt_number = 1
		      AND j.created_at > (SELECT min(created_at) FROM jobs) + interval '2 seconds'
		) t`).Scan(&n, &p50, &p95, &p99, &mx)
	if err != nil {
		return latencyResult{}, fmt.Errorf("measure: %w", err)
	}
	if n == 0 {
		return latencyResult{}, fmt.Errorf("no jobs were measured (is -duration longer than the 2 second warm-up?)")
	}
	return latencyResult{
		achieved: float64(submitted) / duration.Seconds(),
		p50:      p50, p95: p95, p99: p99, max: mx,
	}, nil
}

// submitAtRate submits noop jobs at a steady rate for the given duration.
// 16 goroutines each submit one job at a time on a fixed schedule, staggered so
// that arrivals are spread out. It returns the number of jobs submitted.
func submitAtRate(ctx context.Context, pool *pgxpool.Pool, rate int, duration time.Duration) int {
	const submitters = 16
	interval := time.Duration(float64(time.Second) * submitters / float64(rate))

	var count atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(duration)

	for s := 0; s < submitters; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			next := start.Add(time.Duration(s) * interval / submitters)
			for i := 0; next.Before(end); i++ {
				time.Sleep(time.Until(next)) // a negative wait returns at once, so a late submitter catches up
				_, _, err := api.Submit(ctx, pool, api.SubmitInput{
					TenantID:       "bench",
					Type:           "noop",
					IdempotencyKey: fmt.Sprintf("l-%d-%d", s, i),
				})
				if err != nil {
					log.Fatalf("submit: %v", err)
				}
				count.Add(1)
				next = next.Add(interval)
			}
		}(s)
	}
	wg.Wait()
	return int(count.Load())
}

// medianOf returns the median of pick(r) over the runs.
func medianOf(runs []latencyResult, pick func(latencyResult) float64) float64 {
	vals := make([]float64, len(runs))
	for i, r := range runs {
		vals[i] = pick(r)
	}
	sort.Float64s(vals)
	return vals[len(vals)/2]
}
