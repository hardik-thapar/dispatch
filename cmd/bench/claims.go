package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file compares three ways to claim a job. Only the third one is used by Dispatch.
// The first two are kept ONLY for this comparison. Do not use them in real code.

// claimFunc claims one job and returns its id, or ok = false if it got none.
type claimFunc func(ctx context.Context, pool *pgxpool.Pool, workerID string) (id string, ok bool, err error)

// claimNaive reads the id first, then updates it in a separate statement.
// Another claimer can read the same row between the two steps, so a job can be claimed twice.
func claimNaive(ctx context.Context, pool *pgxpool.Pool, workerID string) (string, bool, error) {
	var id string
	err := pool.QueryRow(ctx,
		`SELECT id::text FROM jobs
		 WHERE status = 'queued' AND run_at <= now()
		 ORDER BY priority ASC, run_at ASC LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	// <-- the gap: another claimer can read the same row here
	_, err = pool.Exec(ctx,
		`UPDATE jobs SET status = 'running', lease_owner = $2,
		        lease_end = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now()
		 WHERE id = $1`, id, workerID)
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// claimForUpdate finds and updates in one statement and locks the row, but it WAITS on rows
// locked by others. After the wait the row no longer matches, and the statement can return nothing.
func claimForUpdate(ctx context.Context, pool *pgxpool.Pool, workerID string) (string, bool, error) {
	return claimOne(ctx, pool, workerID, `FOR UPDATE`)
}

// claimSkipLocked is the same statement with SKIP LOCKED: locked rows are skipped, not waited for.
// This is the strategy Dispatch uses.
func claimSkipLocked(ctx context.Context, pool *pgxpool.Pool, workerID string) (string, bool, error) {
	return claimOne(ctx, pool, workerID, `FOR UPDATE SKIP LOCKED`)
}

func claimOne(ctx context.Context, pool *pgxpool.Pool, workerID, lockClause string) (string, bool, error) {
	var id string
	err := pool.QueryRow(ctx,
		`UPDATE jobs SET status = 'running', lease_owner = $1,
		        lease_end = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now()
		 WHERE id = (SELECT id FROM jobs
		             WHERE status = 'queued' AND run_at <= now()
		             ORDER BY priority ASC, run_at ASC LIMIT 1 `+lockClause+`)
		 RETURNING id::text`, workerID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// claimResult is the outcome of one run of one strategy.
type claimResult struct {
	jobsPerSec float64
	double     int // claims beyond one per job
	empty      int // claims that returned nothing
	claims     int
}

// runClaims compares the three strategies.
func runClaims(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("claims", flag.ExitOnError)
	jobs := fs.Int("jobs", 20000, "queued jobs per run")
	claimers := fs.Int("claimers", 16, "concurrent claimers (at most 19 with the default pool of 20 connections)")
	reps := fs.Int("reps", 3, "runs per strategy (the median is reported)")
	fs.Parse(args)
	if *claimers < 1 || *claimers > 19 {
		log.Fatalf("-claimers must be between 1 and 19")
	}

	pool := connect(ctx)
	defer pool.Close()

	strategies := []struct {
		name  string
		claim claimFunc
	}{
		{"naive select, then update", claimNaive},
		{"FOR UPDATE", claimForUpdate},
		{"FOR UPDATE SKIP LOCKED", claimSkipLocked},
	}

	printEnvironment(ctx, pool)
	fmt.Printf("Claim strategy comparison: %d concurrent claimers, %d queued jobs, one job per claim, median of %d runs\n", *claimers, *jobs, *reps)
	fmt.Println("No handler runs: every claimer just claims jobs until none are queued.")
	fmt.Println()
	fmt.Printf("%-28s %9s %14s %9s %13s\n", "strategy", "jobs/s", "double claims", "% double", "empty claims")

	for _, s := range strategies {
		var rates, doubles, empties, claims []float64
		for rep := 1; rep <= *reps; rep++ {
			r, err := claimRun(ctx, pool, s.claim, *claimers, *jobs)
			if err != nil {
				log.Fatalf("%s, run %d: %v", s.name, rep, err)
			}
			fmt.Fprintf(log.Writer(), "%s run %d/%d: %.0f jobs/s, %d double claims, %d empty claims\n",
				s.name, rep, *reps, r.jobsPerSec, r.double, r.empty)
			rates = append(rates, r.jobsPerSec)
			doubles = append(doubles, float64(r.double))
			empties = append(empties, float64(r.empty))
			claims = append(claims, float64(r.claims))
		}
		d, c := medianUnsorted(doubles), medianUnsorted(claims)
		fmt.Printf("%-28s %9.0f %14.0f %8.1f%% %13.0f\n",
			s.name, medianUnsorted(rates), d, d/c*100, medianUnsorted(empties))
	}

	fmt.Println()
	fmt.Println("jobs/s counts each job once. A double claim is a claim of a job that was already claimed.")
	fmt.Println("An empty claim returned nothing (the claimer retries at once).")
}

// claimRun runs one strategy over a fresh table of queued jobs.
func claimRun(ctx context.Context, pool *pgxpool.Pool, claim claimFunc, claimers, jobs int) (claimResult, error) {
	resetTables(ctx, pool)
	if _, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key)
		 SELECT 'bench', 'noop', 3, 'c' || g FROM generate_series(1, $1::int) g`, jobs); err != nil {
		return claimResult{}, fmt.Errorf("insert jobs: %w", err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE jobs`); err != nil {
		return claimResult{}, fmt.Errorf("analyze: %w", err)
	}

	stop := make(chan struct{})
	errs := make(chan error, claimers)
	got := make([][]string, claimers) // ids claimed by each claimer
	empty := make([]int, claimers)
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < claimers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			workerID := fmt.Sprintf("c%d", w)
			for {
				select {
				case <-stop:
					return
				default:
				}
				id, ok, err := claim(ctx, pool, workerID)
				if err != nil {
					errs <- err
					return
				}
				if ok {
					got[w] = append(got[w], id)
				} else {
					empty[w]++
				}
			}
		}(w)
	}

	// stop when no queued jobs remain
	var runErr error
	for {
		var queued int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'queued'`).Scan(&queued); err != nil {
			runErr = fmt.Errorf("count queued: %w", err)
			break
		}
		if queued == 0 {
			break
		}
		if time.Since(start) > 5*time.Minute {
			runErr = fmt.Errorf("%d jobs still queued after 5 minutes", queued)
			break
		}
		select {
		case err := <-errs:
			runErr = err
		case <-time.After(20 * time.Millisecond):
		}
		if runErr != nil {
			break
		}
	}
	elapsed := time.Since(start)
	close(stop)
	wg.Wait()

	if runErr != nil {
		return claimResult{}, runErr
	}
	select {
	case err := <-errs:
		return claimResult{}, err
	default:
	}

	seen := map[string]bool{}
	claims, emptyTotal := 0, 0
	for w := range got {
		for _, id := range got[w] {
			seen[id] = true
			claims++
		}
		emptyTotal += empty[w]
	}
	if len(seen) != jobs {
		return claimResult{}, fmt.Errorf("only %d of %d jobs were claimed", len(seen), jobs)
	}
	return claimResult{
		jobsPerSec: float64(jobs) / elapsed.Seconds(),
		double:     claims - jobs,
		empty:      emptyTotal,
		claims:     claims,
	}, nil
}

// medianUnsorted returns the median of xs without changing xs.
func medianUnsorted(xs []float64) float64 {
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	return median(c)
}
