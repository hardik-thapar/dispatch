package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runRecovery measures how long it takes for a job to be started again
// after the worker that was running it is killed with kill -9.
func runRecovery(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("recovery", flag.ExitOnError)
	leases := fs.String("leases", "1s,3s,10s", "lease lengths, separated by commas")
	reaper := fs.Duration("reaper", time.Second, "reaper interval")
	poll := fs.Duration("poll", 200*time.Millisecond, "worker poll interval")
	workers := fs.Int("workers", 4, "number of worker processes")
	concurrency := fs.Int("concurrency", 4, "job slots in each worker process")
	jobs := fs.Int("jobs", 10, "long-running jobs in each run")
	reps := fs.Int("reps", 5, "runs per lease length")
	bin := fs.String("worker-bin", "./bin/worker", "path to the worker binary")
	fs.Parse(args)

	var leaseList []time.Duration
	for _, s := range strings.Split(*leases, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil || d <= 0 {
			log.Fatalf("-leases: %q is not a duration such as 3s", s)
		}
		leaseList = append(leaseList, d)
	}
	if _, err := os.Stat(*bin); err != nil {
		log.Fatalf("worker binary not found at %s (build it: go build -o bin/worker ./cmd/worker)", *bin)
	}
	refuseIfWorkersRunning(*bin)

	pool := connect(ctx)
	defer pool.Close()

	printEnvironment(ctx, pool)
	fmt.Println("Crash recovery time: a worker is killed with kill -9 while it runs jobs.")
	fmt.Println("Recovery = time from the kill until another worker STARTS the interrupted job again.")
	fmt.Printf("%d workers, reaper every %v, poll interval %v, %d kills per lease, times in seconds\n\n", *workers, *reaper, *poll, *reps)
	fmt.Printf("%7s %6s %5s %7s %7s %7s   %s\n", "lease", "kills", "jobs", "min", "median", "max", "expected")

	for _, lease := range leaseList {
		var perKill []float64 // one value per kill: the median of that kill's interrupted jobs
		jobCount := 0
		for rep := 1; rep <= *reps; rep++ {
			secs, err := recoveryRun(ctx, pool, *bin, *workers, *concurrency, *jobs, lease, *reaper, *poll)
			if err != nil {
				log.Fatalf("lease=%v run %d: %v", lease, rep, err)
			}
			sort.Float64s(secs)
			fmt.Fprintf(os.Stderr, "lease=%v run %d/%d: %d interrupted jobs, recovered in %.2f to %.2f s\n",
				lease, rep, *reps, len(secs), secs[0], secs[len(secs)-1])
			perKill = append(perKill, secs[len(secs)/2])
			jobCount += len(secs)
		}
		sort.Float64s(perKill)
		low := (2 * lease / 3).Seconds()
		high := (lease + *reaper + *poll*5/4).Seconds()
		fmt.Printf("%7v %6d %5d %7.2f %7.2f %7.2f   %.1f to %.1f\n",
			lease, len(perKill), jobCount, perKill[0], perKill[len(perKill)/2], perKill[len(perKill)-1], low, high)
	}

	fmt.Println()
	fmt.Println("min, median and max are over kills (one value per killed worker).")
	fmt.Println("expected = 2/3 of the lease (a heartbeat was sent shortly before the kill)")
	fmt.Println("           up to lease + reaper interval + poll interval.")
	fmt.Println("Jobs from the same killed worker share one kill time, so they are not independent samples.")
}

// recoveryRun kills one worker and returns the recovery time, in seconds, of each job it was running.
func recoveryRun(ctx context.Context, pool *pgxpool.Pool, bin string, workers, concurrency, jobs int, lease, reaper, poll time.Duration) ([]float64, error) {
	resetTables(ctx, pool)

	// jobs that sleep for 60 seconds, so they are still running when the worker is killed
	_, err := pool.Exec(ctx,
		`INSERT INTO jobs (tenant_id, type, max_attempts, idempotency_key, payload)
		 SELECT 'bench', 'side_effect', 10, 'r' || g, '{"sleep_ms": 60000}'::jsonb
		 FROM generate_series(1, $1::int) g`, jobs)
	if err != nil {
		return nil, fmt.Errorf("insert jobs: %w", err)
	}

	set := startWorkers(bin, workers, []string{
		"DATABASE_URL=" + databaseURL(),
		"WORKER_CONCURRENCY=" + strconv.Itoa(concurrency),
		"WORKER_LEASE=" + lease.String(),
		"REAPER_INTERVAL=" + reaper.String(),
		"WORKER_POLL_INTERVAL=" + poll.String(),
		"DB_MAX_CONNS=8",
	})
	defer set.kill()

	// wait until every job is running
	if err := waitUntil(15*time.Second, func() (bool, error) {
		var running int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'running'`).Scan(&running)
		return running == jobs, err
	}); err != nil {
		return nil, fmt.Errorf("waiting for all jobs to start: %w", err)
	}

	// a random pause, so the kill lands at a different point of the heartbeat cycle each run
	time.Sleep(time.Duration(rand.Int64N(int64(lease))))

	// find which workers hold jobs, and pick one of them at random
	type victim struct {
		index  int
		jobIDs []string
	}
	var candidates []victim
	for i, c := range set.cmds {
		rows, err := pool.Query(ctx,
			`SELECT id FROM jobs WHERE status = 'running' AND lease_owner LIKE '%-' || $1::text`,
			strconv.Itoa(c.Process.Pid))
		if err != nil {
			return nil, fmt.Errorf("find jobs of worker %d: %w", i, err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan job id: %w", err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("find jobs of worker %d: %w", i, err)
		}
		if len(ids) > 0 {
			candidates = append(candidates, victim{index: i, jobIDs: ids})
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no worker is holding any job")
	}
	v := candidates[rand.IntN(len(candidates))]

	// kill it, and note the time with the DATABASE clock
	_ = set.cmds[v.index].Process.Kill()
	var killedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&killedAt); err != nil {
		return nil, fmt.Errorf("read database clock: %w", err)
	}

	// wait until every interrupted job has a new attempt that has started
	started := map[string]time.Time{}
	err = waitUntil(3*(lease+reaper)+15*time.Second, func() (bool, error) {
		rows, err := pool.Query(ctx,
			`SELECT job_id::text, min(started_at) FROM job_attempts
			 WHERE job_id::text = ANY($1::text[]) AND attempt_number > 1
			 GROUP BY job_id`, v.jobIDs)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var t time.Time
			if err := rows.Scan(&id, &t); err != nil {
				return false, err
			}
			started[id] = t
		}
		return len(started) == len(v.jobIDs), rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for the interrupted jobs to restart (%d of %d did): %w", len(started), len(v.jobIDs), err)
	}

	var secs []float64
	for _, id := range v.jobIDs {
		d := started[id].Sub(killedAt).Seconds()
		if d < 0 {
			return nil, fmt.Errorf("job %s restarted %.2f s BEFORE the kill (a healthy lease expired?)", id, -d)
		}
		secs = append(secs, d)
	}
	return secs, nil
}

// waitUntil calls check every 50 ms until it returns true, or the timeout passes.
func waitUntil(timeout time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %v", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
