// Command chaos kills worker processes with kill -9 while they run jobs, then counts what was lost or repeated.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/api"
	"github.com/hardik-thapar/dispatch/internal/db"
)

const tenant = "chaos"

// workerProc is one running worker process.
type workerProc struct {
	cmd  *exec.Cmd
	done chan struct{} // closed when the process has exited
}

func main() {
	jobs := flag.Int("jobs", 2000, "number of jobs to submit")
	workers := flag.Int("workers", 4, "number of worker processes to keep running")
	kills := flag.Int("kills", 20, "how many times to kill a random worker with kill -9 (0 = no crashes)")
	lease := flag.Duration("lease", 3*time.Second, "worker lease length")
	reaperEvery := flag.Duration("reaper", time.Second, "reaper interval")
	workerBin := flag.String("worker-bin", "./bin/worker", "path to the worker binary")
	timeout := flag.Duration("timeout", 10*time.Minute, "stop waiting after this long")
	flag.Parse()

	if _, err := os.Stat(*workerBin); err != nil {
		log.Fatalf("worker binary not found at %s (build it: go build -o bin/worker ./cmd/worker)", *workerBin)
	}
	refuseIfWorkersRunning(*workerBin)

	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"
	}
	pool, err := db.NewPool(ctx, url)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	printEnvironment(ctx, pool)
	fmt.Printf("jobs: %d (half idempotent side_effect, half non-idempotent counter), max attempts 50\n", *jobs)
	fmt.Printf("workers: %d processes x 4 slots, lease %v, reaper every %v, %d planned kills (kill -9)\n\n", *workers, *lease, *reaperEvery, *kills)

	// ---- Step 1: start from an empty database ----
	log.Printf("resetting tables (this DELETES all jobs in the database)")
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_attempts, side_effects, counter_effects`); err != nil {
		log.Fatalf("reset tables: %v", err)
	}

	// ---- Step 2: submit the jobs ----
	// even numbers use the idempotent handler, odd numbers use the non-idempotent control
	for i := 0; i < *jobs; i++ {
		jobType := "side_effect"
		if i%2 == 1 {
			jobType = "counter"
		}
		payload := fmt.Sprintf(`{"after_ms": %d}`, 500+rand.IntN(1000)) // keep running 0.5 to 1.5 s after the effect
		_, _, err := api.Submit(ctx, pool, api.SubmitInput{
			TenantID:       tenant,
			Type:           jobType,
			Payload:        []byte(payload),
			MaxAttempts:    50, // high, so a crashed job is retried and never dies
			IdempotencyKey: fmt.Sprintf("chaos-%d", i),
		})
		if err != nil {
			log.Fatalf("submit job %d: %v", i, err)
		}
	}
	log.Printf("submitted %d jobs", *jobs)

	// ---- Step 3: start the worker processes ----
	var mu sync.Mutex
	var procs []*workerProc

	startWorker := func() {
		cmd := exec.Command(*workerBin)
		cmd.Env = append(os.Environ(),
			"DATABASE_URL="+url,
			"WORKER_CONCURRENCY=4",
			"WORKER_LEASE="+lease.String(),
			"REAPER_INTERVAL="+reaperEvery.String(),
		)
		if err := cmd.Start(); err != nil {
			log.Fatalf("start worker: %v", err)
		}
		p := &workerProc{cmd: cmd, done: make(chan struct{})}
		go func() {
			_ = cmd.Wait()
			close(p.done)
		}()
		mu.Lock()
		procs = append(procs, p)
		mu.Unlock()
	}

	aliveWorkers := func() []*workerProc {
		mu.Lock()
		defer mu.Unlock()
		var alive []*workerProc
		for _, p := range procs {
			select {
			case <-p.done:
			default:
				alive = append(alive, p)
			}
		}
		return alive
	}

	for i := 0; i < *workers; i++ {
		startWorker()
	}
	log.Printf("started %d workers", *workers)

	// ---- Step 4: kill random workers with kill -9, and replace each one ----
	stopKilling := make(chan struct{})
	killerDone := make(chan struct{})
	killed := 0
	go func() {
		defer close(killerDone)
		for killed < *kills {
			wait := time.Duration(2000+rand.IntN(4000)) * time.Millisecond // 2 to 6 seconds apart
			select {
			case <-stopKilling:
				return
			case <-time.After(wait):
			}
			alive := aliveWorkers()
			if len(alive) == 0 {
				continue
			}
			victim := alive[rand.IntN(len(alive))]
			_ = victim.cmd.Process.Kill() // SIGKILL: the process cannot clean up
			<-victim.done
			killed++
			log.Printf("killed a worker (%d/%d)", killed, *kills)
			startWorker()
		}
	}()

	// ---- Step 5: wait until every job is finished ----
	start := time.Now()
	for {
		var finished int
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND status IN ('succeeded', 'dead', 'cancelled')`,
			tenant).Scan(&finished)
		if err != nil {
			log.Fatalf("count finished jobs: %v", err)
		}
		if finished == *jobs {
			break
		}
		if time.Since(start) > *timeout {
			log.Printf("timed out after %v with %d of %d jobs finished", *timeout, finished, *jobs)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	elapsed := time.Since(start)

	close(stopKilling)
	<-killerDone
	for _, p := range aliveWorkers() { // stop the remaining workers politely
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			_ = p.cmd.Process.Kill()
		}
	}

	// ---- Step 6: count and report ----
	if !report(ctx, pool, *jobs, killed, elapsed) {
		os.Exit(1)
	}
}

// report counts what happened and prints the results.
// It returns true if every target was met.
func report(ctx context.Context, pool *pgxpool.Pool, jobs, killed int, elapsed time.Duration) bool {
	// one runs a query that returns a single number.
	one := func(query string, args ...any) int {
		var n int
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			log.Fatalf("report query failed: %v", err)
		}
		return n
	}

	succeeded := one(`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND status = 'succeeded'`, tenant)
	dead := one(`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND status = 'dead'`, tenant)
	unfinished := one(`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND status IN ('queued', 'running')`, tenant)

	// idempotent handler: one row per job at most, and none missing
	sideRows := one(`SELECT count(*) FROM side_effects`)
	sideDistinct := one(`SELECT count(DISTINCT job_id) FROM side_effects`)
	sideMissing := one(`SELECT count(*) FROM jobs j
		WHERE j.tenant_id = $1 AND j.type = 'side_effect' AND j.status = 'succeeded'
		AND NOT EXISTS (SELECT 1 FROM side_effects s WHERE s.job_id = j.id)`, tenant)
	sideReruns := one(`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND type = 'side_effect' AND attempts > 1`, tenant)

	// non-idempotent control: one row per execution
	counterRows := one(`SELECT count(*) FROM counter_effects`)
	counterDistinct := one(`SELECT count(DISTINCT job_id) FROM counter_effects`)
	counterMissing := one(`SELECT count(*) FROM jobs j
		WHERE j.tenant_id = $1 AND j.type = 'counter' AND j.status = 'succeeded'
		AND NOT EXISTS (SELECT 1 FROM counter_effects c WHERE c.job_id = j.id)`, tenant)
	counterReruns := one(`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND type = 'counter' AND attempts > 1`, tenant)

	// attempt history
	attempts := one(`SELECT count(*) FROM job_attempts`)
	crashed := one(`SELECT count(*) FROM job_attempts WHERE result = 'lease_lost'`)
	open := one(`SELECT count(*) FROM job_attempts WHERE finished_at IS NULL`)

	sideDup := sideRows - sideDistinct
	counterDup := counterRows - counterDistinct

	fmt.Println()
	fmt.Println("==================== chaos results ====================")
	fmt.Printf("jobs submitted:                          %d\n", jobs)
	fmt.Printf("workers killed with kill -9:             %d\n", killed)
	fmt.Printf("time until all jobs finished:            %v\n", elapsed.Round(time.Second))
	fmt.Println("--- jobs")
	fmt.Printf("succeeded:                               %d\n", succeeded)
	fmt.Printf("dead (ran out of attempts):              %d\n", dead)
	fmt.Printf("LOST OR STUCK (queued or running):       %d   (target 0)\n", unfinished)
	fmt.Println("--- idempotent handler (side_effect)")
	fmt.Printf("jobs that ran more than once:            %d\n", sideReruns)
	fmt.Printf("succeeded jobs with no effect:           %d   (target 0)\n", sideMissing)
	fmt.Printf("DUPLICATE EFFECTS:                       %d   (target 0)\n", sideDup)
	fmt.Println("--- non-idempotent control (counter)")
	fmt.Printf("jobs that ran more than once:            %d\n", counterReruns)
	fmt.Printf("succeeded jobs with no effect:           %d   (target 0)\n", counterMissing)
	fmt.Printf("DUPLICATE EFFECTS:                       %d   (expected more than 0 after crashes)\n", counterDup)
	fmt.Println("--- attempts")
	fmt.Printf("total attempts:                          %d   (%d extra beyond one per job)\n", attempts, attempts-jobs)
	fmt.Printf("attempts closed as lease_lost (crashed): %d\n", crashed)
	fmt.Printf("attempts still open:                     %d\n", open)
	fmt.Println("=======================================================")

	ok := unfinished == 0 && dead == 0 && sideMissing == 0 && counterMissing == 0 && sideDup == 0
	if ok {
		fmt.Println("RESULT: PASS (no lost jobs, no missing effects, no duplicate idempotent effects)")
	} else {
		fmt.Println("RESULT: FAIL")
	}
	return ok
}

// printEnvironment prints the machine and software the numbers were measured on.
func printEnvironment(ctx context.Context, pool *pgxpool.Pool) {
	var pg string
	if err := pool.QueryRow(ctx, `SHOW server_version`).Scan(&pg); err != nil {
		log.Fatalf("read postgres version: %v", err)
	}
	fmt.Println("Dispatch chaos test")
	fmt.Printf("date:      %s\n", time.Now().Format("2006-01-02 15:04 MST"))
	fmt.Printf("machine:   %s/%s, %d CPUs\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Printf("go:        %s\n", runtime.Version())
	fmt.Printf("postgres:  %s (Docker, same machine)\n", pg)
	fmt.Printf("commit:    %s\n", gitCommit())
}

// gitCommit returns the short commit hash, with "-dirty" if there are uncommitted changes outside results/.
func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	commit := strings.TrimSpace(string(out))
	if status, err := exec.Command("git", "status", "--porcelain", "--", ".", ":(exclude)results").Output(); err == nil && strings.TrimSpace(string(status)) != "" {
		commit += "-dirty"
	}
	return commit
}

// refuseIfWorkersRunning stops the test if other worker processes exist, because they would also take jobs.
func refuseIfWorkersRunning(bin string) {
	out, _ := exec.Command("pgrep", "-f", bin).Output()
	var others []string
	for _, pid := range strings.Fields(string(out)) {
		if pid == fmt.Sprint(os.Getpid()) || pid == fmt.Sprint(os.Getppid()) {
			continue // this program, or the "go run" that started it
		}
		others = append(others, pid)
	}
	if len(others) > 0 {
		log.Fatalf("other worker processes are running (pids: %s). Stop them first: pkill -f %s",
			strings.Join(others, " "), bin)
	}
}
