// Command bench measures Dispatch. Run: go run ./cmd/bench <command>
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/db"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "throughput":
		runThroughput(ctx, os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench <command> [flags]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  throughput   jobs per second against the number of workers")
	os.Exit(2)
}

func databaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"
}

func connect(ctx context.Context) *pgxpool.Pool {
	pool, err := db.NewPool(ctx, databaseURL())
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	return pool
}

// printEnvironment prints the machine and software the numbers were measured on.
func printEnvironment(ctx context.Context, pool *pgxpool.Pool) {
	var pg string
	if err := pool.QueryRow(ctx, `SHOW server_version`).Scan(&pg); err != nil {
		log.Fatalf("read postgres version: %v", err)
	}
	fmt.Println("Dispatch benchmark")
	fmt.Printf("date:      %s\n", time.Now().Format("2006-01-02 15:04 MST"))
	fmt.Printf("machine:   %s/%s, %d CPUs\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Printf("go:        %s\n", runtime.Version())
	fmt.Printf("postgres:  %s (Docker, same machine)\n", pg)
	fmt.Printf("commit:    %s\n", gitCommit())
	fmt.Println()
}

// gitCommit returns the short commit hash, with "-dirty" if there are uncommitted changes.
func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	commit := strings.TrimSpace(string(out))
	if status, err := exec.Command("git", "status", "--porcelain").Output(); err == nil && strings.TrimSpace(string(status)) != "" {
		commit += "-dirty"
	}
	return commit
}

// resetTables empties every table, so each run starts from nothing.
func resetTables(ctx context.Context, pool *pgxpool.Pool) {
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_attempts, side_effects, counter_effects`); err != nil {
		log.Fatalf("reset tables: %v", err)
	}
}

// refuseIfWorkersRunning stops the benchmark if other worker processes exist,
// because they would also take jobs and spoil the numbers.
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

// workerSet is a group of running worker processes.
type workerSet struct {
	cmds []*exec.Cmd
	done []chan struct{} // done[i] is closed when cmds[i] has exited
}

// startWorkers starts n worker processes. env holds extra environment variables.
func startWorkers(bin string, n int, env []string) *workerSet {
	s := &workerSet{}
	for i := 0; i < n; i++ {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), env...)
		if err := cmd.Start(); err != nil {
			log.Fatalf("start worker: %v", err)
		}
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		s.cmds = append(s.cmds, cmd)
		s.done = append(s.done, done)
	}
	return s
}

// stop shuts the workers down politely (SIGTERM), and kills any that take too long.
func (s *workerSet) stop() {
	for _, c := range s.cmds {
		_ = c.Process.Signal(syscall.SIGTERM)
	}
	for i, c := range s.cmds {
		select {
		case <-s.done[i]:
		case <-time.After(10 * time.Second):
			_ = c.Process.Kill()
			<-s.done[i]
		}
	}
}
