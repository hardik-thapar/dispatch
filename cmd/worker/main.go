package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hardik-thapar/dispatch/internal/db"
	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/retry"
	"github.com/hardik-thapar/dispatch/internal/worker"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"
	}

	concurrency := 4
	if v := os.Getenv("WORKER_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			log.Fatalf("WORKER_CONCURRENCY must be a number of at least 1, got %q", v)
		}
		concurrency = n
	}

	host, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", host, os.Getpid())

	// Ctrl+C or SIGTERM cancels ctx, which tells Run to shut down gracefully.
	// (kill -9 cannot be caught: that is the crash the chaos test uses.)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPool(ctx, url)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	reg := handlers.NewRegistry()
	reg.Register("side_effect", handlers.SideEffect(pool))
	reg.Register("counter", handlers.Counter(pool))
	reg.Register("send_webhook", handlers.Webhook(&http.Client{Timeout: 10 * time.Second}))

	w := worker.New(pool, reg, retry.Backoff{Base: time.Second, Max: 5 * time.Minute}, workerID)

	log.Printf("worker %s started with concurrency %d", workerID, concurrency)
	if err := w.Run(ctx, concurrency); err != nil {
		log.Fatalf("worker: %v", err)
	}
	log.Printf("worker %s stopped", workerID)
}