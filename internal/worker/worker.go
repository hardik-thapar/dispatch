package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/retry"
)

// Worker runs claimed jobs: it calls the right handler and records the result.
type Worker struct {
	pool     *pgxpool.Pool
	registry *handlers.Registry
	backoff  retry.Backoff
	id       string // the worker's name, used as the lease owner
}

// New builds a Worker.
func New(pool *pgxpool.Pool, registry *handlers.Registry, backoff retry.Backoff, id string) *Worker {
	return &Worker{pool: pool, registry: registry, backoff: backoff, id: id}
}

func (w *Worker) runJob(ctx context.Context, job queue.Job) error {
	// Step 1: run
	var handlerErr error
	h, found := w.registry.Get(job.Type)
	if !found {
		handlerErr = fmt.Errorf("no handler for type %q", job.Type)
	} else {
		handlerErr = h(ctx, job) // call the handler h with ctx and job
	}

	// Step 2: record
	var recorded bool
	var err error
	if handlerErr == nil {
		recorded, err = queue.Succeed(ctx, w.pool, job.ID, job.Attempts)
	} else {
		recorded, err = queue.Fail(ctx, w.pool, job.ID, job.Attempts, handlerErr.Error(), w.backoff.Delay(job.Attempts))
	}

	// Step 3: check
	if err != nil {
		return fmt.Errorf("record result: %w", err)
	}
	if !recorded {
		log.Printf("worker %s: result for job %s attempt %d ignored (lost the job)", w.id, job.ID, job.Attempts)
	}
	return nil
}
