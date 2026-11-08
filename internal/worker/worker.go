package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/handlers"
	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/retry"
)

// Worker runs claimed jobs: it calls the right handler and records the result.
type Worker struct {
	pool         *pgxpool.Pool
	registry     *handlers.Registry
	backoff      retry.Backoff
	inFlight     atomic.Int32
	lease        time.Duration
	pollInterval time.Duration
	id           string // the worker's name, used as the lease owner
}

// New builds a Worker.
func New(pool *pgxpool.Pool, registry *handlers.Registry, backoff retry.Backoff, id string) *Worker {
	w := &Worker{pool: pool, registry: registry, backoff: backoff, id: id}
	w.lease = 30 * time.Second
	w.pollInterval = 200 * time.Millisecond
	return w
}

func (w *Worker) runJob(ctx context.Context, job queue.Job) error {
	// start the history row; before the handler runs
	if err := queue.StartAttempt(ctx, w.pool, job.ID, job.Attempts, w.id); err != nil {
		log.Printf("worker %s: could not record start of job %s: %v", w.id, job.ID, err)
	}

	// Step 1: run
	var handlerErr error
	var timedOut bool
	timeout := time.Duration(job.MaxRunTime) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	h, found := w.registry.Get(job.Type)
	if !found {
		handlerErr = fmt.Errorf("no handler for type %q", job.Type)
	} else {
		handlerErr = h(runCtx, job) // call the handler h with runCtx and job
	}
	if handlerErr != nil && runCtx.Err() == context.DeadlineExceeded {
		handlerErr = fmt.Errorf("job timed out after %ds", int(timeout.Seconds()))
		timedOut = true
	}
	// Step 2: record
	var recorded bool
	var err error

	if handlerErr == nil {
		recorded, err = queue.Succeed(ctx, w.pool, job.ID, job.Attempts)
	} else {
		recorded, err = queue.Fail(ctx, w.pool, job.ID, job.Attempts, handlerErr.Error(), w.backoff.Delay(job.Attempts))
	}

	if err != nil {
		return fmt.Errorf("record result: %w", err)
	}

	result := "succeeded"
	switch {
	case !recorded:
		result = "lease_lost"
	case timedOut:
		result = "timed_out"
	case handlerErr != nil:
		result = "failed"
	}

	errMsg := ""
	if handlerErr != nil {
		errMsg = handlerErr.Error()
	}
	if err := queue.FinishAttempt(ctx, w.pool, job.ID, job.Attempts, result, errMsg); err != nil {
		log.Printf("worker %s: could not record end of job %s: %v", w.id, job.ID, err)
	}

	// Step 3: check
	if !recorded {
		log.Printf("worker %s: result for job %s attempt %d ignored (lost the job)", w.id, job.ID, job.Attempts)
	}

	return nil
}

func (w *Worker) runPool(ctx context.Context, concurrency int, jobs <-chan queue.Job) {
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := w.runJob(ctx, job)
				w.inFlight.Add(-1)
				if err != nil {
					log.Printf("worker %s: job %s: %v", w.id, job.ID, err)
				}
			}
		}()
	}
	wg.Wait()
}

func (w *Worker) wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(w.pollInterval):
	}
}

func (w *Worker) claimLoop(ctx context.Context, concurrency int, jobs chan<- queue.Job) {
	for {
		if ctx.Err() != nil {
			return
		}

		free := concurrency - int(w.inFlight.Load())
		if free <= 0 {
			w.wait(ctx) // nothing free: wait, then check again
			continue
		}

		claimed, err := queue.Claim(ctx, w.pool, w.id, free, w.lease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker %s: claim failed: %v", w.id, err)
			w.wait(ctx)
			continue
		}
		if len(claimed) == 0 {
			w.wait(ctx)
			continue
		}

		w.inFlight.Add(int32(len(claimed)))
		for _, job := range claimed {
			jobs <- job
		}
	}
}

func (w *Worker) Run(ctx context.Context, concurrency int) error {
	if concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1, got %d", concurrency)
	}

	jobs := make(chan queue.Job, concurrency)
	jobCtx := context.WithoutCancel(ctx)

	poolDone := make(chan struct{})
	go func() {
		w.runPool(jobCtx, concurrency, jobs) // uses jobCtx so even when ctx is cancelled, all jobs that are claimed are finished
		close(poolDone)
	}()

	w.claimLoop(ctx, concurrency, jobs)
	close(jobs)
	<-poolDone // wait until the pool has finished everything
	return nil
}
