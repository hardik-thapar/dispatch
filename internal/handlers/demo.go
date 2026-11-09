package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/queue"
)

// demoPayload is the JSON the demo handlers read from a job's payload.
type demoPayload struct {
	URL     string `json:"url"`      // used by the webhook handler
	SleepMS int    `json:"sleep_ms"` // pretend to work for this
	AfterMS int    `json:"after_ms"` // keep running this long AFTER the effect is writtenlong
}

// parsePayload reads the payload. An empty payload means all defaults.
func parsePayload(job queue.Job) (demoPayload, error) {
	var p demoPayload
	if len(job.Payload) == 0 {
		return p, nil
	}
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return p, fmt.Errorf("bad payload: %w", err)
	}
	return p, nil
}

// sleepCtx waits for ms milliseconds, or stops early if ctx is cancelled.
func sleepCtx(ctx context.Context, ms int) error {
	if ms <= 0 {
		return nil
	}
	select {
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SideEffect is an IDEMPOTENT handler. Its effect is one row in side_effects,
// keyed by tenant and idempotency key. Running the same job twice still leaves one row.
func SideEffect(pool *pgxpool.Pool) Handler {
	return func(ctx context.Context, job queue.Job) error {
		p, err := parsePayload(job)
		if err != nil {
			return err
		}
		if err := sleepCtx(ctx, p.SleepMS); err != nil {
			return err
		}
		key := job.TenantID + ":" + job.IdempotencyKey
		_, err = pool.Exec(ctx,
			`INSERT INTO side_effects (effect_key, job_id) VALUES ($1, $2)
			 ON CONFLICT (effect_key) DO NOTHING`, key, job.ID)
		if err != nil {
			return err
		}
		return sleepCtx(ctx, p.AfterMS)
	}
}

// Counter is a NON-IDEMPOTENT control handler. Every execution adds a row to
// counter_effects, so a job that runs twice leaves two rows. The chaos test
// uses it to show what happens without idempotency.
func Counter(pool *pgxpool.Pool) Handler {
	return func(ctx context.Context, job queue.Job) error {
		p, err := parsePayload(job)
		if err != nil {
			return err
		}
		if err := sleepCtx(ctx, p.SleepMS); err != nil {
			return err
		}
		_, err = pool.Exec(ctx, `INSERT INTO counter_effects (job_id) VALUES ($1)`, job.ID)
		if err != nil {
			return err
		}
		return sleepCtx(ctx, p.AfterMS)
	}
}

// Webhook sends the job's payload as a POST request to the "url" in the payload.
// Any 2xx reply counts as success. Anything else is an error, so the job retries.
func Webhook(client *http.Client) Handler {
	return func(ctx context.Context, job queue.Job) error {
		p, err := parsePayload(job)
		if err != nil {
			return err
		}
		if p.URL == "" {
			return fmt.Errorf("payload has no url")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(job.Payload))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("send webhook: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("webhook returned status %d", resp.StatusCode)
		}
		return nil
	}
}
