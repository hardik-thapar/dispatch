# Dispatch

A fault-tolerant background job queue built on PostgreSQL, written in Go.

Applications hand slow or unreliable work to background workers: sending webhooks, generating reports, calling third-party APIs. Dispatch stores those jobs in Postgres and runs them on a pool of workers, so that:

- a worker crash does not lose or strand a job,
- a retry does not cause a duplicate side effect,
- a failing or hanging job does not pile up and overload anything.

It is a deliberately small version of tools like River, Sidekiq and Celery. It is a learning and engineering project, not a replacement for them.

## How it works

1. A client submits a job over HTTP with a type, payload, priority, optional start time, tenant and an **idempotency key**. Dispatch stores it in Postgres.
2. Workers claim jobs with `SELECT ... FOR UPDATE SKIP LOCKED`, so two workers never take the same job.
3. A claim is a **lease** with an expiry time. Running workers send **heartbeats** to extend their leases, and each job has a **timeout**.
4. If a worker dies, its leases expire and a **reaper** returns the jobs to the queue. An **attempt-number check** (fencing) rejects results from workers that lost their job.
5. A failed job is retried with **exponential backoff and jitter**. After its maximum attempts it becomes **dead** and can be inspected and replayed.
6. Every attempt is recorded in an execution history table.

Job states: `queued`, `running`, `succeeded`, `dead`, `cancelled`. A failed job with attempts left goes back to `queued` with a later `run_at`.

## Guarantees and limits

- **At-least-once execution.** A job accepted by Dispatch is never lost, but after a crash it may run more than once. Dispatch does not promise exactly-once.
- **Idempotency makes repeats harmless.** Submitting the same idempotency key twice creates one job, and handlers are expected to be idempotent.
- **Postgres is the only store.** All state lives in the database, so API and worker processes can be killed at any time.
- **Single database.** Throughput is bounded by Postgres write speed, and the database is a single point of failure.
- **Order is approximate.** Jobs run roughly by priority and due time, not in strict order.
- **No authentication.** The tenant is taken from a request header and trusted.

## Tech stack

Go, Gin, PostgreSQL 16, `pgx/v5`, goose migrations, Docker Compose, testcontainers-go.

## Getting started

```
docker compose up -d
goose -dir migrations postgres "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable" up
go run ./cmd/api
```

Submit a job:

```
curl -i -X POST localhost:8080/jobs \
  -H "X-Tenant-ID: tenant-a" \
  -H "Idempotency-Key: demo-1" \
  -d '{"type":"send_webhook","payload":{"url":"https://example.com"}}'
```

## API

| Route | Purpose |
|---|---|
| `POST /jobs` | Submit a job (`201` new, `200` repeat of the same key and payload, `409` same key with a different payload) |
| `GET /jobs` | List a tenant's jobs, optionally by status |
| `GET /jobs/:id` | Get one job |
| `POST /jobs/:id/cancel` | Cancel a job that has not started |
| `GET /stats` | Job counts by status |

All routes require an `X-Tenant-ID` header. `POST /jobs` also requires an `Idempotency-Key` header.

## Tests

```
go test ./...
```

Tests start a temporary Postgres in Docker, so Docker must be running.