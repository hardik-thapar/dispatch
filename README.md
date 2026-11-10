<p align="center">
  <img src="docs/dispatch_logo.png" alt="dispatch" width="320">
</p>

<p align="center">
  A background job queue on top of PostgreSQL, written in Go.
</p>

---

You give it jobs, workers run them, and if a worker dies halfway the job doesn't get lost.

I wrote it to understand how queues like River and Sidekiq work, so it's small on purpose. If you need a queue for real, use River.

- jobs live in Postgres, nothing else to run
- workers claim with `FOR UPDATE SKIP LOCKED`
- a claim is a lease, kept alive by heartbeats. If the worker dies, a reaper puts the job back
- every update checks the attempt number, so a worker that lost its job can't write a result for it
- retries with exponential backoff and jitter, then a `dead` state you can list and replay
- idempotency key on submit, per tenant

## How a job moves

```
queued -> running -> succeeded
                  -> queued     failed with attempts left, or the lease expired
                  -> dead       out of attempts
queued -> cancelled
dead   -> queued                replay
```

## Run it

Needs Go, Docker and [goose](https://github.com/pressly/goose).

```
docker compose up -d
make migrate
make build
./bin/api        # one terminal
./bin/worker     # another, start a few if you like
```

```
curl -s -X POST localhost:8080/jobs \
  -H "X-Tenant-ID: tenant-a" -H "Idempotency-Key: demo-1" \
  -d '{"type":"side_effect","payload":{"sleep_ms":2000}}'
```

Send it again and you get the same job back. To add a handler, write a `func(ctx, queue.Job) error` and register it in `cmd/worker/main.go`.

## What it promises

At-least-once. If a worker dies mid-job the job runs again, and if the first run already did its side effect you get it twice. Dispatch can't know. Write handlers so that running twice is fine.

I killed workers with `kill -9` 60 times across 3 runs of 2,000 jobs: nothing accepted was lost, and the idempotent handler never doubled its effect. Details in [RESULTS.md](RESULTS.md).

What it doesn't do:

- one Postgres is one point of failure, and its write speed is the ceiling
- workers poll, so a job can wait up to the poll interval (200 ms default)
- the timeout only works if the handler watches its context
- a worker that loses its lease keeps running the handler. The result is thrown away, the side effect may not be
- no auth, the tenant comes from a header
- no UI, no cron

## Numbers

On a laptop with the database on the same machine, no-op jobs:

| | |
|---|---|
| throughput | about 4,600 jobs/s at 16 workers |
| wait before start (p99), 20 ms poll | 2 to 17 ms at 500 to 2,000 jobs/s |
| recovery after `kill -9` | about the lease length (median 2.9 s with a 3 s lease) |
| `SKIP LOCKED` vs plain select-then-update | 15,473 vs 1,112 claims/s, and no double claims |

Methods, charts and caveats are in [RESULTS.md](RESULTS.md). One of them matters: latency at 3,000 jobs/s is unstable and I don't know why yet. The bugs I hit while building are in [BUGS.md](BUGS.md).

## API

Every route needs `X-Tenant-ID`. `POST /jobs` also needs `Idempotency-Key`.

```
POST /jobs                submit. 201 new, 200 same key and payload, 409 same key different payload
GET  /jobs?status=&limit= list jobs (status=dead is the dead-letter list)
GET  /jobs/:id            one job
POST /jobs/:id/cancel     cancel a job that hasn't started
POST /jobs/:id/replay     put a dead job back, 5 more attempts
GET  /stats               counts by status
```

## Config

Environment variables. Worker: `DATABASE_URL`, `WORKER_CONCURRENCY` (4), `WORKER_LEASE` (30s), `REAPER_INTERVAL` (5s), `WORKER_POLL_INTERVAL` (200ms), `DB_MAX_CONNS` (20). API: `ADDR` (`:8080`).

## Layout

```
cmd/       api, worker, chaos (the kill -9 test), bench
internal/  queue, worker, reaper, retry, api, handlers
migrations/
results/   raw output of every benchmark and chaos run
docs/      logo, charts
```

## Tests

`make test` runs `go test -race ./...`. Each test starts its own Postgres with testcontainers, so Docker has to be running. It takes a few minutes. `make chaos` and `make bench` run the crash test and the benchmarks (they delete all jobs in the database).

## Not done

Per-tenant limits and fair claiming, metrics, `LISTEN/NOTIFY`, cancelling running jobs, stopping the handler of a job that lost its lease, and running a handler's own writes in the same transaction as the "succeeded" mark.