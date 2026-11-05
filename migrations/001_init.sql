-- +goose Up
create table jobs (
    id              uuid primary key default gen_random_uuid(),
    tenant_id       text not null,
    type            text not null,
    payload         jsonb not null default '{}',
    status          text not null default 'queued'
                    check (status in ('queued','running','succeeded','dead','cancelled')),
    priority        int not null default 0,
    run_at          timestamptz not null default now(),
    attempts        int not null default 0,
    max_attempts    int not null,
    last_error      text,
    max_run_time    int not null default 30,
    lease_owner     text,
    lease_end       timestamptz,
    idempotency_key text not null,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    finished_at     timestamptz,
    unique (tenant_id, idempotency_key)
);

create index idx_jobs_queued on jobs (priority asc, run_at asc)
    where status = 'queued';

create index idx_jobs_running_lease on jobs (lease_end)
    where status = 'running';

create table job_attempts (
    id             bigint generated always as identity primary key,
    job_id         uuid not null references jobs(id),
    attempt_number int not null,
    worker_id      text not null,
    started_at     timestamptz not null default now(),
    finished_at    timestamptz,
    result         text check (result in ('succeeded','failed','timed_out','lease_lost')),
    error          text,
    unique (job_id, attempt_number)
);

-- +goose Down
drop table job_attempts;
drop table jobs;