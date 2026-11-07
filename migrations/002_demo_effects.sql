-- +goose Up
-- Written by the idempotent demo handler: one row per effect_key, however many times the job runs.
create table side_effects (
    effect_key text primary key,
    job_id     uuid not null,
    created_at timestamptz not null default now()
);

-- Written by the non-idempotent control handler: one row per execution.
create table counter_effects (
    id         bigint generated always as identity primary key,
    job_id     uuid not null,
    created_at timestamptz not null default now()
);

-- +goose Down
drop table counter_effects;
drop table side_effects;