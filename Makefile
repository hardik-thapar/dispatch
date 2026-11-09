DB_URL ?= postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable

.PHONY: build test vet fmt up down migrate chaos bench \
	bench-throughput bench-latency bench-latency-poll20 bench-recovery bench-claims bench-index bench-herd

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

up:
	docker compose up -d

down:
	docker compose stop

migrate:
	goose -dir migrations postgres "$(DB_URL)" up

# The kill -9 test: 2,000 jobs, 20 crashes. Deletes all jobs in the database.
chaos: build
	mkdir -p results
	go run ./cmd/chaos | tee results/chaos.txt

# All benchmarks, about 40 minutes. Do not use the machine meanwhile. Each one deletes all jobs in the database.
bench: bench-throughput bench-latency bench-latency-poll20 bench-recovery bench-claims bench-index bench-herd

bench-throughput: build
	mkdir -p results
	go run ./cmd/bench throughput | tee results/throughput.txt

bench-latency: build
	mkdir -p results
	go run ./cmd/bench latency 2>&1 | tee results/latency.txt

bench-latency-poll20: build
	mkdir -p results
	go run ./cmd/bench latency -poll 20ms -reps 5 2>&1 | tee results/latency-poll20-full.txt

bench-recovery: build
	mkdir -p results
	go run ./cmd/bench recovery -reps 20 2>&1 | tee results/recovery.txt

bench-claims: build
	mkdir -p results
	go run ./cmd/bench claims 2>&1 | tee results/claims.txt

bench-index: build
	mkdir -p results
	go run ./cmd/bench index 2>&1 | tee results/index.txt

bench-herd: build
	mkdir -p results
	go run ./cmd/bench herd 2>&1 | tee results/herd.txt