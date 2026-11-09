package main

import (
	"context"
	"log"
	"os"

	"github.com/hardik-thapar/dispatch/internal/api"
	"github.com/hardik-thapar/dispatch/internal/db"
)

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://dispatch:dispatch@localhost:5432/dispatch?sslmode=disable"
	}
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	pool, err := db.NewPool(context.Background(), url)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	log.Printf("dispatch api listening on %s", addr)
	if err := api.NewRouter(pool).Run(addr); err != nil {
		log.Fatalf("api server: %v", err)
	}
}
