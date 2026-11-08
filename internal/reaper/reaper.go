package reaper

import (
	"context"
	"log"
	"time"

	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Run(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := queue.ReapExpired(ctx, pool)
			if err != nil {
				if ctx.Err() == nil { // an error during shutdown is normal, so do not log it
					log.Printf("reaper: %v", err)
				}
				continue
			}
			if n > 0 {
				log.Printf("reaper: returned %d expired jobs to the queue", n)
			}
		}
	}
}
