package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/hardik-thapar/dispatch/internal/retry"
)

// runHerd shows how jitter spreads out the retries of many jobs that fail at the same moment.
// It is a SIMULATION of the retry schedule, using the real backoff function. It does not run workers.
func runHerd(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("herd", flag.ExitOnError)
	jobs := fs.Int("jobs", 10000, "number of jobs that fail at the same instant")
	base := fs.Duration("base", time.Second, "backoff base delay (what the worker uses)")
	max := fs.Duration("max", 5*time.Minute, "backoff maximum delay (what the worker uses)")
	trials := fs.Int("trials", 5, "random trials per row (the median is reported)")
	fs.Parse(args)

	pool := connect(ctx)
	defer pool.Close()
	printEnvironment(ctx, pool)

	b := retry.Backoff{Base: *base, Max: *max}
	fmt.Printf("Retry herd: %d jobs fail at the same instant, backoff base %v, maximum %v\n", *jobs, *base, *max)
	fmt.Println("This is a SIMULATION of the retry schedule with the real backoff function, not a live run.")
	fmt.Println("Peak = the most retries that start within any one second (whole-second buckets).")
	fmt.Printf("Median of %d random trials for the jitter column.\n\n", *trials)
	fmt.Printf("%7s %18s %17s %15s %10s\n", "attempt", "delay with jitter", "peak/s no jitter", "peak/s jitter", "reduction")

	for _, attempt := range []int{1, 2, 3, 4, 5, 6, 8, 10} {
		// without jitter every job waits the same time, so all retries land in one second
		noJitter := peakPerSecond(*jobs, func() time.Duration { return b.MaxDelay(attempt) })

		var peaks []float64
		for t := 0; t < *trials; t++ {
			peaks = append(peaks, float64(peakPerSecond(*jobs, func() time.Duration { return b.Delay(attempt) })))
		}
		sort.Float64s(peaks)
		withJitter := median(peaks)

		fmt.Printf("%7d %18s %17d %15.0f %9.1fx\n", attempt,
			fmt.Sprintf("%v to %v", (b.MaxDelay(attempt)/2).Round(10*time.Millisecond), b.MaxDelay(attempt)),
			noJitter, withJitter, float64(noJitter)/withJitter)
	}

	fmt.Println()
	fmt.Println("With a short delay, all retries still fall inside one second, so jitter cannot help yet.")
	fmt.Println("The longer the delay, the wider the window the jitter spreads the retries over.")
}

// peakPerSecond draws a delay for each of n jobs and returns the most retries in any one second.
func peakPerSecond(n int, delay func() time.Duration) int {
	buckets := map[int]int{}
	peak := 0
	for i := 0; i < n; i++ {
		sec := int(delay().Seconds())
		buckets[sec]++
		if buckets[sec] > peak {
			peak = buckets[sec]
		}
	}
	return peak
}
