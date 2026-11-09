package retry

import (
	"testing"
	"time"
)

func TestCeiling(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 30 * time.Second}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second}, // 32s would be next, but Max caps it
		{7, 30 * time.Second},
		{100, 30 * time.Second},       // must not overflow
		{1_000_000, 30 * time.Second}, // must not overflow or loop forever
		{0, 1 * time.Second},          // bad input is treated as attempt 1
		{-5, 1 * time.Second},
	}
	for _, tc := range tests {
		if got := b.ceiling(tc.attempt); got != tc.want {
			t.Errorf("ceiling(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestDelay_StaysInsideBounds(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 30 * time.Second}

	for attempt := 1; attempt <= 50; attempt++ {
		c := b.ceiling(attempt)
		for i := 0; i < 200; i++ {
			d := b.Delay(attempt)
			if d < c/2 || d > c {
				t.Fatalf("Delay(%d) = %v, want between %v and %v", attempt, d, c/2, c)
			}
		}
	}
}

func TestDelay_HasJitter(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 30 * time.Second}

	seen := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		seen[b.Delay(5)] = true
	}
	if len(seen) < 10 {
		t.Errorf("100 calls gave only %d different delays, want many (jitter is missing)", len(seen))
	}
}
