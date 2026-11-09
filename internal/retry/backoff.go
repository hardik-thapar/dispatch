package retry

import (
	"math/rand/v2"
	"time"
)

type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

func (b Backoff) ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := b.Base
	if d > b.Max {
		return b.Max
	}

	for i := 1; i < attempt; i++ {
		d = d * 2
		if d >= b.Max {
			return b.Max
		}
	}
	return d
}

func (b Backoff) Delay(attempt int) time.Duration {
	c := b.ceiling(attempt)
	half := c / 2
	return half + rand.N(half+1)
}

// MaxDelay returns the delay without jitter: the longest the job could wait.
func (b Backoff) MaxDelay(attempt int) time.Duration {
	return b.ceiling(attempt)
}
