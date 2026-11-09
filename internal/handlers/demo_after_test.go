package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// The effect is written BEFORE the after_ms wait, so a job that is cut off during
// that wait has already done its work. This is the crash window the chaos test uses.
func TestCounter_EffectIsWrittenBeforeAfterWait(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := Counter(pool)(ctx, demoJob(`{"after_ms": 5000}`))
	if err == nil {
		t.Error("err = nil, want an error (the context ran out during the after wait)")
	}
	if n := countRows(t, pool, "counter_effects"); n != 1 {
		t.Errorf("counter_effects rows = %d, want 1 (the effect is written before the wait)", n)
	}
}
