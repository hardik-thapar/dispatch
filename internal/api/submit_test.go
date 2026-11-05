package api

import (
	"context"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func TestSubmit_NewJob(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	job, created, err := Submit(ctx, pool, SubmitInput{
		TenantID:       "tenant-a",
		Type:           "send_webhook",
		IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if !created {
		t.Error("created = false, want true")
	}
	if job.ID == "" {
		t.Error("job id is empty")
	}
	if job.Status != "queued" {
		t.Errorf("status = %q, want queued", job.Status)
	}
	if job.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", job.Attempts)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("max attempts = %d, want 5 (the default)", job.MaxAttempts)
	}
	if string(job.Payload) != "{}" {
		t.Errorf("payload = %s, want {}", job.Payload)
	}
}