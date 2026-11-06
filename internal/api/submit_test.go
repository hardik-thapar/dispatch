package api

import (
	"context"
	"errors"
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

func TestSubmit_SameKeySamePayload(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	in := SubmitInput{
		TenantID:       "tenant-a",
		Type:           "send_webhook",
		Payload:        []byte(`{"url":"a"}`),
		IdempotencyKey: "key-1",
	}

	first, _, err := Submit(ctx, pool, in)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}

	second, created, err := Submit(ctx, pool, in)
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if created {
		t.Error("created = true, want false")
	}
	if first.ID != second.ID {
		t.Errorf("ids differ: %s and %s", first.ID, second.ID)
	}
}

func TestSubmit_SameKeyDifferentPayload(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx := context.Background()

	in := SubmitInput{
		TenantID:       "tenant-a",
		Type:           "send-webhook",
		Payload:        []byte(`{"url":"a"}`),
		IdempotencyKey: "key-1",
	}

	_, _, err := Submit(ctx, pool, in)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}

	in2 := in
	in2.Payload = []byte(`{"url":"b"}`)

	_, _, err = Submit(ctx, pool, in2)
	if !errors.Is(err, ErrKeyConflict) {
		t.Errorf("err = %v, want ErrKeyConflict", err)
	}

}
