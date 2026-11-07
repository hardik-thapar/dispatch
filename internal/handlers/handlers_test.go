package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/hardik-thapar/dispatch/internal/queue"
)

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("send_email", func(ctx context.Context, job queue.Job) error {
		called = true
		return errors.New("boom")
	})

	h, ok := r.Get("send_email")
	if !ok {
		t.Fatal("Get(send_email): ok = false, want true")
	}
	if err := h(context.Background(), queue.Job{Type: "send_email"}); err == nil || err.Error() != "boom" {
		t.Errorf("handler error = %v, want boom", err)
	}
	if !called {
		t.Error("handler was not called")
	}

	if _, ok := r.Get("unknown_type"); ok {
		t.Error("Get(unknown_type): ok = true, want false")
	}
}
