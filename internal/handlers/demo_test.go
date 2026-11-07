package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

func demoJob(payload string) queue.Job {
	return queue.Job{
		ID:             "33333333-3333-3333-3333-333333333333",
		TenantID:       "tenant-a",
		IdempotencyKey: "key-1",
		Payload:        []byte(payload),
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestSideEffect_IsIdempotent(t *testing.T) {
	pool := testutil.NewTestPool(t)
	h := SideEffect(pool)
	job := demoJob(`{}`)

	for i := 0; i < 2; i++ { // run the same job twice
		if err := h(context.Background(), job); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	if n := countRows(t, pool, "side_effects"); n != 1 {
		t.Errorf("side_effects rows = %d, want 1 (a repeat must not add a second effect)", n)
	}
}

func TestCounter_IsNotIdempotent(t *testing.T) {
	pool := testutil.NewTestPool(t)
	h := Counter(pool)
	job := demoJob(`{}`)

	for i := 0; i < 2; i++ {
		if err := h(context.Background(), job); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	if n := countRows(t, pool, "counter_effects"); n != 2 {
		t.Errorf("counter_effects rows = %d, want 2 (the control handler repeats its effect)", n)
	}
}

func TestSideEffect_StopsWhenContextCancelled(t *testing.T) {
	pool := testutil.NewTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := SideEffect(pool)(ctx, demoJob(`{"sleep_ms": 5000}`))
	if err == nil {
		t.Error("err = nil, want an error (the context ran out)")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("handler took %v, want it to stop soon after the context ended", time.Since(start))
	}
	if n := countRows(t, pool, "side_effects"); n != 0 {
		t.Errorf("side_effects rows = %d, want 0 (the work never finished)", n)
	}
}

func TestWebhook(t *testing.T) {
	var gotBody string
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(status)
	}))
	defer srv.Close()

	h := Webhook(srv.Client())
	job := demoJob(`{"url":"` + srv.URL + `"}`)

	if err := h(context.Background(), job); err != nil {
		t.Fatalf("200 reply: err = %v, want nil", err)
	}
	if gotBody != string(job.Payload) {
		t.Errorf("server received %q, want the job payload", gotBody)
	}

	status = http.StatusInternalServerError
	if err := h(context.Background(), job); err == nil {
		t.Error("500 reply: err = nil, want an error")
	}

	if err := h(context.Background(), demoJob(`{}`)); err == nil {
		t.Error("no url: err = nil, want an error")
	}
}
