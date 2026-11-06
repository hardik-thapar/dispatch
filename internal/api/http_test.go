package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/hardik-thapar/dispatch/internal/queue"
	"github.com/hardik-thapar/dispatch/internal/testutil"
)

// do sends one fake HTTP request to the router and returns the recorded reply.
func do(r http.Handler, method, path, tenant, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHTTP_SubmitStatusCodes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testutil.NewTestPool(t))
	body := `{"type":"send_webhook","payload":{"url":"a"}}`

	first := do(r, "POST", "/jobs", "tenant-a", "key-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first submit: status = %d, want 201 (body: %s)", first.Code, first.Body)
	}
	var job1 queue.Job
	if err := json.Unmarshal(first.Body.Bytes(), &job1); err != nil {
		t.Fatalf("decode first reply: %v", err)
	}

	again := do(r, "POST", "/jobs", "tenant-a", "key-1", body)
	if again.Code != http.StatusOK {
		t.Errorf("same key and payload: status = %d, want 200", again.Code)
	}
	var job2 queue.Job
	_ = json.Unmarshal(again.Body.Bytes(), &job2)
	if job1.ID != job2.ID {
		t.Errorf("ids differ: %s and %s", job1.ID, job2.ID)
	}

	other := do(r, "POST", "/jobs", "tenant-a", "key-1", `{"type":"send_webhook","payload":{"url":"b"}}`)
	if other.Code != http.StatusConflict {
		t.Errorf("same key, different payload: status = %d, want 409", other.Code)
	}

	noKey := do(r, "POST", "/jobs", "tenant-a", "", body)
	if noKey.Code != http.StatusBadRequest {
		t.Errorf("missing Idempotency-Key: status = %d, want 400", noKey.Code)
	}
}

func TestHTTP_GetCancelStats(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := NewRouter(testutil.NewTestPool(t))

	created := do(r, "POST", "/jobs", "tenant-a", "key-1", `{"type":"x"}`)
	var job queue.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if w := do(r, "GET", "/jobs/"+job.ID, "tenant-a", "", ""); w.Code != http.StatusOK {
		t.Errorf("get own job: status = %d, want 200", w.Code)
	}
	if w := do(r, "GET", "/jobs/"+job.ID, "tenant-b", "", ""); w.Code != http.StatusNotFound {
		t.Errorf("get other tenant's job: status = %d, want 404", w.Code)
	}
	if w := do(r, "GET", "/jobs/not-a-uuid", "tenant-a", "", ""); w.Code != http.StatusNotFound {
		t.Errorf("get bad id: status = %d, want 404", w.Code)
	}

	if w := do(r, "POST", "/jobs/"+job.ID+"/cancel", "tenant-a", "", ""); w.Code != http.StatusOK {
		t.Errorf("cancel: status = %d, want 200", w.Code)
	}
	if w := do(r, "POST", "/jobs/"+job.ID+"/cancel", "tenant-a", "", ""); w.Code != http.StatusConflict {
		t.Errorf("cancel twice: status = %d, want 409", w.Code)
	}

	w := do(r, "GET", "/stats", "tenant-a", "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("stats: status = %d, want 200", w.Code)
	}
	var stats queue.Stats
	_ = json.Unmarshal(w.Body.Bytes(), &stats)
	if stats.Cancelled != 1 {
		t.Errorf("stats.Cancelled = %d, want 1", stats.Cancelled)
	}

	list := do(r, "GET", "/jobs?status=cancelled", "tenant-a", "", "")
	var jobs []queue.Job
	_ = json.Unmarshal(list.Body.Bytes(), &jobs)
	if len(jobs) != 1 {
		t.Errorf("list cancelled: got %d jobs, want 1", len(jobs))
	}
}
