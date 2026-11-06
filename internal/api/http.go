package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hardik-thapar/dispatch/internal/queue"
)

type server struct {
	pool *pgxpool.Pool
}

// NewRouter builds the Gin router with all Dispatch routes.
// It takes the database pool and returns the router.
func NewRouter(pool *pgxpool.Pool) *gin.Engine {
	s := &server{pool: pool}
	r := gin.New()
	r.Use(gin.Recovery())

	r.POST("/jobs", s.submitJob)
	r.GET("/jobs", s.listJobs)
	r.GET("/jobs/:id", s.getJob)
	r.POST("/jobs/:id/cancel", s.cancelJob)
	r.GET("/stats", s.getStats)
	return r
}

// submitRequest is the JSON body of POST /jobs.
// The tenant and idempotency key come from headers, not the body.
type submitRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	RunAt       *time.Time      `json:"run_at"` // pointer: nil means "not given"
	MaxAttempts int             `json:"max_attempts"`
}

// POST /jobs
// 201 = new job, 200 = same key and payload seen before, 409 = same key, different payload.
func (s *server) submitJob(c *gin.Context) {
	tenant := c.GetHeader("X-Tenant-ID")
	key := c.GetHeader("Idempotency-Key")

	var req submitRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be valid JSON"})
		return
	}
	if tenant == "" || key == "" || req.Type == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-Tenant-ID header, Idempotency-Key header and type are required"})
		return
	}

	in := SubmitInput{
		TenantID:       tenant,
		Type:           req.Type,
		Payload:        []byte(req.Payload),
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: key,
	}
	if req.RunAt != nil {
		in.RunAt = *req.RunAt
	}

	job, created, err := Submit(c.Request.Context(), s.pool, in)
	switch {
	case errors.Is(err, ErrKeyConflict):
		c.JSON(http.StatusConflict, gin.H{"error": "idempotency key was already used with a different payload"})
	case err != nil:
		serverError(c, err)
	case created:
		c.JSON(http.StatusCreated, job)
	default:
		c.JSON(http.StatusOK, job)
	}
}

// GET /jobs?status=queued&limit=20
func (s *server) listJobs(c *gin.Context) {
	tenant, ok := requireTenant(c)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(c.Query("limit")) // a bad number becomes 0, which List treats as "default"

	jobs, err := queue.List(c.Request.Context(), s.pool, tenant, c.Query("status"), limit)
	if err != nil {
		serverError(c, err)
		return
	}
	c.JSON(http.StatusOK, jobs)
}

// GET /jobs/:id
func (s *server) getJob(c *gin.Context) {
	tenant, ok := requireTenant(c)
	if !ok {
		return
	}
	id, ok := requireJobID(c)
	if !ok {
		return
	}

	job, err := queue.Get(c.Request.Context(), s.pool, tenant, id)
	if errors.Is(err, queue.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	if err != nil {
		serverError(c, err)
		return
	}
	c.JSON(http.StatusOK, job)
}

// POST /jobs/:id/cancel
func (s *server) cancelJob(c *gin.Context) {
	tenant, ok := requireTenant(c)
	if !ok {
		return
	}
	id, ok := requireJobID(c)
	if !ok {
		return
	}

	job, err := queue.Cancel(c.Request.Context(), s.pool, tenant, id)
	switch {
	case errors.Is(err, queue.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
	case errors.Is(err, queue.ErrNotCancellable):
		c.JSON(http.StatusConflict, gin.H{"error": "job is not queued, cannot cancel"})
	case err != nil:
		serverError(c, err)
	default:
		c.JSON(http.StatusOK, job)
	}
}

// GET /stats
func (s *server) getStats(c *gin.Context) {
	tenant, ok := requireTenant(c)
	if !ok {
		return
	}
	stats, err := queue.GetStats(c.Request.Context(), s.pool, tenant)
	if err != nil {
		serverError(c, err)
		return
	}
	c.JSON(http.StatusOK, stats)
}

// requireTenant reads the X-Tenant-ID header.
// If it is missing it writes a 400 reply and returns ok = false.
func requireTenant(c *gin.Context) (tenant string, ok bool) {
	tenant = c.GetHeader("X-Tenant-ID")
	if tenant == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-Tenant-ID header is required"})
		return "", false
	}
	return tenant, true
}

// requireJobID reads :id from the URL and checks it is a valid UUID.
// A bad id gets a 404, because no job can have that id.
func requireJobID(c *gin.Context) (id string, ok bool) {
	id = c.Param("id")
	if _, err := uuid.Parse(id); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return "", false
	}
	return id, true
}

// serverError logs the real error and sends a generic 500.
// We do not show database errors to clients.
func serverError(c *gin.Context, err error) {
	log.Printf("internal error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
}
