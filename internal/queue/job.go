package queue

import (
	"encoding/json"
	"time"
)

// Job is one row of the jobs table.
// The json tags set the field names the API returns.
type Job struct {
	ID          string          `json:"id"`
	TenantID    string          `json:"tenant_id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"` // jsonb; RawMessage keeps it as real JSON, not base64
	Status      string          `json:"status"`
	Priority    int             `json:"priority"`
	RunAt       time.Time       `json:"run_at"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	// These fields can be NULL in the database, so they are pointers.
	LastError      *string    `json:"last_error"`
	MaxRunTime     int        `json:"max_run_time"` // seconds
	LeaseOwner     *string    `json:"lease_owner"`
	LeaseEnd       *time.Time `json:"lease_end"`
	IdempotencyKey string     `json:"idempotency_key"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	FinishedAt     *time.Time `json:"finished_at"`
}
