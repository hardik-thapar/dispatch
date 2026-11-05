package queue

import (
	"time"
)

type Job struct {
	ID          string
	TenantID    string
	Type        string
	Payload     []byte //JSONB
	Status      string
	Priority    int
	RunAt       time.Time
	Attempts    int
	MaxAttempts int
	//as this feild can be nul, and string is never nul, so we sue *string that can be nil equivalent to null in db
	LastError      *string
	MaxRunTime     int
	LeaseOwner     *string
	LeaseEnd       *time.Time
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}
