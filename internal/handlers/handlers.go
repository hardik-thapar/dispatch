package handlers

import (
	"context"

	"github.com/hardik-thapar/dispatch/internal/queue"
)

// Handler is the code that does the real work for one kind of job.
// It gets the claimed job (with its payload) and returns nil on success
// or an error on failure. The context is cancelled on timeout or shutdown,
// so a handler should stop its work when ctx.Done() fires.
type Handler func(ctx context.Context, job queue.Job) error

// Registry maps a job type name (for example "send_webhook") to its Handler.
type Registry struct {
	byType map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byType: make(map[string]Handler)}
}

// Register adds a handler for a job type. It takes the type name and the handler.
// Register all handlers at start-up, before workers run (the map is not locked).
func (r *Registry) Register(jobType string, h Handler) {
	r.byType[jobType] = h
}

// Get finds the handler for a job type.
// It returns the handler and true, or nil and false if the type is unknown.
func (r *Registry) Get(jobType string) (Handler, bool) {
	h, ok := r.byType[jobType]
	return h, ok
}
