package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hardik-thapar/dispatch/internal/queue"
)

// POST /jobs/:id/replay
// 200 = job is queued again, 404 = no such job, 409 = job is not dead.
func (s *server) replayJob(c *gin.Context) {
	tenant, ok := requireTenant(c)
	if !ok {
		return
	}
	id, ok := requireJobID(c)
	if !ok {
		return
	}

	job, err := queue.Replay(c.Request.Context(), s.pool, tenant, id)
	switch {
	case errors.Is(err, queue.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
	case errors.Is(err, queue.ErrNotReplayable):
		c.JSON(http.StatusConflict, gin.H{"error": "job is not dead, cannot replay"})
	case err != nil:
		serverError(c, err)
	default:
		c.JSON(http.StatusOK, job)
	}
}
