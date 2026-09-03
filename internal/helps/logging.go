package helps

import (
	"context"
	"strings"

	log "github.com/sirupsen/logrus"
)

// LogWithRequestID returns a logrus entry. The public plugin does not import
// CLIProxyAPI internal request-id helpers, so missing IDs stay off the fields.
func LogWithRequestID(ctx context.Context) *log.Entry {
	entry := log.NewEntry(log.StandardLogger())
	if ctx == nil {
		return entry
	}
	if id, ok := ctx.Value("request_id").(string); ok {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			return entry.WithField("request_id", trimmed)
		}
	}
	return entry
}
