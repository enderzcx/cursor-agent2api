package cursoragentv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

func historySessionID(fingerprint, key string) string {
	digest := sha256.Sum256([]byte(fingerprint + "\x00" + key))
	return "history_" + hex.EncodeToString(digest[:])
}

// Reuse the existing reservation and file lease. A concurrent retry waits for
// the committed snapshot, not a second model invocation.
func (m *SessionManager) StartHistory(request ManagedRequest) (*ManagedOutput, error) {
	ctx, cancel := context.WithTimeout(requestContext(request), m.runTimeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output, err := m.Start(request)
		if !errors.Is(err, ErrSessionActive) && !errors.Is(err, ErrStateLeaseHeld) {
			return output, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
