package cursoragentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLegacyCompletedReceiptSurvivesSegmentUpgrade(t *testing.T) {
	for _, followUp := range []string{"", "legacy follow-up"} {
		t.Run(followUp, func(t *testing.T) {
			root := t.TempDir()
			store, err := NewFileStateStore(root)
			require.NoError(t, err)
			fingerprint := strings.Repeat("a", 64)
			results := []ToolResult{{ToolCallID: "legacy-tool", Content: "ok"}}
			owner := OwnerLease{SessionID: "legacy-session", ConversationID: "legacy-conversation", CredentialHash: fingerprint, BillingRunID: "already-settled-before-upgrade"}
			tombstone, err := newCompletedTombstone(owner, results, []Event{{Type: EventText, Text: "legacy reply"}, {Type: EventDone}}, true, followUp, time.Now().UTC())
			require.NoError(t, err)
			// Pre-upgrade filenames did not include a follow-up component.
			digest := sha256.Sum256([]byte(fingerprint + "\x00legacy-tool"))
			path := filepath.Join(store.completedRoot, hex.EncodeToString(digest[:])+".json")
			require.NoError(t, writeCompletedTombstoneAtomic(path, tombstone))
			restarted, err := NewFileStateStore(root)
			require.NoError(t, err)
			replay, err := restarted.LookupCompleted(fingerprint, results, followUp)
			require.NoError(t, err)
			require.Equal(t, owner.BillingRunID, replay.BillingRunID, "do not invent a new bill for an already settled replay")
			require.Equal(t, []EventType{EventText, EventDone}, eventTypes(replay.Events))
			_, err = restarted.LookupCompleted(fingerprint, results, "different follow-up")
			require.ErrorIs(t, err, ErrSessionNotPending)
		})
	}
}
