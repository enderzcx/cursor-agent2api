package cursoragentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Full-history replays share the existing completed snapshot store, event codec,
// five-minute retention and billing receipt. They have a request identity rather
// than a pending-tool index; no synthetic tool is invented to key the cache.
func (s *FileStateStore) requestCompletedPaths(fingerprint, key string) (string, string) {
	digest := sha256.Sum256([]byte("history-request\x00" + fingerprint + "\x00" + key))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.completedRoot, name+".json"), filepath.Join(s.completedRoot, name+".lock")
}

func (s *FileStateStore) LookupHistory(fingerprint, key string) (CompletedReplay, error) {
	path, lockPath := s.requestCompletedPaths(fingerprint, key)
	lease, err := acquirePendingIndexLease(lockPath)
	if err != nil {
		return CompletedReplay{}, err
	}
	defer releaseHandle(lease) //nolint:errcheck
	record, err := readCompletedTombstoneFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return CompletedReplay{}, ErrSessionNotPending
	}
	if err != nil {
		return CompletedReplay{}, err
	}
	if record.RequestKey != key || record.CredentialFingerprint != fingerprint {
		return CompletedReplay{}, fmt.Errorf("Cursor history replay identity mismatch")
	}
	if completedTombstoneExpired(record, s.now().UTC(), s.ttl) {
		return CompletedReplay{}, ErrSessionNotPending
	}
	events, err := eventsFromPersisted(record.Events)
	if err != nil {
		return CompletedReplay{}, err
	}
	return CompletedReplay{SessionID: record.SessionID, ConversationID: record.ConversationID, BillingRunID: record.BillingRunID, RequestSegmentID: record.RequestSegmentID, Events: events, HasPending: record.HasPending}, nil
}

func (s *FileStateStore) SaveHistory(owner OwnerLease, key string, events []Event) error {
	if !visibleHistoryOutput(events) {
		return fmt.Errorf("Cursor Agent v1 completed without visible output or a tool handoff")
	}
	persisted, err := persistEvents(events)
	if err != nil {
		return err
	}
	handoff := false
	for _, event := range events {
		if event.Type == EventToolCall {
			handoff = true
		}
	}
	record := completedTombstone{Version: stateVersion, RequestKey: key, CredentialFingerprint: owner.CredentialHash, Events: persisted, BillingRunID: owner.BillingRunID, RequestSegmentID: owner.RequestSegmentID, SessionID: owner.SessionID, ConversationID: owner.ConversationID, HasPending: handoff, Ephemeral: true, UpdatedAt: s.now().UTC()}
	return s.update(owner, func(current *StateSnapshot) error {
		path, lockPath := s.requestCompletedPaths(owner.CredentialHash, key)
		lease, err := acquirePendingIndexLease(lockPath)
		if err != nil {
			return err
		}
		defer releaseHandle(lease) //nolint:errcheck
		previous, readErr := readCompletedTombstoneFile(path)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		if readErr == nil && !completedTombstoneExpired(previous, s.now().UTC(), s.ttl) {
			if previous.RequestKey != key || previous.CredentialFingerprint != owner.CredentialHash {
				return fmt.Errorf("Cursor history replay identity mismatch")
			}
			return nil
		}
		if err := writeCompletedTombstoneAtomic(path, record); err != nil {
			return err
		}
		current.Checkpoint = nil
		current.Blobs = nil
		current.PendingTools = nil
		current.PendingBatchIDs = nil
		current.EphemeralComplete = true
		current.CompletedAt = record.UpdatedAt
		return nil
	})
}
