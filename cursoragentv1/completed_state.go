package cursoragentv1

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

type persistedEvent struct {
	Type           EventType         `json:"type"`
	Text           string            `json:"text,omitempty"`
	Signature      string            `json:"signature,omitempty"`
	ProgressEvents int               `json:"progress_events,omitempty"`
	Tokens         int64             `json:"tokens,omitempty"`
	Usage          *UsageObservation `json:"usage,omitempty"`
	ToolCall       *ToolCall         `json:"tool_call,omitempty"`
	ToolResult     *ToolResult       `json:"tool_result,omitempty"`
	HostedSearch   *HostedSearch     `json:"hosted_search,omitempty"`
}

type completedTombstone struct {
	RequestKey            string                `json:"request_key,omitempty"`
	RequestSegmentID      string                `json:"request_segment_id,omitempty"`
	Version               int                   `json:"version"`
	CredentialFingerprint string                `json:"credential_fingerprint"`
	BatchIDs              []string              `json:"batch_ids"`
	ResultDigest          string                `json:"result_digest"`
	Results               []CompletedToolResult `json:"results"`
	Events                []persistedEvent      `json:"events"`
	BillingRunID          string                `json:"billing_run_id"`
	SessionID             string                `json:"session_id"`
	ConversationID        string                `json:"conversation_id"`
	HasPending            bool                  `json:"has_pending,omitempty"`
	FollowUpText          string                `json:"follow_up_text,omitempty"`
	Ephemeral             bool                  `json:"ephemeral,omitempty"`
	UpdatedAt             time.Time             `json:"updated_at"`
}

func (s *FileStateStore) SaveCompleted(owner OwnerLease, results []ToolResult, events []Event, ephemeral bool, followUp string) error {
	completed, err := newCompletedTombstone(owner, results, events, ephemeral, followUp, s.now().UTC())
	if err != nil {
		return err
	}
	return s.update(owner, func(current *StateSnapshot) error {
		completed.ConversationID = current.ConversationID
		// Internal snapshots must refer to the client request that actually paid
		// for the work, even when that request also included follow-up text.
		completed.BillingRunID = owner.BillingRunID
		if completed.BillingRunID == "" {
			completed.BillingRunID = current.BillingRunID
		}
		completed.HasPending = len(current.PendingTools) > 0
		if followUp != "" {
			current.FollowUpText = followUp
		}
		if err := s.writeCompletedTombstone(completed); err != nil {
			return err
		}
		if ephemeral && !completed.HasPending {
			current.EphemeralComplete = true
			current.CompletedAt = completed.UpdatedAt
		}
		return nil
	})
}

func (s *FileStateStore) LookupCompleted(credentialFingerprint string, results []ToolResult, followUp string) (CompletedReplay, error) {
	credentialFingerprint = strings.TrimSpace(credentialFingerprint)
	canonical, err := canonicalToolResults(results)
	if err != nil {
		return CompletedReplay{}, err
	}
	if s == nil || strings.TrimSpace(s.completedRoot) == "" || credentialFingerprint == "" {
		return CompletedReplay{}, ErrSessionNotPending
	}
	batchIDs := completedBatchIDs(canonical)
	if len(batchIDs) == 0 {
		return CompletedReplay{}, ErrSessionNotPending
	}
	if err := s.pruneExpiredCompleted(false); err != nil {
		return CompletedReplay{}, err
	}
	tombstone, err := s.lookupCompletedTombstone(credentialFingerprint, canonical, followUp)
	if err != nil {
		return CompletedReplay{}, err
	}
	events, err := eventsFromPersisted(tombstone.Events)
	if err != nil {
		return CompletedReplay{}, err
	}
	return CompletedReplay{
		SessionID: tombstone.SessionID, ConversationID: tombstone.ConversationID,
		BillingRunID: tombstone.BillingRunID, RequestSegmentID: tombstone.RequestSegmentID, OriginalBatch: append([]string(nil), tombstone.BatchIDs...),
		Events: events, HasPending: tombstone.HasPending, FollowUpText: tombstone.FollowUpText,
	}, nil
}

func (s *FileStateStore) lookupCompletedTombstone(credentialFingerprint string, canonical []CompletedToolResult, followUp string) (completedTombstone, error) {
	batchIDs := completedBatchIDs(canonical)
	tombstone, err := s.matchCompletedTombstone(credentialFingerprint, batchIDs, canonical, followUp)
	if err == nil || !errors.Is(err, ErrSessionNotPending) {
		return tombstone, err
	}
	return s.lookupCompletedCovering(credentialFingerprint, canonical, followUp)
}

func (s *FileStateStore) matchCompletedTombstone(credentialFingerprint string, batchIDs []string, canonical []CompletedToolResult, followUp string) (completedTombstone, error) {
	tombstone, err := s.readCompletedTombstone(credentialFingerprint, batchIDs, followUp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return completedTombstone{}, ErrSessionNotPending
		}
		return completedTombstone{}, err
	}
	if tombstone.CredentialFingerprint != credentialFingerprint || !slices.Equal(tombstone.BatchIDs, batchIDs) {
		return completedTombstone{}, ErrSessionNotPending
	}
	if normalizeFollowUp(tombstone.FollowUpText) != normalizeFollowUp(followUp) {
		return completedTombstone{}, ErrSessionNotPending
	}
	if completedTombstoneExpired(tombstone, s.now().UTC(), s.ttl) {
		if removeErr := s.removeExpiredCompletedTombstone(credentialFingerprint, batchIDs, followUp); removeErr != nil {
			return completedTombstone{}, removeErr
		}
		return completedTombstone{}, ErrSessionNotPending
	}
	matched, ok := completedResultsForBatch(canonical, batchIDs)
	if !ok {
		return completedTombstone{}, ErrSessionNotPending
	}
	digest := toolResultDigest(matched)
	if subtle.ConstantTimeCompare([]byte(tombstone.ResultDigest), []byte(digest)) != 1 {
		return completedTombstone{}, ErrToolResultConflict
	}
	return tombstone, nil
}

func (s *FileStateStore) lookupCompletedCovering(credentialFingerprint string, canonical []CompletedToolResult, followUp string) (completedTombstone, error) {
	submittedIDs := completedBatchIDs(canonical)
	matchesBySession := make(map[string][]completedTombstone)
	seen := make(map[string]struct{})
	conflict := false
	for _, id := range submittedIDs {
		pointer, err := s.readCompletedToolPointer(credentialFingerprint, id)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return completedTombstone{}, err
		}
		if pointer.CredentialFingerprint != credentialFingerprint || !batchSubsetOf(pointer.BatchIDs, submittedIDs) {
			continue
		}
		key := strings.Join(pointer.BatchIDs, "\x00")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		tombstone, matchErr := s.matchCompletedTombstone(credentialFingerprint, pointer.BatchIDs, canonical, followUp)
		if errors.Is(matchErr, ErrSessionNotPending) {
			continue
		}
		if errors.Is(matchErr, ErrToolResultConflict) {
			conflict = true
			continue
		}
		if matchErr != nil {
			return completedTombstone{}, matchErr
		}
		matchesBySession[tombstone.SessionID] = append(matchesBySession[tombstone.SessionID], tombstone)
	}
	if conflict {
		return completedTombstone{}, ErrToolResultConflict
	}
	if len(matchesBySession) > 1 {
		return completedTombstone{}, ErrPendingAmbiguous
	}
	var latest completedTombstone
	found := false
	for _, matches := range matchesBySession {
		for _, tombstone := range matches {
			if !found || tombstone.UpdatedAt.After(latest.UpdatedAt) {
				latest = tombstone
				found = true
			}
		}
	}
	if !found {
		return completedTombstone{}, ErrSessionNotPending
	}
	return latest, nil
}

func completedTombstoneExpired(tombstone completedTombstone, now time.Time, ttl time.Duration) bool {
	if tombstone.UpdatedAt.IsZero() {
		return false
	}
	if tombstone.Ephemeral {
		ttl = completedTombstoneTTL
	}
	if ttl <= 0 {
		return false
	}
	return now.Sub(tombstone.UpdatedAt) > ttl
}

func persistEvents(events []Event) ([]persistedEvent, error) {
	var snapshot eventSnapshot
	for _, event := range events {
		if err := snapshot.append(event); err != nil {
			return nil, err
		}
	}
	persisted := make([]persistedEvent, 0, len(snapshot.events))
	for _, event := range snapshot.events {
		persisted = append(persisted, persistedEventFrom(event))
	}
	return persisted, nil
}

func persistedEventFrom(event Event) persistedEvent {
	return persistedEvent{Type: event.Type, Text: event.Text, Signature: event.Signature, ProgressEvents: event.ProgressEvents, Tokens: event.Tokens, Usage: cloneUsageObservation(event.Usage), ToolCall: cloneToolCall(event.ToolCall), ToolResult: cloneToolResult(event.ToolResult), HostedSearch: cloneHostedSearch(event.HostedSearch)}
}

func eventsFromPersisted(events []persistedEvent) ([]Event, error) {
	out := make([]Event, 0, len(events))
	for _, event := range events {
		if event.Type == 0 {
			return nil, fmt.Errorf("Cursor Agent v1 completed snapshot event is invalid")
		}
		out = append(out, Event{Type: event.Type, Text: event.Text, Signature: event.Signature, ProgressEvents: event.ProgressEvents, Tokens: event.Tokens, Usage: cloneUsageObservation(event.Usage), ToolCall: cloneToolCall(event.ToolCall), ToolResult: cloneToolResult(event.ToolResult), HostedSearch: cloneHostedSearch(event.HostedSearch)})
	}
	return out, nil
}

func newCompletedTombstone(owner OwnerLease, results []ToolResult, events []Event, ephemeral bool, followUp string, now time.Time) (completedTombstone, error) {
	canonical, err := canonicalToolResults(results)
	if err != nil {
		return completedTombstone{}, err
	}
	persisted, err := persistEvents(events)
	if err != nil {
		return completedTombstone{}, err
	}
	return completedTombstone{
		Version: stateVersion, CredentialFingerprint: owner.CredentialHash,
		BatchIDs: completedBatchIDs(canonical), ResultDigest: toolResultDigest(canonical),
		Results: canonical, Events: persisted, BillingRunID: owner.BillingRunID, RequestSegmentID: owner.RequestSegmentID,
		SessionID: owner.SessionID, ConversationID: owner.ConversationID,
		FollowUpText: strings.TrimSpace(followUp), Ephemeral: ephemeral, UpdatedAt: now,
	}, nil
}

func (s *FileStateStore) completedIndexPaths(credentialFingerprint string, batchIDs []string, followUp string) (string, string) {
	// Preserve the pre-segment filename for ordinary tool-result replays.
	material := strings.TrimSpace(credentialFingerprint) + "\x00" + strings.Join(normalizeToolIDs(batchIDs), "\x00")
	if text := normalizeFollowUp(followUp); text != "" {
		material += "\x00" + text
	}
	digest := sha256.Sum256([]byte(material))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.completedRoot, name+".json"), filepath.Join(s.completedRoot, name+".lock")
}

func (s *FileStateStore) writeCompletedTombstone(tombstone completedTombstone) error {
	indexPath, lockPath := s.completedIndexPaths(tombstone.CredentialFingerprint, tombstone.BatchIDs, tombstone.FollowUpText)
	handle, err := acquirePendingIndexLease(lockPath)
	if err != nil {
		return err
	}
	defer releaseHandle(handle) //nolint:errcheck
	existing, err := readCompletedTombstoneFile(indexPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if existing.CredentialFingerprint != tombstone.CredentialFingerprint || !slices.Equal(existing.BatchIDs, tombstone.BatchIDs) || normalizeFollowUp(existing.FollowUpText) != normalizeFollowUp(tombstone.FollowUpText) {
			return fmt.Errorf("Cursor Agent v1 completed index identity mismatch")
		}
		if subtle.ConstantTimeCompare([]byte(existing.ResultDigest), []byte(tombstone.ResultDigest)) != 1 {
			return ErrToolResultConflict
		}
		tombstone.Events = existing.Events
		tombstone.FollowUpText = existing.FollowUpText
		tombstone.Results = existing.Results
		tombstone.BillingRunID = existing.BillingRunID
		tombstone.SessionID = existing.SessionID
		tombstone.ConversationID = existing.ConversationID
		if !tombstone.Ephemeral {
			tombstone.Ephemeral = existing.Ephemeral
		}
	}
	tombstone.Version = stateVersion
	if err := writeCompletedTombstoneAtomic(indexPath, tombstone); err != nil {
		return err
	}
	if normalizeFollowUp(tombstone.FollowUpText) != "" {
		return nil
	}
	return s.writeCompletedToolPointers(tombstone.CredentialFingerprint, tombstone.BatchIDs)
}

func (s *FileStateStore) readCompletedTombstone(credentialFingerprint string, batchIDs []string, followUp string) (completedTombstone, error) {
	indexPath, lockPath := s.completedIndexPaths(credentialFingerprint, batchIDs, followUp)
	handle, err := acquirePendingIndexLease(lockPath)
	if err != nil {
		return completedTombstone{}, err
	}
	defer releaseHandle(handle) //nolint:errcheck
	tombstone, err := readCompletedTombstoneFile(indexPath)
	if errors.Is(err, os.ErrNotExist) && normalizeFollowUp(followUp) != "" {
		// Older releases stored a follow-up under the base tool-result key.
		// matchCompletedTombstone still checks its exact follow-up and results.
		return s.readCompletedTombstone(credentialFingerprint, batchIDs, "")
	}
	return tombstone, err
}

func (s *FileStateStore) removeExpiredCompletedTombstone(credentialFingerprint string, batchIDs []string, followUp string) error {
	indexPath, lockPath := s.completedIndexPaths(credentialFingerprint, batchIDs, followUp)
	handle, err := acquirePendingIndexLease(lockPath)
	if errors.Is(err, ErrStateLeaseHeld) {
		return nil
	}
	if err != nil {
		return err
	}
	defer releaseHandle(handle) //nolint:errcheck
	tombstone, err := readCompletedTombstoneFile(indexPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !completedTombstoneExpired(tombstone, s.now().UTC(), s.ttl) {
		return nil
	}
	if err := s.removeFile(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if normalizeFollowUp(tombstone.FollowUpText) != "" {
		return nil
	}
	return s.removeCompletedToolPointers(credentialFingerprint, batchIDs)
}

func readCompletedTombstoneFile(path string) (completedTombstone, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is a SHA-256-derived filename under the configured private state root
	if err != nil {
		return completedTombstone{}, err
	}
	var tombstone completedTombstone
	if err := jsonx.Unmarshal(data, &tombstone); err != nil {
		return completedTombstone{}, fmt.Errorf("decode Cursor Agent v1 completed snapshot: %w", err)
	}
	version, err := migrateRecordVersion(tombstone.Version)
	if err != nil {
		return completedTombstone{}, err
	}
	tombstone.Version = version
	tombstone.BatchIDs = normalizeToolIDs(tombstone.BatchIDs)
	return tombstone, nil
}

func writeCompletedTombstoneAtomic(path string, tombstone completedTombstone) error {
	data, err := jsonx.Marshal(tombstone)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".cursor-agent-v1-completed-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (s *FileStateStore) pruneExpiredCompleted(force bool) error {
	if s == nil || strings.TrimSpace(s.completedRoot) == "" || s.now == nil {
		return nil
	}
	now := s.now().UTC()
	if !force {
		s.gcMu.Lock()
		skip := !s.lastGC.IsZero() && now.Sub(s.lastGC) < stateGCInterval
		s.gcMu.Unlock()
		if skip {
			return nil
		}
	}
	entries, err := os.ReadDir(s.completedRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(s.completedRoot, entry.Name())
		lockPath := strings.TrimSuffix(path, ".json") + ".lock"
		handle, lockErr := acquirePendingIndexLease(lockPath)
		if errors.Is(lockErr, ErrStateLeaseHeld) {
			continue
		}
		if lockErr != nil {
			return lockErr
		}
		tombstone, readErr := readCompletedTombstoneFile(path)
		if readErr != nil {
			_ = releaseHandle(handle)
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			return readErr
		}
		if !completedTombstoneExpired(tombstone, now, s.ttl) {
			if releaseErr := releaseHandle(handle); releaseErr != nil {
				return releaseErr
			}
			continue
		}
		if err := s.removeFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = releaseHandle(handle)
			return err
		}
		if normalizeFollowUp(tombstone.FollowUpText) == "" {
			if err := s.removeCompletedToolPointers(tombstone.CredentialFingerprint, tombstone.BatchIDs); err != nil {
				_ = releaseHandle(handle)
				return err
			}
		}
		if releaseErr := releaseHandle(handle); releaseErr != nil {
			return releaseErr
		}
	}
	return nil
}

func (s *FileStateStore) completedToolPointerPaths(credentialFingerprint, toolCallID string) (string, string) {
	material := strings.TrimSpace(credentialFingerprint) + "\x00completed-tool\x00" + strings.TrimSpace(toolCallID)
	digest := sha256.Sum256([]byte(material))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.completedToolRoot, name+".json"), filepath.Join(s.completedToolRoot, name+".lock")
}

func (s *FileStateStore) writeCompletedToolPointers(credentialFingerprint string, batchIDs []string) error {
	if s == nil || strings.TrimSpace(s.completedToolRoot) == "" {
		return nil
	}
	if err := os.MkdirAll(s.completedToolRoot, 0o700); err != nil {
		return err
	}
	now := s.now().UTC()
	for _, id := range normalizeToolIDs(batchIDs) {
		pointer := toolBatchPointer{
			Version: stateVersion, CredentialFingerprint: credentialFingerprint,
			ToolCallID: id, BatchIDs: normalizeToolIDs(batchIDs), UpdatedAt: now,
		}
		indexPath, lockPath := s.completedToolPointerPaths(credentialFingerprint, id)
		if err := s.writeToolBatchPointerAt(indexPath, lockPath, pointer); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStateStore) removeCompletedToolPointers(credentialFingerprint string, batchIDs []string) error {
	if s == nil || strings.TrimSpace(s.completedToolRoot) == "" {
		return nil
	}
	normalized := normalizeToolIDs(batchIDs)
	for _, id := range normalized {
		indexPath, lockPath := s.completedToolPointerPaths(credentialFingerprint, id)
		if err := s.removeToolBatchPointer(indexPath, lockPath, credentialFingerprint, id, normalized); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStateStore) readCompletedToolPointer(credentialFingerprint, toolCallID string) (toolBatchPointer, error) {
	indexPath, lockPath := s.completedToolPointerPaths(credentialFingerprint, toolCallID)
	return s.readToolBatchPointer(indexPath, lockPath)
}

func (s *FileStateStore) rebuildCompletedIndex() error {
	if s == nil || strings.TrimSpace(s.completedRoot) == "" {
		return nil
	}
	if strings.TrimSpace(s.completedToolRoot) != "" {
		if err := os.MkdirAll(s.completedToolRoot, 0o700); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(s.completedRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		tombstone, readErr := readCompletedTombstoneFile(filepath.Join(s.completedRoot, entry.Name()))
		if readErr != nil {
			return readErr
		}
		if completedTombstoneExpired(tombstone, s.now().UTC(), s.ttl) {
			if tombstone.RequestKey != "" {
				continue
			} // shared expiry GC owns request snapshots
			if err := s.removeExpiredCompletedTombstone(tombstone.CredentialFingerprint, tombstone.BatchIDs, tombstone.FollowUpText); err != nil {
				return err
			}
			continue
		}
		if tombstone.RequestKey != "" {
			continue
		} // no tool-pointer index for request-scoped replay
		if normalizeFollowUp(tombstone.FollowUpText) != "" {
			continue
		}
		if err := s.writeCompletedToolPointers(tombstone.CredentialFingerprint, tombstone.BatchIDs); err != nil {
			return err
		}
		count++
		if count > s.maxFiles {
			return fmt.Errorf("Cursor Agent v1 completed index exceeds %d snapshots", s.maxFiles)
		}
	}
	return nil
}
