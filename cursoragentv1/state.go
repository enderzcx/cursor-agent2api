package cursoragentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const (
	stateVersionV1 = 1
	stateVersion   = 2
)

const (
	// Request-scale caps match the Claude / Cookie / Kiro ingress order of
	// magnitude (users remember 256 MiB; production MAX_REQUEST_BODY_MB is 512).
	// Connect frames stay at maxConnectPayloadSize (16 MiB): the length field
	// is uint32, but raising the local cap without Cursor-server evidence would
	// only replace a clear local error with an upstream reset.
	maxStateFileSize       = 256 << 20
	maxPendingTools        = 256
	maxPendingToolBytes    = 256 << 20
	maxCompletedEventBytes = 16 << 20
	maxStateFiles          = 10000
	defaultStateTTL        = 30 * 24 * time.Hour
	// 5m retention covers the guaranteed 3m replay plus a 2m
	// transport/scheduling buffer. TTL is not sliding and is not
	// refreshed on replay.
	completedTombstoneTTL = 5 * time.Minute
	stateGCInterval       = time.Hour
)

var (
	ErrRetiredOwner       = errors.New("Cursor Agent v1 state owner is retired")
	ErrCredentialMismatch = errors.New("Cursor Agent v1 credential does not own the session state")
	ErrStateLeaseHeld     = errors.New("Cursor Agent v1 session state is leased by another runtime")
	ErrStatePending       = errors.New("Cursor Agent v1 session state is waiting for tool results")
	ErrPendingAmbiguous   = errors.New("Cursor Agent v1 tool results match multiple pending sessions")
	ErrToolResultConflict = errors.New("Cursor Agent v1 tool result conflicts with the completed snapshot")
)

type PendingTool struct {
	MessageID          uint32 `json:"message_id"`
	ExecID             string `json:"exec_id,omitempty"`
	ToolCallID         string `json:"tool_call_id"`
	UpstreamToolCallID string `json:"upstream_tool_call_id,omitempty"`
	Name               string `json:"name"`
	Arguments          string `json:"arguments"`
}

type StateSnapshot struct {
	Version               int               `json:"version"`
	RuntimeProfile        string            `json:"runtime_profile,omitempty"`
	SessionID             string            `json:"session_id"`
	ConversationID        string            `json:"conversation_id"`
	CredentialFingerprint string            `json:"credential_fingerprint"`
	ToolCatalogDigest     string            `json:"tool_catalog_digest,omitempty"`
	ToolCatalog           []ToolDefinition  `json:"tool_catalog,omitempty"`
	BillingRunID          string            `json:"billing_run_id,omitempty"`
	UsageCarry            []Event           `json:"usage_carry,omitempty"`
	OwnerGeneration       uint64            `json:"owner_generation"`
	OwnerToken            string            `json:"owner_token"`
	Checkpoint            []byte            `json:"checkpoint,omitempty"`
	Blobs                 map[string][]byte `json:"blobs,omitempty"`
	PendingTools          []PendingTool     `json:"pending_tools,omitempty"`
	PendingBatchIDs       []string          `json:"pending_batch_ids,omitempty"`
	FollowUpText          string            `json:"follow_up_text,omitempty"`
	EphemeralComplete     bool              `json:"ephemeral_complete,omitempty"`
	CompletedAt           time.Time         `json:"completed_at,omitempty"`
	UpdatedAt             time.Time         `json:"updated_at"`
}

type CompletedToolResult struct {
	ToolCallID string      `json:"tool_call_id"`
	Content    string      `json:"content"`
	IsError    bool        `json:"is_error,omitempty"`
	Media      []MediaPart `json:"media,omitempty"`
}

type CompletedReplay struct {
	RequestSegmentID string
	SessionID        string
	ConversationID   string
	BillingRunID     string
	OriginalBatch    []string
	Events           []Event
	HasPending       bool
	FollowUpText     string
}

type pendingIndex struct {
	Version               int       `json:"version"`
	CredentialFingerprint string    `json:"credential_fingerprint"`
	BatchIDs              []string  `json:"batch_ids"`
	SessionIDs            []string  `json:"session_ids"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type toolBatchPointer struct {
	Version               int       `json:"version"`
	CredentialFingerprint string    `json:"credential_fingerprint"`
	ToolCallID            string    `json:"tool_call_id"`
	BatchIDs              []string  `json:"batch_ids"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type OwnerLease struct {
	RequestSegmentID string
	SessionID        string
	Generation       uint64
	Token            string
	CredentialHash   string
	ConversationID   string
	BillingRunID     string
	handle           *leaseHandle
}

type leaseHandle struct {
	mu       sync.Mutex
	file     *os.File
	released bool
}

type ClaimResult struct {
	Replay      *CompletedReplay
	Owner       OwnerLease
	Snapshot    StateSnapshot
	ColdRebuild bool
}

type ClaimOptions struct {
	RuntimeProfile        string
	HistoryRequestKey     string
	AllowPending          bool
	RequireExisting       bool
	AllowCredentialChange bool
	ToolCatalogDigest     string
	ToolCatalog           []ToolDefinition
	RequireToolCatalog    bool
	NewBillingRunID       string
	ReuseBillingRun       bool
}

type StateStore interface {
	LookupHistory(fingerprint, key string) (CompletedReplay, error)
	SaveHistory(owner OwnerLease, key string, events []Event) error
	Claim(sessionID, conversationID, credentialFingerprint string, options ClaimOptions) (ClaimResult, error)
	SaveCheckpoint(owner OwnerLease, checkpoint []byte, blobs map[string][]byte) error
	SavePending(owner OwnerLease, pending []PendingTool) error
	SaveCompleted(owner OwnerLease, results []ToolResult, events []Event, ephemeral bool, followUp string) error
	SaveFollowUp(owner OwnerLease, followUp string) error
	SetBillingRunID(owner OwnerLease, billingRunID string) error
	SaveUsageCarry(owner OwnerLease, events []Event) error
	Delete(owner OwnerLease) error
	DeleteSession(sessionID, credentialFingerprint string) error
	Release(owner OwnerLease) error
	ResolvePending(credentialFingerprint string, toolCallIDs []string) (string, error)
	OriginalPendingBatch(credentialFingerprint string, toolCallIDs []string) ([]string, error)
	LookupCompleted(credentialFingerprint string, results []ToolResult, followUp string) (CompletedReplay, error)
}

type FileStateStore struct {
	root              string
	pendingRoot       string
	pendingToolRoot   string
	completedRoot     string
	completedToolRoot string
	ttl               time.Duration
	now               func() time.Time
	gcMu              sync.Mutex
	lastGC            time.Time
	maxFiles          int
	removeFile        func(string) error
}

func NewFileStateStore(root string) (*FileStateStore, error) {
	return newFileStateStore(root, defaultStateTTL, time.Now)
}

func newFileStateStore(root string, ttl time.Duration, now func() time.Time) (*FileStateStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("Cursor Agent v1 state directory is required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create Cursor Agent v1 state directory: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil { // #nosec G302 -- directories require execute permission; owner-only 0700 is intentional
		return nil, fmt.Errorf("secure Cursor Agent v1 state directory: %w", err)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("Cursor Agent v1 state TTL must be positive")
	}
	if now == nil {
		now = time.Now
	}
	pendingRoot := filepath.Join(root, "pending-index")
	if err := os.MkdirAll(pendingRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create Cursor Agent v1 pending index: %w", err)
	}
	pendingToolRoot := filepath.Join(pendingRoot, "by-tool")
	if err := os.MkdirAll(pendingToolRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create Cursor Agent v1 pending tool index: %w", err)
	}
	completedRoot := filepath.Join(root, "completed-index")
	if err := os.MkdirAll(completedRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create Cursor Agent v1 completed index: %w", err)
	}
	completedToolRoot := filepath.Join(completedRoot, "by-tool")
	if err := os.MkdirAll(completedToolRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create Cursor Agent v1 completed tool index: %w", err)
	}
	store := &FileStateStore{
		root: root, pendingRoot: pendingRoot, pendingToolRoot: pendingToolRoot,
		completedRoot: completedRoot, completedToolRoot: completedToolRoot,
		ttl: ttl, now: now, maxFiles: maxStateFiles, removeFile: os.Remove,
	}
	if err := store.pruneExpired(true); err != nil {
		return nil, err
	}
	if err := store.rebuildPendingIndex(); err != nil {
		return nil, err
	}
	if err := store.rebuildCompletedIndex(); err != nil {
		return nil, err
	}
	return store, nil
}

func CredentialFingerprint(credentialScope, accountKey string) (string, error) {
	credentialScope = strings.TrimSpace(credentialScope)
	accountKey = strings.TrimSpace(accountKey)
	if credentialScope == "" || accountKey == "" {
		return "", fmt.Errorf("Cursor Agent v1 credential scope and stable account key are required")
	}
	material := "scope:" + credentialScope + "\x00account:" + accountKey
	digest := sha256.Sum256([]byte(material))
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (s *FileStateStore) Claim(sessionID, conversationID, credentialFingerprint string, options ClaimOptions) (ClaimResult, error) {
	if err := s.pruneExpired(false); err != nil {
		return ClaimResult{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	conversationID = strings.TrimSpace(conversationID)
	credentialFingerprint = strings.TrimSpace(credentialFingerprint)
	if sessionID == "" || conversationID == "" || credentialFingerprint == "" {
		return ClaimResult{}, fmt.Errorf("Cursor Agent v1 claim requires session, conversation, and credential fingerprint")
	}
	statePath, lockPath := s.paths(sessionID)
	handle, err := acquireLease(lockPath)
	if err != nil {
		return ClaimResult{}, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			_ = releaseHandle(handle)
		}
	}()
	current, err := readState(statePath)
	if options.HistoryRequestKey != "" {
		replay, replayErr := s.LookupHistory(credentialFingerprint, options.HistoryRequestKey)
		if replayErr == nil {
			return ClaimResult{Replay: &replay}, nil
		}
		if !errors.Is(replayErr, ErrSessionNotPending) {
			return ClaimResult{}, replayErr
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ClaimResult{}, err
	}
	if err == nil && stateExpired(current, s.now().UTC(), s.ttl) {
		if err := s.removeFile(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ClaimResult{}, fmt.Errorf("remove expired Cursor Agent v1 state: %w", err)
		}
		if indexErr := s.syncPendingIndex(current.CredentialFingerprint, current.SessionID, current.PendingBatchIDs, nil); indexErr != nil {
			return ClaimResult{}, indexErr
		}
		err = os.ErrNotExist
	}
	created := false
	if errors.Is(err, os.ErrNotExist) {
		if options.RequireExisting {
			return ClaimResult{}, os.ErrNotExist
		}
		current = StateSnapshot{Version: stateVersion, SessionID: sessionID, ConversationID: conversationID}
		created = true
	}
	if strings.TrimSpace(current.RuntimeProfile) == "" {
		current.RuntimeProfile = runtimeAgentV1
		if created {
			current.RuntimeProfile = normalizeRuntimeProfile(options.RuntimeProfile)
		}
	}
	previousPendingBatch := append([]string(nil), current.PendingBatchIDs...)
	previousCredentialFingerprint := current.CredentialFingerprint
	if !options.AllowPending && len(current.PendingTools) > 0 {
		return ClaimResult{}, ErrStatePending
	}
	toolCatalogDigest := strings.TrimSpace(options.ToolCatalogDigest)
	toolCatalog, err := validateToolCatalog(options.ToolCatalog)
	if err != nil {
		return ClaimResult{}, err
	}
	if options.RequireToolCatalog {
		// Full definitions are authoritative for an existing run; the manager
		// restores them instead of injecting new caller declarations. Legacy
		// digest-only snapshots still need a match, checked before any writes.
		if current.ToolCatalogDigest == "" || (len(current.ToolCatalog) == 0 && toolCatalogDigest != "" && current.ToolCatalogDigest != toolCatalogDigest) {
			return ClaimResult{}, ErrSessionNotPending
		}
	}
	coldRebuild := current.CredentialFingerprint != "" && current.CredentialFingerprint != credentialFingerprint
	if coldRebuild && !options.AllowCredentialChange {
		return ClaimResult{}, ErrCredentialMismatch
	}
	if options.HistoryRequestKey != "" {
		current.Checkpoint = nil
		current.Blobs = nil
		current.PendingTools = nil
		current.PendingBatchIDs = nil
		current.ConversationID = conversationID
		current.EphemeralComplete = false
		current.CompletedAt = time.Time{}
	}
	if coldRebuild {
		current.Checkpoint = nil
		current.Blobs = nil
		current.PendingTools = nil
		current.PendingBatchIDs = nil
		current.ToolCatalog = nil
		current.BillingRunID = ""
		current.ConversationID = conversationID
	}
	if options.ReuseBillingRun || (options.AllowPending && isMeteredTurn(current.BillingRunID)) {
		if strings.TrimSpace(current.BillingRunID) == "" {
			current.BillingRunID = uuid.NewString()
		}
	} else {
		current.UsageCarry = nil
		current.BillingRunID = strings.TrimSpace(options.NewBillingRunID)
		if current.BillingRunID == "" {
			current.BillingRunID = uuid.NewString()
		}
	}
	if !options.RequireToolCatalog && len(current.PendingTools) == 0 {
		current.ToolCatalogDigest = toolCatalogDigest
		current.ToolCatalog = cloneToolDefinitions(toolCatalog)
	}
	if current.ConversationID == "" {
		current.ConversationID = conversationID
	}
	current.Version = stateVersion
	current.SessionID = sessionID
	current.CredentialFingerprint = credentialFingerprint
	current.OwnerGeneration++
	current.OwnerToken = uuid.NewString()
	current.UpdatedAt = s.now().UTC()
	if err := writeStateAtomic(statePath, current); err != nil {
		return ClaimResult{}, err
	}
	if len(previousPendingBatch) > 0 && (previousCredentialFingerprint != current.CredentialFingerprint || !slices.Equal(normalizeToolIDs(previousPendingBatch), normalizeToolIDs(current.PendingBatchIDs))) {
		if err := s.syncPendingIndex(previousCredentialFingerprint, current.SessionID, previousPendingBatch, nil); err != nil {
			return ClaimResult{}, err
		}
	}
	if len(current.PendingBatchIDs) > 0 {
		if err := s.syncPendingIndex(current.CredentialFingerprint, current.SessionID, nil, current.PendingBatchIDs); err != nil {
			return ClaimResult{}, err
		}
	}
	owner := OwnerLease{
		SessionID: sessionID, Generation: current.OwnerGeneration, Token: current.OwnerToken,
		CredentialHash: credentialFingerprint, ConversationID: current.ConversationID, BillingRunID: current.BillingRunID, handle: handle,
	}
	releaseOnError = false
	return ClaimResult{Owner: owner, Snapshot: cloneSnapshot(current), ColdRebuild: coldRebuild}, nil
}

func (s *FileStateStore) SaveUsageCarry(owner OwnerLease, events []Event) error {
	return s.update(owner, func(current *StateSnapshot) error {
		current.UsageCarry = usageEvidenceEvents(events)
		return nil
	})
}

func acquirePendingIndexLease(lockPath string) (*leaseHandle, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		handle, err := acquireLease(lockPath)
		if err == nil {
			return handle, nil
		}
		lastErr = err
		if !errors.Is(err, ErrStateLeaseHeld) {
			return nil, err
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, lastErr
}

func (s *FileStateStore) SaveCheckpoint(owner OwnerLease, checkpoint []byte, blobs map[string][]byte) error {
	return s.update(owner, func(current *StateSnapshot) error {
		if len(checkpoint) > maxConnectPayloadSize {
			return errExceedsConnectLimit("Cursor Agent v1 checkpoint")
		}
		if _, err := validateBlobStore(blobs, maxBlobSize, maxBlobStoreSize); err != nil {
			return err
		}
		current.Checkpoint = append([]byte(nil), checkpoint...)
		current.Blobs = cloneBlobs(blobs)
		return nil
	})
}

func (s *FileStateStore) SavePending(owner OwnerLease, pending []PendingTool) error {
	var previousBatch []string
	var nextBatch []string
	err := s.update(owner, func(current *StateSnapshot) error {
		if err := validatePendingTools(pending); err != nil {
			return err
		}
		previousBatch = append([]string(nil), current.PendingBatchIDs...)
		if len(pending) == 0 {
			current.PendingBatchIDs = nil
		} else if len(current.PendingBatchIDs) == 0 || len(pending) > len(current.PendingTools) {
			current.PendingBatchIDs = pendingToolIDs(pending)
		}
		nextBatch = append([]string(nil), current.PendingBatchIDs...)
		current.PendingTools = append([]PendingTool(nil), pending...)
		return nil
	})
	if err != nil {
		return err
	}
	return s.syncPendingIndex(owner.CredentialHash, owner.SessionID, previousBatch, nextBatch)
}

func (s *FileStateStore) SaveFollowUp(owner OwnerLease, followUp string) error {
	return s.update(owner, func(current *StateSnapshot) error {
		current.FollowUpText = strings.TrimSpace(followUp)
		return nil
	})
}

func (s *FileStateStore) SetBillingRunID(owner OwnerLease, billingRunID string) error {
	billingRunID = strings.TrimSpace(billingRunID)
	if billingRunID == "" {
		return fmt.Errorf("Cursor Agent v1 billing segment id is required")
	}
	return s.update(owner, func(current *StateSnapshot) error {
		current.BillingRunID = billingRunID
		return nil
	})
}

func (s *FileStateStore) Delete(owner OwnerLease) error {
	return s.withOwner(owner, func(path string) error {
		current, err := readState(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !ownerMatches(current, owner) {
			return ErrRetiredOwner
		}
		if err := s.removeFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete Cursor Agent v1 state: %w", err)
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
		return s.syncPendingIndex(current.CredentialFingerprint, current.SessionID, current.PendingBatchIDs, nil)
	})
}

func (s *FileStateStore) DeleteSession(sessionID, credentialFingerprint string) error {
	sessionID = strings.TrimSpace(sessionID)
	credentialFingerprint = strings.TrimSpace(credentialFingerprint)
	if sessionID == "" || credentialFingerprint == "" {
		return ErrCredentialMismatch
	}
	statePath, lockPath := s.paths(sessionID)
	handle, err := acquireLease(lockPath)
	if err != nil {
		return err
	}
	defer releaseHandle(handle) //nolint:errcheck
	return func(path string) error {
		current, err := readState(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.CredentialFingerprint != credentialFingerprint {
			return ErrCredentialMismatch
		}
		if err := s.removeFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete Cursor Agent v1 state: %w", err)
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
		return s.syncPendingIndex(current.CredentialFingerprint, current.SessionID, current.PendingBatchIDs, nil)
	}(statePath)
}

func (s *FileStateStore) update(owner OwnerLease, mutate func(*StateSnapshot) error) error {
	if strings.TrimSpace(owner.SessionID) == "" || strings.TrimSpace(owner.Token) == "" {
		return ErrRetiredOwner
	}
	return s.withOwner(owner, func(path string) error {
		current, err := readState(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrRetiredOwner
			}
			return err
		}
		if !ownerMatches(current, owner) {
			return ErrRetiredOwner
		}
		if err := mutate(&current); err != nil {
			return err
		}
		current.UpdatedAt = s.now().UTC()
		return writeStateAtomic(path, current)
	})
}

func ownerMatches(current StateSnapshot, owner OwnerLease) bool {
	return current.SessionID == owner.SessionID &&
		current.OwnerGeneration == owner.Generation &&
		current.OwnerToken == owner.Token &&
		current.CredentialFingerprint == owner.CredentialHash &&
		current.ConversationID == owner.ConversationID
}

func (s *FileStateStore) Release(owner OwnerLease) error {
	if owner.handle == nil {
		return nil
	}
	return releaseHandle(owner.handle)
}

func (s *FileStateStore) ResolvePending(credentialFingerprint string, toolCallIDs []string) (string, error) {
	credentialFingerprint = strings.TrimSpace(credentialFingerprint)
	if s == nil || strings.TrimSpace(s.root) == "" || credentialFingerprint == "" || len(toolCallIDs) == 0 {
		return "", ErrSessionNotPending
	}
	batchIDs := normalizeToolIDs(toolCallIDs)
	if len(batchIDs) == 0 {
		return "", ErrSessionNotPending
	}
	if err := s.pruneExpired(false); err != nil {
		return "", err
	}
	sessionID, err := s.resolvePendingExact(credentialFingerprint, batchIDs)
	if err == nil || !errors.Is(err, ErrSessionNotPending) {
		return sessionID, err
	}
	coveringBatch, coveringErr := s.pendingCoveringBatch(credentialFingerprint, batchIDs)
	if coveringErr != nil {
		return "", coveringErr
	}
	if len(coveringBatch) == 0 || slices.Equal(coveringBatch, batchIDs) {
		return "", ErrSessionNotPending
	}
	return s.resolvePendingExact(credentialFingerprint, coveringBatch)
}

func (s *FileStateStore) resolvePendingExact(credentialFingerprint string, batchIDs []string) (string, error) {
	indexPath, _ := s.pendingIndexPaths(credentialFingerprint, batchIDs)
	index, err := readPendingIndex(indexPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrSessionNotPending
		}
		return "", err
	}
	if index.CredentialFingerprint != credentialFingerprint || !slices.Equal(index.BatchIDs, batchIDs) || len(index.SessionIDs) == 0 {
		return "", ErrSessionNotPending
	}
	matches := make([]string, 0, len(index.SessionIDs))
	validSessions := make(map[string]struct{}, len(index.SessionIDs))
	for _, sessionID := range index.SessionIDs {
		statePath, _ := s.paths(sessionID)
		state, readErr := readState(statePath)
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			return "", readErr
		}
		if state.CredentialFingerprint != credentialFingerprint || len(state.PendingTools) == 0 {
			continue
		}
		stateBatch := state.PendingBatchIDs
		if len(stateBatch) == 0 {
			stateBatch = pendingToolIDs(state.PendingTools)
		}
		if !slices.Equal(normalizeToolIDs(stateBatch), batchIDs) {
			continue
		}
		matches = append(matches, state.SessionID)
		validSessions[state.SessionID] = struct{}{}
	}
	for _, sessionID := range index.SessionIDs {
		if _, valid := validSessions[sessionID]; !valid {
			if err := s.mutatePendingIndex(credentialFingerprint, batchIDs, sessionID, false); err != nil {
				return "", err
			}
		}
	}
	if len(matches) == 0 {
		return "", ErrSessionNotPending
	}
	if len(matches) > 1 {
		return "", ErrPendingAmbiguous
	}
	return matches[0], nil
}

func (s *FileStateStore) OriginalPendingBatch(credentialFingerprint string, toolCallIDs []string) ([]string, error) {
	submitted := normalizeToolIDs(toolCallIDs)
	if len(submitted) == 0 {
		return nil, ErrSessionNotPending
	}
	covering, err := s.pendingCoveringBatch(credentialFingerprint, submitted)
	if err != nil {
		return nil, err
	}
	if len(covering) == 0 {
		return submitted, nil
	}
	return covering, nil
}

func (s *FileStateStore) pruneExpired(force bool) error {
	if s == nil || s.now == nil || s.ttl <= 0 {
		return nil
	}
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	now := s.now().UTC()
	if !force && !s.lastGC.IsZero() && now.Sub(s.lastGC) < stateGCInterval {
		return nil
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), ".cursor-agent-v1-") {
			if info, infoErr := entry.Info(); infoErr == nil && now.Sub(info.ModTime()) > s.ttl {
				if removeErr := os.Remove(filepath.Join(s.root, entry.Name())); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
					return fmt.Errorf("remove expired Cursor Agent v1 temporary state: %w", removeErr)
				}
			}
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		statePath := filepath.Join(s.root, entry.Name())
		state, readErr := readState(statePath)
		if readErr != nil {
			return readErr
		}
		if !stateExpired(state, now, s.ttl) {
			continue
		}
		lockPath := strings.TrimSuffix(statePath, ".json") + ".lock"
		handle, lockErr := acquireLease(lockPath)
		if errors.Is(lockErr, ErrStateLeaseHeld) {
			continue
		}
		if lockErr != nil {
			return lockErr
		}
		latest, latestErr := readState(statePath)
		if latestErr == nil && stateExpired(latest, now, s.ttl) {
			if removeErr := s.removeFile(statePath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				_ = releaseHandle(handle)
				return fmt.Errorf("remove expired Cursor Agent v1 state: %w", removeErr)
			}
			if indexErr := s.syncPendingIndex(latest.CredentialFingerprint, latest.SessionID, latest.PendingBatchIDs, nil); indexErr != nil {
				_ = releaseHandle(handle)
				return indexErr
			}
		}
		if releaseErr := releaseHandle(handle); releaseErr != nil {
			return releaseErr
		}
	}
	s.lastGC = now
	return s.pruneExpiredCompleted(true)
}

func pendingToolIDs(pending []PendingTool) []string {
	ids := make([]string, 0, len(pending))
	for _, tool := range pending {
		ids = append(ids, strings.TrimSpace(tool.ToolCallID))
	}
	sort.Strings(ids)
	return ids
}

func normalizeToolIDs(ids []string) []string {
	normalized := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}
	sort.Strings(normalized)
	return normalized
}

func (s *FileStateStore) pendingIndexPaths(credentialFingerprint string, batchIDs []string) (string, string) {
	material := strings.TrimSpace(credentialFingerprint) + "\x00" + strings.Join(normalizeToolIDs(batchIDs), "\x00")
	digest := sha256.Sum256([]byte(material))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.pendingRoot, name+".json"), filepath.Join(s.pendingRoot, name+".lock")
}

func (s *FileStateStore) syncPendingIndex(credentialFingerprint, sessionID string, previousBatch, nextBatch []string) error {
	previousBatch = normalizeToolIDs(previousBatch)
	nextBatch = normalizeToolIDs(nextBatch)
	if len(previousBatch) > 0 && !slices.Equal(previousBatch, nextBatch) {
		if err := s.mutatePendingIndex(credentialFingerprint, previousBatch, sessionID, false); err != nil {
			return err
		}
	}
	if len(nextBatch) > 0 {
		return s.mutatePendingIndex(credentialFingerprint, nextBatch, sessionID, true)
	}
	return nil
}

func (s *FileStateStore) mutatePendingIndex(credentialFingerprint string, batchIDs []string, sessionID string, add bool) error {
	indexPath, lockPath := s.pendingIndexPaths(credentialFingerprint, batchIDs)
	handle, err := acquirePendingIndexLease(lockPath)
	if err != nil {
		return err
	}
	defer releaseHandle(handle) //nolint:errcheck
	index, err := readPendingIndex(indexPath)
	if errors.Is(err, os.ErrNotExist) {
		index = pendingIndex{Version: stateVersion, CredentialFingerprint: credentialFingerprint, BatchIDs: normalizeToolIDs(batchIDs)}
	} else if err != nil {
		return err
	}
	if index.CredentialFingerprint != credentialFingerprint || !slices.Equal(index.BatchIDs, normalizeToolIDs(batchIDs)) {
		return fmt.Errorf("Cursor Agent v1 pending index identity mismatch")
	}
	sessions := make(map[string]struct{}, len(index.SessionIDs)+1)
	for _, current := range index.SessionIDs {
		if current != sessionID {
			sessions[current] = struct{}{}
		}
	}
	if add {
		sessions[sessionID] = struct{}{}
	}
	index.SessionIDs = index.SessionIDs[:0]
	for current := range sessions {
		index.SessionIDs = append(index.SessionIDs, current)
	}
	sort.Strings(index.SessionIDs)
	if len(index.SessionIDs) == 0 {
		if err := s.removeFile(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return s.removePendingToolPointers(credentialFingerprint, batchIDs)
	}
	if add {
		if err := s.writePendingToolPointers(credentialFingerprint, batchIDs); err != nil {
			return err
		}
	}
	if len(index.SessionIDs) > s.maxFiles {
		return fmt.Errorf("Cursor Agent v1 pending index exceeds %d sessions", s.maxFiles)
	}
	index.UpdatedAt = s.now().UTC()
	return writePendingIndexAtomic(indexPath, index)
}

func (s *FileStateStore) pendingCoveringBatch(credentialFingerprint string, submittedIDs []string) ([]string, error) {
	var candidate []string
	for _, id := range normalizeToolIDs(submittedIDs) {
		pointer, err := s.readPendingToolPointer(credentialFingerprint, id)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		if pointer.CredentialFingerprint != credentialFingerprint || !batchSubsetOf(pointer.BatchIDs, submittedIDs) {
			continue
		}
		if len(candidate) == 0 {
			candidate = append([]string(nil), pointer.BatchIDs...)
			continue
		}
		if !slices.Equal(candidate, pointer.BatchIDs) {
			return nil, ErrPendingAmbiguous
		}
	}
	return candidate, nil
}

func (s *FileStateStore) pendingToolPointerPaths(credentialFingerprint, toolCallID string) (string, string) {
	material := strings.TrimSpace(credentialFingerprint) + "\x00tool\x00" + strings.TrimSpace(toolCallID)
	digest := sha256.Sum256([]byte(material))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.pendingToolRoot, name+".json"), filepath.Join(s.pendingToolRoot, name+".lock")
}

func (s *FileStateStore) writePendingToolPointers(credentialFingerprint string, batchIDs []string) error {
	if strings.TrimSpace(s.pendingToolRoot) == "" {
		return nil
	}
	if err := os.MkdirAll(s.pendingToolRoot, 0o700); err != nil {
		return err
	}
	now := s.now().UTC()
	for _, id := range normalizeToolIDs(batchIDs) {
		pointer := toolBatchPointer{
			Version: stateVersion, CredentialFingerprint: credentialFingerprint,
			ToolCallID: id, BatchIDs: normalizeToolIDs(batchIDs), UpdatedAt: now,
		}
		indexPath, lockPath := s.pendingToolPointerPaths(credentialFingerprint, id)
		if err := s.writeToolBatchPointerAt(indexPath, lockPath, pointer); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStateStore) removePendingToolPointers(credentialFingerprint string, batchIDs []string) error {
	normalized := normalizeToolIDs(batchIDs)
	for _, id := range normalized {
		indexPath, lockPath := s.pendingToolPointerPaths(credentialFingerprint, id)
		if err := s.removeToolBatchPointer(indexPath, lockPath, credentialFingerprint, id, normalized); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStateStore) readPendingToolPointer(credentialFingerprint, toolCallID string) (toolBatchPointer, error) {
	indexPath, lockPath := s.pendingToolPointerPaths(credentialFingerprint, toolCallID)
	return s.readToolBatchPointer(indexPath, lockPath)
}

func (s *FileStateStore) writeToolBatchPointerAt(indexPath, lockPath string, pointer toolBatchPointer) error {
	handle, err := acquirePendingIndexLease(lockPath)
	if err != nil {
		return err
	}
	defer releaseHandle(handle) //nolint:errcheck
	pointer.Version = stateVersion
	pointer.BatchIDs = normalizeToolIDs(pointer.BatchIDs)
	data, err := jsonx.Marshal(pointer)
	if err != nil {
		return err
	}
	return writePrivateFileAtomic(indexPath, data)
}

func (s *FileStateStore) readToolBatchPointer(indexPath, lockPath string) (toolBatchPointer, error) {
	handle, err := acquirePendingIndexLease(lockPath)
	if err != nil {
		return toolBatchPointer{}, err
	}
	defer releaseHandle(handle)         //nolint:errcheck
	data, err := os.ReadFile(indexPath) // #nosec G304 -- path is a SHA-256-derived filename under the configured private state root
	if err != nil {
		return toolBatchPointer{}, err
	}
	var pointer toolBatchPointer
	if err := jsonx.Unmarshal(data, &pointer); err != nil {
		return toolBatchPointer{}, fmt.Errorf("decode Cursor Agent v1 tool index: %w", err)
	}
	version, err := migrateRecordVersion(pointer.Version)
	if err != nil {
		return toolBatchPointer{}, err
	}
	pointer.Version = version
	pointer.BatchIDs = normalizeToolIDs(pointer.BatchIDs)
	return pointer, nil
}

func (s *FileStateStore) removeToolBatchPointer(indexPath, lockPath, credentialFingerprint, toolCallID string, batchIDs []string) error {
	handle, err := acquirePendingIndexLease(lockPath)
	if errors.Is(err, ErrStateLeaseHeld) {
		return nil
	}
	if err != nil {
		return err
	}
	defer releaseHandle(handle)         //nolint:errcheck
	data, err := os.ReadFile(indexPath) // #nosec G304 -- path is a SHA-256-derived filename under the configured private state root
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var pointer toolBatchPointer
	if err := jsonx.Unmarshal(data, &pointer); err != nil {
		return fmt.Errorf("decode Cursor Agent v1 tool index: %w", err)
	}
	if pointer.CredentialFingerprint != credentialFingerprint || pointer.ToolCallID != toolCallID || !slices.Equal(normalizeToolIDs(pointer.BatchIDs), normalizeToolIDs(batchIDs)) {
		return nil
	}
	return s.removeFile(indexPath)
}

func writePrivateFileAtomic(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".cursor-agent-v1-index-*.tmp")
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

func (s *FileStateStore) rebuildPendingIndex() error {
	if strings.TrimSpace(s.pendingToolRoot) != "" {
		if err := os.MkdirAll(s.pendingToolRoot, 0o700); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		state, readErr := readState(filepath.Join(s.root, entry.Name()))
		if readErr != nil {
			return readErr
		}
		if len(state.PendingTools) == 0 {
			continue
		}
		batchIDs := state.PendingBatchIDs
		if len(batchIDs) == 0 {
			batchIDs = pendingToolIDs(state.PendingTools)
		}
		if err := s.syncPendingIndex(state.CredentialFingerprint, state.SessionID, nil, batchIDs); err != nil {
			return err
		}
	}
	return nil
}

func readPendingIndex(path string) (pendingIndex, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is a SHA-256-derived filename under the configured private state root
	if err != nil {
		return pendingIndex{}, err
	}
	var index pendingIndex
	if err := jsonx.Unmarshal(data, &index); err != nil {
		return pendingIndex{}, fmt.Errorf("decode Cursor Agent v1 pending index: %w", err)
	}
	version, err := migrateRecordVersion(index.Version)
	if err != nil {
		return pendingIndex{}, err
	}
	index.Version = version
	index.BatchIDs = normalizeToolIDs(index.BatchIDs)
	return index, nil
}

func writePendingIndexAtomic(path string, index pendingIndex) error {
	data, err := jsonx.Marshal(index)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".cursor-agent-v1-pending-*.tmp")
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

func (s *FileStateStore) withOwner(owner OwnerLease, fn func(path string) error) error {
	if s == nil || strings.TrimSpace(s.root) == "" || owner.handle == nil {
		return ErrRetiredOwner
	}
	owner.handle.mu.Lock()
	defer owner.handle.mu.Unlock()
	if owner.handle.released {
		return ErrRetiredOwner
	}
	statePath, _ := s.paths(owner.SessionID)
	return fn(statePath)
}

func acquireLease(lockPath string) (*leaseHandle, error) {
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- lockPath is SHA-256-derived under the configured private state root
	if err != nil {
		return nil, fmt.Errorf("open Cursor Agent v1 state lock: %w", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrStateLeaseHeld
		}
		return nil, fmt.Errorf("lock Cursor Agent v1 state: %w", err)
	}
	return &leaseHandle{file: lock}, nil
}

func releaseHandle(handle *leaseHandle) error {
	if handle == nil {
		return nil
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.released {
		return nil
	}
	handle.released = true
	if err := unix.Flock(int(handle.file.Fd()), unix.LOCK_UN); err != nil {
		_ = handle.file.Close()
		return fmt.Errorf("unlock Cursor Agent v1 state: %w", err)
	}
	if err := handle.file.Close(); err != nil {
		return fmt.Errorf("close Cursor Agent v1 state lock: %w", err)
	}
	return nil
}

func (s *FileStateStore) paths(sessionID string) (string, string) {
	digest := sha256.Sum256([]byte(sessionID))
	name := hex.EncodeToString(digest[:])
	return filepath.Join(s.root, name+".json"), filepath.Join(s.root, name+".lock")
}

func readState(path string) (StateSnapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		return StateSnapshot{}, err
	}
	if info.Size() > maxStateFileSize {
		return StateSnapshot{}, fmt.Errorf("Cursor Agent v1 state exceeds %d bytes", maxStateFileSize)
	}
	data, err := os.ReadFile(path) // #nosec G304 -- path is a SHA-256-derived filename under the configured private state root
	if err != nil {
		return StateSnapshot{}, err
	}
	var state StateSnapshot
	if err := jsonx.Unmarshal(data, &state); err != nil {
		return StateSnapshot{}, fmt.Errorf("decode Cursor Agent v1 state: %w", err)
	}
	version, err := migrateRecordVersion(state.Version)
	if err != nil {
		return StateSnapshot{}, err
	}
	state.Version = version
	return state, nil
}

func validatePendingTools(pending []PendingTool) error {
	if len(pending) > maxPendingTools {
		return fmt.Errorf("Cursor Agent v1 pending tools exceed %d", maxPendingTools)
	}
	total := 0
	seen := make(map[string]struct{}, len(pending))
	seenUpstream := make(map[string]struct{}, len(pending))
	for _, tool := range pending {
		toolCallID := strings.TrimSpace(tool.ToolCallID)
		if toolCallID == "" {
			return fmt.Errorf("Cursor Agent v1 pending tool id is required")
		}
		if _, exists := seen[toolCallID]; exists {
			return fmt.Errorf("Cursor Agent v1 pending tool id %q is duplicated", toolCallID)
		}
		seen[toolCallID] = struct{}{}
		upstreamToolCallID := strings.TrimSpace(tool.UpstreamToolCallID)
		if strings.HasPrefix(toolCallID, "toolu_bf_agentv1_") && upstreamToolCallID == "" {
			return fmt.Errorf("Cursor Agent v1 routed pending tool is missing its upstream id")
		}
		if upstreamToolCallID != "" {
			if _, exists := seenUpstream[upstreamToolCallID]; exists {
				return fmt.Errorf("Cursor Agent v1 upstream pending tool id %q is duplicated", upstreamToolCallID)
			}
			seenUpstream[upstreamToolCallID] = struct{}{}
		}
		total += len(tool.ExecID) + len(toolCallID) + len(upstreamToolCallID) + len(tool.Name) + len(tool.Arguments)
		if total > maxPendingToolBytes {
			return fmt.Errorf("Cursor Agent v1 pending tool data exceeds %d bytes", maxPendingToolBytes)
		}
	}
	return nil
}

func validateToolCatalog(tools []ToolDefinition) ([]ToolDefinition, error) {
	if len(tools) == 0 {
		return nil, nil
	}
	normalized, err := normalizeToolDefinitions(tools)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, tool := range normalized {
		total += len(tool.Name) + len(tool.Description) + len(tool.InputSchema)
		if total > maxPendingToolBytes {
			return nil, fmt.Errorf("Cursor Agent v1 tool catalog exceeds %d bytes", maxPendingToolBytes)
		}
	}
	return normalized, nil
}

func writeStateAtomic(path string, state StateSnapshot) error {
	data, err := jsonx.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode Cursor Agent v1 state: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".cursor-agent-v1-*.tmp")
	if err != nil {
		return fmt.Errorf("create Cursor Agent v1 state temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("secure Cursor Agent v1 state temp file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write Cursor Agent v1 state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync Cursor Agent v1 state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("close Cursor Agent v1 state: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("replace Cursor Agent v1 state: %w", err)
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path) // #nosec G304 -- caller supplies only the configured state root or its private index directory
	if err != nil {
		return fmt.Errorf("open Cursor Agent v1 state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync Cursor Agent v1 state directory: %w", err)
	}
	return nil
}

func cloneSnapshot(state StateSnapshot) StateSnapshot {
	state.UsageCarry = usageEvidenceEvents(state.UsageCarry)
	state.Checkpoint = append([]byte(nil), state.Checkpoint...)
	state.Blobs = cloneBlobs(state.Blobs)
	state.PendingTools = append([]PendingTool(nil), state.PendingTools...)
	state.PendingBatchIDs = append([]string(nil), state.PendingBatchIDs...)
	state.ToolCatalog = cloneToolDefinitions(state.ToolCatalog)
	return state
}

func cloneToolDefinitions(tools []ToolDefinition) []ToolDefinition {
	if len(tools) == 0 {
		return nil
	}
	cloned := make([]ToolDefinition, len(tools))
	for index, tool := range tools {
		cloned[index] = tool
		cloned[index].InputSchema = append([]byte(nil), tool.InputSchema...)
	}
	return cloned
}

func cloneBlobs(blobs map[string][]byte) map[string][]byte {
	if len(blobs) == 0 {
		return nil
	}
	cloned := make(map[string][]byte, len(blobs))
	for key, value := range blobs {
		cloned[key] = append([]byte(nil), value...)
	}
	return cloned
}

func migrateRecordVersion(version int) (int, error) {
	switch version {
	case stateVersionV1, stateVersion:
		return stateVersion, nil
	default:
		return 0, fmt.Errorf("unsupported Cursor Agent v1 state version %d", version)
	}
}

func stateExpired(state StateSnapshot, now time.Time, ttl time.Duration) bool {
	if state.UpdatedAt.IsZero() {
		return false
	}
	if state.EphemeralComplete && len(state.PendingTools) == 0 {
		completedAt := state.CompletedAt
		if completedAt.IsZero() {
			completedAt = state.UpdatedAt
		}
		return now.Sub(completedAt) > completedTombstoneTTL
	}
	return ttl > 0 && now.Sub(state.UpdatedAt) > ttl
}

func canonicalToolResults(results []ToolResult) ([]CompletedToolResult, error) {
	if len(results) == 0 || len(results) > maxPendingTools {
		return nil, ErrSessionNotPending
	}
	canonical := make([]CompletedToolResult, 0, len(results))
	seen := make(map[string]struct{}, len(results))
	total := 0
	for _, result := range results {
		id := strings.TrimSpace(result.ToolCallID)
		if id == "" {
			return nil, ErrSessionNotPending
		}
		if _, exists := seen[id]; exists {
			return nil, ErrSessionNotPending
		}
		seen[id] = struct{}{}
		item := CompletedToolResult{ToolCallID: id, Content: result.Content, IsError: result.IsError, Media: cloneMedia(result.Media)}
		total += len(item.ToolCallID) + len(item.Content)
		for _, media := range item.Media {
			total += len(media.Data)
		}
		if total > maxPendingToolBytes {
			return nil, fmt.Errorf("Cursor Agent v1 completed tool data exceeds %d bytes", maxPendingToolBytes)
		}
		canonical = append(canonical, item)
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ToolCallID < canonical[j].ToolCallID })
	return canonical, nil
}

func completedBatchIDs(results []CompletedToolResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, result.ToolCallID)
	}
	return normalizeToolIDs(ids)
}

func toolResultDigest(results []CompletedToolResult) string {
	material := make([]byte, 0, 256)
	for _, result := range results {
		material = append(material, result.ToolCallID...)
		material = append(material, 0)
		material = append(material, result.Content...)
		material = append(material, 0)
		if result.IsError {
			material = append(material, '1')
		} else {
			material = append(material, '0')
		}
		material = append(material, 0)
	}
	digest := sha256.Sum256(material)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func cloneToolCall(call *ToolCall) *ToolCall {
	if call == nil {
		return nil
	}
	cloned := *call
	return &cloned
}

func cloneToolResult(result *ToolResult) *ToolResult {
	if result == nil {
		return nil
	}
	cloned := *result
	cloned.Media = cloneMedia(result.Media)
	return &cloned
}

func cloneEvent(event Event) Event {
	event.Checkpoint = nil
	event.Blobs = nil
	event.Usage = cloneUsageObservation(event.Usage)
	event.ToolCall = cloneToolCall(event.ToolCall)
	event.ToolResult = cloneToolResult(event.ToolResult)
	event.HostedSearch = cloneHostedSearch(event.HostedSearch)
	return event
}
