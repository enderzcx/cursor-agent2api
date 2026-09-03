package cursoragentv1

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCredentialFingerprintUsesStableAccountKeyAndScope(t *testing.T) {
	firstFingerprint, err := CredentialFingerprint("channel:6401", "account-1")
	require.NoError(t, err)
	secondFingerprint, err := CredentialFingerprint("channel:6401", "account-1")
	require.NoError(t, err)
	require.Equal(t, firstFingerprint, secondFingerprint)
	require.NotContains(t, firstFingerprint, "account-1")
	differentScope, err := CredentialFingerprint("channel:6402", "account-1")
	require.NoError(t, err)
	require.NotEqual(t, firstFingerprint, differentScope)
	otherUser, err := CredentialFingerprint("tenant:1:user:8:channel:6401", "account-1")
	require.NoError(t, err)
	require.NotEqual(t, firstFingerprint, otherUser)
}

func TestStateStorePinsRuntimeProfileAcrossChannelSwitch(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	first, err := store.Claim("profile-session", "conversation", "fingerprint", ClaimOptions{RuntimeProfile: runtimeSand})
	require.NoError(t, err)
	require.Equal(t, runtimeSand, first.Snapshot.RuntimeProfile)
	require.NoError(t, store.Release(first.Owner))

	continued, err := store.Claim("profile-session", "ignored", "fingerprint", ClaimOptions{RuntimeProfile: runtimeAgentV1, RequireExisting: true})
	require.NoError(t, err)
	require.Equal(t, runtimeSand, continued.Snapshot.RuntimeProfile)
	require.NoError(t, store.Release(continued.Owner))

	legacy, err := store.Claim("legacy-profile-session", "conversation", "fingerprint", ClaimOptions{})
	require.NoError(t, err)
	require.Equal(t, runtimeAgentV1, legacy.Snapshot.RuntimeProfile)
	require.NoError(t, store.Release(legacy.Owner))
	statePath, _ := store.paths("legacy-profile-session")
	legacyState, err := readState(statePath)
	require.NoError(t, err)
	legacyState.RuntimeProfile = ""
	require.NoError(t, writeStateAtomic(statePath, legacyState))
	legacyContinued, err := store.Claim("legacy-profile-session", "ignored", "fingerprint", ClaimOptions{RuntimeProfile: runtimeSand, RequireExisting: true})
	require.NoError(t, err)
	require.Equal(t, runtimeAgentV1, legacyContinued.Snapshot.RuntimeProfile)
	require.NoError(t, store.Release(legacyContinued.Owner))
}

func TestFileStateStorePersistsCheckpointAcrossRestartWithoutCredentialMaterial(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("channel:6401", "account-1")
	require.NoError(t, err)

	claim, err := store.Claim("tenant:session-1", "conversation-1", fingerprint, ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.False(t, claim.ColdRebuild)
	require.NoError(t, store.SaveCheckpoint(claim.Owner, []byte("checkpoint"), map[string][]byte{"blob": []byte("value")}))
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{MessageID: 7, ExecID: "exec-7", ToolCallID: "tool-7", Name: "lookup", Arguments: `{"id":7}`}}))
	require.NoError(t, store.Release(claim.Owner))

	restarted, err := NewFileStateStore(root)
	require.NoError(t, err)
	resolvedSession, err := restarted.ResolvePending(fingerprint, []string{"tool-7"})
	require.NoError(t, err)
	require.Equal(t, "tenant:session-1", resolvedSession)
	next, err := restarted.Claim("tenant:session-1", "ignored-new-conversation", fingerprint, ClaimOptions{AllowPending: true, RequireExisting: true})
	require.NoError(t, err)
	require.False(t, next.ColdRebuild)
	require.Equal(t, "conversation-1", next.Owner.ConversationID)
	require.Equal(t, []byte("checkpoint"), next.Snapshot.Checkpoint)
	require.Equal(t, []byte("value"), next.Snapshot.Blobs["blob"])
	require.Equal(t, "tool-7", next.Snapshot.PendingTools[0].ToolCallID)
	require.NoError(t, restarted.Release(next.Owner))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 4, "state, lock, pending index, and completed index should be the only artifacts")
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), "tenant")
		info, statErr := entry.Info()
		require.NoError(t, statErr)
		if entry.IsDir() {
			require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
		} else {
			require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		}
		if strings.HasSuffix(entry.Name(), ".json") {
			data, readErr := os.ReadFile(filepath.Join(root, entry.Name()))
			require.NoError(t, readErr)
			require.NotContains(t, string(data), "opaque-access")
			require.NotContains(t, string(data), "opaque-refresh")
		}
	}
}

func TestFileStateStoreReusesBillingRunOnlyForToolResume(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	first, err := store.Claim("responses-session", "conversation", fingerprint, ClaimOptions{NewBillingRunID: "run-1"})
	require.NoError(t, err)
	require.Equal(t, "run-1", first.Owner.BillingRunID)
	require.NoError(t, store.SavePending(first.Owner, []PendingTool{{ToolCallID: "tool-1", Name: "lookup", Arguments: `{}`}}))
	require.NoError(t, store.Release(first.Owner))

	resume, err := store.Claim("responses-session", "conversation", fingerprint, ClaimOptions{AllowPending: true, RequireExisting: true, ReuseBillingRun: true})
	require.NoError(t, err)
	require.Equal(t, "run-1", resume.Owner.BillingRunID)
	require.NoError(t, store.SavePending(resume.Owner, nil))
	require.NoError(t, store.Release(resume.Owner))

	next, err := store.Claim("responses-session", "conversation", fingerprint, ClaimOptions{RequireExisting: true, NewBillingRunID: "run-2"})
	require.NoError(t, err)
	require.Equal(t, "run-2", next.Owner.BillingRunID)
	require.NoError(t, store.Release(next.Owner))
}

func TestFileStateStoreFencesRetiredOwnerAndColdRebuildsOnCredentialChange(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	first, err := store.Claim("session-1", "conversation-1", "fingerprint-a", ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.NoError(t, store.SaveCheckpoint(first.Owner, []byte("checkpoint-a"), map[string][]byte{"a": []byte("a")}))

	require.NoError(t, store.Release(first.Owner))
	second, err := store.Claim("session-1", "conversation-2", "fingerprint-a", ClaimOptions{RequireExisting: true, AllowCredentialChange: true})
	require.NoError(t, err)
	require.Equal(t, "conversation-1", second.Owner.ConversationID)
	require.ErrorIs(t, store.SavePending(first.Owner, []PendingTool{{ToolCallID: "late"}}), ErrRetiredOwner)
	require.NoError(t, store.SavePending(second.Owner, []PendingTool{{ToolCallID: "current"}}))

	require.NoError(t, store.Release(second.Owner))
	third, err := store.Claim("session-1", "conversation-3", "fingerprint-b", ClaimOptions{AllowPending: true, RequireExisting: true, AllowCredentialChange: true})
	require.NoError(t, err)
	require.True(t, third.ColdRebuild)
	require.Equal(t, "conversation-3", third.Owner.ConversationID)
	require.Empty(t, third.Snapshot.Checkpoint)
	require.Empty(t, third.Snapshot.Blobs)
	require.Empty(t, third.Snapshot.PendingTools)
	require.ErrorIs(t, store.SaveCheckpoint(second.Owner, []byte("late"), nil), ErrRetiredOwner)
	require.NoError(t, store.Release(third.Owner))
}

func TestFileStateStoreResumeCredentialMismatchDoesNotErasePendingState(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	first, err := store.Claim("session", "conversation", "fingerprint-a", ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(first.Owner, []PendingTool{{ToolCallID: "tool", Name: "lookup", Arguments: `{}`}}))
	require.NoError(t, store.Release(first.Owner))

	_, err = store.Claim("session", "other", "fingerprint-b", ClaimOptions{AllowPending: true, RequireExisting: true})
	require.ErrorIs(t, err, ErrCredentialMismatch)

	restored, err := store.Claim("session", "ignored", "fingerprint-a", ClaimOptions{AllowPending: true, RequireExisting: true})
	require.NoError(t, err)
	require.Equal(t, "tool", restored.Snapshot.PendingTools[0].ToolCallID)
	require.Equal(t, "conversation", restored.Snapshot.ConversationID)
	require.NoError(t, store.Release(restored.Owner))
}

func TestFileStateStoreConcurrentClaimsHaveOneWritableOwner(t *testing.T) {
	root := t.TempDir()
	const claimCount = 12
	results := make(chan ClaimResult, claimCount)
	errorsCh := make(chan error, claimCount)
	var wait sync.WaitGroup
	for index := 0; index < claimCount; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			store, err := NewFileStateStore(root)
			if err != nil {
				errorsCh <- err
				return
			}
			result, err := store.Claim("shared-session", "shared-conversation", "shared-fingerprint", ClaimOptions{AllowCredentialChange: true})
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	leaseErrors := 0
	for err := range errorsCh {
		if errors.Is(err, ErrStateLeaseHeld) {
			leaseErrors++
		} else {
			require.NoError(t, err)
		}
	}

	claims := make([]ClaimResult, 0, claimCount)
	for result := range results {
		claims = append(claims, result)
	}
	require.Len(t, claims, 1)
	require.Equal(t, claimCount-1, leaseErrors)

	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claims[0].Owner, []PendingTool{{ToolCallID: "winner"}}))
	require.NoError(t, store.Release(claims[0].Owner))
}

func TestFileStateStoreDeleteRequiresCurrentOwner(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	first, err := store.Claim("session", "conversation", "fingerprint", ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.NoError(t, store.Release(first.Owner))
	second, err := store.Claim("session", "conversation", "fingerprint", ClaimOptions{RequireExisting: true, AllowCredentialChange: true})
	require.NoError(t, err)
	require.ErrorIs(t, store.Delete(first.Owner), ErrRetiredOwner)
	require.NoError(t, store.Delete(second.Owner))
	require.NoError(t, store.Delete(second.Owner))
	require.NoError(t, store.Release(second.Owner))
}

func TestFileStateStoreDeleteSessionRequiresCredentialFingerprint(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	claim, err := store.Claim("session", "conversation", "fingerprint-a", ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.NoError(t, store.Release(claim.Owner))
	require.ErrorIs(t, store.DeleteSession("session", "fingerprint-b"), ErrCredentialMismatch)
	require.NoError(t, store.DeleteSession("session", "fingerprint-a"))
	require.NoError(t, store.DeleteSession("session", "fingerprint-a"))
}

func TestFileStateStoreReturnsCorruptStateError(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	path, _ := store.paths("session")
	require.NoError(t, os.WriteFile(path, []byte(`{"version":999}`), 0o600))
	_, err = store.Claim("session", "conversation", "fingerprint", ClaimOptions{RequireExisting: true})
	require.Error(t, err)
	require.False(t, errors.Is(err, os.ErrNotExist))
	require.Contains(t, err.Error(), "unsupported Cursor Agent v1 state version 999")
}

func TestResolvePendingIsCredentialBoundAndRejectsAmbiguity(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("channel:6401", "account-1")
	require.NoError(t, err)
	otherFingerprint, err := CredentialFingerprint("channel:6402", "account-2")
	require.NoError(t, err)

	seed := func(sessionID, credential string) {
		claim, claimErr := store.Claim(sessionID, "conversation-"+sessionID, credential, ClaimOptions{ToolCatalogDigest: "sha256:tools"})
		require.NoError(t, claimErr)
		require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "shared-call", Name: "lookup", Arguments: `{}`}}))
		require.NoError(t, store.Release(claim.Owner))
	}
	seed("session-one", fingerprint)
	seed("other-account", otherFingerprint)

	resolved, err := store.ResolvePending(fingerprint, []string{"shared-call"})
	require.NoError(t, err)
	require.Equal(t, "session-one", resolved)
	_, err = store.ResolvePending(otherFingerprint, []string{"missing-call"})
	require.ErrorIs(t, err, ErrSessionNotPending)

	seed("session-two", fingerprint)
	_, err = store.ResolvePending(fingerprint, []string{"shared-call"})
	require.ErrorIs(t, err, ErrPendingAmbiguous)
}

func TestResolvePendingUsesExactOriginalBatchAfterPartialApply(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("channel:6401", "account-1")
	require.NoError(t, err)
	claim, err := store.Claim("parallel-session", "conversation", fingerprint, ClaimOptions{ToolCatalogDigest: "sha256:tools"})
	require.NoError(t, err)
	batch := []PendingTool{{ToolCallID: "call-a", Name: "a"}, {ToolCallID: "call-b", Name: "b"}}
	require.NoError(t, store.SavePending(claim.Owner, batch))
	require.NoError(t, store.SavePending(claim.Owner, batch[1:]), "partial apply must retain the original batch identity")
	require.NoError(t, store.Release(claim.Owner))

	_, err = store.ResolvePending(fingerprint, []string{"call-b"})
	require.ErrorIs(t, err, ErrSessionNotPending)
	resolved, err := store.ResolvePending(fingerprint, []string{"call-a", "call-b"})
	require.NoError(t, err)
	require.Equal(t, "parallel-session", resolved)
}

func TestFileStateStoreTTLPrunesOnlyExpiredUnleasedState(t *testing.T) {
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	store, err := newFileStateStore(t.TempDir(), time.Hour, func() time.Time { return now })
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	claim, err := store.Claim("leased", "conversation", fingerprint, ClaimOptions{})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-live"}}))
	statePath, _ := store.paths("leased")
	now = now.Add(2 * time.Hour)
	require.NoError(t, store.pruneExpired(true))
	_, err = os.Stat(statePath)
	require.NoError(t, err, "an active file lease must fence TTL cleanup")
	require.NoError(t, store.Release(claim.Owner))
	require.NoError(t, store.pruneExpired(true))
	_, err = os.Stat(statePath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = store.ResolvePending(fingerprint, []string{"tool-live"})
	require.ErrorIs(t, err, ErrSessionNotPending)
}

func TestResolvePendingPrunesExpiredFilesBeforeLimit(t *testing.T) {
	now := time.Date(2026, 8, 29, 2, 0, 0, 0, time.UTC)
	store, err := newFileStateStore(t.TempDir(), time.Hour, func() time.Time { return now })
	require.NoError(t, err)
	store.maxFiles = 2
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	live, err := store.Claim("live", "conversation", fingerprint, ClaimOptions{})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(live.Owner, []PendingTool{{ToolCallID: "tool-live"}}))
	require.NoError(t, store.Release(live.Owner))

	now = now.Add(-2 * time.Hour)
	store.lastGC = time.Time{}
	for _, sessionID := range []string{"old-1", "old-2", "old-3"} {
		claim, claimErr := store.Claim(sessionID, "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, claimErr)
		require.NoError(t, store.Release(claim.Owner))
	}
	now = now.Add(2 * time.Hour)
	store.lastGC = time.Time{}
	resolved, err := store.ResolvePending(fingerprint, []string{"tool-live"})
	require.NoError(t, err)
	require.Equal(t, "live", resolved)
}

func TestResolvePendingUsesIndexWithoutScanningCompletedResponses(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	claim, err := store.Claim("pending", "conversation", fingerprint, ClaimOptions{})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-indexed"}}))
	require.NoError(t, store.Release(claim.Owner))
	for index := 0; index < 100; index++ {
		name := filepath.Join(store.root, fmt.Sprintf("completed-%03d.json", index))
		require.NoError(t, os.WriteFile(name, []byte(`not-json`), 0o600))
	}
	store.lastGC = store.now().UTC()
	resolved, err := store.ResolvePending(fingerprint, []string{"tool-indexed"})
	require.NoError(t, err)
	require.Equal(t, "pending", resolved)
}

func TestPendingDeleteFailureOrderingNeverLeavesStateWithoutIndex(t *testing.T) {
	seed := func(t *testing.T) (*FileStateStore, OwnerLease, string, string) {
		t.Helper()
		store, err := NewFileStateStore(t.TempDir())
		require.NoError(t, err)
		fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
		require.NoError(t, err)
		claim, err := store.Claim("pending", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-delete"}}))
		statePath, _ := store.paths("pending")
		return store, claim.Owner, fingerprint, statePath
	}

	t.Run("state delete failure keeps index discoverable", func(t *testing.T) {
		store, owner, fingerprint, statePath := seed(t)
		originalRemove := store.removeFile
		store.removeFile = func(path string) error {
			if path == statePath {
				return errors.New("injected state delete failure")
			}
			return originalRemove(path)
		}
		require.ErrorContains(t, store.Delete(owner), "injected state delete failure")
		resolved, err := store.ResolvePending(fingerprint, []string{"tool-delete"})
		require.NoError(t, err)
		require.Equal(t, "pending", resolved)
		store.removeFile = originalRemove
		require.NoError(t, store.Delete(owner))
		require.NoError(t, store.Release(owner))
	})

	t.Run("index delete failure leaves only stale index", func(t *testing.T) {
		store, owner, fingerprint, statePath := seed(t)
		originalRemove := store.removeFile
		store.removeFile = func(path string) error {
			if strings.HasPrefix(path, store.pendingRoot) && filepath.Ext(path) == ".json" {
				return errors.New("injected index delete failure")
			}
			return originalRemove(path)
		}
		require.ErrorContains(t, store.Delete(owner), "injected index delete failure")
		_, err := os.Stat(statePath)
		require.ErrorIs(t, err, os.ErrNotExist)
		store.removeFile = originalRemove
		_, err = store.ResolvePending(fingerprint, []string{"tool-delete"})
		require.ErrorIs(t, err, ErrSessionNotPending)
		require.NoError(t, store.Release(owner))
	})

	t.Run("crash after state delete is self healing", func(t *testing.T) {
		store, owner, fingerprint, statePath := seed(t)
		require.NoError(t, os.Remove(statePath))
		_, err := store.ResolvePending(fingerprint, []string{"tool-delete"})
		require.ErrorIs(t, err, ErrSessionNotPending)
		require.NoError(t, store.Release(owner))
	})
}

func TestToolCatalogHasNoItemCountCap(t *testing.T) {
	tools := make([]ToolDefinition, 175)
	for index := range tools {
		tools[index] = ToolDefinition{Name: fmt.Sprintf("tool_%03d", index), InputSchema: []byte(`{"type":"object"}`)}
	}
	normalized, err := validateToolCatalog(tools)
	require.NoError(t, err)
	require.Len(t, normalized, 175)

	many := make([]ToolDefinition, 300)
	for index := range many {
		many[index] = ToolDefinition{Name: fmt.Sprintf("tool_%03d", index), InputSchema: []byte(`{"type":"object"}`)}
	}
	normalized, err = validateToolCatalog(many)
	require.NoError(t, err)
	require.Len(t, normalized, 300)
}

func TestFileStateStoreVersionZeroRecordsFailClosed(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := newFileStateStore(t.TempDir(), time.Hour, func() time.Time { return now })
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)

	t.Run("pending index", func(t *testing.T) {
		indexPath, _ := store.pendingIndexPaths(fingerprint, []string{"tool-0"})
		require.NoError(t, writePendingIndexAtomic(indexPath, pendingIndex{
			Version: 0, CredentialFingerprint: fingerprint, BatchIDs: []string{"tool-0"},
			SessionIDs: []string{"v0-pending"}, UpdatedAt: now,
		}))
		_, err := store.ResolvePending(fingerprint, []string{"tool-0"})
		require.ErrorContains(t, err, "unsupported Cursor Agent v1 state version 0")
	})

	t.Run("completed tombstone", func(t *testing.T) {
		results := []CompletedToolResult{{ToolCallID: "tool-0", Content: "ok"}}
		require.NoError(t, writeCompletedTombstoneAtomic(mustCompletedIndexPath(t, store, fingerprint, []string{"tool-0"}), completedTombstone{
			Version: 0, CredentialFingerprint: fingerprint, BatchIDs: []string{"tool-0"},
			ResultDigest: toolResultDigest(results), Results: results,
			Events: []persistedEvent{{Type: EventDone}}, SessionID: "v0-completed",
			UpdatedAt: now,
		}))
		_, err := store.LookupCompleted(fingerprint, []ToolResult{{ToolCallID: "tool-0", Content: "ok"}}, "")
		require.ErrorContains(t, err, "unsupported Cursor Agent v1 state version 0")
	})
}

func TestConfiguredTTLAppliesToPendingAndNonEphemeralCompleted(t *testing.T) {
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	results := []ToolResult{{ToolCallID: "tool-ttl", Content: "ok"}}
	events := []Event{{Type: EventDone}}

	t.Run("short pending ttl", func(t *testing.T) {
		now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
		store, err := newFileStateStore(t.TempDir(), 10*time.Minute, func() time.Time { return now })
		require.NoError(t, err)
		claim, err := store.Claim("short-pending", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-ttl"}}))
		require.NoError(t, store.Release(claim.Owner))

		now = now.Add(10*time.Minute - time.Second)
		require.NoError(t, store.pruneExpired(true))
		resolved, err := store.ResolvePending(fingerprint, []string{"tool-ttl"})
		require.NoError(t, err)
		require.Equal(t, "short-pending", resolved)

		now = now.Add(2 * time.Second)
		require.NoError(t, store.pruneExpired(true))
		_, err = store.ResolvePending(fingerprint, []string{"tool-ttl"})
		require.ErrorIs(t, err, ErrSessionNotPending)
	})

	t.Run("long pending ttl", func(t *testing.T) {
		now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
		store, err := newFileStateStore(t.TempDir(), 24*time.Hour, func() time.Time { return now })
		require.NoError(t, err)
		claim, err := store.Claim("long-pending", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-ttl"}}))
		require.NoError(t, store.Release(claim.Owner))
		now = now.Add(10 * time.Minute)
		require.NoError(t, store.pruneExpired(true))
		resolved, err := store.ResolvePending(fingerprint, []string{"tool-ttl"})
		require.NoError(t, err)
		require.Equal(t, "long-pending", resolved)
	})

	t.Run("short non-ephemeral completed ttl", func(t *testing.T) {
		now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
		store, err := newFileStateStore(t.TempDir(), 10*time.Minute, func() time.Time { return now })
		require.NoError(t, err)
		claim, err := store.Claim("short-completed", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, store.SaveCompleted(claim.Owner, results, events, false, ""))
		require.NoError(t, store.Release(claim.Owner))

		now = now.Add(10*time.Minute - time.Second)
		replay, err := store.LookupCompleted(fingerprint, results, "")
		require.NoError(t, err)
		require.Equal(t, "short-completed", replay.SessionID)

		now = now.Add(2 * time.Second)
		_, err = store.LookupCompleted(fingerprint, results, "")
		require.ErrorIs(t, err, ErrSessionNotPending)
	})

	t.Run("long non-ephemeral completed ttl", func(t *testing.T) {
		now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
		store, err := newFileStateStore(t.TempDir(), 24*time.Hour, func() time.Time { return now })
		require.NoError(t, err)
		claim, err := store.Claim("long-completed", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, store.SaveCompleted(claim.Owner, results, events, false, ""))
		require.NoError(t, store.Release(claim.Owner))
		now = now.Add(10 * time.Minute)
		replay, err := store.LookupCompleted(fingerprint, results, "")
		require.NoError(t, err)
		require.Equal(t, "long-completed", replay.SessionID)
		now = now.Add(24 * time.Hour)
		_, err = store.LookupCompleted(fingerprint, results, "")
		require.ErrorIs(t, err, ErrSessionNotPending)
	})

	t.Run("ephemeral completed retains five minutes for short and long store ttl", func(t *testing.T) {
		for _, ttl := range []time.Duration{time.Second, 24 * time.Hour} {
			now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
			store, err := newFileStateStore(t.TempDir(), ttl, func() time.Time { return now })
			require.NoError(t, err)
			claim, err := store.Claim("ephemeral-completed", "conversation", fingerprint, ClaimOptions{})
			require.NoError(t, err)
			require.NoError(t, store.SaveCompleted(claim.Owner, results, events, true, ""))
			require.NoError(t, store.Release(claim.Owner))
			now = now.Add(3*time.Minute + 5*time.Second)
			replay, err := store.LookupCompleted(fingerprint, results, "")
			require.NoError(t, err)
			require.Equal(t, "ephemeral-completed", replay.SessionID)
			now = now.Add(completedTombstoneTTL - (3*time.Minute + 5*time.Second) + time.Second)
			_, err = store.LookupCompleted(fingerprint, results, "")
			require.ErrorIs(t, err, ErrSessionNotPending)
		}
	})
}

func TestTwoFileStateStoresInterleaveRefreshAndGC(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	clock := func() time.Time { return now }
	storeA, err := newFileStateStore(root, time.Hour, clock)
	require.NoError(t, err)
	storeB, err := newFileStateStore(root, time.Hour, clock)
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	results := []ToolResult{{ToolCallID: "tool-gc", Content: "ok"}}
	events := []Event{{Type: EventText, Text: "kept"}, {Type: EventDone}}

	t.Run("refreshed completed tombstone survives other store gc", func(t *testing.T) {
		claim, err := storeA.Claim("gc-session", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, storeA.SaveCompleted(claim.Owner, results, events, false, ""))
		require.NoError(t, storeA.Release(claim.Owner))

		now = now.Add(2 * time.Hour)
		tombstone, err := storeA.readCompletedTombstone(fingerprint, []string{"tool-gc"}, "")
		require.NoError(t, err)
		tombstone.UpdatedAt = now
		require.NoError(t, storeA.writeCompletedTombstone(tombstone))

		require.NoError(t, storeB.pruneExpiredCompleted(true))
		replay, err := storeB.LookupCompleted(fingerprint, results, "")
		require.NoError(t, err)
		require.Equal(t, "gc-session", replay.SessionID)
	})

	t.Run("gc does not steal a live session lock inode", func(t *testing.T) {
		now = time.Date(2026, 8, 30, 16, 0, 0, 0, time.UTC)
		storeA.lastGC = time.Time{}
		storeB.lastGC = time.Time{}
		claim, err := storeA.Claim("lock-session", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, storeA.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-lock"}}))
		statePath, lockPath := storeA.paths("lock-session")
		now = now.Add(2 * time.Hour)
		require.NoError(t, storeB.pruneExpired(true))
		_, err = os.Stat(statePath)
		require.NoError(t, err, "leased session json must not be pruned")
		_, err = os.Stat(lockPath)
		require.NoError(t, err, "runtime lock files must not be unlinked")
		_, err = storeB.Claim("lock-session", "conversation", fingerprint, ClaimOptions{RequireExisting: true, AllowPending: true})
		require.ErrorIs(t, err, ErrStateLeaseHeld)
		require.NoError(t, storeA.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-lock"}}))
		require.NoError(t, storeA.Release(claim.Owner))
	})

	t.Run("expired json is deleted without unlinking lock", func(t *testing.T) {
		now = time.Date(2026, 8, 30, 20, 0, 0, 0, time.UTC)
		storeA.lastGC = time.Time{}
		storeB.lastGC = time.Time{}
		claim, err := storeA.Claim("expired-lock", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, storeA.SaveCompleted(claim.Owner, results, events, false, ""))
		require.NoError(t, storeA.Release(claim.Owner))
		indexPath, lockPath := storeA.completedIndexPaths(fingerprint, []string{"tool-gc"}, "")
		now = now.Add(2 * time.Hour)
		require.NoError(t, storeB.pruneExpiredCompleted(true))
		_, err = os.Stat(indexPath)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(lockPath)
		require.NoError(t, err, "completed lock pathname must remain for flock identity")
	})

	t.Run("concurrent refresh and gc keep a live tombstone", func(t *testing.T) {
		now = time.Date(2026, 8, 30, 22, 0, 0, 0, time.UTC)
		storeA.lastGC = now
		storeB.lastGC = now
		claim, err := storeA.Claim("race-session", "conversation", fingerprint, ClaimOptions{})
		require.NoError(t, err)
		require.NoError(t, storeA.SaveCompleted(claim.Owner, results, events, false, ""))
		require.NoError(t, storeA.Release(claim.Owner))
		tombstone, err := storeA.readCompletedTombstone(fingerprint, []string{"tool-gc"}, "")
		require.NoError(t, err)

		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			for index := 0; index < 40; index++ {
				tombstone.UpdatedAt = now
				require.NoError(t, storeA.writeCompletedTombstone(tombstone))
			}
		}()
		go func() {
			defer wait.Done()
			for index := 0; index < 40; index++ {
				require.NoError(t, storeB.pruneExpiredCompleted(true))
			}
		}()
		wait.Wait()
		replay, err := storeB.LookupCompleted(fingerprint, results, "")
		require.NoError(t, err)
		require.Equal(t, "race-session", replay.SessionID)
	})
}

func mustCompletedIndexPath(t *testing.T, store *FileStateStore, fingerprint string, batchIDs []string) string {
	t.Helper()
	path, _ := store.completedIndexPaths(fingerprint, batchIDs, "")
	return path
}
