package cursoragentv1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Adding WebSearch to the new release must not reject old pending tool results
// after a sidecar restart. Nor may it inject new tools into an existing run.
func TestSearchRolloutColdResumeKeepsFrozenCallerCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("search-rollout-pending")
	original := []ToolDefinition{{Name: "Bash", InputSchema: []byte(`{"type":"object"}`)}}
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "existing-conversation", fingerprint, ClaimOptions{AllowPending: true, ToolCatalogDigest: "old-catalog", ToolCatalog: original})
	require.NoError(t, err)
	require.NoError(t, store.SaveCheckpoint(claim.Owner, []byte("saved-checkpoint"), nil))
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "pending-local-tool", Name: "Bash", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))
	engine := &scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		require.True(t, resumed.Resume)
		require.Equal(t, original, resumed.Tools)
		require.Equal(t, []byte("saved-checkpoint"), resumed.InitialCheckpoint)
		result := <-resumed.ToolResults
		require.Equal(t, "pending-local-tool", result.ToolCallID)
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request.ToolCatalogDigest = "new-catalog"
	request.Run.Tools = append(append([]ToolDefinition(nil), original...), ToolDefinition{Name: "WebSearch", InputSchema: []byte(`{"type":"object"}`)})
	output, err := manager.ResumeTool(request, ToolResult{ToolCallID: "pending-local-tool", Content: "local tool finished"})
	require.NoError(t, err)
	collectManagedEvents(output.Events)
	require.NoError(t, <-output.Result)
}

func TestSearchRolloutLegacyMismatchDoesNotMutateLedgerCarry(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("legacy-mismatch-carry")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{ToolCatalogDigest: "old-digest", NewBillingRunID: "legacy-billing"})
	require.NoError(t, err)
	require.NoError(t, store.SaveUsageCarry(claim.Owner, []Event{{Type: EventUsage, Tokens: 7}}))
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "pending-tool", Name: "tool_a", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))
	path, _ := store.paths(request.SessionID)
	before, err := readState(path)
	require.NoError(t, err)
	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		t.Fatal("invalid legacy catalog must not run")
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request.ToolCatalogDigest = "changed-digest"
	_, err = manager.ResumeTool(request, ToolResult{ToolCallID: "pending-tool", Content: "done"})
	require.ErrorIs(t, err, ErrSessionNotPending)
	after, err := readState(path)
	require.NoError(t, err)
	require.Equal(t, before.BillingRunID, after.BillingRunID)
	require.Equal(t, before.UsageCarry, after.UsageCarry)
	require.Equal(t, before.OwnerGeneration, after.OwnerGeneration)
	require.Equal(t, before.PendingTools, after.PendingTools)
	require.Equal(t, before.PendingBatchIDs, after.PendingBatchIDs)
}
