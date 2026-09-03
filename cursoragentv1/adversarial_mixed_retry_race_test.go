package cursoragentv1

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type adversarialMixedReleaseGate struct {
	StateStore
	entered chan struct{}
	allow   chan struct{}
	count   atomic.Int32
	once    sync.Once
}

func (s *adversarialMixedReleaseGate) Release(owner OwnerLease) error {
	if s.count.Add(1) == 1 {
		close(s.entered)
		<-s.allow
	}
	return s.StateStore.Release(owner)
}

func (s *adversarialMixedReleaseGate) unblock() { s.once.Do(func() { close(s.allow) }) }

// Pause the original HTTP consumer at the actual Execute boundary between
// prepare and maybeChainFollowUp. A duplicate is then allowed to complete D
// first. Channel gates, not scheduler delays, make this ordering reproducible.
func TestAnchoredAdversarialMixedRetryWinsFollowUpWithoutLosingOriginalUsage(t *testing.T) {
	base, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	store := &adversarialMixedReleaseGate{StateStore: base, entered: make(chan struct{}), allow: make(chan struct{})}
	defer store.unblock()
	var nativeRuns atomic.Int32
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		if nativeRuns.Add(1) == 1 {
			emit(Event{Type: EventUsage, Tokens: 11})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "mixed-race-tool", Name: "lookup", Arguments: `{}`}})
			emit(Event{Type: EventTurnEnded})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				emit(Event{Type: EventUsage, Tokens: 7})
				emit(Event{Type: EventHostedSearchCall})
				emit(Event{Type: EventText, Text: "RESULT_BASE"})
				emit(testMeasuredTerminal(7))
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		emit(Event{Type: EventUsage, Tokens: 5})
		emit(Event{Type: EventText, Text: "FOLLOWUP:" + request.UserText})
		emit(testMeasuredTerminal(5))
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, 5*time.Second)
	require.NoError(t, err)
	executor := newExecutorForTest(manager, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := claudeExecutorOptions(false)
	auth := testExecutorAuth()
	first, err := executor.Execute(ctx, auth, anchoredClaudeExecutorRequest("mixed-race-session", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
	}`), opts)
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	body, err := jsonx.Marshal(map[string]any{
		"model": "claude-sonnet-4-6",
		"messages": []any{map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": ids[0], "content": "ok"},
			map[string]any{"type": "text", "text": "foo"},
		}}},
		"tools": []any{map[string]any{"name": "lookup", "input_schema": map[string]any{"type": "object"}}},
	})
	require.NoError(t, err)
	leaderRequest := anchoredClaudeExecutorRequest("mixed-race-leader", string(body))
	leaderTurn, leaderOutput, err := executor.prepare(ctx, auth, leaderRequest, opts)
	require.NoError(t, err)
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("B did not reach its persisted snapshot/release boundary")
	}

	type responseResult struct {
		response cliproxyexecutor.Response
		err      error
	}
	duplicateResult := make(chan responseResult, 1)
	go func() {
		response, callErr := executor.Execute(ctx, auth, anchoredClaudeExecutorRequest("mixed-race-duplicate", string(body)), opts)
		duplicateResult <- responseResult{response, callErr}
	}()
	store.unblock()
	var duplicate responseResult
	select {
	case duplicate = <-duplicateResult:
	case <-ctx.Done():
		t.Fatal("duplicate did not finish the follow-up")
	}
	require.NoError(t, duplicate.err)
	waitUntilInactive(t, manager, "mixed-race-session")
	assert.Equal(t, int64(12), adversarialOutputTokens(t, duplicate.response), "first completed HTTP answer must retain B=7 plus D=5 even when duplicate wins D")
	assert.Equal(t, int64(1), adversarialHostedRequests(t, duplicate.response))
	assert.Equal(t, int32(2), nativeRuns.Load())

	// Resume the original consumer only after the duplicate's D is durable.
	// It must use that answer, not run the already-completed follow-up again.
	finalOutput, err := executor.maybeChainFollowUp(ctx, &leaderTurn, leaderOutput)
	require.NoError(t, err)
	var tokens int64
	for event := range finalOutput.Events {
		if event.Type == EventUsage {
			tokens += event.Tokens
		}
	}
	require.NoError(t, <-finalOutput.Result)
	waitUntilInactive(t, manager, "mixed-race-session")
	assert.Equal(t, int64(12), tokens)
	assert.Equal(t, int32(2), nativeRuns.Load(), "delayed leader must not repeat D after the duplicate finished it")
	assert.Equal(t, duplicate.response.Headers.Get(HeaderUsageReceipt), usageReceiptID(leaderTurn))
}
