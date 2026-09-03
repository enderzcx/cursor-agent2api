package cursoragentv1

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One client request may internally consume both the result continuation and a
// follow-up. The owner contract charges that request once, for all observable
// output, while keeping the original result replay distinct from follow-ups.
func TestAnchoredAdversarialMixedRequestUsageAndReplayIdentity(t *testing.T) {
	var nativeRuns atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		if nativeRuns.Add(1) == 1 {
			emit(Event{Type: EventUsage, Tokens: 11})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "adversarial-tool", Name: "lookup", Arguments: `{}`}})
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
	}})
	call := func(session string, messages []any) cliproxyexecutor.Response {
		t.Helper()
		payload, err := jsonx.Marshal(map[string]any{
			"model": "claude-sonnet-4-6", "messages": messages,
			"tools": []any{map[string]any{"name": "lookup", "input_schema": map[string]any{"type": "object"}}},
		})
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		response, err := executor.Execute(ctx, testExecutorAuth(), anchoredClaudeExecutorRequest(session, string(payload)), claudeExecutorOptions(false))
		require.NoError(t, err)
		return response
	}
	first := call("adversarial-mixed", []any{map[string]any{"role": "user", "content": "lookup"}})
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	assert.NotEmpty(t, first.Headers.Get(HeaderUsageReceipt))
	assert.Equal(t, int64(2), adversarialOutputTokens(t, first), "pending output is tokenizer estimate of lookup{}, not the native delta")
	resultMessage := func(text string) []any {
		blocks := []any{map[string]any{"type": "tool_result", "tool_use_id": ids[0], "content": "ok"}}
		if text != "" {
			blocks = append(blocks, map[string]any{"type": "text", "text": text})
		}
		return []any{map[string]any{"role": "user", "content": blocks}}
	}

	mixed := call("adversarial-foo", resultMessage("foo"))
	waitUntilInactive(t, executor.manager, "adversarial-mixed")
	assert.Equal(t, int64(12), adversarialOutputTokens(t, mixed), "7 result tokens plus 5 follow-up tokens, not just the last native generation")
	assert.Equal(t, int64(1), adversarialHostedRequests(t, mixed), "observable intermediate hosted work must survive aggregation")
	assert.Contains(t, string(mixed.Payload), "FOLLOWUP:foo")
	assert.NotContains(t, string(mixed.Payload), "RESULT_BASE", "internal intermediate text need not be shown twice")
	assert.Equal(t, first.Headers.Get(HeaderUsageReceipt), mixed.Headers.Get(HeaderUsageReceipt))
	assert.NotEmpty(t, mixed.Headers.Get(HeaderUsageReceipt))
	assert.Empty(t, mixed.Headers.Get(HeaderUsageReplay))
	assert.Equal(t, int32(2), nativeRuns.Load())

	retry := call("adversarial-foo-retry", resultMessage("foo"))
	assert.Equal(t, "1", retry.Headers.Get(HeaderUsageReplay))
	assert.Equal(t, mixed.Headers.Get(HeaderUsageReceipt), retry.Headers.Get(HeaderUsageReceipt))
	assert.Equal(t, int64(12), adversarialOutputTokens(t, retry))
	assert.Equal(t, int64(1), adversarialHostedRequests(t, retry))
	assert.JSONEq(t, string(mixed.Payload), string(retry.Payload))
	assert.Equal(t, int32(2), nativeRuns.Load(), "identical client retry must not regenerate")

	baseReplay := call("adversarial-base-before-bar", resultMessage(""))
	assert.Equal(t, "1", baseReplay.Headers.Get(HeaderUsageReplay))
	assert.NotEmpty(t, baseReplay.Headers.Get(HeaderUsageReceipt))
	assert.Equal(t, mixed.Headers.Get(HeaderUsageReceipt), baseReplay.Headers.Get(HeaderUsageReceipt), "the internal base snapshot must reference the real client request receipt, not an uncharged synthetic receipt")
	assert.Equal(t, int64(7), adversarialOutputTokens(t, baseReplay), "follow-up must not overwrite the original result snapshot")
	assert.Equal(t, int64(1), adversarialHostedRequests(t, baseReplay))
	assert.Contains(t, string(baseReplay.Payload), "RESULT_BASE")

	changed := call("adversarial-bar", resultMessage("bar"))
	waitUntilInactive(t, executor.manager, "adversarial-mixed")
	assert.Empty(t, changed.Headers.Get(HeaderUsageReplay), "different follow-up text is a new client generation")
	assert.NotEqual(t, mixed.Headers.Get(HeaderUsageReceipt), changed.Headers.Get(HeaderUsageReceipt))
	assert.NotEmpty(t, changed.Headers.Get(HeaderUsageReceipt))
	assert.Equal(t, int64(5), adversarialOutputTokens(t, changed), "already replayed result tokens must not be charged again")
	assert.Zero(t, adversarialHostedRequests(t, changed), "old hosted-work evidence must not be recharged on a new follow-up")
	assert.Contains(t, string(changed.Payload), "FOLLOWUP:bar")
	assert.Equal(t, int32(3), nativeRuns.Load())

	baseAfter := call("adversarial-base-after-bar", resultMessage(""))
	assert.Equal(t, "1", baseAfter.Headers.Get(HeaderUsageReplay))
	assert.Equal(t, baseReplay.Headers.Get(HeaderUsageReceipt), baseAfter.Headers.Get(HeaderUsageReceipt))
	assert.Equal(t, int64(7), adversarialOutputTokens(t, baseAfter))
	assert.Contains(t, string(baseAfter.Payload), "RESULT_BASE")
	assert.JSONEq(t, string(baseReplay.Payload), string(baseAfter.Payload))
	assert.Equal(t, int32(3), nativeRuns.Load())
}

func adversarialOutputTokens(t *testing.T, response cliproxyexecutor.Response) int64 {
	t.Helper()
	var payload struct {
		Usage struct {
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	require.NoError(t, jsonx.Unmarshal(response.Payload, &payload))
	return payload.Usage.OutputTokens
}

func adversarialHostedRequests(t *testing.T, response cliproxyexecutor.Response) int64 {
	t.Helper()
	var payload struct {
		Usage struct {
			HostedRequests int64 `json:"cursor_agent_v1_hosted_search_requests"`
		} `json:"usage"`
	}
	require.NoError(t, jsonx.Unmarshal(response.Payload, &payload))
	return payload.Usage.HostedRequests
}

// A covering set is not authorization to choose one of two live sessions.
// Seed durable owners through the existing StateStore API, then ask the public
// manager to resume; neither candidate may reach the provider.
func TestAdversarialCoveringResultsAcrossLiveOwnersAreAmbiguous(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("adversarial-owner")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	for _, suffix := range []string{"a", "b"} {
		claim, err := store.Claim("adversarial-owner-"+suffix, "conversation-"+suffix, fingerprint, ClaimOptions{
			ToolCatalogDigest: request.ToolCatalogDigest, ToolCatalog: request.Run.Tools,
		})
		require.NoError(t, err)
		require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-" + suffix, Name: "lookup"}}))
		require.NoError(t, store.Release(claim.Owner))
	}
	var providerRuns atomic.Int32
	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		providerRuns.Add(1)
		return fmt.Errorf("ambiguous request must not reach provider")
	}}, store, time.Second)
	require.NoError(t, err)
	output, err := manager.ResumeTools(request, []ToolResult{
		{ToolCallID: "tool-a", Content: "a"}, {ToolCallID: "tool-b", Content: "b"},
	})
	assert.Nil(t, output)
	assert.ErrorIs(t, err, ErrPendingAmbiguous)
	assert.Zero(t, providerRuns.Load())
}
