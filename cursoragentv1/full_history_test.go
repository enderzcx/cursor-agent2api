package cursoragentv1

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/stretchr/testify/require"
)

func TestStructuredHistoryPreservesToolFacts(t *testing.T) {
	var messages []any
	require.NoError(t, jsonx.Unmarshal([]byte(`[{"role":"user","content":"inspect"},{"role":"assistant","content":[{"type":"tool_use","id":"read1","name":"Read","input":{"path":"file"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"read1","content":"","is_error":true},{"type":"text","text":"explain the failure"}]}]`), &messages))
	history, err := structuredHistory(messages, true)
	require.NoError(t, err)
	require.Len(t, history, 3)
	require.JSONEq(t, `{"role":"assistant","content":[{"type":"tool-call","toolCallId":"read1","toolName":"Read","args":{"path":"file"}}]}`, string(history[1]))
	require.JSONEq(t, `{"role":"tool","id":"read1","content":[{"type":"tool-result","toolCallId":"read1","toolName":"Read","result":{"is_error":true,"content":""}}]}`, string(history[2]))
}

func TestStructuredHistoryPreservesCompletedHostedEvidence(t *testing.T) {
	for _, result := range []string{
		`{"type":"web_search_tool_result","tool_use_id":"hosted1","content":[{"type":"web_search_result","url":"https://example.com/source","title":"Source","encrypted_content":"opaque"}]}`,
		`{"type":"web_fetch_tool_result","tool_use_id":"hosted1","content":{"type":"web_fetch_result","url":"https://example.com/source","content":{"type":"document","source":{"type":"text","media_type":"text/plain","data":"retrieved body"}}}}`,
		`{"type":"web_search_tool_result","tool_use_id":"hosted1","content":{"type":"web_search_tool_result_error","error_code":"unavailable"}}`,
	} {
		var messages []any
		require.NoError(t, jsonx.Unmarshal([]byte(`[{"role":"assistant","content":[{"type":"server_tool_use","id":"hosted1","name":"web_search","input":{"query":"q"}},`+result+`]},{"role":"user","content":"recall"}]`), &messages))
		history, err := structuredHistory(messages, true)
		require.NoError(t, err)
		require.Len(t, history, 2)
		var original, converted map[string]any
		require.NoError(t, jsonx.Unmarshal([]byte(result), &original))
		require.NoError(t, jsonx.Unmarshal(history[1], &converted))
		part := converted["content"].([]any)[0].(map[string]any)
		// Plaintext evidence (url/title/document/error) is preserved exactly;
		// provider ciphertext such as encrypted_content is never replayed on
		// another provider and is replaced by an explicit note.
		require.Equal(t, withoutProviderCiphertext(original["content"]), part["result"], "hosted evidence must not become a fabricated success or lose document/source fields")
		require.NotContains(t, string(history[1]), `"encrypted_content"`)
		if strings.Contains(result, "https://example.com/source") {
			require.Contains(t, string(history[1]), `https://example.com/source`)
		}
		require.Equal(t, "hosted1", part["toolCallId"])
	}
}

func TestStructuredHistoryPreservesCitationExcerpts(t *testing.T) {
	var messages []any
	require.NoError(t, jsonx.Unmarshal([]byte(`[{"role":"assistant","content":[{"type":"text","text":"Found it","citations":[{"type":"web_search_result_location","url":"https://example.com","title":"Source","cited_text":"unique excerpt"}]}]},{"role":"user","content":"recall"}]`), &messages))
	history, err := structuredHistory(messages, true)
	require.NoError(t, err)
	require.Contains(t, string(history[0]), "unique excerpt")
	require.Contains(t, string(history[0]), "https://example.com")
	require.NotContains(t, string(history[0]), "tool-call")
}

func TestAbsentMetadataIsNotLiteralNilIdentity(t *testing.T) {
	require.Empty(t, metadataString(map[string]any{"session": "x"}, "beefapi_request_id"))
	require.Empty(t, metadataString(map[string]any{"beefapi_request_id": nil}, "beefapi_request_id"))
	require.Equal(t, "0", metadataString(map[string]any{"value": 0}, "value"))
}

func TestHistoryRequestKeyPreservesAgentV1DigestAndSeparatesSand(t *testing.T) {
	base := translatedTurn{publicModel: "claude-sonnet-4-6", managed: ManagedRequest{Run: RunRequest{Model: "claude-4.6-sonnet-medium", UserText: "hello"}}}
	legacy, err := historyRequestKey(base, 7, false)
	require.NoError(t, err)
	agent := base
	agent.managed.Run.RuntimeProfile = runtimeAgentV1
	agentKey, err := historyRequestKey(agent, 7, false)
	require.NoError(t, err)
	require.Equal(t, legacy, agentKey)
	sand := base
	sand.managed.Run.RuntimeProfile = runtimeSand
	sandKey, err := historyRequestKey(sand, 7, false)
	require.NoError(t, err)
	require.NotEqual(t, legacy, sandKey)
}

func TestMessagesFullHistoryMultiToolReplayAndIdentity(t *testing.T) {
	var runs atomic.Int32
	engine := &scriptedEngine{run: func(_ context.Context, r RunRequest, emit func(Event)) error {
		runs.Add(1)
		require.False(t, r.Resume)
		require.Empty(t, r.InitialCheckpoint)
		require.NotEmpty(t, r.HistoryRequestKey)
		history := string(bytes.Join(r.HistoryMessages, nil))
		require.NotContains(t, history, "BEEFAPI_TOOL_EVENT")
		if r.UserText == "explain instead" {
			require.NotContains(t, history, "explain instead")
			emit(Event{Type: EventText, Text: "explanation"})
		} else if len(r.Tools) == 0 {
			emit(Event{Type: EventText, Text: "no tools"})
		} else if bytes.Count([]byte(history), []byte(`"type":"tool-result"`)) >= 2 {
			emit(Event{Type: EventText, Text: "both files checked"})
		} else {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "native-read", Name: "Read", Arguments: `{"path":"file"}`}})
		}
		emit(Event{Type: EventUsage, Tokens: 5})
		emit(Event{Type: EventDone})
		return nil
	}}
	executor := testExecutor(t, engine)
	tools := []any{map[string]any{"name": "Read", "input_schema": map[string]any{"type": "object"}}}
	messages := []any{map[string]any{"role": "user", "content": "inspect two files"}}
	requestBody := func(extra string, none bool) string {
		copyMessages := append([]any(nil), messages...)
		if extra != "" {
			last := copyMessages[len(copyMessages)-1].(map[string]any)
			parts := append([]any(nil), last["content"].([]any)...)
			parts = append(parts, map[string]any{"type": "text", "text": extra})
			copyMessages[len(copyMessages)-1] = map[string]any{"role": "user", "content": parts}
		}
		body := map[string]any{"model": "claude-sonnet-4-6", "messages": copyMessages, "tools": tools}
		if none {
			body["tool_choice"] = map[string]any{"type": "none"}
		}
		raw, err := jsonx.Marshal(body)
		require.NoError(t, err)
		return string(raw)
	}
	execute := func(id, body string, token int) (map[string]any, string, bool) {
		req := claudeExecutorRequest(id, body)
		req.Metadata["beefapi_request_id"] = id
		req.Metadata["beefapi_token_id"] = token
		response, err := executor.Execute(context.Background(), testExecutorAuth(), req, claudeExecutorOptions(false))
		require.NoError(t, err)
		var reply map[string]any
		require.NoError(t, jsonx.Unmarshal(response.Payload, &reply))
		return reply, response.Headers.Get(HeaderUsageReceipt), response.Headers.Get(HeaderUsageReplay) == "1"
	}
	appendResult := func(reply map[string]any, text string) {
		blocks := reply["content"].([]any)
		id := ""
		for _, raw := range blocks {
			b := raw.(map[string]any)
			if b["type"] == "tool_use" {
				id = stringValue(b["id"])
			}
		}
		require.NotEmpty(t, id)
		messages = append(messages, map[string]any{"role": "assistant", "content": blocks}, map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": id, "content": text}}})
	}
	first, receipt1, _ := execute("http1", requestBody("", false), 1)
	require.Equal(t, false, first["usage"].(map[string]any)["cursor_agent_v1_usage_pending"])
	appendResult(first, "")
	secondBody := requestBody("", false)
	second, receipt2, _ := execute("http2", secondBody, 1)
	require.NotEqual(t, receipt1, receipt2)
	appendResult(second, "second file")
	third, receipt3, _ := execute("http3", requestBody("", false), 1)
	require.Equal(t, "end_turn", third["stop_reason"])
	require.NotEqual(t, receipt2, receipt3)
	replay, receiptReplay, replayed := execute("http-retry", secondBody, 1)
	require.True(t, replayed)
	require.Equal(t, receipt2, receiptReplay)
	require.Equal(t, second, replay)
	require.EqualValues(t, 3, runs.Load())
	_, tokenReceipt, tokenReplay := execute("token-change", secondBody, 2)
	require.False(t, tokenReplay)
	require.NotEqual(t, receipt2, tokenReceipt)
	mixed, _, _ := execute("mixed", requestBody("explain instead", false), 1)
	require.Equal(t, "end_turn", mixed["stop_reason"])
	none, _, _ := execute("none", requestBody("", true), 1)
	require.Equal(t, "end_turn", none["stop_reason"])
}

func TestStructuredHistoryRejectsUnknownAndUnpaired(t *testing.T) {
	for _, body := range []string{
		`[{"role":"user","content":[{"type":"tool_result","tool_use_id":"missing","content":"x"}]}]`,
		`[{"role":"user","content":[{"type":"document","data":"x"}]}]`,
		`[{"role":"assistant","content":[{"type":"server_tool_use","id":"srv","name":"web_search","input":{}}]}]`,
	} {
		var messages []any
		require.NoError(t, jsonx.Unmarshal([]byte(body), &messages))
		_, err := structuredHistory(messages, false)
		require.Error(t, err)
	}
}

func TestStructuredHistoryClosesUnansweredCallerToolUse(t *testing.T) {
	// Interrupted turn: the client moved on to a new user message without a
	// tool_result. Anthropic direct would 400; Kiro tolerates it. Close the call
	// with an error placeholder right after its assistant message so pairing
	// stays intact, and do the same for a dangling call at the very end.
	body := `[
		{"role":"user","content":"read it"},
		{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"path":"a"}},{"type":"tool_use","id":"t2","name":"Read","input":{"path":"b"}}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":"b body"},{"type":"text","text":"never mind, do this instead"}]},
		{"role":"assistant","content":[{"type":"tool_use","id":"t3","name":"Bash","input":{"cmd":"ls"}}]}
	]`
	var messages []any
	require.NoError(t, jsonx.Unmarshal([]byte(body), &messages))
	history, err := structuredHistory(messages, false)
	require.NoError(t, err)
	joined := ""
	for _, message := range history {
		joined += string(message) + "\n"
	}
	require.Contains(t, joined, `"toolCallId":"t1"`)
	require.Contains(t, joined, orphanToolResultText)
	require.Contains(t, joined, `"toolCallId":"t3"`)
	firstPlaceholder := strings.Index(joined, orphanToolResultText)
	nextUser := strings.Index(joined, "never mind")
	require.Greater(t, firstPlaceholder, 0)
	require.Less(t, firstPlaceholder, nextUser)
	require.Contains(t, joined, `"b body"`)
	require.Equal(t, 2, strings.Count(joined, orphanToolResultText))
}

func TestHistoryRequestReplaySurvivesOwnerExitAndRestart(t *testing.T) {
	now := time.Now()
	root := t.TempDir()
	store, err := newFileStateStore(root, time.Hour, func() time.Time { return now })
	require.NoError(t, err)
	var runs atomic.Int32
	engine := &scriptedEngine{run: func(_ context.Context, r RunRequest, emit func(Event)) error {
		runs.Add(1)
		require.False(t, r.Resume)
		require.Empty(t, r.InitialCheckpoint)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "native-read", Name: "Read", Arguments: `{"path":"file"}`}})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("http-1")
	request.Run.HistoryRequestKey = "history:one"
	first, err := manager.StartHistory(request)
	require.NoError(t, err)
	firstEvents := collectManagedEvents(first.Events)
	require.NoError(t, <-first.Result)
	require.Equal(t, []EventType{EventToolCall, EventDone}, eventTypes(firstEvents))
	now = now.Add(3 * time.Minute)
	restartedStore, err := newFileStateStore(root, time.Hour, func() time.Time { return now })
	require.NoError(t, err)
	restarted, err := newSessionManager(engine, restartedStore, time.Second)
	require.NoError(t, err)
	request.SessionID = "different-http-id"
	request.BillingRunID = "different-reservation"
	replay, err := restarted.StartHistory(request)
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.Equal(t, first.BillingRunID, replay.BillingRunID)
	require.Equal(t, first.RequestSegmentID, replay.RequestSegmentID)
	require.Equal(t, firstEvents, collectManagedEvents(replay.Events))
	require.NoError(t, <-replay.Result)
	require.EqualValues(t, 1, runs.Load())
	// A genuinely different continuation executes and funds a distinct request.
	request.Run.HistoryRequestKey = "history:next"
	next, err := restarted.StartHistory(request)
	require.NoError(t, err)
	collectManagedEvents(next.Events)
	require.NoError(t, <-next.Result)
	require.NotEqual(t, first.BillingRunID, next.BillingRunID)
	require.EqualValues(t, 2, runs.Load())
}

func TestHistoryConcurrentIdenticalRequestsRunOnce(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	release := make(chan struct{})
	started := make(chan struct{})
	var runs atomic.Int32
	engine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		if runs.Add(1) == 1 {
			close(started)
		}
		<-release
		emit(Event{Type: EventText, Text: "answer"})
		emit(Event{Type: EventDone})
		return nil
	}}
	firstManager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	secondManager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("concurrent")
	request.Run.HistoryRequestKey = "history:concurrent"
	first, err := firstManager.StartHistory(request)
	require.NoError(t, err)
	<-started
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		second, err := secondManager.StartHistory(request)
		require.NoError(t, err)
		if err == nil {
			require.True(t, second.Replay)
			collectManagedEvents(second.Events)
			require.NoError(t, <-second.Result)
		}
	}()
	close(release)
	collectManagedEvents(first.Events)
	require.NoError(t, <-first.Result)
	wg.Wait()
	require.EqualValues(t, 1, runs.Load())
}

func TestHistoryDoesNotCommitEmptySuccess(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	req := managedTestRequest("empty")
	req.Run.HistoryRequestKey = "history:empty"
	output, err := manager.StartHistory(req)
	require.NoError(t, err)
	require.Empty(t, collectManagedEvents(output.Events))
	require.Error(t, <-output.Result)
	fp, err := CredentialFingerprint(req.CredentialScope, req.AccountKey)
	require.NoError(t, err)
	_, err = store.LookupHistory(fp, req.Run.HistoryRequestKey)
	require.ErrorIs(t, err, ErrSessionNotPending)
}

type historySaveFailure struct{ StateStore }

func (s *historySaveFailure) SaveHistory(OwnerLease, string, []Event) error {
	return errors.New("injected history persistence failure")
}

func TestHistorySnapshotFailureNeverPublishesSuccessfulTerminal(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventText, Text: "partial answer"})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, &historySaveFailure{store}, time.Second)
	require.NoError(t, err)
	req := managedTestRequest("save-failure")
	req.Run.HistoryRequestKey = "history:save-failure"
	output, err := manager.StartHistory(req)
	require.NoError(t, err)
	events := collectManagedEvents(output.Events)
	require.NotContains(t, eventTypes(events), EventDone)
	require.ErrorContains(t, <-output.Result, "persistence failure")
	fp, err := CredentialFingerprint(req.CredentialScope, req.AccountKey)
	require.NoError(t, err)
	_, err = store.LookupHistory(fp, req.Run.HistoryRequestKey)
	require.ErrorIs(t, err, ErrSessionNotPending)
}

func TestHistoryExpiredReplayCreatesNewGeneration(t *testing.T) {
	now := time.Now()
	store, err := newFileStateStore(t.TempDir(), time.Hour, func() time.Time { return now })
	require.NoError(t, err)
	var runs atomic.Int32
	engine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventText, Text: "answer"})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	req := managedTestRequest("expiry")
	req.Run.HistoryRequestKey = "history:expiry"
	first, err := manager.StartHistory(req)
	require.NoError(t, err)
	collectManagedEvents(first.Events)
	require.NoError(t, <-first.Result)
	now = now.Add(6 * time.Minute)
	req.BillingRunID = "new-http-id"
	next, err := manager.StartHistory(req)
	require.NoError(t, err)
	collectManagedEvents(next.Events)
	require.NoError(t, <-next.Result)
	require.False(t, next.Replay)
	require.NotEqual(t, first.BillingRunID, next.BillingRunID)
	require.EqualValues(t, 2, runs.Load())
}
