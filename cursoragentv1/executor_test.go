package cursoragentv1

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestExecutorMessagesNonStreamText(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
		require.Equal(t, "hello", request.UserText)
		require.Equal(t, "be concise", request.SystemText)
		emit(Event{Type: EventText, Text: "world"})
		emit(Event{Type: EventUsage, Tokens: 7})
		emit(Event{Type: EventDone})
		return nil
	}})
	response, err := executor.Execute(context.Background(), testExecutorAuth(), claudeExecutorRequest("session-text", `{
		"model":"claude-sonnet-4-6","system":"be concise","messages":[{"role":"user","content":"hello"}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Regexp(t, `^cursor-agent-v1:turn:[a-f0-9]{64}$`, response.Headers.Get(HeaderUsageReceipt))
	require.JSONEq(t, `{
		"id":"ignored","type":"message","role":"assistant","content":[{"type":"text","text":"world"}],
		"model":"claude-sonnet-4-6","stop_reason":"end_turn","stop_sequence":null,
		"usage":{"input_tokens":6,"output_tokens":1,"cursor_agent_v1_output_token_delta_observed":true,"cursor_agent_v1_output_token_delta":7,"cursor_agent_v1_usage_pending":false}
	}`, replaceJSONID(string(response.Payload)))
}

func TestTranslateClaudeCodeTrailingSystemContextKeepsActiveUserAndSystem(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5-high",
		"system":[
			{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.233; cc_entrypoint=sdk-cli;"},
			{"type":"text","text":"base system"}
		],
		"messages":[
			{"role":"user","content":[{"type":"text","text":"hello"}]},
			{"role":"system","content":[{"type":"text","text":"Available agent types and skills"}]}
		]
	}`)
	req := claudeExecutorRequest("claude-code-trailing-system", string(payload))
	req.Model = "claude-opus-5-high"

	turn, err := translateExecutionRequest(
		req,
		cliproxyexecutor.Options{
			SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload,
		},
		"tenant:1:user:7:channel:6401",
		"account-1",
		"https://agent.example",
		"access",
		"cli-test",
	)
	require.NoError(t, err)
	require.Equal(t, "hello", turn.managed.Run.UserText)
	require.Contains(t, turn.managed.Run.SystemText, "base system")
	require.Contains(t, turn.managed.Run.SystemText, "Available agent types and skills")
	require.Empty(t, turn.managed.Run.HistoryMessages)
}

func TestExecutorStreamsClaudeCodeTrailingSystemRequest(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
		require.Equal(t, "hello", request.UserText)
		require.Contains(t, request.SystemText, "Available agent types and skills")
		emit(Event{Type: EventText, Text: "world"})
		emit(Event{Type: EventDone})
		return nil
	}})
	payload := `{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.233; cc_entrypoint=sdk-cli;",
		"messages":[
			{"role":"user","content":"hello"},
			{"role":"system","content":"Available agent types and skills"}
		],
		"stream":true
	}`
	req := claudeExecutorRequest("claude-code-trailing-system-stream", payload)
	req.Model = "claude-opus-5-high"
	stream, err := executor.ExecuteStream(
		context.Background(),
		testExecutorAuth(),
		req,
		cliproxyexecutor.Options{Stream: true, SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: req.Payload},
	)
	require.NoError(t, err)
	var output strings.Builder
	for chunk := range stream.Chunks {
		require.NoError(t, chunk.Err)
		output.Write(chunk.Payload)
	}
	require.Contains(t, output.String(), `"text":"world"`)
	require.Contains(t, output.String(), `"type":"message_stop"`)
}

func TestTranslateTrailingContextAlignsWithKiro(t *testing.T) {
	for _, test := range []struct {
		name         string
		payload      string
		wantUserText string
		wantSystem   string
		wantHistory  int
	}{
		{
			name: "ordinary trailing system is folded",
			payload: `{
				"model":"claude-opus-5-high","system":"ordinary client",
				"messages":[{"role":"user","content":"hello"},{"role":"system","content":"dynamic"}]
			}`,
			wantUserText: "hello",
			wantSystem:   "dynamic",
			wantHistory:  0,
		},
		{
			name: "non text trailing system is skipped",
			payload: `{
				"model":"claude-opus-5-high",
				"system":"x-anthropic-billing-header: cc_version=2.1.233; cc_entrypoint=sdk-cli;",
				"messages":[{"role":"user","content":"hello"},{"role":"system","content":[{"type":"image"}]}]
			}`,
			wantUserText: "hello",
			wantHistory:  0,
		},
		{
			name: "trailing assistant continues",
			payload: `{
				"model":"claude-opus-5-high","system":"You are Claude Code",
				"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]
			}`,
			wantUserText: continueTurnUserText,
			wantHistory:  2,
		},
		{
			name: "trailing system after assistant continues",
			payload: `{
				"model":"claude-opus-5-high","system":"You are Claude Code",
				"messages":[
					{"role":"user","content":"hello"},
					{"role":"assistant","content":"hi"},
					{"role":"system","content":"Available agent types and skills"}
				]
			}`,
			wantUserText: continueTurnUserText,
			wantSystem:   "Available agent types and skills",
			wantHistory:  2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := claudeExecutorRequest("trailing-context-"+test.name, test.payload)
			turn, err := translateExecutionRequest(
				req,
				cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: req.Payload},
				"tenant:1:user:7:channel:6401",
				"account-1",
				"https://agent.example",
				"access",
				"cli-test",
			)
			require.NoError(t, err)
			require.Equal(t, test.wantUserText, turn.managed.Run.UserText)
			require.Len(t, turn.managed.Run.HistoryMessages, test.wantHistory)
			if test.wantSystem != "" {
				require.Contains(t, turn.managed.Run.SystemText, test.wantSystem)
			}
		})
	}
}

func TestTranslateAcceptsClaudeCodeScaleToolCatalog(t *testing.T) {
	tools := make([]any, 0, 300)
	for i := 0; i < 300; i++ {
		tools = append(tools, map[string]any{
			"name":         fmt.Sprintf("tool_%03d", i),
			"description":  "caller tool",
			"input_schema": map[string]any{"type": "object"},
		})
	}
	root := map[string]any{
		"model":    "claude-sonnet-4-6",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools":    tools,
	}
	payload, err := jsonx.Marshal(root)
	require.NoError(t, err)
	turn, err := translateExecutionRequest(
		claudeExecutorRequest("large-catalog", string(payload)),
		cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, ResponseFormat: sdktranslator.FormatClaude, OriginalRequest: payload},
		"tenant:1:user:7:channel:6401",
		"account-1",
		"https://agent.example",
		"access",
		"cli-test",
	)
	require.NoError(t, err)
	require.Len(t, turn.managed.Run.Tools, 300)
}

func TestUsageReceiptIDIsStableForOneAgentSession(t *testing.T) {
	turn := translatedTurn{managed: ManagedRequest{SessionID: "session-1", BillingRunID: "run-1", CredentialScope: "tenant:1:user:7:channel:6401", AccountKey: "account-1"}}
	first := usageReceiptID(turn)
	require.Equal(t, first, usageReceiptID(turn))
	require.Regexp(t, `^cursor-agent-v1:[a-f0-9]{64}$`, first)
	turn.managed.SessionID = "session-2"
	require.Equal(t, first, usageReceiptID(turn), "conversation/session changes do not redefine one billing run")
	turn.managed.BillingRunID = "run-2"
	require.NotEqual(t, first, usageReceiptID(turn))
}

func TestExecutorRetainsHistoryReplayAndResponsesOwner(t *testing.T) {
	var historyKey string
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, req RunRequest, emit func(Event)) error {
		if req.HistoryRequestKey != "" {
			historyKey = req.HistoryRequestKey
		}
		emit(Event{Type: EventText, Text: "done"})
		emit(Event{Type: EventDone})
		return nil
	}})
	store := executor.manager.store.(*FileStateStore)
	_, err := executor.Execute(context.Background(), testExecutorAuth(), claudeExecutorRequest("chat-complete", `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.NotEmpty(t, historyKey)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	chatPath, _ := store.paths(historySessionID(fingerprint, historyKey))
	snapshot, err := readState(chatPath)
	require.NoError(t, err)
	require.True(t, snapshot.EphemeralComplete)
	require.Empty(t, snapshot.PendingTools)
	require.Empty(t, snapshot.Checkpoint)
	replay, err := store.LookupHistory(fingerprint, historyKey)
	require.NoError(t, err)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(replay.Events))

	responsesBody := `{"model":"claude-sonnet-4-6","input":"hi"}`
	responses := cliproxyexecutor.Request{Model: "claude-sonnet-4-6", Payload: []byte(responsesBody), Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "responses-complete"}}
	_, err = executor.Execute(context.Background(), testExecutorAuth(), responses, cliproxyexecutor.Options{SourceFormat: responses.Format, ResponseFormat: responses.Format, OriginalRequest: responses.Payload})
	require.NoError(t, err)
	responsesPath, _ := store.paths("responses-complete")
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(responsesPath)
		return statErr == nil
	}, time.Second, 10*time.Millisecond)
}

func TestAnchoredExecutorParallelToolsParkAndResumeWithoutSessionID(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		require.Len(t, request.Tools, 2)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-a", Name: "alpha", Arguments: `{"value":1}`}})
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-b", Name: "beta", Arguments: `{"value":2}`}})
		emit(Event{Type: EventTurnEnded})
		for index := 0; index < 2; index++ {
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		emit(Event{Type: EventText, Text: "resumed"})
		emit(Event{Type: EventDone})
		return nil
	}})
	tools := `[
		{"name":"alpha","description":"A","input_schema":{"type":"object","properties":{"value":{"type":"integer"}}}},
		{"name":"beta","description":"B","input_schema":{"type":"object","properties":{"value":{"type":"integer"}}}}
	]`
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("session-tools", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"use tools"}],"tools":`+tools+`
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(first.Payload), `"stop_reason":"tool_use"`)
	publicIDs := agentV1ToolCallIDs(first.Payload)
	require.Len(t, publicIDs, 2)

	continuation := `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[
			{"type":"tool_result","tool_use_id":"call-a","content":"one"},
			{"type":"tool_result","tool_use_id":"call-b","content":"two"}
		]}]
	}`
	continuation = strings.Replace(continuation, "call-a", publicIDs[0], 1)
	continuation = strings.Replace(continuation, "call-b", publicIDs[1], 1)
	second, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("new-client-session-is-ignored", continuation), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Equal(t, first.Headers.Get(HeaderUsageReceipt), second.Headers.Get(HeaderUsageReceipt))
	require.Contains(t, string(second.Payload), `"text":"resumed"`)
	require.Contains(t, string(second.Payload), `"stop_reason":"end_turn"`)
}

func TestMessagesToolResultWithTrailingSystemRebuilds(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		if len(request.HistoryMessages) == 0 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-read", Name: "Read", Arguments: `{"file_path":"marker.txt"}`}})
		} else {
			require.False(t, request.Resume)
			require.Contains(t, string(bytes.Join(request.HistoryMessages, nil)), `"result":"marker"`)
			require.Equal(t, historyContinuationText, request.UserText)
			emit(Event{Type: EventText, Text: "resumed"})
		}
		emit(Event{Type: EventDone})
		return nil
	}})
	firstPayload := `{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.233; cc_entrypoint=sdk-cli;",
		"messages":[{"role":"user","content":"read marker"}],
		"tools":[{"name":"Read","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}]
	}`
	firstRequest := claudeExecutorRequest("claude-code-tool-resume", firstPayload)
	firstRequest.Model = "claude-opus-5-high"
	first, err := executor.Execute(context.Background(), testExecutorAuth(), firstRequest, claudeExecutorOptions(false))
	require.NoError(t, err)
	publicIDs := agentV1ToolCallIDs(first.Payload)
	require.Len(t, publicIDs, 1)

	continuation := `{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.233; cc_entrypoint=sdk-cli;",
		"messages":[
			{"role":"user","content":"read marker"},` + string(first.Payload) + `,
			{"role":"user","content":[
				{"type":"tool_result","tool_use_id":"PUBLIC_ID","content":"marker"},
				{"type":"text","text":"[Your previous response had no visible output. Please continue and produce a user-visible response.]"}
			]},
			{"role":"system","content":"Available agent types and skills"}
		]
	}`
	continuation = strings.Replace(continuation, "PUBLIC_ID", publicIDs[0], 1)
	secondRequest := claudeExecutorRequest("new-client-session-is-ignored", continuation)
	secondRequest.Model = "claude-opus-5-high"
	second, err := executor.Execute(context.Background(), testExecutorAuth(), secondRequest, claudeExecutorOptions(false))
	require.NoError(t, err)
	require.NotEqual(t, first.Headers.Get(HeaderUsageReceipt), second.Headers.Get(HeaderUsageReceipt))
	require.Contains(t, string(second.Payload), `"text":"resumed"`)
}

func TestTranslateClaudeCodeNativeSearchPolicyUsesHostedTools(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=cli;",
		"messages":[{"role":"user","content":"search"}],
		"tools":[
			{"name":"WebSearch","input_schema":{"type":"object"}},
			{"name":"WebFetch","input_schema":{"type":"object"}},
			{"name":"Bash","input_schema":{"type":"object"}}
		]
	}`)
	req := claudeExecutorRequest("claude-code-native-search", string(payload))
	req.Model = "claude-opus-5-high"
	headers := make(http.Header)
	headers.Set(headerCursorAgentV1NativeWebSearch, "true")
	turn, err := translateExecutionRequest(
		req,
		cliproxyexecutor.Options{
			SourceFormat:    req.Format,
			ResponseFormat:  req.Format,
			OriginalRequest: payload,
			Headers:         headers,
		},
		"tenant:1:user:7:channel:6401",
		"account-1",
		"https://agent.example",
		"access",
		"cli-test",
	)
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.AllowHostedFetch)
	require.Len(t, turn.managed.Run.Tools, 2)
	require.Equal(t, "WebSearch", turn.managed.Run.Tools[0].Name)
	require.Equal(t, "Bash", turn.managed.Run.Tools[1].Name)
}

func TestTranslateNativeSearchPolicyDoesNotRewriteUnmarkedClaudeClients(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5-high",
		"system":"ordinary Anthropic client",
		"messages":[{"role":"user","content":"search"}],
		"tools":[{"name":"WebSearch","input_schema":{"type":"object"}}]
	}`)
	req := claudeExecutorRequest("ordinary-claude-client", string(payload))
	headers := make(http.Header)
	headers.Set(headerCursorAgentV1NativeWebSearch, "true")
	turn, err := translateExecutionRequest(
		req,
		cliproxyexecutor.Options{
			SourceFormat: req.Format, ResponseFormat: req.Format,
			OriginalRequest: payload, Headers: headers,
		},
		"tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test",
	)
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.Len(t, turn.managed.Run.Tools, 1)
	require.Equal(t, "WebSearch", turn.managed.Run.Tools[0].Name)
}

func TestTranslateClaudeCodeNativeSearchPolicyRejectsForcedSearch(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=cli;",
		"messages":[{"role":"user","content":"search"}],
		"tool_choice":{"type":"tool","name":"WebSearch"},
		"tools":[
			{"name":"WebSearch","input_schema":{"type":"object"}},
			{"name":"WebFetch","input_schema":{"type":"object"}}
		]
	}`)
	req := claudeExecutorRequest("claude-code-forced-native-search", string(payload))
	headers := make(http.Header)
	headers.Set(headerCursorAgentV1NativeWebSearch, "true")
	turn, err := translateExecutionRequest(
		req,
		cliproxyexecutor.Options{
			SourceFormat: req.Format, ResponseFormat: req.Format,
			OriginalRequest: payload, Headers: headers,
		},
		"tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test",
	)
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.RequireToolCall)
	require.Len(t, turn.managed.Run.Tools, 1)
	require.Equal(t, "WebSearch", turn.managed.Run.Tools[0].Name)
}

func TestTranslateClaudeCodeNativeSearchPolicyRejectsPartialToolPair(t *testing.T) {
	t.Run("WebSearch", func(t *testing.T) {
		payload := []byte(`{
			"model":"claude-opus-5-high",
			"system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=cli;",
			"messages":[{"role":"user","content":"search"}],
			"tools":[{"name":"WebSearch","input_schema":{"type":"object"}}]
		}`)
		req := claudeExecutorRequest("claude-code-partial-native-search", string(payload))
		headers := make(http.Header)
		headers.Set(headerCursorAgentV1NativeWebSearch, "true")
		turn, err := translateExecutionRequest(
			req,
			cliproxyexecutor.Options{
				SourceFormat: req.Format, ResponseFormat: req.Format,
				OriginalRequest: payload, Headers: headers,
			},
			"tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test",
		)
		require.NoError(t, err)
		require.False(t, turn.managed.Run.AllowHostedSearch)
		require.Len(t, turn.managed.Run.Tools, 1)
		require.Equal(t, "WebSearch", turn.managed.Run.Tools[0].Name)
	})
	t.Run("WebFetch", func(t *testing.T) {
		payload := []byte(`{
			"model":"claude-opus-5-high",
			"system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=cli;",
			"messages":[{"role":"user","content":"search"}],
			"tools":[{"name":"WebFetch","input_schema":{"type":"object"}}]
		}`)
		req := claudeExecutorRequest("claude-code-partial-native-fetch", string(payload))
		headers := make(http.Header)
		headers.Set(headerCursorAgentV1NativeWebSearch, "true")
		turn, err := translateExecutionRequest(
			req,
			cliproxyexecutor.Options{
				SourceFormat: req.Format, ResponseFormat: req.Format,
				OriginalRequest: payload, Headers: headers,
			},
			"tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test",
		)
		require.NoError(t, err)
		require.False(t, turn.managed.Run.AllowHostedSearch)
		require.True(t, turn.managed.Run.AllowHostedFetch)
		require.Empty(t, turn.managed.Run.Tools)
	})
}

func TestTranslateClaudeCodeNativeSearchPolicyNoneDisablesPartialToolPair(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=cli;",
		"messages":[{"role":"user","content":"do not search"}],
		"tool_choice":{"type":"none"},
		"tools":[{"name":"WebSearch","input_schema":{"type":"object"}}]
	}`)
	req := claudeExecutorRequest("claude-code-disabled-partial-native-search", string(payload))
	headers := make(http.Header)
	headers.Set(headerCursorAgentV1NativeWebSearch, "true")
	turn, err := translateExecutionRequest(
		req,
		cliproxyexecutor.Options{
			SourceFormat: req.Format, ResponseFormat: req.Format,
			OriginalRequest: payload, Headers: headers,
		},
		"tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test",
	)
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.Empty(t, turn.managed.Run.Tools)
}

func TestClaudeProjectionReportsHostedSearchUsage(t *testing.T) {
	projection := &claudeProjection{model: "claude-opus-5", messageID: "msg-search", responseFormat: sdktranslator.FormatClaude}
	_, _, _, err := projection.handle(Event{Type: EventHostedSearchCall})
	require.NoError(t, err)
	_, terminal, _, err := projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	require.True(t, terminal)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.JSONEq(t, `{
		"id":"msg-search","type":"message","role":"assistant","content":[],"model":"claude-opus-5",
		"stop_reason":"end_turn","stop_sequence":null,
		"usage":{"input_tokens":0,"output_tokens":0,"cursor_agent_v1_hosted_search_requests":1}
	}`, string(payload))
	require.Contains(t, projection.claudeSSE.String(), `"cursor_agent_v1_hosted_search_requests":1`)
}

func TestResponsesNextUserActionRotatesBillingReceipt(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventText, Text: "done"})
		emit(Event{Type: EventDone})
		return nil
	}})
	request := func(requestID, payload string) cliproxyexecutor.Request {
		return cliproxyexecutor.Request{
			Model: "glm-5.2", Payload: []byte(payload), Format: sdktranslator.FormatOpenAIResponse,
			Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "responses-session", "beefapi_request_id": requestID},
		}
	}
	firstRequest := request("request-1", `{"model":"glm-5.2","input":"one"}`)
	first, err := executor.Execute(context.Background(), testExecutorAuth(), firstRequest, cliproxyexecutor.Options{SourceFormat: firstRequest.Format, ResponseFormat: firstRequest.Format, OriginalRequest: firstRequest.Payload})
	require.NoError(t, err)
	secondRequest := request("request-2", `{"model":"glm-5.2","previous_response_id":"responses-session","input":"two"}`)
	second, err := executor.Execute(context.Background(), testExecutorAuth(), secondRequest, cliproxyexecutor.Options{SourceFormat: secondRequest.Format, ResponseFormat: secondRequest.Format, OriginalRequest: secondRequest.Payload})
	require.NoError(t, err)
	require.NotEqual(t, first.Headers.Get(HeaderUsageReceipt), second.Headers.Get(HeaderUsageReceipt))
}

func TestExecutorStreamReturnsPreSemanticUnauthorizedSynchronously(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		return &httpStatusError{code: http.StatusUnauthorized, text: "expired"}
	}})
	_, err := executor.ExecuteStream(context.Background(), testExecutorAuth(), claudeExecutorRequest("session-401", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hello"}]
	}`), claudeExecutorOptions(true))
	var status interface{ StatusCode() int }
	require.ErrorAs(t, err, &status)
	require.Equal(t, http.StatusUnauthorized, status.StatusCode())
}

func TestExecutorStreamKeepsPostSemanticUnauthorizedTerminal(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventText, Text: "committed"})
		return &httpStatusError{code: http.StatusUnauthorized, text: "late-expired"}
	}})
	stream, err := executor.ExecuteStream(context.Background(), testExecutorAuth(), claudeExecutorRequest("session-late-401", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hello"}]
	}`), claudeExecutorOptions(true))
	require.NoError(t, err, "the first semantic payload commits the existing run")
	var sawPayload bool
	var terminal error
	for chunk := range stream.Chunks {
		if len(chunk.Payload) > 0 {
			sawPayload = true
		}
		if chunk.Err != nil {
			terminal = chunk.Err
		}
	}
	require.True(t, sawPayload)
	var status interface{ StatusCode() int }
	require.ErrorAs(t, terminal, &status)
	require.Equal(t, http.StatusUnauthorized, status.StatusCode())
}

func TestExecutorNonStreamDoesNotExposeRetryable401AfterSemanticOutput(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventText, Text: "internally-produced"})
		return &httpStatusError{code: http.StatusUnauthorized, text: "late-expired"}
	}})
	_, err := executor.Execute(context.Background(), testExecutorAuth(), claudeExecutorRequest("session-nonstream-late-401", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hello"}]
	}`), claudeExecutorOptions(false))
	var status interface{ StatusCode() int }
	require.ErrorAs(t, err, &status)
	require.Equal(t, http.StatusBadGateway, status.StatusCode())
}

func TestExecutorStreamingLargeParallelBatchReturnsBeforeDrain(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, _ RunRequest, emit func(Event)) error {
		for index := 0; index < maxPendingTools; index++ {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: fmt.Sprintf("call-%02d", index), Name: "lookup", Arguments: `{}`}})
		}
		emit(Event{Type: EventTurnEnded})
		<-ctx.Done()
		return ctx.Err()
	}})
	pendingSaved := make(chan struct{})
	executor.manager.store = &pendingSavedStore{StateStore: executor.manager.store, saved: pendingSaved}
	// The batch persists 64 indexed tools. The behavioral deadline starts
	// after that I/O, rather than mistaking fsync latency for downstream drain.
	executor.manager.runTimeout = 30 * time.Second
	cancelRequest := ManagedRequest{SessionID: "session-large-batch", CredentialScope: "tenant:1:user:7:channel:6401", AccountKey: "account-1"}
	t.Cleanup(func() { _ = executor.manager.Cancel(cancelRequest) })
	started := time.Now()
	result := make(chan *cliproxyexecutor.StreamResult, 1)
	errors := make(chan error, 1)
	go func() {
		stream, err := executor.ExecuteStream(context.Background(), testExecutorAuth(), claudeExecutorRequest("session-large-batch", `{
			"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
			"tools":[{"name":"lookup","input_schema":{"type":"object","properties":{}}}]
		}`), claudeExecutorOptions(true))
		if err != nil {
			errors <- err
			return
		}
		result <- stream
	}()
	select {
	case err := <-errors:
		t.Fatal(err)
	case <-pendingSaved:
	case <-time.After(30 * time.Second):
		t.Fatal("the first tool batch did not finish persistence")
	}
	t.Logf("durable tool batch prepared after %s; checking return before any downstream drain", time.Since(started))
	var stream *cliproxyexecutor.StreamResult
	select {
	case err := <-errors:
		t.Fatal(err)
	case stream = <-result:
	case <-time.After(time.Second):
		t.Fatal("ExecuteStream blocked while its first tool batch waited for downstream drain")
	}
	chunks := 0
	for range stream.Chunks {
		chunks++
	}
	require.Greater(t, chunks, 64)
	require.NoError(t, executor.manager.Cancel(cancelRequest))
}

func TestExecutorStreamCancellationStopsManagedRunAfterFirstPayload(t *testing.T) {
	stopped := make(chan struct{})
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventText, Text: "first"})
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := executor.ExecuteStream(ctx, testExecutorAuth(), claudeExecutorRequest("session-cancel-stream", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hello"}]
	}`), claudeExecutorOptions(true))
	require.NoError(t, err)
	chunk := <-stream.Chunks
	require.NotEmpty(t, chunk.Payload)
	cancel()
	for range stream.Chunks {
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("managed Agent run survived downstream cancellation")
	}
}

func TestAnchoredExecutorMultiChunkReplayDisconnectDoesNotCancelActiveFollowUp(t *testing.T) {
	var runs atomic.Int32
	followUpStarted := make(chan struct{})
	followUpRelease := make(chan struct{})
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-overlap", Name: "lookup", Arguments: `{}`}})
			emit(Event{Type: EventTurnEnded})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				for index := 0; index < 80; index++ {
					emit(Event{Type: EventText, Text: fmt.Sprintf("chunk-%02d", index)})
				}
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		close(followUpStarted)
		select {
		case <-followUpRelease:
			emit(Event{Type: EventText, Text: "follow-up-alive"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	sessionID := "session-replay-overlap"
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest(sessionID, `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	continuation := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + ids[0] + `","content":"ok"}]}]}`
	second, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored-complete", continuation), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(second.Payload), "chunk-79")
	waitUntilInactive(t, executor.manager, sessionID)

	replayCtx, cancelReplay := context.WithCancel(context.Background())
	stream, err := executor.ExecuteStream(replayCtx, testExecutorAuth(), anchoredClaudeExecutorRequest("ignored-replay", continuation), claudeExecutorOptions(true))
	require.NoError(t, err)

	followErr := make(chan error, 1)
	followPayload := make(chan []byte, 1)
	go func() {
		response, execErr := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest(sessionID, `{
			"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"continue"}]
		}`), claudeExecutorOptions(false))
		if execErr != nil {
			followErr <- execErr
			return
		}
		followPayload <- response.Payload
		followErr <- nil
	}()
	select {
	case <-followUpStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for overlapping follow-up run")
	}
	cancelReplay()
	for range stream.Chunks {
	}
	close(followUpRelease)
	select {
	case err := <-followErr:
		require.NoError(t, err, "replay disconnect must not cancel a newer active run in the same session")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for overlapping follow-up to finish")
	}
	require.Contains(t, string(<-followPayload), `"text":"follow-up-alive"`)
	require.Equal(t, int32(2), runs.Load())
}

func TestAnchoredExecutorStreamCancellationAfterParkPreservesContinuation(t *testing.T) {
	ids := make([]string, maxPendingTools)
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, _ RunRequest, emit func(Event)) error {
		for index := 0; index < maxPendingTools; index++ {
			ids[index] = fmt.Sprintf("parked-%02d", index)
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: ids[index], Name: "lookup", Arguments: `{}`}})
		}
		emit(Event{Type: EventTurnEnded})
		<-ctx.Done()
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	auth := testExecutorAuth()
	auth.Attributes["credential_scope"] = "test:6401"
	stream, err := executor.ExecuteStream(ctx, auth, anchoredClaudeExecutorRequest("session-park-cancel", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"lookup","input_schema":{"type":"object","properties":{}}}]
	}`), claudeExecutorOptions(true))
	require.NoError(t, err)
	cancel()
	for range stream.Chunks {
	}
	fingerprint, err := CredentialFingerprint("test:6401", "account-1")
	require.NoError(t, err)
	resolved, err := executor.manager.store.ResolvePending(fingerprint, ids)
	require.NoError(t, err)
	require.Equal(t, "session-park-cancel", resolved)
	require.NoError(t, executor.manager.Cancel(ManagedRequest{SessionID: resolved, CredentialScope: "test:6401", AccountKey: "account-1"}))
}

func TestExecutorReusesCPATranslatorsForChatAndResponses(t *testing.T) {
	for _, test := range []struct {
		name    string
		format  sdktranslator.Format
		payload string
		want    string
	}{
		{name: "chat", format: sdktranslator.FormatOpenAI, payload: `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hello"}]}`, want: `"choices"`},
		{name: "responses", format: sdktranslator.FormatOpenAIResponse, payload: `{"model":"claude-sonnet-4-6","input":"hello"}`, want: `"output"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
				require.Equal(t, "hello", request.UserText)
				emit(Event{Type: EventText, Text: "translated"})
				emit(Event{Type: EventDone})
				return nil
			}})
			req := cliproxyexecutor.Request{
				Model: "claude-sonnet-4-6", Payload: []byte(test.payload), Format: test.format,
				Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "session-" + test.name},
			}
			opts := cliproxyexecutor.Options{SourceFormat: test.format, ResponseFormat: test.format, OriginalRequest: []byte(test.payload)}
			response, err := executor.Execute(context.Background(), testExecutorAuth(), req, opts)
			require.NoError(t, err)
			require.Contains(t, string(response.Payload), test.want)
			require.Contains(t, string(response.Payload), "translated")
			if test.format == sdktranslator.FormatOpenAIResponse {
				require.Contains(t, string(response.Payload), `"id":"session-responses"`)
			}
		})
	}
}

func TestExecutorKeepsPublicModelAcrossProtocolAndStreamingModes(t *testing.T) {
	const publicModel = "claude-sonnet-4-6"
	const intentModel = "claude-4.6-sonnet-medium"
	tests := []struct {
		name    string
		format  sdktranslator.Format
		payload string
	}{
		{name: "messages", format: sdktranslator.FormatClaude, payload: `{"model":"claude-4.6-sonnet-medium","messages":[{"role":"user","content":"hello"}]}`},
		{name: "chat", format: sdktranslator.FormatOpenAI, payload: `{"model":"claude-4.6-sonnet-medium","messages":[{"role":"user","content":"hello"}]}`},
		{name: "responses", format: sdktranslator.FormatOpenAIResponse, payload: `{"model":"claude-4.6-sonnet-medium","input":"hello"}`},
	}
	for _, test := range tests {
		for _, stream := range []bool{false, true} {
			mode := "non-stream"
			if stream {
				mode = "stream"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
					require.Equal(t, intentModel, request.Model)
					emit(Event{Type: EventText, Text: "public"})
					emit(Event{Type: EventDone})
					return nil
				}})
				req := cliproxyexecutor.Request{
					Model: intentModel, Payload: []byte(test.payload), Format: test.format,
					Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "public-" + test.name},
				}
				opts := cliproxyexecutor.Options{
					Stream: stream, SourceFormat: test.format, ResponseFormat: test.format, OriginalRequest: []byte(test.payload),
					Metadata: map[string]any{cliproxyexecutor.RequestedModelMetadataKey: publicModel},
				}
				if !stream {
					response, err := executor.Execute(context.Background(), testExecutorAuth(), req, opts)
					require.NoError(t, err)
					require.Contains(t, string(response.Payload), `"model":"`+publicModel+`"`)
					require.NotContains(t, string(response.Payload), `"model":"`+intentModel+`"`)
					return
				}
				response, err := executor.ExecuteStream(context.Background(), testExecutorAuth(), req, opts)
				require.NoError(t, err)
				var output strings.Builder
				for chunk := range response.Chunks {
					require.NoError(t, chunk.Err)
					output.Write(chunk.Payload)
				}
				require.Contains(t, output.String(), `"model":"`+publicModel+`"`)
				require.NotContains(t, output.String(), `"model":"`+intentModel+`"`)
			})
		}
	}
}

func TestResponsesContinuationRejectsCredentialOwnerChange(t *testing.T) {
	var runs atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		if runs.Add(1) > 1 {
			t.Fatal("credential-mismatched continuation started a new Agent run")
		}
		emit(Event{Type: EventText, Text: "first"})
		emit(Event{Type: EventDone})
		return nil
	}})
	sessionID := "resp_bf_agentv1_u7_c6401_0123456789abcdef0123456789abcdef"
	firstBody := `{"model":"claude-sonnet-4-6","input":"hello"}`
	first := cliproxyexecutor.Request{Model: "claude-sonnet-4-6", Payload: []byte(firstBody), Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID}}
	_, err := executor.Execute(context.Background(), testExecutorAuth(), first, cliproxyexecutor.Options{SourceFormat: first.Format, ResponseFormat: first.Format, OriginalRequest: first.Payload})
	require.NoError(t, err)

	other := testExecutorAuth()
	other.Metadata["account_id"] = "account-2"
	continuationBody := `{"model":"claude-sonnet-4-6","previous_response_id":"` + sessionID + `","input":"continue"}`
	continuation := cliproxyexecutor.Request{Model: "claude-sonnet-4-6", Payload: []byte(continuationBody), Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID}}
	_, err = executor.Execute(context.Background(), other, continuation, cliproxyexecutor.Options{SourceFormat: continuation.Format, ResponseFormat: continuation.Format, OriginalRequest: continuation.Payload})
	var status interface{ StatusCode() int }
	require.ErrorAs(t, err, &status)
	require.Equal(t, http.StatusConflict, status.StatusCode())
	require.Equal(t, int32(1), runs.Load())
}

func TestExecutorRefreshRotatesCredentialWithoutChangingAccountIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "Bearer api-key", request.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"accessToken":"next-access","refreshToken":"next-refresh"}`))
	}))
	defer server.Close()
	executor := &Executor{client: server.Client(), refreshURL: server.URL}
	next, err := executor.Refresh(context.Background(), testExecutorAuth())
	require.NoError(t, err)
	require.Equal(t, "next-access", next.Metadata["access_token"])
	require.Equal(t, "next-refresh", next.Metadata["refresh_token"])
	require.Equal(t, "account-1", next.Metadata["account_id"])
	require.Equal(t, "api-key", next.Metadata["api_key"])
}

func TestExecutorRefreshActivatesAPIKeyOnlyCredential(t *testing.T) {
	token := testCursorJWT("auth0|user-77")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "Bearer key_fresh", request.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"accessToken":"` + token + `","refreshToken":"rt"}`))
	}))
	defer server.Close()
	executor := &Executor{client: server.Client(), refreshURL: server.URL}
	auth := &cliproxyauth.Auth{ID: "fresh.json", Provider: providerID, Metadata: map[string]any{"api_key": "key_fresh"}}
	next, err := executor.Refresh(context.Background(), auth)
	require.NoError(t, err)
	require.Equal(t, token, next.Metadata["access_token"])
	require.Equal(t, "auth0|user-77", next.Metadata["account_id"], "activation must adopt the minted token identity")
	require.Empty(t, auth.Metadata["account_id"], "input auth must not be mutated")

	stale := &cliproxyauth.Auth{ID: "stale.json", Provider: providerID, Metadata: map[string]any{"api_key": "key_fresh", "access_token": token}}
	_, err = executor.Refresh(context.Background(), stale)
	require.ErrorContains(t, err, "account id is required", "a token without a recorded identity must still be rejected")
}

func TestExecutorRefreshRejectsLegacyRefreshTokenWithoutAPIKey(t *testing.T) {
	auth := testExecutorAuth()
	delete(auth.Metadata, "api_key")
	_, err := (&Executor{}).Refresh(context.Background(), auth)
	require.ErrorContains(t, err, "API key is required")
}

func TestExecutorConcurrentRefreshUsesOneAccountFlight(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte(`{"accessToken":"next-access","refreshToken":"next-refresh"}`))
	}))
	defer server.Close()
	executor := &Executor{client: server.Client(), refreshURL: server.URL}
	const count = 20
	type refreshResult struct {
		index int
		auth  *cliproxyauth.Auth
	}
	results := make(chan refreshResult, count)
	errors := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			auth := testExecutorAuth()
			auth.Attributes["credential_scope"] = fmt.Sprintf("channel:%d", index)
			auth.ProxyURL = fmt.Sprintf("http://proxy-%d.example", index)
			result, err := executor.Refresh(context.Background(), auth)
			if err != nil {
				errors <- err
				return
			}
			results <- refreshResult{index: index, auth: result}
		}(index)
	}
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	for result := range results {
		require.Equal(t, "next-access", result.auth.Metadata["access_token"])
		require.Equal(t, fmt.Sprintf("channel:%d", result.index), result.auth.Attributes["credential_scope"])
		require.Equal(t, fmt.Sprintf("http://proxy-%d.example", result.index), result.auth.ProxyURL)
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestExecutorRefreshSurvivesFirstWaiterCancellation(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		close(started)
		<-release
		_, _ = w.Write([]byte(`{"accessToken":"next-access","refreshToken":"next-refresh"}`))
	}))
	defer server.Close()
	executor := &Executor{client: server.Client(), refreshURL: server.URL}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := executor.Refresh(firstCtx, testExecutorAuth())
		firstResult <- err
	}()
	<-started
	secondResult := make(chan *cliproxyauth.Auth, 1)
	secondError := make(chan error, 1)
	go func() {
		auth, err := executor.Refresh(context.Background(), testExecutorAuth())
		if err != nil {
			secondError <- err
			return
		}
		secondResult <- auth
	}()
	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)
	close(release)
	select {
	case err := <-secondError:
		t.Fatal(err)
	case auth := <-secondResult:
		require.Equal(t, "next-access", auth.Metadata["access_token"])
	case <-time.After(time.Second):
		t.Fatal("second refresh waiter did not survive first waiter cancellation")
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestExecutorCarriesSelectedChannelProxyToAgentRun(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
		require.Equal(t, "http://proxy.example:8080", request.ProxyURL)
		emit(Event{Type: EventText, Text: "proxied"})
		emit(Event{Type: EventDone})
		return nil
	}})
	auth := testExecutorAuth()
	auth.ProxyURL = "http://proxy.example:8080"
	response, err := executor.Execute(context.Background(), auth, claudeExecutorRequest("session-proxy", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hello"}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(response.Payload), "proxied")
}

func TestExecutorProjectsCallerToolsForChatAndResponses(t *testing.T) {
	for _, test := range []struct {
		name    string
		format  sdktranslator.Format
		payload string
		want    []string
	}{
		{name: "chat", format: sdktranslator.FormatOpenAI, payload: `{
			"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
			"tools":[{"type":"function","function":{"name":"lookup","description":"Lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}}]
		}`, want: []string{`"tool_calls"`, `"name":"lookup"`, `"arguments":"{\"id\":1}"`}},
		{name: "responses", format: sdktranslator.FormatOpenAIResponse, payload: `{
			"model":"claude-sonnet-4-6","input":"lookup",
			"tools":[{"type":"function","name":"lookup","description":"Lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}]
		}`, want: []string{`"type":"function_call"`, `"name":"lookup"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
				require.Len(t, request.Tools, 1)
				emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-lookup", Name: "lookup", Arguments: `{"id":1}`}})
				emit(Event{Type: EventTurnEnded})
				<-ctx.Done()
				return ctx.Err()
			}})
			sessionID := "session-tool-" + test.name
			req := cliproxyexecutor.Request{
				Model: "claude-sonnet-4-6", Payload: []byte(test.payload), Format: test.format,
				Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID},
			}
			opts := cliproxyexecutor.Options{SourceFormat: test.format, ResponseFormat: test.format, OriginalRequest: []byte(test.payload)}
			response, err := executor.Execute(context.Background(), testExecutorAuth(), req, opts)
			require.NoError(t, err)
			require.NoError(t, executor.manager.Cancel(ManagedRequest{SessionID: sessionID, CredentialScope: "tenant:1:user:7:channel:6401", AccountKey: "account-1"}))
			for _, wanted := range test.want {
				require.Contains(t, string(response.Payload), wanted)
			}
			require.Len(t, agentV1ToolCallIDs(response.Payload), 1)
		})
	}
}

func TestResponsesToolStreamIncludesCompletedBeforePark(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-stream", Name: "shell", Arguments: `{"cmd":"pwd"}`}})
		emit(Event{Type: EventTurnEnded})
		<-ctx.Done()
		return ctx.Err()
	}})
	payload := []byte(`{"model":"glm-5.2","input":"run pwd","tools":[{"type":"function","name":"shell","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}]}`)
	req := cliproxyexecutor.Request{Model: "glm-5.2-high", Payload: payload, Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "stream-tool-session"}}
	stream, err := executor.ExecuteStream(context.Background(), testExecutorAuth(), req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload})
	require.NoError(t, err)
	var output strings.Builder
	for chunk := range stream.Chunks {
		require.NoError(t, chunk.Err)
		output.Write(chunk.Payload)
	}
	require.Contains(t, output.String(), `"type":"response.output_item.done"`)
	require.Contains(t, output.String(), `"type":"response.completed"`)
	require.Contains(t, output.String(), `"status":"completed"`)
	require.NotContains(t, output.String(), `"status":"incomplete"`)
}

func TestResponsesToolResultWithoutPendingSessionRestartsFromTranscript(t *testing.T) {
	// Production 2026-09-02 23:08-23:13: Codex CLI (store=false, full transcript,
	// no previous_response_id) hit type 64 after a sidecar cutover / after the
	// conversation had been served by Kiro. Every turn 409'd "no matching
	// pending tool" until retry landed on Kiro. Messages/Chat already rebuild
	// from history; Responses must do the same when the transcript is present.
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		require.NotEmpty(t, request.HistoryMessages, "stateless restart must carry the transcript")
		require.Equal(t, historyContinuationText, request.UserText)
		joined := strings.Join(func() []string {
			out := make([]string, 0, len(request.HistoryMessages))
			for _, raw := range request.HistoryMessages {
				out = append(out, string(raw))
			}
			return out
		}(), "\n")
		require.Contains(t, joined, "call_kiro_7f3a")
		require.Contains(t, joined, "foreign-result")
		emit(Event{Type: EventText, Text: "restarted-from-transcript"})
		emit(Event{Type: EventDone})
		return nil
	}})
	payload := `{"model":"claude-opus-4-8","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"lookup"}]},{"type":"function_call","call_id":"call_kiro_7f3a","name":"lookup","arguments":"{\"id\":1}"},{"type":"function_call_output","call_id":"call_kiro_7f3a","output":"foreign-result"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}]}`
	req := cliproxyexecutor.Request{Model: "claude-opus-4-8", Payload: []byte(payload), Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "codex-store-false-turn"}}
	response, err := executor.Execute(context.Background(), testExecutorAuth(), req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: []byte(payload)})
	require.NoError(t, err)
	require.Contains(t, string(response.Payload), "restarted-from-transcript")

	// A stateful continuation has nothing to rebuild from and keeps its 409.
	stateful := `{"model":"claude-opus-4-8","previous_response_id":"resp_gone","input":[{"type":"function_call_output","call_id":"call_gone","output":"late"}]}`
	statefulReq := cliproxyexecutor.Request{Model: "claude-opus-4-8", Payload: []byte(stateful), Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "codex-stateful-turn"}}
	_, err = executor.Execute(context.Background(), testExecutorAuth(), statefulReq, cliproxyexecutor.Options{SourceFormat: statefulReq.Format, ResponseFormat: statefulReq.Format, OriginalRequest: []byte(stateful)})
	require.Error(t, err)
	require.ErrorContains(t, err, "no matching pending tool")
}

func TestExecutorChatAndResponsesToolResultRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name         string
		format       sdktranslator.Format
		first        string
		continuation string
		want         string
	}{
		{name: "chat", format: sdktranslator.FormatOpenAI,
			first:        `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}}]}`,
			continuation: `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"},{"role":"assistant","tool_calls":[{"id":"call-roundtrip","type":"function","function":{"name":"lookup","arguments":"{\"id\":1}"}}]},{"role":"tool","tool_call_id":"call-roundtrip","content":"result"}]}`,
			want:         `"choices"`},
		{name: "responses", format: sdktranslator.FormatOpenAIResponse,
			first:        `{"model":"claude-sonnet-4-6","input":"lookup","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}]}`,
			continuation: `{"model":"claude-sonnet-4-6","input":[{"type":"function_call","call_id":"call-roundtrip","name":"lookup","arguments":"{\"id\":1}"},{"type":"function_call_output","call_id":"call-roundtrip","output":"result"}]}`,
			want:         `"output"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
				if request.HistoryRequestKey != "" {
					if len(request.HistoryMessages) > 0 {
						emit(Event{Type: EventText, Text: "roundtrip-complete"})
					} else {
						emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-roundtrip", Name: "lookup", Arguments: `{"id":1}`}})
					}
					emit(Event{Type: EventDone})
					return nil
				}
				emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-roundtrip", Name: "lookup", Arguments: `{"id":1}`}})
				emit(Event{Type: EventTurnEnded})
				select {
				case result := <-request.ToolResults:
					require.Equal(t, "call-roundtrip", result.ToolCallID)
					require.Equal(t, "result", result.Content)
					emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
					emit(Event{Type: EventText, Text: "roundtrip-complete"})
					emit(Event{Type: EventDone})
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}})
			firstReq := cliproxyexecutor.Request{Model: "claude-sonnet-4-6", Payload: []byte(test.first), Format: test.format, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "roundtrip-" + test.name}}
			opts := cliproxyexecutor.Options{SourceFormat: test.format, ResponseFormat: test.format, OriginalRequest: []byte(test.first)}
			firstResponse, err := executor.Execute(context.Background(), testExecutorAuth(), firstReq, opts)
			require.NoError(t, err)
			publicIDs := agentV1ToolCallIDs(firstResponse.Payload)
			require.Len(t, publicIDs, 1)

			continuation := strings.ReplaceAll(test.continuation, "call-roundtrip", publicIDs[0])
			continuationReq := cliproxyexecutor.Request{Model: "claude-sonnet-4-6", Payload: []byte(continuation), Format: test.format, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "client-generated-new-id"}}
			continuationOpts := cliproxyexecutor.Options{SourceFormat: test.format, ResponseFormat: test.format, OriginalRequest: []byte(test.continuation)}
			response, err := executor.Execute(context.Background(), testExecutorAuth(), continuationReq, continuationOpts)
			require.NoError(t, err)
			require.Contains(t, string(response.Payload), test.want)
			require.Contains(t, string(response.Payload), "roundtrip-complete")
		})
	}
}

func TestAnchoredExecutorResumedTurnCanParkAgain(t *testing.T) {
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-first", Name: "lookup", Arguments: `{}`}})
		emit(Event{Type: EventTurnEnded})
		select {
		case first := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &first})
		case <-ctx.Done():
			return ctx.Err()
		}
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-second", Name: "lookup", Arguments: `{}`}})
		emit(Event{Type: EventTurnEnded})
		select {
		case second := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &second})
		case <-ctx.Done():
			return ctx.Err()
		}
		emit(Event{Type: EventText, Text: "two-rounds-complete"})
		emit(Event{Type: EventDone})
		return nil
	}})
	tool := `"tools":[{"name":"lookup","input_schema":{"type":"object","properties":{}}}]`
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("stable-session", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"start"}],`+tool+`
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	firstIDs := agentV1ToolCallIDs(first.Payload)
	require.Len(t, firstIDs, 1)

	secondBody := `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-first","content":"one"}]}]
	}`
	secondBody = strings.ReplaceAll(secondBody, "call-first", firstIDs[0])
	second, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("mismatched-client-session", secondBody), claudeExecutorOptions(false))
	require.NoError(t, err)
	secondIDs := agentV1ToolCallIDs(second.Payload)
	require.Len(t, secondIDs, 1)

	thirdBody := `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-second","content":"two"}]}]
	}`
	thirdBody = strings.ReplaceAll(thirdBody, "call-second", secondIDs[0])
	third, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("another-mismatched-session", thirdBody), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(third.Payload), "two-rounds-complete")
}

func testExecutor(t *testing.T, engine runEngine) *Executor {
	t.Helper()
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() {
		deadline := time.Now().Add(time.Second)
		for {
			manager.mu.Lock()
			busy := len(manager.active) + len(manager.starting) + len(manager.flights)
			manager.mu.Unlock()
			if busy == 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		_ = os.RemoveAll(store.completedRoot)
	})
	return newExecutorForTest(manager, nil)
}

func TestExecutorCredentialRequiresCanonicalAccountID(t *testing.T) {
	auth := testExecutorAuth()
	delete(auth.Metadata, "account_id")
	auth.Metadata["email"] = "fallback@example.com"
	auth.Metadata["refresh_token"] = "fallback-refresh"

	_, _, _, _, _, _, err := executorCredential(auth)
	require.ErrorContains(t, err, "stable account identity is missing")
}

func TestExecutorMapsAnthropicServerSearchInsteadOfCallerTool(t *testing.T) {
	payload := []byte(`{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"latest"}],
		"tools":[{"type":"web_search_20250305","name":"web_search"},{"name":"Bash","input_schema":{"type":"object"}}]
	}`)
	req := claudeExecutorRequest("session-hosted-search", string(payload))
	turn, err := translateExecutionRequest(req, withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)
	require.False(t, turn.managed.Run.AllowHostedFetch)
	require.False(t, turn.managed.Run.RequireHostedSearch)
	require.Len(t, turn.managed.Run.Tools, 1)
	require.Equal(t, "Bash", turn.managed.Run.Tools[0].Name)
}

func TestExecutorIgnoresServerSearchHintsButRejectsUnknownFields(t *testing.T) {
	// user_location is a ranking hint that Kiro / the Anthropic relay forward
	// without effect; a field we have never seen still fails closed.
	req := claudeExecutorRequest("session-search-hint", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"latest"}],
		"tools":[{"type":"web_search_20250305","name":"web_search","user_location":{"type":"approximate"},"cache_control":{"type":"ephemeral"}}]
	}`)
	turn, err := translateExecutionRequest(req, withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)

	req = claudeExecutorRequest("session-search-unknown", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"latest"}],
		"tools":[{"type":"web_search_20250305","name":"web_search","brand_new_field":1}]
	}`)
	_, err = translateExecutionRequest(req, withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.ErrorContains(t, err, "cannot faithfully apply")
}

func TestChatWebSearchOptionsEnableHostedSearch(t *testing.T) {
	payload := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"latest"}],"web_search_options":{}}`)
	req := cliproxyexecutor.Request{Model: "glm-5.2", Payload: payload, Format: sdktranslator.FormatOpenAI, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "chat-search"}}
	turn, err := translateExecutionRequest(req, withHostedSearchHeader(cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.AllowHostedFetch)
}

func TestChatWebSearchOptionsHintsAreIgnoredLikeOtherClaudePathways(t *testing.T) {
	for name, options := range map[string]string{
		"context-size": `{"search_context_size":"medium"}`,
		"location":     `{"user_location":{"type":"approximate","country":"US"}}`,
	} {
		payload := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"latest"}],"web_search_options":` + options + `}`)
		req := cliproxyexecutor.Request{Model: "glm-5.2", Payload: payload, Format: sdktranslator.FormatOpenAI, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "chat-search-" + name}}
		turn, err := translateExecutionRequest(req, withHostedSearchHeader(cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
		require.NoError(t, err, name)
		require.True(t, turn.managed.Run.AllowHostedSearch, name)
	}
	payload := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"latest"}],"web_search_options":"yes"}`)
	req := cliproxyexecutor.Request{Model: "glm-5.2", Payload: payload, Format: sdktranslator.FormatOpenAI, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "chat-search-bad"}}
	_, err := translateExecutionRequest(req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.ErrorContains(t, err, "must be an object")
}

func TestHostedSearchNeedsNoChannelCapabilityHeader(t *testing.T) {
	// Every Cursor account carries hosted search/fetch; the relay header that
	// used to gate it is optional so a hand-created channel behaves like an
	// imported one.
	for _, test := range []struct {
		name    string
		format  sdktranslator.Format
		payload string
	}{
		{name: "messages", format: sdktranslator.FormatClaude, payload: `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"latest"}],"tools":[{"type":"web_search_20250305","name":"web_search"}]}`},
		{name: "chat", format: sdktranslator.FormatOpenAI, payload: `{"model":"glm-5.2","messages":[{"role":"user","content":"latest"}],"web_search_options":{}}`},
		{name: "responses", format: sdktranslator.FormatOpenAIResponse, payload: `{"model":"glm-5.2","input":"latest","tools":[{"type":"web_search","external_web_access":true}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := cliproxyexecutor.Request{Model: "glm-5.2", Payload: []byte(test.payload), Format: test.format, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "no-capability-" + test.name}}
			turn, err := translateExecutionRequest(req, cliproxyexecutor.Options{SourceFormat: test.format, ResponseFormat: test.format, OriginalRequest: []byte(test.payload)}, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
			require.NoError(t, err)
			require.True(t, turn.managed.Run.AllowHostedSearch)
		})
	}
}

func TestClaudeCodeCatalogTranslatesWithoutAdaptorHeader(t *testing.T) {
	// Production 2026-09-02: every Claude Code turn carries WebFetch, so a
	// channel without the legacy header 422'd on plain conversation.
	payload := []byte(`{
		"model":"claude-opus-5-high",
		"system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=cli;",
		"messages":[{"role":"user","content":"search"}],
		"tools":[{"name":"WebSearch","input_schema":{"type":"object"}},{"name":"WebFetch","input_schema":{"type":"object"}},{"name":"Bash","input_schema":{"type":"object"}}]
	}`)
	req := claudeExecutorRequest("claude-code-native-search-no-header", string(payload))
	turn, err := translateExecutionRequest(req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedFetch)
	names := make([]string, 0, len(turn.managed.Run.Tools))
	for _, tool := range turn.managed.Run.Tools {
		names = append(names, tool.Name)
	}
	require.ElementsMatch(t, []string{"WebSearch", "Bash"}, names)
}

func TestResponsesOptionalSearchUsesCursorNativePolicy(t *testing.T) {
	for _, test := range []struct {
		name       string
		tool       string
		wantSearch bool
	}{
		{name: "codex disabled search is dropped", tool: `{"type":"web_search","external_web_access":false}`},
		{name: "live search enables native tools", tool: `{"type":"web_search","external_web_access":true}`, wantSearch: true},
		{name: "optional x search is dropped", tool: `{"type":"x_search"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := []byte(`{"model":"glm-5.2","input":"hello","tools":[{"type":"function","name":"pwd","parameters":{"type":"object"}},` + test.tool + `],"tool_choice":"auto"}`)
			req := cliproxyexecutor.Request{Model: "glm-5.2-high", Payload: payload, Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "search-session"}}
			opts := cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}
			if test.wantSearch {
				opts = withHostedSearchHeader(opts)
			}
			turn, err := translateExecutionRequest(req, opts, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
			require.NoError(t, err)
			require.Equal(t, test.wantSearch, turn.managed.Run.AllowHostedSearch)
			require.Len(t, turn.managed.Run.Tools, 1)
			require.Equal(t, "pwd", turn.managed.Run.Tools[0].Name)
		})
	}
}

func TestResponsesRequiredOrUnfaithfulSearchFailsClosed(t *testing.T) {
	for _, payload := range []string{
		`{"model":"glm-5.2","input":"hello","tools":[{"type":"web_search"}],"tool_choice":"required"}`,
		`{"model":"glm-5.2","input":"hello","tools":[{"type":"web_search","filters":{"allowed_domains":["example.com"]}}]}`,
	} {
		req := cliproxyexecutor.Request{Model: "glm-5.2-high", Payload: []byte(payload), Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "search-session"}}
		_, err := translateExecutionRequest(req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: req.Payload}, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
		require.Error(t, err)
	}
}

func TestResponsesNamedCallerToolDropsUnselectedHostedSearch(t *testing.T) {
	payload := []byte(`{"model":"glm-5.2","input":"hello","tools":[{"type":"function","name":"pwd","parameters":{"type":"object"}},{"type":"x_search"},{"type":"web_search","external_web_access":true}],"tool_choice":{"type":"function","name":"pwd"}}`)
	req := cliproxyexecutor.Request{Model: "glm-5.2-high", Payload: payload, Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "named-search-session"}}
	turn, err := translateExecutionRequest(req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.RequireToolCall)
	require.Len(t, turn.managed.Run.Tools, 1)
	require.Equal(t, "pwd", turn.managed.Run.Tools[0].Name)
}

func TestChatFullHistorySerializesCompletedToolTurnsForCursor(t *testing.T) {
	payload := []byte(`{
		"model":"glm-5.2","messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"toolu_1","type":"function","function":{"name":"Bash","arguments":"{\"command\":\"pwd\"}"}}]},
			{"role":"tool","tool_call_id":"toolu_1","content":"/workspace"},
			{"role":"user","content":"continue"}
		],"tools":[{"type":"function","function":{"name":"Bash","parameters":{"type":"object"}}}]
	}`)
	req := cliproxyexecutor.Request{Model: "glm-5.2", Payload: payload, Format: sdktranslator.FormatOpenAI, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "codebuddy-next-turn"}}
	turn, err := translateExecutionRequest(req, cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Len(t, turn.managed.Run.HistoryMessages, 3)
	history := string(bytes.Join(turn.managed.Run.HistoryMessages, nil))
	require.NotContains(t, history, `"type":"tool_use"`)
	require.NotContains(t, history, `"type":"tool_result"`)
	require.Contains(t, history, `"type":"tool-call"`)
	require.Contains(t, history, `"type":"tool-result"`)
	require.Contains(t, history, `"result":"/workspace"`)
	require.NotContains(t, history, "BEEFAPI_TOOL_EVENT")
}

// tinyPNGBase64 is a valid 1x1 PNG so media tests exercise real base64 decoding.
const tinyPNGBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

func TestClaudeFullHistoryKeepsToolResultMediaAsNativeParts(t *testing.T) {
	payload := []byte(`{
		"model":"claude-fable-5-1","messages":[
			{"role":"user","content":"capture the page"},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_devtools","name":"chrome-devtools","input":{"action":"screenshot"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_devtools","content":[
				{"type":"text","text":"screenshot captured"},
				{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + tinyPNGBase64 + `"}}
			]}]},
			{"role":"assistant","content":[{"type":"text","text":"I captured it."}]},
			{"role":"user","content":"continue"}
		],
		"tools":[{"name":"chrome-devtools","input_schema":{"type":"object"}}]
	}`)
	req := claudeExecutorRequest("claude-history-media", string(payload))
	turn, err := translateExecutionRequest(req, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	history := string(bytes.Join(turn.managed.Run.HistoryMessages, nil))
	require.Contains(t, history, "screenshot captured")
	require.Contains(t, history, `"type":"tool-result"`)
	// The screenshot is preserved as an experimental_content image part, and the
	// plain result keeps the text-only contract for providers without media.
	require.Contains(t, history, `"experimental_content":[{"text":"screenshot captured","type":"text"},{"image":"`+tinyPNGBase64+`","mimeType":"image/png","type":"image"}]`)
	require.Contains(t, history, `"result":[{"text":"screenshot captured","type":"text"}]`)
	require.NotContains(t, history, `"type":"image","source"`)
}

func TestClaudeFullHistoryKeepsReasoningAsNativeParts(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5","messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":[
				{"type":"thinking","thinking":"private reasoning","signature":"provider-signature"},
				{"type":"redacted_thinking","data":"opaque-redacted"},
				{"type":"text","text":"I will inspect it."},
				{"type":"tool_use","id":"toolu_read","name":"Read","input":{"path":"file"}}
			]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_read","content":"done"}]},
			{"role":"assistant","content":[{"type":"thinking","thinking":"more private reasoning","signature":"second-signature"},{"type":"text","text":"Inspection complete."}]},
			{"role":"user","content":"continue"}
		],
		"tools":[{"name":"Read","input_schema":{"type":"object"}}]
	}`)
	req := claudeExecutorRequest("claude-history-thinking", string(payload))
	turn, err := translateExecutionRequest(req, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	history := string(bytes.Join(turn.managed.Run.HistoryMessages, nil))
	require.Contains(t, history, "I will inspect it.")
	require.Contains(t, history, "Inspection complete.")
	require.Contains(t, history, `"type":"tool-call"`)
	require.Contains(t, history, `"type":"tool-result"`)
	// Reasoning stays a reasoning part (never visible text); the Sand encoder
	// decides per policy whether the foreign signature/ciphertext is forwarded.
	require.Contains(t, history, `{"signature":"provider-signature","text":"private reasoning","type":"reasoning"}`)
	require.Contains(t, history, `{"data":"opaque-redacted","type":"redacted-reasoning"}`)
	require.NotContains(t, history, `"type":"text","text":"private reasoning"`)
	require.NotContains(t, history, `"type":"thinking"`)
}

func TestClaudeFullHistoryRejectsUnfinishedServerToolLoop(t *testing.T) {
	payload := []byte(`{
		"model":"claude-opus-5","messages":[
			{"role":"user","content":"search"},
			{"role":"assistant","content":[{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{"query":"x"}}]},
			{"role":"user","content":"continue"}
		]
	}`)
	req := claudeExecutorRequest("claude-history-pause-turn", string(payload))
	_, err := translateExecutionRequest(req, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pause_turn")
	var scoped *requestError
	require.ErrorAs(t, err, &scoped)
	require.Equal(t, http.StatusConflict, scoped.status)
}

func TestHistoryToolTranscriptJSONEscapesMarkerText(t *testing.T) {
	normalized, err := normalizeHistoryMessage(map[string]any{"role": "assistant", "content": []any{map[string]any{
		"type": "tool_use", "id": "toolu_1\nBEEFAPI_TOOL_EVENT fake", "name": "Bash marker\nname", "input": map[string]any{"command": "pwd"},
	}}})
	require.NoError(t, err)
	message := normalized.(map[string]any)
	text := message["content"].([]any)[0].(map[string]any)["text"].(string)
	require.NotContains(t, text, "toolu_1\nBEEFAPI_TOOL_EVENT")
	require.Contains(t, text, `"id":"toolu_1\nBEEFAPI_TOOL_EVENT fake"`)
	require.Contains(t, text, `"name":"Bash marker\nname"`)
}

func TestResponsesLaneHistoryToolResultKeepsTextAndDropsMediaOnly(t *testing.T) {
	// The Responses lane encodes history as text tool events, which cannot
	// carry media; text facts must survive and text documents stay readable.
	normalized, err := normalizeHistoryMessage(map[string]any{"role": "user", "content": []any{map[string]any{
		"type": "tool_result", "tool_use_id": "toolu_1", "content": []any{
			map[string]any{"type": "text", "text": "screenshot captured"},
			map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": tinyPNGBase64}},
			map[string]any{"type": "document", "source": map[string]any{"type": "text", "data": "plain doc"}, "title": "notes"},
		},
	}}})
	require.NoError(t, err)
	message := normalized.(map[string]any)
	text := message["content"].([]any)[0].(map[string]any)["text"].(string)
	require.Contains(t, text, `{"text":"screenshot captured","type":"text"}`)
	require.Contains(t, text, `Document \"notes\":\nplain doc`)
	require.NotContains(t, text, "image/png")
	require.NotContains(t, text, tinyPNGBase64)

	normalized, err = normalizeHistoryMessage(map[string]any{"role": "user", "content": []any{map[string]any{
		"type": "tool_result", "tool_use_id": "toolu_2", "content": []any{
			map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": tinyPNGBase64}},
		},
	}}})
	require.NoError(t, err)
	message = normalized.(map[string]any)
	text = message["content"].([]any)[0].(map[string]any)["text"].(string)
	require.Contains(t, text, `"content":[]`)
}

func TestHistoryToolResultRejectsUnknownContentAndInvalidErrorFlag(t *testing.T) {
	for _, content := range []any{
		[]any{map[string]any{"type": "opaque", "value": "x"}},
		map[string]any{"value": "opaque"},
	} {
		_, err := normalizeHistoryMessage(map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "toolu_1", "content": content}}})
		require.Error(t, err)
	}
	_, err := normalizeHistoryMessage(map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok", "is_error": "false"}}})
	require.ErrorContains(t, err, "boolean")
}

func TestAnchoredAnthropicMixedToolResultAndTextIsParsedForCompletedFollowUp(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"},{"type":"text","text":"continue"}]}]}`
	turn, err := translateExecutionRequest(anchoredClaudeExecutorRequest("mixed-native", payload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, "continue", turn.managed.FollowUpText)
	require.Empty(t, turn.managed.Run.UserText)
	require.Empty(t, turn.managed.SessionID)
}

func TestCallerToolPolicyNormalizesNoneNamedAndParallel(t *testing.T) {
	none, err := translateExecutionRequest(cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatClaude,
		Payload:  []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"web_search_20250305","name":"web_search"}],"tool_choice":{"type":"none"}}`),
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "none"},
	}, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Empty(t, none.managed.Run.Tools)
	require.Empty(t, none.managed.ToolCatalogDigest)
	require.True(t, none.managed.DisableCallerTools)
	require.False(t, none.managed.Run.RequireToolCall)

	named, err := translateExecutionRequest(cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatClaude,
		Payload:  []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"tools":[{"name":"first","input_schema":{"type":"object"}},{"name":"second","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"second","disable_parallel_tool_use":true}}`),
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "named"},
	}, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Len(t, named.managed.Run.Tools, 1)
	require.Equal(t, "second", named.managed.Run.Tools[0].Name)
	require.True(t, named.managed.Run.RequireToolCall)
	require.False(t, named.managed.DisableCallerTools)
	require.Equal(t, 1, named.managed.Run.MaxParallelTools)
}

func TestSandHonorsCallerParallelWhileAgentV1StaysSerial(t *testing.T) {
	payload := []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"tools":[{"name":"first","input_schema":{"type":"object"}},{"name":"second","input_schema":{"type":"object"}}]}`)
	agent, err := translateExecutionRequest(cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatClaude,
		Payload:  payload,
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "agent-serial"},
	}, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, 1, agent.managed.Run.MaxParallelTools)

	sandOpts := claudeExecutorOptions(false)
	sandOpts.Metadata = map[string]any{"beefapi_cursor_runtime_profile": runtimeSand}
	sand, err := translateExecutionRequest(cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatClaude,
		Payload:  payload,
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "sand-parallel"},
	}, sandOpts, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, 0, sand.managed.Run.MaxParallelTools)

	sandSerial, err := translateExecutionRequest(cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatClaude,
		Payload:  []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}],"tools":[{"name":"first","input_schema":{"type":"object"}}],"parallel_tool_calls":false}`),
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "sand-serial"},
	}, sandOpts, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, 1, sandSerial.managed.Run.MaxParallelTools)
}

func TestAnchoredTranslateMixedToolResultNoneDisablesCallerTools(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","tool_choice":{"type":"none"},"tools":[{"name":"beefapi_conformance_canary","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"},{"type":"text","text":"continue"}]}]}`
	turn, err := translateExecutionRequest(anchoredClaudeExecutorRequest("mixed-none", payload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, "continue", turn.managed.FollowUpText)
	require.Empty(t, turn.managed.Run.Tools)
	require.Empty(t, turn.managed.ToolCatalogDigest)
	require.True(t, turn.managed.DisableCallerTools)
}

func TestAnchoredTranslateOmittedToolsDoesNotDisableCallerTools(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"},{"type":"text","text":"continue"}]}]}`
	turn, err := translateExecutionRequest(anchoredClaudeExecutorRequest("mixed-omitted", payload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, "continue", turn.managed.FollowUpText)
	require.Empty(t, turn.managed.Run.Tools)
	require.Empty(t, turn.managed.ToolCatalogDigest)
	require.False(t, turn.managed.DisableCallerTools)
}

func testExecutorAuth() *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		ID: "beefapi-cursor-agent-v1-6401", Provider: providerID, Attributes: map[string]string{"credential_scope": "tenant:1:user:7:channel:6401"},
		Metadata: map[string]any{"api_key": "api-key", "access_token": "access", "refresh_token": "refresh", "account_id": "account-1", "base_url": "https://agent.example", "client_version": "cli-test"},
	}
}

func claudeExecutorRequest(sessionID, payload string) cliproxyexecutor.Request {
	return cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Payload: []byte(payload), Format: sdktranslator.FormatClaude,
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID},
	}
}

// Legacy pending/checkpoint contracts now belong to the Responses lane. Keep
// those lifecycle regression scenarios using the real CPA Responses converter;
// full-history Messages/Chat contracts are exercised in full_history_test.go.
func anchoredClaudeExecutorRequest(sessionID, payload string) cliproxyexecutor.Request {
	request := claudeExecutorRequest(sessionID, payload)
	request.Payload = sdktranslator.TranslateRequest(sdktranslator.FormatClaude, sdktranslator.FormatCodex, request.Model, []byte(payload), false)
	request.Format = sdktranslator.FormatOpenAIResponse
	return request
}

func claudeExecutorOptions(stream bool) cliproxyexecutor.Options {
	headers := http.Header{}
	headers.Set(headerMeteringMode, "turn-v2")
	return cliproxyexecutor.Options{Stream: stream, SourceFormat: sdktranslator.FormatClaude, ResponseFormat: sdktranslator.FormatClaude, Headers: headers}
}

func withHostedSearchHeader(opts cliproxyexecutor.Options) cliproxyexecutor.Options {
	if opts.Headers == nil {
		opts.Headers = make(http.Header)
	}
	opts.Headers.Set(headerCursorAgentV1NativeWebSearch, "true")
	return opts
}

func replaceJSONID(payload string) string {
	start := strings.Index(payload, `"id":"`)
	if start < 0 {
		return payload
	}
	valueStart := start + len(`"id":"`)
	end := strings.Index(payload[valueStart:], `"`)
	if end < 0 {
		return payload
	}
	return payload[:valueStart] + "ignored" + payload[valueStart+end:]
}

var agentV1ToolCallIDPattern = regexp.MustCompile(`toolu_bf_agentv1_u7_c6401_[a-f0-9]{32}`)

func agentV1ToolCallIDs(payload []byte) []string {
	matches := agentV1ToolCallIDPattern.FindAllString(string(payload), -1)
	unique := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if _, exists := seen[match]; exists {
			continue
		}
		seen[match] = struct{}{}
		unique = append(unique, match)
	}
	return unique
}

// Observe the ownership handoff, without bypassing its durable write.
type pendingSavedStore struct {
	StateStore
	saved chan struct{}
}

func (s *pendingSavedStore) SavePending(owner OwnerLease, pending []PendingTool) error {
	err := s.StateStore.SavePending(owner, pending)
	if err == nil {
		close(s.saved)
	}
	return err
}
