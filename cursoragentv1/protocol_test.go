package cursoragentv1

import (
	"context"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestDecodeTypedWebSearchToolCallStartedAndCompleted(t *testing.T) {
	started, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId: "ws-1",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{
									Args: &agentv1.WebSearchArgs{SearchTerm: "Grok Bot use cases", ToolCallId: "ws-1"},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err := decodeServerMessage(started)
	require.NoError(t, err)
	require.Equal(t, wireToolCallStarted, decoded.Type)
	require.Equal(t, "ws-1", decoded.HostedSearch.CallID)
	require.Equal(t, HostedSearchKindWebSearch, decoded.HostedSearch.Kind)
	require.Equal(t, "Grok Bot use cases", decoded.HostedSearch.Query)

	completed, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: "ws-1",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{
									Args:   &agentv1.WebSearchArgs{SearchTerm: "Grok Bot use cases", ToolCallId: "ws-1"},
									Result: &agentv1.WebSearchResult{Result: &agentv1.WebSearchResult_Success{Success: &agentv1.WebSearchSuccess{References: []*agentv1.WebSearchReference{{Title: "Use cases", Url: "https://docs.x.ai/grok-bot/use-cases", Chunk: "Grok Bot can search"}}}}},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err = decodeServerMessage(completed)
	require.NoError(t, err)
	require.Equal(t, wireToolCallCompleted, decoded.Type)
	require.Equal(t, "https://docs.x.ai/grok-bot/use-cases", decoded.HostedSearch.References[0].URL)
}

func TestDecodeTypedWebFetchAndExaResults(t *testing.T) {
	fetch, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: "fetch-1",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebFetchToolCall{
								WebFetchToolCall: &agentv1.FetchToolCall{
									Args:   &agentv1.FetchArgs{Url: "https://docs.x.ai/grok-bot/use-cases", ToolCallId: "fetch-1"},
									Result: &agentv1.FetchResult{Result: &agentv1.FetchResult_Success{Success: &agentv1.FetchSuccess{Url: "https://docs.x.ai/grok-bot/use-cases", Content: "fetched body"}}},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err := decodeServerMessage(fetch)
	require.NoError(t, err)
	require.Equal(t, HostedSearchKindWebFetch, decoded.HostedSearch.Kind)
	require.Equal(t, "fetched body", decoded.HostedSearch.Content)

	legacyFetch, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId: "fetch-24",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_FetchToolCall{
								FetchToolCall: &agentv1.FetchToolCall{
									Args: &agentv1.FetchArgs{Url: "https://example.com/field-24", ToolCallId: "fetch-24"},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err = decodeServerMessage(legacyFetch)
	require.NoError(t, err)
	require.Equal(t, wireToolCallStarted, decoded.Type)
	require.Equal(t, HostedSearchKindWebFetch, decoded.HostedSearch.Kind)
	require.Equal(t, "https://example.com/field-24", decoded.HostedSearch.URL)

	legacyDone, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: "fetch-24",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_FetchToolCall{
								FetchToolCall: &agentv1.FetchToolCall{
									Args:   &agentv1.FetchArgs{Url: "https://example.com/field-24", ToolCallId: "fetch-24"},
									Result: &agentv1.FetchResult{Result: &agentv1.FetchResult_Success{Success: &agentv1.FetchSuccess{Url: "https://example.com/field-24", Content: "field 24 body"}}},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err = decodeServerMessage(legacyDone)
	require.NoError(t, err)
	require.Equal(t, wireToolCallCompleted, decoded.Type)
	require.Equal(t, "field 24 body", decoded.HostedSearch.Content)

	exa, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: "exa-1",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_ExaSearchToolCall{
								ExaSearchToolCall: &agentv1.ExaSearchToolCall{
									Args:   &agentv1.ExaSearchArgs{Query: "Grok Bot", ToolCallId: "exa-1"},
									Result: &agentv1.ExaSearchResult{Result: &agentv1.ExaSearchResult_Success{Success: &agentv1.ExaSearchSuccess{References: []*agentv1.ExaSearchReference{{Title: "Exa", Url: "https://exa.ai", Text: "index"}}}}},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err = decodeServerMessage(exa)
	require.NoError(t, err)
	require.Equal(t, HostedSearchKindExaSearch, decoded.HostedSearch.Kind)
	require.Equal(t, "https://exa.ai", decoded.HostedSearch.References[0].URL)
}

func TestDecodeHidesNativeShellAndMCPStreamAnnouncements(t *testing.T) {
	shell, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId:   "shell-1",
						ToolCall: &agentv1.ToolCall{Tool: &agentv1.ToolCall_ShellToolCall{ShellToolCall: &agentv1.ShellToolCall{}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err := decodeServerMessage(shell)
	require.NoError(t, err)
	require.Equal(t, wireIgnored, decoded.Type)

	mcp, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId:   "mcp-1",
						ToolCall: &agentv1.ToolCall{Tool: &agentv1.ToolCall_McpToolCall{McpToolCall: &agentv1.McpToolCall{Args: &agentv1.McpArgs{Name: "Bash", ToolCallId: "mcp-1"}}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err = decodeServerMessage(mcp)
	require.NoError(t, err)
	require.Equal(t, wireStreamMCP, decoded.Type)
	require.Equal(t, "Bash", decoded.ToolName)
	require.Equal(t, "mcp-1", decoded.ToolCallID)
}

func TestDecodeFailsClosedOnUnknownToolCallField(t *testing.T) {
	tool := appendBytesField(nil, 88, []byte("drift"))
	started := appendStringField(nil, 1, "unknown-1")
	started = appendBytesField(started, 2, tool)
	update := appendBytesField(nil, 2, started)
	payload := appendBytesField(nil, agentServerInteractionUpdate, update)
	decoded, err := decodeServerMessage(payload)
	require.Nil(t, decoded)
	require.ErrorContains(t, err, "protocol drift")
	require.ErrorContains(t, err, "88")
}

func TestDecodeFailsClosedOnUnknownInteractionUpdate(t *testing.T) {
	payload := appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 42, []byte{0x08, 0x01}))
	decoded, err := decodeServerMessage(payload)
	require.Nil(t, decoded)
	require.ErrorContains(t, err, "protocol drift")
}

func TestDecodeTreatsFeedbackRequestAsAdvisory(t *testing.T) {
	decoded, err := decodeServerMessage(encodeServerFeedbackRequest())
	require.NoError(t, err)
	require.Equal(t, wireUnknown, decoded.Type)
	require.Empty(t, decoded.Text)
	require.Zero(t, decoded.ID)
	require.Empty(t, decoded.ToolCallID)
}

func TestDecodeFeedbackRequestLeavesTextToolAndTurnIntact(t *testing.T) {
	advisory := encodeServerFeedbackRequest()
	text := appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 1, appendStringField(nil, 1, "hello")))
	decoded, err := decodeServerMessage(append(append([]byte(nil), advisory...), text...))
	require.NoError(t, err)
	require.Equal(t, wireTextDelta, decoded.Type)
	require.Equal(t, "hello", decoded.Text)

	started, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId: "ws-advisory",
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{
									Args: &agentv1.WebSearchArgs{SearchTerm: "q", ToolCallId: "ws-advisory"},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err = decodeServerMessage(append(append([]byte(nil), advisory...), started...))
	require.NoError(t, err)
	require.Equal(t, wireToolCallStarted, decoded.Type)
	require.Equal(t, "ws-advisory", decoded.CallID)

	turn := appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 14, nil))
	decoded, err = decodeServerMessage(append(append([]byte(nil), advisory...), turn...))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, decoded.Type)
}

func TestDecodeRejectsMalformedFeedbackRequest(t *testing.T) {
	wrongWire := appendBytesField(nil, agentServerInteractionUpdate, appendVarintField(nil, 21, 1))
	decoded, err := decodeServerMessage(wrongWire)
	require.Nil(t, decoded)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "req-")

	truncated := appendBytesField(nil, agentServerInteractionUpdate, protowire.AppendTag(nil, 21, protowire.BytesType))
	truncated = protowire.AppendVarint(truncated, 8)
	truncated = append(truncated, 0x01)
	decoded, err = decodeServerMessage(truncated)
	require.Nil(t, decoded)
	require.Error(t, err)

	malformedKnown := appendBytesField(nil, 21, appendVarintField(nil, 1, 7))
	decoded, err = decodeServerMessage(appendBytesField(nil, agentServerInteractionUpdate, malformedKnown))
	require.Nil(t, decoded)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "req-")
}

func TestDecodeFeedbackRequestStillRejectsUnknown42(t *testing.T) {
	update := appendBytesField(nil, 21, appendStringField(nil, 1, "req-1"))
	update = appendBytesField(update, 42, []byte{0x08, 0x01})
	decoded, err := decodeServerMessage(appendBytesField(nil, agentServerInteractionUpdate, update))
	require.Nil(t, decoded)
	require.ErrorContains(t, err, "protocol drift")
	require.ErrorContains(t, err, "42")
}

func encodeServerFeedbackRequest() []byte {
	body := appendStringField(nil, 1, "req-1")
	body = appendStringField(body, 2, "composer-2")
	body = appendBytesField(body, 3, appendStringField(appendStringField(nil, 1, "cat"), 2, "label"))
	body = appendVarintField(body, 5, 1)
	body = appendStringField(body, 6, "title")
	return appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 21, body))
}

func TestDecodeThinkingAndTokenDeltaStillWork(t *testing.T) {
	thinking, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ThinkingDelta{ThinkingDelta: &agentv1.ThinkingDeltaUpdate{Text: "plan"}},
			},
		},
	})
	require.NoError(t, err)
	decoded, err := decodeServerMessage(thinking)
	require.NoError(t, err)
	require.Equal(t, wireThinkingDelta, decoded.Type)
	require.Equal(t, "plan", decoded.Text)
}

func TestRunnerCorrelatesInterleavedHostedSearch(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	events := make(chan Event, 16)
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), request, func(event Event) { events <- event })
	}()
	assertRunWasWritten(t, upstream)

	firstStart, err := proto.Marshal(webSearchStarted("ws-a", "first"))
	require.NoError(t, err)
	secondStart, err := proto.Marshal(webSearchStarted("ws-b", "second"))
	require.NoError(t, err)
	firstDone, err := proto.Marshal(webSearchCompleted("ws-a", "first", "https://a.example", "A"))
	require.NoError(t, err)
	secondDone, err := proto.Marshal(webSearchCompleted("ws-b", "second", "https://b.example", "B"))
	require.NoError(t, err)
	upstream.data <- frameConnect(firstStart)
	upstream.data <- frameConnect(secondStart)
	first := <-events
	second := <-events
	require.Equal(t, EventHostedSearchStarted, first.Type)
	require.Equal(t, "ws-a", first.HostedSearch.CallID)
	require.Equal(t, EventHostedSearchStarted, second.Type)
	require.Equal(t, "ws-b", second.HostedSearch.CallID)
	upstream.data <- frameConnect(secondDone)
	upstream.data <- frameConnect(firstDone)
	completedB := <-events
	completedA := <-events
	require.Equal(t, "ws-b", completedB.HostedSearch.CallID)
	require.Equal(t, "https://b.example", completedB.HostedSearch.References[0].URL)
	require.Equal(t, "ws-a", completedA.HostedSearch.CallID)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestRunnerAnswersAllowlistPrecheckWithoutEnablingNativeShell(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, nil) }()
	assertRunWasWritten(t, upstream)

	precheck, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_ExecServerMessage{
			ExecServerMessage: &agentv1.ExecServerMessage{
				Id:     41,
				ExecId: "exec-allowlist",
				Message: &agentv1.ExecServerMessage_WebFetchAllowlistPrecheckArgs{
					WebFetchAllowlistPrecheckArgs: &agentv1.WebFetchAllowlistPrecheckArgs{Url: "https://example.com"},
				},
			},
		},
	})
	require.NoError(t, err)
	upstream.data <- frameConnect(precheck)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case write := <-upstream.writes:
			if topLevelField(t, write) != agentClientExecMessage {
				continue
			}
			payload := write[connectHeaderSize:]
			client := &agentv1.AgentClientMessage{}
			require.NoError(t, proto.Unmarshal(payload, client))
			result := client.GetExecClientMessage().GetWebFetchAllowlistPrecheckResult()
			require.NotNil(t, result)
			require.False(t, result.GetAllowlisted())
			return
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatal("missing allowlist precheck reply")
}

func TestRunnerHidesNativeShellToolCallFromCallerTools(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	events := make(chan Event, 4)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)
	shell, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId:   "shell-hidden",
						ToolCall: &agentv1.ToolCall{Tool: &agentv1.ToolCall_PiBashToolCall{PiBashToolCall: &agentv1.PiBashToolCall{}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	upstream.data <- frameConnect(shell)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	for {
		select {
		case event := <-events:
			require.NotEqual(t, EventToolCall, event.Type)
			require.NotEqual(t, EventHostedSearchStarted, event.Type)
			require.NotEqual(t, EventHostedSearchCompleted, event.Type)
		default:
			return
		}
	}
}

func TestProjectionEmitsClaudeServerToolUseNotCallerWebSearch(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: sdktranslator.FormatClaude,
		managed:        ManagedRequest{SessionID: "msg-search"},
	})
	search := &HostedSearch{
		CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "Grok Bot use cases",
		References: []HostedSearchReference{{Title: "Use cases", URL: "https://docs.x.ai/grok-bot/use-cases", Chunk: "Grok Bot can search"}},
	}
	chunks, _, _, err := projection.handle(Event{Type: EventThinking, Text: "looking it up"})
	require.NoError(t, err)
	require.Empty(t, chunks)
	_, _, _, err = projection.handle(Event{Type: EventHostedSearchStarted, HostedSearch: search})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventHostedSearchCompleted, HostedSearch: search})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventText, Text: "Grok Bot can search."})
	require.NoError(t, err)
	_, terminal, _, err := projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	require.True(t, terminal)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	output := string(payload)
	require.NotContains(t, output, `"type":"thinking"`)
	require.NotContains(t, output, `"signature"`)
	require.Contains(t, output, `"type":"server_tool_use"`)
	require.Contains(t, output, `"name":"web_search"`)
	require.Contains(t, output, `"type":"web_search_tool_result"`)
	require.Contains(t, output, `"type":"web_search_result_location"`)
	require.NotContains(t, output, `"name":"WebSearch"`)
	require.NotContains(t, output, `"type":"tool_use"`)
	require.NotContains(t, projection.claudeSSE.String(), `"type":"thinking_delta"`)
	require.Contains(t, projection.claudeSSE.String(), `"type":"server_tool_use"`)
}

func TestClaudeThinkingIsOmittedWithoutAnthropicSignature(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: sdktranslator.FormatClaude,
		managed:        ManagedRequest{SessionID: "msg-think"},
	})
	chunks, _, _, err := projection.handle(Event{Type: EventThinking, Text: "secret plan"})
	require.NoError(t, err)
	require.Empty(t, chunks)
	_, terminal, _, err := projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	require.True(t, terminal)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.NotContains(t, string(payload), `"type":"thinking"`)
	require.NotContains(t, string(payload), `"type":"redacted_thinking"`)
	require.NotContains(t, projection.claudeSSE.String(), `"thinking"`)
}

func TestProjectionResponsesEquivalentDoesNotEmitFunctionCall(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAIResponse,
		original:       []byte(`{"model":"gpt-5","input":"latest"}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}]}`),
		managed:        ManagedRequest{SessionID: "resp-search"},
	})
	search := &HostedSearch{CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "latest", References: []HostedSearchReference{{Title: "Example", URL: "https://example.com", Chunk: "hello"}}}
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventHostedSearchStarted, HostedSearch: search},
		{Type: EventHostedSearchCompleted, HostedSearch: search},
		{Type: EventText, Text: "hello"},
		{Type: EventDone},
	} {
		chunks, terminal, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
		if event.Type == EventDone {
			require.True(t, terminal)
		}
	}
	joined := string(bytesJoin(stream))
	require.Contains(t, joined, `"type":"web_search_call"`)
	require.NotContains(t, joined, `"type":"function_call"`)
	require.NotContains(t, joined, `"type":"server_tool_use"`)
	require.NotContains(t, joined, `"name":"WebSearch"`)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.Contains(t, string(payload), `"type":"web_search_call"`)
	require.Contains(t, string(payload), `"type":"url_citation"`)
	require.NotContains(t, string(payload), `"type":"function_call"`)
	require.NotContains(t, string(payload), `"type":"server_tool_use"`)
	requireMonotonicResponsesSequence(t, stream)
}

func requireMonotonicResponsesSequence(t *testing.T, chunks [][]byte) {
	t.Helper()
	var last int
	seen := 0
	joined := string(bytesJoin(chunks))
	for _, line := range strings.Split(joined, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var root map[string]any
		if err := jsonx.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &root); err != nil {
			continue
		}
		raw, ok := root["sequence_number"]
		if !ok {
			continue
		}
		seq := int(raw.(float64))
		if seen > 0 {
			require.Equal(t, last+1, seq)
		} else {
			require.Equal(t, 1, seq)
		}
		last = seq
		seen++
	}
	require.Greater(t, seen, 1)
}

func TestProjectionChatUsesAnnotationsNotToolCalls(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAI,
		original:       []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}]}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}]}`),
		managed:        ManagedRequest{SessionID: "chat-search"},
	})
	search := &HostedSearch{CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "latest", References: []HostedSearchReference{{Title: "Example", URL: "https://example.com", Chunk: "hello"}}}
	_, _, _, err := projection.handle(Event{Type: EventHostedSearchCompleted, HostedSearch: search})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventText, Text: "hello"})
	require.NoError(t, err)
	chunks, terminal, _, err := projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	require.True(t, terminal)
	joined := string(bytesJoin(chunks))
	require.Contains(t, joined, `"type":"url_citation"`)
	require.NotContains(t, joined, `"function":{"name":"WebSearch"`)
	require.NotContains(t, joined, `"function":{"name":"web_search"`)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.Contains(t, string(payload), `"type":"url_citation"`)
	require.NotContains(t, string(payload), `"tool_calls"`)
}

func TestProjectionChatKeepsCallerToolCallsWithSearchAnnotations(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAI,
		original:       []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}],"tools":[{"type":"function","function":{"name":"Bash"}}]}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}],"tools":[{"name":"Bash","input_schema":{"type":"object"}}]}`),
		managed:        ManagedRequest{SessionID: "chat-search-tools"},
	})
	search := &HostedSearch{CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "latest", References: []HostedSearchReference{{Title: "Example", URL: "https://example.com"}}}
	_, _, _, err := projection.handle(Event{Type: EventHostedSearchCompleted, HostedSearch: search})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call_bash", Name: "Bash", Arguments: `{"command":"pwd"}`}})
	require.NoError(t, err)
	_, terminal, _, err := projection.handle(Event{Type: EventTurnEnded})
	require.NoError(t, err)
	require.True(t, terminal)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.Contains(t, string(payload), `"name":"Bash"`)
	require.Contains(t, string(payload), `"type":"url_citation"`)
	require.NotContains(t, string(payload), `"name":"WebSearch"`)
	require.NotContains(t, string(payload), `"name":"web_search"`)
}

func TestProjectionDoesNotDuplicateInterleavedTextBlocks(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: sdktranslator.FormatClaude,
		managed:        ManagedRequest{SessionID: "msg-interleave"},
	})
	search := &HostedSearch{CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "q", References: []HostedSearchReference{{Title: "T", URL: "https://example.com"}}}
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventText, Text: "before "},
		{Type: EventHostedSearchStarted, HostedSearch: search},
		{Type: EventHostedSearchCompleted, HostedSearch: search},
		{Type: EventText, Text: "after"},
		{Type: EventDone},
	} {
		chunks, terminal, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
		if event.Type == EventDone {
			require.True(t, terminal)
		}
	}
	payload, err := projection.nonStream()
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	content, ok := root["content"].([]any)
	require.True(t, ok)
	require.Equal(t, []string{"text", "server_tool_use", "web_search_tool_result", "text"}, contentBlockTypes(t, content))
	require.Equal(t, "before ", jsonString(content[0].(map[string]any)["text"]))
	require.Equal(t, "after", jsonString(content[3].(map[string]any)["text"]))
	require.NotContains(t, jsonString(content[3].(map[string]any)["text"]), "before")
	sse := projection.claudeSSE.String()
	starts := parseContentBlockStarts(t, sse)
	require.Equal(t, []string{"text", "server_tool_use", "web_search_tool_result", "text"}, sseBlockTypes(starts))
	require.Equal(t, []int{0, 1, 2, 3}, sseBlockIndexes(starts))
	require.Equal(t, "before ", collectIndexedDeltas(sse, "text_delta", "text")[0])
	require.Equal(t, "after", collectIndexedDeltas(sse, "text_delta", "text")[3])
	require.NotContains(t, string(bytesJoin(stream)), `"text":"before after"`)
}

func TestProjectionDoesNotDuplicateChatThinkingSegments(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAI,
		original:       []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`),
		managed:        ManagedRequest{SessionID: "chat-think-interleave"},
	})
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventThinking, Text: "plan "},
		{Type: EventText, Text: "answer"},
		{Type: EventThinking, Text: "more"},
		{Type: EventDone},
	} {
		chunks, terminal, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
		if event.Type == EventDone {
			require.True(t, terminal)
		}
	}
	require.Equal(t, []any{
		map[string]any{"type": "thinking", "thinking": "plan "},
		map[string]any{"type": "text", "text": "answer"},
		map[string]any{"type": "thinking", "thinking": "more"},
	}, projection.blocks)
	sse := projection.claudeSSE.String()
	starts := parseContentBlockStarts(t, sse)
	require.Equal(t, []string{"thinking", "text", "thinking"}, sseBlockTypes(starts))
	require.Equal(t, []int{0, 1, 2}, sseBlockIndexes(starts))
	require.Equal(t, "plan ", collectIndexedDeltas(sse, "thinking_delta", "thinking")[0])
	require.Equal(t, "answer", collectIndexedDeltas(sse, "text_delta", "text")[1])
	require.Equal(t, "more", collectIndexedDeltas(sse, "thinking_delta", "thinking")[2])
	require.NotContains(t, sse, `"thinking":"plan more"`)
	require.NotContains(t, string(bytesJoin(stream)), `"thinking":"plan more"`)
}

func TestResponsesInterleavedTextIsNotDuplicated(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAIResponse,
		original:       []byte(`{"model":"gpt-5","input":"latest"}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}]}`),
		managed:        ManagedRequest{SessionID: "resp-interleave"},
	})
	search := &HostedSearch{CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "q", References: []HostedSearchReference{{Title: "T", URL: "https://example.com"}}}
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventText, Text: "before "},
		{Type: EventHostedSearchStarted, HostedSearch: search},
		{Type: EventHostedSearchCompleted, HostedSearch: search},
		{Type: EventText, Text: "after"},
		{Type: EventDone},
	} {
		chunks, _, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
	}
	payload, err := projection.nonStream()
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	output, _ := root["output"].([]any)
	var texts []string
	for _, raw := range output {
		item, _ := raw.(map[string]any)
		if stringValue(item["type"]) != "message" {
			continue
		}
		content, _ := item["content"].([]any)
		part, _ := content[0].(map[string]any)
		texts = append(texts, jsonString(part["text"]))
	}
	require.Equal(t, []string{"before ", "after"}, texts)
	joined := string(bytesJoin(stream))
	require.Equal(t, []string{"before ", "after"}, responsesOutputTextDeltas(joined))
	require.NotContains(t, joined, `"delta":"before after"`)
}

func TestResponsesInterleavedReasoningIsNotDuplicated(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAIResponse,
		original:       []byte(`{"model":"gpt-5","input":"hi"}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`),
		managed:        ManagedRequest{SessionID: "resp-think-interleave"},
	})
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventThinking, Text: "plan "},
		{Type: EventText, Text: "answer"},
		{Type: EventThinking, Text: "more"},
		{Type: EventDone},
	} {
		chunks, _, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
	}
	payload, err := projection.nonStream()
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	require.Equal(t, []string{"reasoning", "message", "reasoning"}, responsesOutputTypes(t, root["output"]))
	require.Equal(t, []string{"plan ", "more"}, responsesReasoningTexts(t, root["output"]))
	require.Equal(t, []string{"answer"}, responsesMessageTexts(t, root["output"]))
	joined := string(bytesJoin(stream))
	require.Equal(t, []string{"plan ", "more"}, responsesReasoningDeltas(joined))
	require.NotContains(t, joined, `"delta":"plan more"`)
}

func TestProjectionField24FetchEmitsWebFetchServerTool(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: sdktranslator.FormatClaude,
		managed:        ManagedRequest{SessionID: "msg-fetch-24"},
	})
	search := &HostedSearch{CallID: "fetch-24", Kind: HostedSearchKindWebFetch, URL: "https://example.com/field-24", Query: "https://example.com/field-24", Content: "body"}
	_, _, _, err := projection.handle(Event{Type: EventHostedSearchStarted, HostedSearch: search})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventHostedSearchCompleted, HostedSearch: search})
	require.NoError(t, err)
	_, terminal, _, err := projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	require.True(t, terminal)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.Contains(t, string(payload), `"name":"web_fetch"`)
	require.Contains(t, string(payload), `"type":"web_fetch_tool_result"`)
	require.Contains(t, string(payload), `"https://example.com/field-24"`)
	require.NotContains(t, string(payload), `"type":"tool_use"`)
}

type sseBlockStart struct {
	Index int
	Type  string
}

func parseContentBlockStarts(t *testing.T, sse string) []sseBlockStart {
	t.Helper()
	var starts []sseBlockStart
	for _, line := range strings.Split(sse, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var root map[string]any
		if err := jsonx.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &root); err != nil {
			continue
		}
		if stringValue(root["type"]) != "content_block_start" {
			continue
		}
		block, _ := root["content_block"].(map[string]any)
		index, _ := root["index"].(float64)
		starts = append(starts, sseBlockStart{Index: int(index), Type: stringValue(block["type"])})
	}
	require.NotEmpty(t, starts)
	return starts
}

func sseBlockTypes(starts []sseBlockStart) []string {
	types := make([]string, 0, len(starts))
	for _, start := range starts {
		types = append(types, start.Type)
	}
	return types
}

func sseBlockIndexes(starts []sseBlockStart) []int {
	indexes := make([]int, 0, len(starts))
	for _, start := range starts {
		indexes = append(indexes, start.Index)
	}
	return indexes
}

func contentBlockTypes(t *testing.T, content []any) []string {
	t.Helper()
	types := make([]string, 0, len(content))
	for _, raw := range content {
		block, ok := raw.(map[string]any)
		require.True(t, ok)
		types = append(types, stringValue(block["type"]))
	}
	return types
}

func collectIndexedDeltas(sse, deltaType, field string) map[int]string {
	out := map[int]string{}
	for _, line := range strings.Split(sse, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var root map[string]any
		if err := jsonx.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &root); err != nil {
			continue
		}
		if jsonString(root["type"]) != "content_block_delta" {
			continue
		}
		delta, _ := root["delta"].(map[string]any)
		if jsonString(delta["type"]) != deltaType {
			continue
		}
		index, _ := root["index"].(float64)
		out[int(index)] += jsonString(delta[field])
	}
	return out
}

func responsesOutputTypes(t *testing.T, raw any) []string {
	t.Helper()
	output, ok := raw.([]any)
	require.True(t, ok)
	types := make([]string, 0, len(output))
	for _, item := range output {
		block, _ := item.(map[string]any)
		types = append(types, stringValue(block["type"]))
	}
	return types
}

func responsesReasoningTexts(t *testing.T, raw any) []string {
	t.Helper()
	output, _ := raw.([]any)
	var texts []string
	for _, item := range output {
		block, _ := item.(map[string]any)
		if jsonString(block["type"]) != "reasoning" {
			continue
		}
		summary, _ := block["summary"].([]any)
		require.NotEmpty(t, summary)
		part, _ := summary[0].(map[string]any)
		texts = append(texts, jsonString(part["text"]))
	}
	return texts
}

func responsesMessageTexts(t *testing.T, raw any) []string {
	t.Helper()
	output, _ := raw.([]any)
	var texts []string
	for _, item := range output {
		block, _ := item.(map[string]any)
		if jsonString(block["type"]) != "message" {
			continue
		}
		content, _ := block["content"].([]any)
		part, _ := content[0].(map[string]any)
		texts = append(texts, jsonString(part["text"]))
	}
	return texts
}

func jsonString(value any) string {
	text, _ := value.(string)
	return text
}

func responsesOutputTextDeltas(sse string) []string {
	return collectResponsesDeltas(sse, "response.output_text.delta")
}

func responsesReasoningDeltas(sse string) []string {
	return collectResponsesDeltas(sse, "response.reasoning_summary_text.delta")
}

func collectResponsesDeltas(sse, eventType string) []string {
	byIndex := map[int]string{}
	var order []int
	for _, line := range strings.Split(sse, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var root map[string]any
		if err := jsonx.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &root); err != nil {
			continue
		}
		if jsonString(root["type"]) != eventType {
			continue
		}
		index, _ := root["output_index"].(float64)
		key := int(index)
		if _, exists := byIndex[key]; !exists {
			order = append(order, key)
		}
		byIndex[key] += jsonString(root["delta"])
	}
	out := make([]string, 0, len(order))
	for _, key := range order {
		out = append(out, byIndex[key])
	}
	return out
}

func TestRunnerField24FetchIsHostedWebFetch(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedFetch = true
	events := make(chan Event, 8)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)
	started, err := proto.Marshal(fetchToolCallStarted("fetch-24", "https://example.com/field-24"))
	require.NoError(t, err)
	done, err := proto.Marshal(fetchToolCallCompleted("fetch-24", "https://example.com/field-24", "body"))
	require.NoError(t, err)
	upstream.data <- frameConnect(started)
	upstream.data <- frameConnect(done)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	start := <-events
	require.Equal(t, EventHostedSearchStarted, start.Type)
	require.Equal(t, HostedSearchKindWebFetch, start.HostedSearch.Kind)
	require.Equal(t, "https://example.com/field-24", start.HostedSearch.URL)
	complete := <-events
	require.Equal(t, EventHostedSearchCompleted, complete.Type)
	require.Equal(t, "body", complete.HostedSearch.Content)
}

func TestRunnerStreamMCPDoesNotSwallowMatchingExec(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	toolResults := make(chan ToolResult, 1)
	request := baseRunRequest()
	request.Tools = []ToolDefinition{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
	request.ToolResults = toolResults
	request.MaxParallelTools = 1
	events := make(chan Event, 8)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)
	streamed, err := proto.Marshal(streamMCPStarted("tool-31", "lookup"))
	require.NoError(t, err)
	upstream.data <- frameConnect(streamed)
	select {
	case event := <-events:
		t.Fatalf("stream MCP must wait for exec, got %#v", event)
	case <-time.After(20 * time.Millisecond):
	}
	upstream.data <- frameConnect(encodeServerMCP(t, 31, "exec-mcp", "tool-31", "lookup", map[string]any{"city": "Paris"}))
	toolCall := <-events
	require.Equal(t, EventToolCall, toolCall.Type)
	require.Equal(t, "tool-31", toolCall.ToolCall.ToolCallID)
	require.Equal(t, "lookup", toolCall.ToolCall.Name)
	park := <-events
	require.Equal(t, EventTurnEnded, park.Type)
	toolResults <- ToolResult{ToolCallID: "tool-31", Content: "ok"}
	require.Equal(t, EventToolResultAccepted, (<-events).Type)
	completed := streamMCPStarted("tool-31", "lookup")
	started := completed.GetInteractionUpdate().GetToolCallStarted()
	completed.GetInteractionUpdate().Message = &agentv1.InteractionUpdate_ToolCallCompleted{ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{CallId: started.CallId, ToolCall: started.ToolCall}}
	completedBytes, err := proto.Marshal(completed)
	require.NoError(t, err)
	upstream.data <- frameConnect(completedBytes)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestPartialStreamMCPWaitsForAuthoritativeExec(t *testing.T) {
	for _, message := range []*decodedWireMessage{{CallID: "partial"}, {ToolName: "lookup"}, {}} {
		streamed := map[string]decodedWireMessage{}
		err := recordStreamMCP([]ToolDefinition{{Name: "lookup"}}, message, nil, streamed)
		require.NoError(t, err)
		require.Empty(t, streamed, "partial transcript must neither execute nor create a false pending call")
	}
}

func TestRunnerStreamMCPWithoutExecFailsClosed(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.Tools = []ToolDefinition{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
	request.ToolResults = make(chan ToolResult)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(Event) {}) }()
	assertRunWasWritten(t, upstream)
	streamed, err := proto.Marshal(streamMCPStarted("tool-lost", "lookup"))
	require.NoError(t, err)
	upstream.data <- frameConnect(streamed)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	err = <-result
	require.ErrorContains(t, err, "streamed caller tool")
	require.ErrorContains(t, err, "lookup")
}

func TestRunnerUncataloguedStreamMCPIsIgnoredUntilEnd(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.Tools = []ToolDefinition{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
	events := make(chan Event, 8)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)
	streamed, err := proto.Marshal(streamMCPStarted("internal-1", "internal"))
	require.NoError(t, err)
	upstream.data <- frameConnect(streamed)
	select {
	case event := <-events:
		t.Fatalf("uncatalogued stream MCP must not become a caller tool, got %#v", event)
	case <-time.After(20 * time.Millisecond):
	}
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, EventUsageObservation, (<-events).Type)
	require.Equal(t, EventDone, (<-events).Type)
}

func TestRunnerExecThenStreamMCPDoesNotDuplicateCallerTool(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	toolResults := make(chan ToolResult, 1)
	request := baseRunRequest()
	request.Tools = []ToolDefinition{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
	request.ToolResults = toolResults
	request.MaxParallelTools = 1
	events := make(chan Event, 8)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerMCP(t, 31, "exec-mcp", "tool-31", "lookup", map[string]any{"city": "Paris"}))
	toolCall := <-events
	require.Equal(t, EventToolCall, toolCall.Type)
	require.Equal(t, "tool-31", toolCall.ToolCall.ToolCallID)
	require.Equal(t, EventTurnEnded, (<-events).Type)
	streamed, err := proto.Marshal(streamMCPStarted("tool-31", "lookup"))
	require.NoError(t, err)
	upstream.data <- frameConnect(streamed)
	select {
	case event := <-events:
		t.Fatalf("matching stream MCP must not swallow or duplicate exec, got %#v", event)
	case <-time.After(20 * time.Millisecond):
	}
	toolResults <- ToolResult{ToolCallID: "tool-31", Content: "ok"}
	require.Equal(t, EventToolResultAccepted, (<-events).Type)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestHostedSearchDisabledFailsClosedOnNativeSearchFrames(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	started, err := proto.Marshal(webSearchStarted("ws-1", "latest"))
	require.NoError(t, err)
	upstream.data <- frameConnect(started)
	require.ErrorContains(t, <-result, "hosted search is not enabled")
}

func bytesJoin(chunks [][]byte) []byte {
	return []byte(strings.Join(func() []string {
		parts := make([]string, 0, len(chunks))
		for _, chunk := range chunks {
			parts = append(parts, string(chunk))
		}
		return parts
	}(), ""))
}

func fetchToolCallStarted(id, url string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_FetchToolCall{
								FetchToolCall: &agentv1.FetchToolCall{Args: &agentv1.FetchArgs{Url: url, ToolCallId: id}},
							},
						},
					},
				},
			},
		},
	}
}

func fetchToolCallCompleted(id, url, content string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_FetchToolCall{
								FetchToolCall: &agentv1.FetchToolCall{
									Args:   &agentv1.FetchArgs{Url: url, ToolCallId: id},
									Result: &agentv1.FetchResult{Result: &agentv1.FetchResult_Success{Success: &agentv1.FetchSuccess{Url: url, Content: content}}},
								},
							},
						},
					},
				},
			},
		},
	}
}

func streamMCPStarted(id, name string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_McpToolCall{
								McpToolCall: &agentv1.McpToolCall{Args: &agentv1.McpArgs{Name: name, ToolName: name, ToolCallId: id}},
							},
						},
					},
				},
			},
		},
	}
}

func webSearchStarted(id, query string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallStarted{
					ToolCallStarted: &agentv1.ToolCallStartedUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{Args: &agentv1.WebSearchArgs{SearchTerm: query, ToolCallId: id}},
							},
						},
					},
				},
			},
		},
	}
}

func webSearchCompleted(id, query, url, title string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{
									Args:   &agentv1.WebSearchArgs{SearchTerm: query, ToolCallId: id},
									Result: &agentv1.WebSearchResult{Result: &agentv1.WebSearchResult_Success{Success: &agentv1.WebSearchSuccess{References: []*agentv1.WebSearchReference{{Title: title, Url: url, Chunk: title}}}}},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestUnknownServerEnvelopeFieldsAreForwardCompatible(t *testing.T) {
	for _, payload := range [][]byte{appendBytesField(nil, 8, []byte("extension")), appendVarintField(nil, 8, 1)} {
		decoded, err := decodeServerMessage(payload)
		require.NoError(t, err)
		require.Equal(t, wireUnknown, decoded.Type, "an extension is neither execution nor completion")
		text := appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 1, appendStringField(nil, 1, "hello")))
		decoded, err = decodeServerMessage(append(payload, text...))
		require.NoError(t, err)
		require.Equal(t, wireTextDelta, decoded.Type)
		require.Equal(t, "hello", decoded.Text)
	}
	_, err := decodeServerMessage([]byte{0x42, 0xff})
	require.Error(t, err, "malformed extension framing is still fatal")
}

func TestEncodeAllowlistPrecheckUsesTypedOneof(t *testing.T) {
	encoded, err := encodeAllowlistPrecheck(9, "exec-9", "web_fetch", false)
	require.NoError(t, err)
	client := &agentv1.AgentClientMessage{}
	require.NoError(t, proto.Unmarshal(encoded, client))
	require.False(t, client.GetExecClientMessage().GetWebFetchAllowlistPrecheckResult().GetAllowlisted())
}
