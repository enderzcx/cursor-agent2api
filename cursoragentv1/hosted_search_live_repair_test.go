package cursoragentv1

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestHostedSearchDefersStartUntilQueryReady(t *testing.T) {
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

	partial, err := proto.Marshal(webSearchPartial("ws-live", ""))
	require.NoError(t, err)
	started, err := proto.Marshal(webSearchStarted("ws-live", "Cursor CLI"))
	require.NoError(t, err)
	done, err := proto.Marshal(webSearchCompleted("ws-live", "Cursor CLI", "https://cursor.com/docs/cli/overview", "Cursor CLI"))
	require.NoError(t, err)
	upstream.data <- frameConnect(partial)
	select {
	case event := <-events:
		t.Fatalf("empty partial must not start hosted search, got %#v", event)
	default:
	}
	upstream.data <- frameConnect(started)
	start := <-events
	require.Equal(t, EventHostedSearchStarted, start.Type)
	require.Equal(t, "Cursor CLI", start.HostedSearch.Query)
	require.Equal(t, "ws-live", start.HostedSearch.CallID)
	upstream.data <- frameConnect(done)
	complete := <-events
	require.Equal(t, EventHostedSearchCompleted, complete.Type)
	require.Equal(t, "Cursor CLI", complete.HostedSearch.Query)
	require.Equal(t, "https://cursor.com/docs/cli/overview", complete.HostedSearch.References[0].URL)
	select {
	case event := <-events:
		require.NotEqual(t, EventHostedSearchStarted, event.Type)
	default:
	}
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)

	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			assertHostedSearchProjectionKeepsQuery(t, format, start, complete)
		})
	}
}

func TestHostedSearchCompletedOnlyLegacyStillStarts(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	events := make(chan Event, 8)
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), request, func(event Event) { events <- event })
	}()
	assertRunWasWritten(t, upstream)
	done, err := proto.Marshal(webSearchCompleted("ws-only", "q", "https://example.com", "Example"))
	require.NoError(t, err)
	upstream.data <- frameConnect(done)
	complete := <-events
	require.Equal(t, EventHostedSearchCompleted, complete.Type)
	require.Equal(t, "q", complete.HostedSearch.Query)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestHostedSearchCompletionDeliversWithoutQuery(t *testing.T) {
	events := []Event{
		{Type: EventHostedSearchCompleted, HostedSearch: &HostedSearch{CallID: "ws-empty", Kind: HostedSearchKindWebSearch}},
		{Type: EventText, Text: "no query"},
		{Type: EventDone},
	}
	projection, stream := runProjection(t, sdktranslator.FormatClaude, "msg-empty-search", events...)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	require.Contains(t, string(payload), `"query":""`)
	require.Contains(t, string(payload), `"type":"web_search_tool_result"`)
	require.NotEmpty(t, stream)
}

func TestLeadingHostedSearchLinksExtraction(t *testing.T) {
	chunk, err := os.ReadFile(filepath.Join("testdata", "hosted-search-links", "leading-links.txt"))
	require.NoError(t, err)
	extracted := parseLeadingHostedSearchLinks(string(chunk))
	require.Len(t, extracted, 2)
	require.Equal(t, "Cursor CLI", extracted[0].Title)
	require.Equal(t, "https://cursor.com/docs/cli/overview", extracted[0].URL)
	require.Empty(t, extracted[0].Chunk)
	require.Equal(t, "https://cursor.com/cli", extracted[1].URL)
	require.NotContains(t, extracted[0].URL, "install")
	require.NotContains(t, extracted[1].URL, "install")

	require.Empty(t, parseLeadingHostedSearchLinks("See https://cursor.com/docs/cli/overview first.\nLinks:\n1. [Later](https://cursor.com/cli)\n"))
	require.Empty(t, parseLeadingHostedSearchLinks("1. [Cursor CLI](https://cursor.com/docs/cli/overview)\n"))
	require.Empty(t, parseLeadingHostedSearchLinks("Links:\nnot a list\n1. [Cursor CLI](https://cursor.com/docs/cli/overview)\n"))
	malformed := parseLeadingHostedSearchLinks("Links:\n1. [Bad](file:///tmp/source)\n2. [OK](https://cursor.com/cli)\n")
	require.Len(t, malformed, 1)
	require.Equal(t, "https://cursor.com/cli", malformed[0].URL)

	boundary := parseLeadingHostedSearchLinks("Links:\n1. [Cursor CLI](https://cursor.com/docs/cli/overview)\nInstall: https://cursor.com/install\n2. [Should not](https://cursor.com/cli)\n")
	require.Len(t, boundary, 1)
	require.Equal(t, "https://cursor.com/docs/cli/overview", boundary[0].URL)
}

func TestWebSearchWrapperLinksReachAllProjections(t *testing.T) {
	chunk, err := os.ReadFile(filepath.Join("testdata", "hosted-search-links", "leading-links.txt"))
	require.NoError(t, err)
	completed, err := proto.Marshal(webSearchCompletedWithChunk("ws-wrap", "Cursor CLI", "Web search results", "", string(chunk)))
	require.NoError(t, err)
	decoded, err := decodeServerMessage(completed)
	require.NoError(t, err)
	require.Equal(t, wireToolCallCompleted, decoded.Type)
	require.Len(t, decoded.HostedSearch.References, 2)
	require.Equal(t, "https://cursor.com/docs/cli/overview", decoded.HostedSearch.References[0].URL)
	require.Equal(t, "https://cursor.com/cli", decoded.HostedSearch.References[1].URL)

	malformedURL, err := proto.Marshal(webSearchCompletedWithChunk("ws-bad-url", "Cursor CLI", hostedSearchWrapperTitle, "file:///tmp/source", string(chunk)))
	require.NoError(t, err)
	decodedBad, err := decodeServerMessage(malformedURL)
	require.NoError(t, err)
	require.Len(t, decodedBad.HostedSearch.References, 1)
	require.Equal(t, "file:///tmp/source", decodedBad.HostedSearch.References[0].URL)

	otherTitleMsg, err := proto.Marshal(webSearchCompletedWithChunk("ws-other-title", "Cursor CLI", "Some page", "", string(chunk)))
	require.NoError(t, err)
	decodedOther, err := decodeServerMessage(otherTitleMsg)
	require.NoError(t, err)
	require.Len(t, decodedOther.HostedSearch.References, 1)
	require.Equal(t, "", decodedOther.HostedSearch.References[0].URL)
	require.Equal(t, "Some page", decodedOther.HostedSearch.References[0].Title)

	structured := []HostedSearchReference{{Title: "Kept", URL: "https://example.com/source", Chunk: string(chunk)}}
	require.Equal(t, structured, expandHostedSearchLinkList(structured))

	invalidURL := []HostedSearchReference{{Title: hostedSearchWrapperTitle, URL: "file:///tmp/source", Chunk: string(chunk)}}
	require.Equal(t, invalidURL, expandHostedSearchLinkList(invalidURL))

	otherTitle := []HostedSearchReference{{Title: "Cursor CLI", URL: "", Chunk: string(chunk)}}
	require.Equal(t, otherTitle, expandHostedSearchLinkList(otherTitle))

	whitespaceTitle := []HostedSearchReference{{Title: " Web search results ", URL: "", Chunk: string(chunk)}}
	expanded := expandHostedSearchLinkList(whitespaceTitle)
	require.Len(t, expanded, 2)
	require.Equal(t, "https://cursor.com/docs/cli/overview", expanded[0].URL)

	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			usage := projectUsage(t, format,
				Event{Type: EventHostedSearchCompleted, HostedSearch: decoded.HostedSearch},
				Event{Type: EventText, Text: "ok"},
				Event{Type: EventDone},
			)
			require.EqualValues(t, 2, usage["cursor_agent_v1_citation_count"])
			projection, _ := runProjection(t, format, "msg-wrap-"+string(format),
				Event{Type: EventHostedSearchCompleted, HostedSearch: decoded.HostedSearch},
				Event{Type: EventText, Text: "ok"},
				Event{Type: EventDone},
			)
			payload, err := projection.nonStream()
			require.NoError(t, err)
			output := string(payload)
			require.Contains(t, output, "https://cursor.com/docs/cli/overview")
			require.Contains(t, output, "https://cursor.com/cli")
			require.NotContains(t, output, "https://cursor.com/install")
			require.NotContains(t, output, `"url":""`)
		})
	}
}

func assertHostedSearchProjectionKeepsQuery(t *testing.T, format sdktranslator.Format, start, complete Event) {
	t.Helper()
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: format,
		original:       []byte(`{"model":"claude-opus-5"}`),
		claudeRequest:  []byte(`{"model":"claude-opus-5"}`),
		managed:        ManagedRequest{SessionID: "msg-live-search"},
	})
	var starts int
	for _, event := range []Event{start, complete, {Type: EventText, Text: "done"}, {Type: EventDone}} {
		chunks, _, _, err := projection.handle(event)
		require.NoError(t, err)
		for _, chunk := range chunks {
			if strings.Contains(string(chunk), `"type":"server_tool_use"`) || strings.Contains(string(chunk), `"type":"web_search_call"`) {
				starts++
			}
		}
	}
	payload, err := projection.nonStream()
	require.NoError(t, err)
	output := string(payload)
	require.Contains(t, output, "Cursor CLI")
	if format == sdktranslator.FormatClaude {
		require.Contains(t, output, `"query":"Cursor CLI"`)
		require.NotContains(t, output, `"query":""`)
	}
	require.GreaterOrEqual(t, starts, 1)
}

func webSearchPartial(id, query string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_PartialToolCall{
					PartialToolCall: &agentv1.PartialToolCallUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{
									Args: &agentv1.WebSearchArgs{SearchTerm: query, ToolCallId: id},
								},
							},
						},
					},
				},
			},
		},
	}
}

func webSearchCompletedWithChunk(id, query, title, url, chunk string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionUpdate{
			InteractionUpdate: &agentv1.InteractionUpdate{
				Message: &agentv1.InteractionUpdate_ToolCallCompleted{
					ToolCallCompleted: &agentv1.ToolCallCompletedUpdate{
						CallId: id,
						ToolCall: &agentv1.ToolCall{
							Tool: &agentv1.ToolCall_WebSearchToolCall{
								WebSearchToolCall: &agentv1.WebSearchToolCall{
									Args: &agentv1.WebSearchArgs{SearchTerm: query, ToolCallId: id},
									Result: &agentv1.WebSearchResult{Result: &agentv1.WebSearchResult_Success{Success: &agentv1.WebSearchSuccess{References: []*agentv1.WebSearchReference{
										{Title: title, Url: url, Chunk: chunk},
									}}}},
								},
							},
						},
					},
				},
			},
		},
	}
}
