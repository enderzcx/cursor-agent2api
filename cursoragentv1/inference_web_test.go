package cursoragentv1

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

type fakeInferenceHostedClient struct {
	searches  int
	fetches   int
	lastQuery string
}

func (f *fakeInferenceHostedClient) Search(_ context.Context, _ RunRequest, query, _ string) (*HostedSearch, any, error) {
	f.searches++
	f.lastQuery = query
	search := &HostedSearch{Kind: HostedSearchKindWebSearch, Query: query, Content: "official answer", References: []HostedSearchReference{{Title: "Cursor", URL: "https://cursor.com/docs", Chunk: "official docs"}}}
	return search, map[string]any{"answer": search.Content, "documents": []any{map[string]any{"url": search.References[0].URL, "title": search.References[0].Title, "text": search.References[0].Chunk}}}, nil
}

func (f *fakeInferenceHostedClient) Fetch(_ context.Context, _ RunRequest, rawURL string) (*HostedSearch, any, error) {
	f.fetches++
	fetch := &HostedSearch{Kind: HostedSearchKindWebFetch, Query: rawURL, URL: rawURL, Content: "fetched content"}
	return fetch, map[string]any{"content": fetch.Content}, nil
}

func TestCursorInferenceHostedClientUsesOfficialRPCs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer access-token", r.Header.Get("Authorization"))
		require.Equal(t, runtimeSand, r.Header.Get("X-Cursor-Client-Type"))
		require.Equal(t, "cli-test", r.Header.Get("X-Cursor-Client-Version"))
		require.Equal(t, "1", r.Header.Get("Connect-Protocol-Version"))
		switch r.URL.Path {
		case inferenceWebSearchPath:
			var request map[string]any
			require.NoError(t, jsonx.DecodeJson(r.Body, &request))
			require.Equal(t, "latest Cursor docs", request["searchTerm"])
			require.Equal(t, "grok-4.6", request["modelId"])
			body, err := jsonx.Marshal(map[string]any{"answer": "answer", "documents": []any{map[string]any{"url": "https://cursor.com/docs", "title": "Docs", "text": "body"}}})
			require.NoError(t, err)
			_, _ = w.Write(body)
		case inferenceWebFetchPath:
			var request map[string]any
			require.NoError(t, jsonx.DecodeJson(r.Body, &request))
			require.Equal(t, "https://cursor.com/docs", request["url"])
			body, err := jsonx.Marshal(map[string]any{"success": map[string]any{"content": "fetched"}})
			require.NoError(t, err)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &cursorInferenceHostedClient{baseURL: server.URL, client: server.Client()}
	request := RunRequest{AccessToken: "access-token", ClientVersion: "cli-test", Model: "cursor-grok-4.6-medium"}
	search, _, err := client.Search(context.Background(), request, "latest Cursor docs", "test")
	require.NoError(t, err)
	require.Equal(t, "answer", search.Content)
	require.Len(t, search.References, 1)
	fetch, _, err := client.Fetch(context.Background(), request, "https://cursor.com/docs")
	require.NoError(t, err)
	require.Equal(t, "fetched", fetch.Content)
}

func TestCursorInferenceHostedClientEnforcesCallerDomainRestrictions(t *testing.T) {
	fetched := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case inferenceWebSearchPath:
			body, err := jsonx.Marshal(map[string]any{"answer": "blended answer", "documents": []any{
				map[string]any{"url": "https://docs.python.org/3/library/os.html", "title": "os", "text": "allowed"},
				map[string]any{"url": "https://random.blog/os", "title": "blog", "text": "filtered"},
			}})
			require.NoError(t, err)
			_, _ = w.Write(body)
		case inferenceWebFetchPath:
			fetched++
			body, err := jsonx.Marshal(map[string]any{"success": map[string]any{"content": "fetched"}})
			require.NoError(t, err)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &cursorInferenceHostedClient{baseURL: server.URL, client: server.Client()}
	request := RunRequest{
		AccessToken: "access-token", ClientVersion: "cli-test", Model: "cursor-grok-4.6-medium",
		HostedSearchDomains: hostedDomainPolicy{Allowed: []string{"docs.python.org"}},
		HostedFetchDomains:  hostedDomainPolicy{Blocked: []string{"random.blog"}},
	}
	search, content, err := client.Search(context.Background(), request, "os module", "")
	require.NoError(t, err)
	require.Len(t, search.References, 1)
	require.Equal(t, "https://docs.python.org/3/library/os.html", search.References[0].URL)
	// The blended answer was built from filtered-out sources too, so it is
	// withheld; the model only sees permitted documents.
	require.Empty(t, search.Content)
	result := content.(map[string]any)
	require.NotContains(t, result, "answer")
	require.Len(t, result["documents"], 1)

	fetch, fetchContent, err := client.Fetch(context.Background(), request, "https://random.blog/os")
	require.NoError(t, err)
	require.Zero(t, fetched)
	require.Contains(t, fetch.Error, "allowed domains")
	require.Contains(t, fetchContent.(map[string]any), "error")

	_, _, err = client.Fetch(context.Background(), request, "https://docs.python.org/3/")
	require.NoError(t, err)
	require.Equal(t, 1, fetched)
}

func TestPrepareInferenceTurnAttachesPrivateHostedTools(t *testing.T) {
	turn := translatedTurn{
		claudeRequest: []byte(`{"messages":[{"role":"user","content":"latest"}]}`),
		managed: ManagedRequest{Run: RunRequest{
			RuntimeProfile: runtimeSand, Model: "cursor-grok-4.6-medium", ConversationID: "search-session",
			UserText: "latest", AllowHostedSearch: true, AllowHostedFetch: true, RequireHostedSearch: true,
		}},
	}
	err := prepareInferenceTurn(&turn, cliproxyexecutor.Request{Format: sdktranslator.FormatClaude})
	require.NoError(t, err)
	require.True(t, inferenceToolDeclared(turn.managed.Run.Tools, inferenceWebSearchToolName))
	require.True(t, inferenceToolDeclared(turn.managed.Run.Tools, inferenceWebFetchToolName))
	require.Contains(t, turn.managed.Run.SystemText, "must call "+inferenceWebSearchToolName)
}

func TestPrepareInferenceTurnRejectsPrivateHostedToolNameCollision(t *testing.T) {
	run := RunRequest{AllowHostedSearch: true, Tools: []ToolDefinition{{Name: inferenceWebSearchToolName}}}
	err := attachInferenceHostedTools(&run)
	require.ErrorContains(t, err, "reserved hosted-web name")
}

func TestRequiredResponsesWebSearchIsSandOnly(t *testing.T) {
	payload := []byte(`{"model":"cursor-grok-4.6-medium","input":"latest","tools":[{"type":"web_search","external_web_access":true}],"tool_choice":"required"}`)
	_, hosted, err := normalizeAgentV1ResponsesRequest(payload, true)
	require.NoError(t, err)
	require.True(t, hosted.AllowSearch)
	require.True(t, hosted.RequireSearch)

	_, _, err = normalizeAgentV1ResponsesRequest(payload, false)
	require.ErrorContains(t, err, "required web_search is unsupported")
}

func TestTranslateRequiredResponsesHostedChoiceFormsAreSandOnly(t *testing.T) {
	choices := []string{`"required"`, `"web_search"`, `{"type":"web_search"}`, `{"type":"web_search_preview"}`}
	for _, choice := range choices {
		t.Run(choice, func(t *testing.T) {
			payload := []byte(`{"model":"cursor-grok-4.6-medium","input":"latest","tools":[{"type":"web_search","external_web_access":true}],"tool_choice":` + choice + `}`)
			request := claudeExecutorRequest("responses-required-search", string(payload))
			request.Model = "cursor-grok-4.6-medium"
			request.Format = sdktranslator.FormatOpenAIResponse
			options := withHostedSearchHeader(cliproxyexecutor.Options{
				SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse,
				OriginalRequest: payload, Metadata: map[string]any{"beefapi_cursor_runtime_profile": runtimeSand},
			})
			turn, err := translateExecutionRequest(request, options, "tenant:1:user:1:channel:301", "account", defaultInferenceBaseURL, "access", "cli-test")
			require.NoError(t, err)
			require.True(t, turn.managed.Run.AllowHostedSearch)
			require.True(t, turn.managed.Run.RequireHostedSearch)

			options.Metadata["beefapi_cursor_runtime_profile"] = runtimeAgentV1
			_, err = translateExecutionRequest(request, options, "tenant:1:user:1:channel:301", "account", defaultInferenceBaseURL, "access", "cli-test")
			require.ErrorContains(t, err, "required web_search is unsupported")
		})
	}
}

func TestConsumeInferenceStreamExecutesHostedSearchAndContinues(t *testing.T) {
	tool := appendStringField(nil, 1, "search-1")
	tool = appendStringField(tool, 2, inferenceWebSearchToolName)
	tool = appendStringField(tool, 3, `{"search_term":"Cursor docs","explanation":"verify"}`)
	tool = appendVarintField(tool, 4, 1)
	wire := append(frameConnect(appendBytesField(nil, 2, tool)), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	client := &fakeInferenceHostedClient{}
	tracker := &inferenceHostedUseTracker{}
	request := RunRequest{
		HistoryRequestKey: "history", Model: "cursor-grok-4.6-medium", AllowHostedSearch: true, RequireHostedSearch: true,
		Tools: []ToolDefinition{{Name: inferenceWebSearchToolName}},
	}
	var events []Event
	outcome := inferenceSegmentOutcome{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), request, func(event Event) { events = append(events, event) }, &outcome, client, tracker)
	require.NoError(t, err)
	require.True(t, outcome.Continue)
	require.False(t, outcome.ToolSeen)
	require.NotEmpty(t, outcome.Checkpoint)
	require.Equal(t, 1, client.searches)
	require.Equal(t, []EventType{EventHostedSearchCall, EventHostedSearchStarted, EventHostedSearchCompleted, EventCheckpoint}, inferenceEventTypes(events))

	checkpoint, err := decodeInferenceCheckpoint(outcome.Checkpoint)
	require.NoError(t, err)
	require.Len(t, checkpoint.Messages, 2)
	var toolResult map[string]any
	require.NoError(t, jsonx.Unmarshal(checkpoint.Messages[1], &toolResult))
	require.Equal(t, "tool", toolResult["role"])

	text := appendStringField(nil, 1, "SEARCH_OK")
	secondWire := append(frameConnect(appendBytesField(nil, 1, text)), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events = nil
	err = consumeInferenceStream(context.Background(), bytes.NewReader(secondWire), request, func(event Event) { events = append(events, event) }, nil, client, tracker)
	require.NoError(t, err)
	require.Contains(t, inferenceEventTypes(events), EventDone)
}

func TestConsumeInferenceStreamKeepsCallerToolPendingBesideHostedResult(t *testing.T) {
	searchTool := appendStringField(nil, 1, "search-mixed")
	searchTool = appendStringField(searchTool, 2, inferenceWebSearchToolName)
	searchTool = appendStringField(searchTool, 3, `{"search_term":"Cursor docs"}`)
	searchTool = appendVarintField(searchTool, 4, 1)
	callerTool := appendStringField(nil, 1, "caller-mixed")
	callerTool = appendStringField(callerTool, 2, "lookup")
	callerTool = appendStringField(callerTool, 3, `{"id":1}`)
	callerTool = appendVarintField(callerTool, 4, 1)
	wire := append(frameConnect(appendBytesField(nil, 2, searchTool)), frameConnect(appendBytesField(nil, 2, callerTool))...)
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	request := RunRequest{
		HistoryRequestKey: "history", Model: "grok-4.6", AllowHostedSearch: true, RequireToolCall: true,
		Tools: []ToolDefinition{{Name: inferenceWebSearchToolName}, {Name: "lookup"}},
	}
	client := &fakeInferenceHostedClient{}
	tracker := &inferenceHostedUseTracker{}
	var events []Event
	outcome := inferenceSegmentOutcome{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), request, func(event Event) { events = append(events, event) }, &outcome, client, tracker)
	require.NoError(t, err)
	require.True(t, outcome.ToolSeen)
	require.True(t, outcome.Continue)
	checkpoint, err := decodeInferenceCheckpoint(outcome.Checkpoint)
	require.NoError(t, err)
	require.Equal(t, "lookup", inferenceCheckpointToolName(checkpoint.Messages, "caller-mixed"))
	require.Equal(t, []EventType{EventToolCall, EventHostedSearchCall, EventHostedSearchStarted, EventHostedSearchCompleted, EventCheckpoint, EventTurnEnded}, inferenceEventTypes(events))
}

func TestInferenceHostedToolEnforcesMaxUsesAndRequiredCompletion(t *testing.T) {
	limit := 1
	request := RunRequest{Model: "grok-4.6", HostedSearchMaxUses: &limit, RequireHostedSearch: true}
	client := &fakeInferenceHostedClient{}
	tracker := &inferenceHostedUseTracker{}
	call := inferenceToolCall{ID: "search-1", Name: inferenceWebSearchToolName, Arguments: `{"search_term":"one"}`}
	_, _, err := executeInferenceHostedCall(context.Background(), client, tracker, request, call)
	require.NoError(t, err)
	call.ID = "search-2"
	_, _, err = executeInferenceHostedCall(context.Background(), client, tracker, request, call)
	require.ErrorContains(t, err, "max_uses=1")
	require.NoError(t, tracker.validateRequired(request))
	require.ErrorContains(t, (&inferenceHostedUseTracker{}).validateRequired(request), "required hosted search")
}

func TestInferenceHostedFetchRunsInsideThePrivateLoop(t *testing.T) {
	client := &fakeInferenceHostedClient{}
	tracker := &inferenceHostedUseTracker{}
	request := RunRequest{Model: "grok-4.6", RequireHostedFetch: true}
	call := inferenceToolCall{ID: "fetch-1", Name: inferenceWebFetchToolName, Arguments: `{"url":"https://cursor.com/docs"}`}
	result, fetch, err := executeInferenceHostedCall(context.Background(), client, tracker, request, call)
	require.NoError(t, err)
	require.Equal(t, HostedSearchKindWebFetch, fetch.Kind)
	require.Equal(t, "fetched content", fetch.Content)
	require.False(t, result.IsError)
	require.Equal(t, 1, client.fetches)
	require.NoError(t, tracker.validateRequired(request))
}

func TestInferenceHostedSearchFallsBackToCurrentUserTextForEmptyModelArgs(t *testing.T) {
	client := &fakeInferenceHostedClient{}
	tracker := &inferenceHostedUseTracker{}
	request := RunRequest{Model: "grok-4.6", UserText: "official Cursor documentation"}
	call := inferenceToolCall{ID: "search-empty", Name: inferenceWebSearchToolName, Arguments: `{}`}
	_, _, err := executeInferenceHostedCall(context.Background(), client, tracker, request, call)
	require.NoError(t, err)
	require.Equal(t, "official Cursor documentation", client.lastQuery)
}

func TestInferenceHostedFetchRejectsCredentialedURL(t *testing.T) {
	client := &cursorInferenceHostedClient{}
	_, _, err := client.Fetch(context.Background(), RunRequest{}, "https://user:secret@example.com/private")
	require.ErrorContains(t, err, "must not contain credentials")
}

func inferenceEventTypes(events []Event) []EventType {
	types := make([]EventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}
