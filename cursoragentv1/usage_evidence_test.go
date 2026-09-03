package cursoragentv1

import (
	"bytes"
	"context"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestProjectionUsageEvidenceIsEventGrounded(t *testing.T) {
	searchWithCitation := &HostedSearch{
		CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "q",
		References: []HostedSearchReference{{Title: "T", URL: "https://example.com"}},
	}
	searchWithoutCitation := &HostedSearch{CallID: "ws-2", Kind: HostedSearchKindWebSearch, Query: "q"}

	t.Run("absent stays absent", func(t *testing.T) {
		usage := projectUsage(t, sdktranslator.FormatClaude,
			Event{Type: EventText, Text: "hello"},
			Event{Type: EventDone},
		)
		require.EqualValues(t, 0, usage["output_tokens"])
		_, hasApproved := usage["cursor_agent_v1_hosted_search_requests"]
		_, hasCompleted := usage["cursor_agent_v1_completed_search_count"]
		_, hasCitation := usage["cursor_agent_v1_citation_count"]
		_, hasProgress := usage["cursor_agent_v1_progress_event_count"]
		_, hasObserved := usage["cursor_agent_v1_output_token_delta_observed"]
		_, hasDelta := usage["cursor_agent_v1_output_token_delta"]
		require.False(t, hasApproved)
		require.False(t, hasCompleted)
		require.False(t, hasCitation)
		require.False(t, hasProgress)
		require.False(t, hasObserved)
		require.False(t, hasDelta)
	})

	t.Run("approved search does not imply completed search", func(t *testing.T) {
		usage := projectUsage(t, sdktranslator.FormatClaude,
			Event{Type: EventHostedSearchCall},
			Event{Type: EventDone},
		)
		require.EqualValues(t, 1, usage["cursor_agent_v1_hosted_search_requests"])
		_, hasCompleted := usage["cursor_agent_v1_completed_search_count"]
		_, hasCitation := usage["cursor_agent_v1_citation_count"]
		_, hasProgress := usage["cursor_agent_v1_progress_event_count"]
		require.False(t, hasCompleted)
		require.False(t, hasCitation)
		require.False(t, hasProgress)
	})

	t.Run("completed search with zero citations is observed zero", func(t *testing.T) {
		usage := projectUsage(t, sdktranslator.FormatClaude,
			Event{Type: EventHostedSearchStarted, HostedSearch: searchWithoutCitation},
			Event{Type: EventHostedSearchCompleted, HostedSearch: searchWithoutCitation},
			Event{Type: EventDone},
		)
		require.EqualValues(t, 1, usage["cursor_agent_v1_completed_search_count"])
		require.EqualValues(t, 0, usage["cursor_agent_v1_citation_count"])
		_, hasApproved := usage["cursor_agent_v1_hosted_search_requests"]
		require.False(t, hasApproved)
		require.EqualValues(t, 2, usage["cursor_agent_v1_progress_event_count"])
	})

	t.Run("positive evidence includes citations progress and token delta", func(t *testing.T) {
		usage := projectUsage(t, sdktranslator.FormatClaude,
			Event{Type: EventHostedSearchCall},
			Event{Type: EventThinking, Text: "plan"},
			Event{Type: EventThinkingCompleted},
			Event{Type: EventHostedSearchStarted, HostedSearch: searchWithCitation},
			Event{Type: EventHostedSearchCompleted, HostedSearch: searchWithCitation},
			Event{Type: EventUsage, Tokens: 4},
			Event{Type: EventText, Text: "hello"},
			Event{Type: EventDone},
		)
		require.EqualValues(t, 1, usage["cursor_agent_v1_hosted_search_requests"])
		require.EqualValues(t, 1, usage["cursor_agent_v1_completed_search_count"])
		require.EqualValues(t, 1, usage["cursor_agent_v1_citation_count"])
		require.EqualValues(t, 4, usage["cursor_agent_v1_progress_event_count"])
		require.Equal(t, true, usage["cursor_agent_v1_output_token_delta_observed"])
		require.EqualValues(t, 4, usage["cursor_agent_v1_output_token_delta"])
		require.EqualValues(t, 4, usage["output_tokens"])
	})

	t.Run("observed zero token delta is present", func(t *testing.T) {
		usage := projectUsage(t, sdktranslator.FormatClaude,
			Event{Type: EventUsage, Tokens: 0},
			Event{Type: EventDone},
		)
		require.Equal(t, true, usage["cursor_agent_v1_output_token_delta_observed"])
		require.EqualValues(t, 0, usage["cursor_agent_v1_output_token_delta"])
		require.EqualValues(t, 0, usage["output_tokens"])
	})

	t.Run("claude thinking still counts native progress", func(t *testing.T) {
		projection := newClaudeProjection(context.Background(), translatedTurn{
			publicModel:    "claude-opus-5",
			responseFormat: sdktranslator.FormatClaude,
			managed:        ManagedRequest{SessionID: "msg-progress"},
		})
		_, _, _, err := projection.handle(Event{Type: EventThinking, Text: "secret"})
		require.NoError(t, err)
		_, terminal, _, err := projection.handle(Event{Type: EventThinkingCompleted})
		require.NoError(t, err)
		require.False(t, terminal)
		_, terminal, _, err = projection.handle(Event{Type: EventDone})
		require.NoError(t, err)
		require.True(t, terminal)
		payload, err := projection.nonStream()
		require.NoError(t, err)
		require.NotContains(t, string(payload), `"type":"thinking"`)
		usage := usageMap(t, payload)
		require.EqualValues(t, 2, usage["cursor_agent_v1_progress_event_count"])
	})
}

func TestProjectionUsageEvidenceCoversChatAndResponses(t *testing.T) {
	search := &HostedSearch{
		CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "q",
		References: []HostedSearchReference{{Title: "T", URL: "https://example.com"}},
	}
	events := []Event{
		{Type: EventHostedSearchCall},
		{Type: EventHostedSearchStarted, HostedSearch: search},
		{Type: EventHostedSearchCompleted, HostedSearch: search},
		{Type: EventUsage, Tokens: 0},
		{Type: EventText, Text: "hello"},
		{Type: EventDone},
	}

	t.Run("chat nonstream and stream", func(t *testing.T) {
		projection, stream := runProjection(t, sdktranslator.FormatOpenAI, "chat-usage", events...)
		payload, err := projection.nonStream()
		require.NoError(t, err)
		assertObservedSearchUsage(t, usageMap(t, payload))
		assertObservedSearchUsage(t, usageFromStream(t, stream))
	})

	t.Run("responses nonstream and stream", func(t *testing.T) {
		projection, stream := runProjection(t, sdktranslator.FormatOpenAIResponse, "resp-usage", events...)
		payload, err := projection.nonStream()
		require.NoError(t, err)
		assertObservedSearchUsage(t, usageMap(t, payload))
		requireOfficialResponsesUsageShape(t, usageMap(t, payload))
		created, inProgress, completed := responsesLifecycleUsage(t, stream)
		require.Nil(t, created)
		require.Nil(t, inProgress)
		assertObservedSearchUsage(t, completed)
		requireOfficialResponsesUsageShape(t, completed)
	})
}

func projectUsage(t *testing.T, format sdktranslator.Format, events ...Event) map[string]any {
	t.Helper()
	projection, _ := runProjection(t, format, "msg-usage", events...)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	return usageMap(t, payload)
}

func runProjection(t *testing.T, format sdktranslator.Format, sessionID string, events ...Event) (*claudeProjection, [][]byte) {
	t.Helper()
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: format,
		original:       []byte(`{"model":"claude-opus-5"}`),
		claudeRequest:  []byte(`{"model":"claude-opus-5"}`),
		managed:        ManagedRequest{SessionID: sessionID, RequestSegmentID: "projection-fixture-segment"},
	})
	var stream [][]byte
	for _, event := range events {
		chunks, _, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
	}
	return projection, stream
}

func usageMap(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	usage, _ := root["usage"].(map[string]any)
	require.NotNil(t, usage)
	return usage
}

func usageFromStream(t *testing.T, chunks [][]byte) map[string]any {
	t.Helper()
	var found map[string]any
	for _, chunk := range chunks {
		_, body, _ := splitSSEJSONPayload(chunk)
		var root map[string]any
		if jsonx.Unmarshal(body, &root) != nil {
			continue
		}
		usage, _ := root["usage"].(map[string]any)
		if usage != nil {
			found = usage
		}
	}
	require.NotNil(t, found)
	return found
}

func usageFromResponsesStream(t *testing.T, chunks [][]byte) map[string]any {
	t.Helper()
	_, _, completed := responsesLifecycleUsage(t, chunks)
	require.NotNil(t, completed)
	return completed
}

func responsesLifecycleUsage(t *testing.T, chunks [][]byte) (created, inProgress, completed map[string]any) {
	t.Helper()
	var createdRaw, inProgressRaw any
	var createdSet, inProgressSet bool
	for _, chunk := range chunks {
		for _, line := range bytes.Split(chunk, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			var root map[string]any
			if jsonx.Unmarshal(bytes.TrimSpace(line[5:]), &root) != nil {
				continue
			}
			response, _ := root["response"].(map[string]any)
			if response == nil {
				continue
			}
			usage, exists := response["usage"]
			switch jsonString(root["type"]) {
			case "response.created":
				createdRaw, createdSet = usage, exists
			case "response.in_progress":
				inProgressRaw, inProgressSet = usage, exists
			case "response.completed":
				completed, _ = usage.(map[string]any)
			}
		}
	}
	require.True(t, createdSet, "response.created must include usage")
	require.True(t, inProgressSet, "response.in_progress must include usage")
	if createdRaw != nil {
		created, _ = createdRaw.(map[string]any)
	}
	if inProgressRaw != nil {
		inProgress, _ = inProgressRaw.(map[string]any)
	}
	require.NotNil(t, completed, "response.completed must include usage")
	return created, inProgress, completed
}

func assertObservedSearchUsage(t *testing.T, usage map[string]any) {
	t.Helper()
	require.EqualValues(t, 1, usage["cursor_agent_v1_hosted_search_requests"])
	require.EqualValues(t, 1, usage["cursor_agent_v1_completed_search_count"])
	require.EqualValues(t, 1, usage["cursor_agent_v1_citation_count"])
	require.EqualValues(t, 2, usage["cursor_agent_v1_progress_event_count"])
	require.Equal(t, true, usage["cursor_agent_v1_output_token_delta_observed"])
	require.EqualValues(t, 0, usage["cursor_agent_v1_output_token_delta"])
}
