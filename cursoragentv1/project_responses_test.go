package cursoragentv1

import (
	"context"
	"strings"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestResponsesFunctionCallTurnIsCompleted(t *testing.T) {
	projection := newResponsesProjection("resp-tool")
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-1", Name: "lookup", Arguments: `{"id":1}`}},
		{Type: EventTurnEnded},
	} {
		chunks, terminal, park, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
		if event.Type == EventTurnEnded {
			require.True(t, terminal)
			require.True(t, park)
		}
	}
	events := parseResponsesSSEEvents(t, stream)
	require.Equal(t, []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.function_call_arguments.done",
		"response.output_item.done",
		"response.completed",
	}, responsesEventNames(events))
	requireResponsesMonotonicSequence(t, events)
	require.Equal(t, []int{0, 0, 0, 0}, responsesOutputIndexes(events, "response.output_item.added", "response.function_call_arguments.delta", "response.function_call_arguments.done", "response.output_item.done"))
	require.Equal(t, "in_progress", nestedString(events[2], "item", "status"))
	require.Equal(t, "completed", nestedString(events[5], "item", "status"))
	require.Equal(t, "completed", nestedString(events[6], "response", "status"))
	require.Nil(t, nestedMap(events[6], "response")["incomplete_details"])

	payload, err := projection.nonStream()
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	require.Equal(t, "completed", jsonString(root["status"]))
	require.Nil(t, root["incomplete_details"])
	output, _ := root["output"].([]any)
	require.Len(t, output, 1)
	item, _ := output[0].(map[string]any)
	require.Equal(t, "function_call", jsonString(item["type"]))
	require.Equal(t, "completed", jsonString(item["status"]))
}

func TestResponsesSSELifecycleIsStrictlyOrdered(t *testing.T) {
	projection := newResponsesProjection("resp-lifecycle")
	search := &HostedSearch{CallID: "ws-1", Kind: HostedSearchKindWebSearch, Query: "q", References: []HostedSearchReference{{Title: "T", URL: "https://example.com"}}}
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventThinking, Text: "plan"},
		{Type: EventHostedSearchStarted, HostedSearch: search},
		{Type: EventHostedSearchCompleted, HostedSearch: search},
		{Type: EventText, Text: "hello"},
		{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-1", Name: "lookup", Arguments: `{"id":1}`}},
		{Type: EventTurnEnded},
	} {
		chunks, _, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
	}
	events := parseResponsesSSEEvents(t, stream)
	require.Equal(t, []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.reasoning_summary_part.added",
		"response.reasoning_summary_text.delta",
		"response.reasoning_summary_text.done",
		"response.reasoning_summary_part.done",
		"response.output_item.done",
		"response.output_item.added",
		"response.web_search_call.in_progress",
		"response.web_search_call.searching",
		"response.web_search_call.completed",
		"response.output_item.done",
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.delta",
		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",
		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.function_call_arguments.done",
		"response.output_item.done",
		"response.completed",
	}, responsesEventNames(events))
	requireResponsesMonotonicSequence(t, events)
	require.Equal(t, []int{0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3}, collectEventOutputIndexes(events))
	require.Equal(t, "completed", nestedString(events[len(events)-1], "response", "status"))
}

func TestResponsesNonStreamUsesAllocatedOutputIndexOrder(t *testing.T) {
	projection := newResponsesProjection("resp-order")
	first := &HostedSearch{CallID: "a", Kind: HostedSearchKindWebSearch, Query: "one"}
	second := &HostedSearch{CallID: "b", Kind: HostedSearchKindWebSearch, Query: "two"}
	for _, event := range []Event{
		{Type: EventHostedSearchStarted, HostedSearch: first},
		{Type: EventHostedSearchStarted, HostedSearch: second},
		{Type: EventHostedSearchCompleted, HostedSearch: second},
		{Type: EventHostedSearchCompleted, HostedSearch: first},
		{Type: EventDone},
	} {
		_, _, _, err := projection.handle(event)
		require.NoError(t, err)
	}
	payload, err := projection.nonStream()
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	output, _ := root["output"].([]any)
	require.Len(t, output, 2)
	require.Equal(t, "search", nestedString(output[0].(map[string]any), "action", "type"))
	require.Equal(t, "one", nestedString(output[0].(map[string]any), "action", "query"))
	require.Equal(t, "two", nestedString(output[1].(map[string]any), "action", "query"))
	require.Equal(t, "completed", jsonString(root["status"]))
}

func TestResponsesInterleavedSpansHaveUniqueIDs(t *testing.T) {
	projection := newResponsesProjection("resp-ids")
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventThinking, Text: "plan"},
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
	output, _ := root["output"].([]any)
	require.Equal(t, []string{"reasoning", "message", "reasoning"}, responsesOutputTypes(t, root["output"]))
	firstID := jsonString(output[0].(map[string]any)["id"])
	messageID := jsonString(output[1].(map[string]any)["id"])
	secondID := jsonString(output[2].(map[string]any)["id"])
	require.True(t, strings.HasPrefix(firstID, "rs_"))
	require.True(t, strings.HasPrefix(messageID, "msg_"))
	require.True(t, strings.HasPrefix(secondID, "rs_"))
	require.NotEqual(t, firstID, secondID)
	require.NotEqual(t, firstID, messageID)
	events := parseResponsesSSEEvents(t, stream)
	requireResponsesMonotonicSequence(t, events)
	addedIDs := make([]string, 0, 3)
	for _, event := range events {
		if jsonString(event["type"]) != "response.output_item.added" {
			continue
		}
		addedIDs = append(addedIDs, nestedString(event, "item", "id"))
	}
	require.Equal(t, []string{firstID, messageID, secondID}, addedIDs)
}

func TestResponsesCreatedAndInProgressUsageIsNull(t *testing.T) {
	projection := newResponsesProjection("resp-usage-start")
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventUsage, Tokens: 9},
		{Type: EventText, Text: "hello"},
		{Type: EventDone},
	} {
		chunks, _, _, err := projection.handle(event)
		require.NoError(t, err)
		stream = append(stream, chunks...)
	}
	events := parseResponsesSSEEvents(t, stream)
	require.Equal(t, "response.created", jsonString(events[0]["type"]))
	require.Equal(t, "response.in_progress", jsonString(events[1]["type"]))
	for _, event := range events[:2] {
		response := nestedMap(event, "response")
		require.Equal(t, "in_progress", jsonString(response["status"]))
		usage, exists := response["usage"]
		require.True(t, exists, "%s must include usage", jsonString(event["type"]))
		require.Nil(t, usage, "%s usage must be null, not a partial object", jsonString(event["type"]))
	}
}

func TestResponsesTerminalUsageMatchesOfficialDetailsShape(t *testing.T) {
	projection := newResponsesProjection("resp-usage-terminal")
	var stream [][]byte
	for _, event := range []Event{
		{Type: EventUsage, Tokens: 9},
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
	events := parseResponsesSSEEvents(t, stream)
	completed := events[len(events)-1]
	require.Equal(t, "response.completed", jsonString(completed["type"]))
	requireOfficialResponsesUsageShape(t, nestedMap(nestedMap(completed, "response"), "usage"))
	require.EqualValues(t, 9, nestedMap(nestedMap(completed, "response"), "usage")["output_tokens"])
	require.Equal(t, true, nestedMap(nestedMap(completed, "response"), "usage")["cursor_agent_v1_output_token_delta_observed"])
	require.EqualValues(t, 9, nestedMap(nestedMap(completed, "response"), "usage")["cursor_agent_v1_output_token_delta"])
	_, claimsCacheObserved := nestedMap(nestedMap(completed, "response"), "usage")["cursor_agent_v1_cached_tokens_observed"]
	require.False(t, claimsCacheObserved)

	payload, err := projection.nonStream()
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, jsonx.Unmarshal(payload, &root))
	usage := nestedMap(root, "usage")
	requireOfficialResponsesUsageShape(t, usage)
	require.EqualValues(t, 9, usage["output_tokens"])
	require.Equal(t, true, usage["cursor_agent_v1_output_token_delta_observed"])
	require.EqualValues(t, 9, usage["cursor_agent_v1_output_token_delta"])
	_, claimsCacheObserved = usage["cursor_agent_v1_cached_tokens_observed"]
	require.False(t, claimsCacheObserved)
}

func newResponsesProjection(sessionID string) *claudeProjection {
	return newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "gpt-5",
		responseFormat: sdktranslator.FormatOpenAIResponse,
		original:       []byte(`{"model":"gpt-5","input":"latest"}`),
		claudeRequest:  []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"latest"}]}`),
		managed:        ManagedRequest{SessionID: sessionID},
	})
}

func parseResponsesSSEEvents(t *testing.T, chunks [][]byte) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, line := range strings.Split(string(bytesJoin(chunks)), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var root map[string]any
		require.NoError(t, jsonx.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &root))
		events = append(events, root)
	}
	require.NotEmpty(t, events)
	return events
}

func responsesEventNames(events []map[string]any) []string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, jsonString(event["type"]))
	}
	return names
}

func requireResponsesMonotonicSequence(t *testing.T, events []map[string]any) {
	t.Helper()
	for i, event := range events {
		seq, ok := event["sequence_number"].(float64)
		require.True(t, ok, "event %d missing sequence_number", i)
		require.Equal(t, i+1, int(seq), "event %d sequence_number", i)
	}
}

func responsesOutputIndexes(events []map[string]any, types ...string) []int {
	wanted := map[string]bool{}
	for _, typ := range types {
		wanted[typ] = true
	}
	var indexes []int
	for _, event := range events {
		if len(wanted) > 0 && !wanted[jsonString(event["type"])] {
			continue
		}
		if raw, ok := event["output_index"].(float64); ok {
			indexes = append(indexes, int(raw))
		}
	}
	return indexes
}

func collectEventOutputIndexes(events []map[string]any) []int {
	var indexes []int
	for _, event := range events {
		raw, ok := event["output_index"].(float64)
		if !ok {
			continue
		}
		indexes = append(indexes, int(raw))
	}
	return indexes
}

func requireOfficialResponsesUsageShape(t *testing.T, usage map[string]any) {
	t.Helper()
	require.NotNil(t, usage)
	require.Contains(t, usage, "input_tokens")
	require.Contains(t, usage, "output_tokens")
	require.Contains(t, usage, "total_tokens")
	inputDetails, _ := usage["input_tokens_details"].(map[string]any)
	require.NotNil(t, inputDetails, "usage.input_tokens_details")
	require.Contains(t, inputDetails, "cached_tokens")
	require.Contains(t, inputDetails, "cache_write_tokens")
	outputDetails, _ := usage["output_tokens_details"].(map[string]any)
	require.NotNil(t, outputDetails, "usage.output_tokens_details")
	require.Contains(t, outputDetails, "reasoning_tokens")
}

func nestedMap(root map[string]any, key string) map[string]any {
	value, _ := root[key].(map[string]any)
	return value
}

func nestedString(root map[string]any, keys ...string) string {
	current := any(root)
	for _, key := range keys {
		object, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = object[key]
	}
	return jsonString(current)
}
