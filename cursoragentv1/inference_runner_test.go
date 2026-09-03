package cursoragentv1

import (
	"bytes"
	"context"
	"math"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDecodeInferenceInt64SaturatesOversizedVarint(t *testing.T) {
	encoded := appendVarintField(nil, 1, math.MaxUint64)
	require.Equal(t, int64(math.MaxInt64), decodeInt64(encoded, 1))
}

func TestPrepareInferenceTurnDefersResponsesToolOutputToCheckpointResume(t *testing.T) {
	turn := translatedTurn{
		claudeRequest: []byte(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"done"}]}]}`),
		managed: ManagedRequest{
			ActiveResults: []ToolResult{{ToolCallID: "call-1", Content: "done"}},
			Run:           RunRequest{RuntimeProfile: runtimeSand},
		},
	}
	err := prepareInferenceTurn(&turn, cliproxyexecutor.Request{Format: sdktranslator.FormatOpenAIResponse})
	require.NoError(t, err)
	require.Empty(t, turn.managed.Run.HistoryMessages)
	require.Empty(t, turn.managed.Run.HistoryRequestKey)
}

func TestPrepareInferenceTurnKeepsMessagesToolHistory(t *testing.T) {
	turn := translatedTurn{
		publicModel: "claude-sonnet-4-6",
		claudeRequest: []byte(`{"messages":[
			{"role":"user","content":"use tool"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"q":"x"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"done"}]}
		]}`),
		managed: ManagedRequest{
			ActiveResults: []ToolResult{{ToolCallID: "call-1", Content: "done"}},
			Run:           RunRequest{RuntimeProfile: runtimeSand, Model: "claude-sonnet-4-6"},
		},
	}
	err := prepareInferenceTurn(&turn, cliproxyexecutor.Request{Format: sdktranslator.FormatClaude})
	require.NoError(t, err)
	require.Len(t, turn.managed.Run.HistoryMessages, 3)
	require.Contains(t, string(turn.managed.Run.HistoryMessages[1]), `"type":"tool-call"`)
	require.Contains(t, string(turn.managed.Run.HistoryMessages[2]), `"type":"tool-result"`)
	require.NotEmpty(t, turn.managed.Run.HistoryRequestKey)
}

func TestInferenceModelSelectionSeparatesDisplayAndResolvedIDs(t *testing.T) {
	id, effort, err := inferenceModelSelection("cursor-grok-4.6-high")
	require.NoError(t, err)
	require.Equal(t, "grok-4.6", id)
	require.Equal(t, "high", effort)

	id, effort, err = inferenceModelSelection("claude-sonnet-4-6")
	require.NoError(t, err)
	require.Equal(t, "claude-sonnet-4-6", id)
	require.Empty(t, effort)
}

func TestInferenceRequestedModelMatchesCursorSonnet5(t *testing.T) {
	selection, err := inferenceRequestedModel("claude-sonnet-5")
	require.NoError(t, err)
	require.Equal(t, "claude-sonnet-5", selection.ID)
	require.True(t, selection.MaxMode)
	require.False(t, selection.BuiltIn)
	require.Equal(t, []inferenceModelParameter{{ID: "thinking", Value: "true"}, {ID: "context", Value: "1m"}, {ID: "effort", Value: "high"}}, selection.Parameters)
}

func TestEncodeInferenceRequestCarriesCursorSonnet5Selector(t *testing.T) {
	payload, err := encodeInferenceRequest(RunRequest{Model: "claude-sonnet-5", UserText: "hello"})
	require.NoError(t, err)
	requested := bytesField(payload, 7)
	require.Equal(t, "claude-sonnet-5", decodeString(requested, 1))
	require.True(t, decodeBool(requested, 2))
	require.False(t, decodeBool(requested, 4))
	require.Equal(t, 3, countBytesFields(requested, 3))
	parameters := map[string]string{}
	for _, encoded := range allBytesFields(requested, 3) {
		parameters[decodeString(encoded, 1)] = decodeString(encoded, 2)
	}
	require.Equal(t, map[string]string{"thinking": "true", "context": "1m", "effort": "high"}, parameters)
}

func TestEncodeInferenceToolWrapsJSONSchemaLikeCursor(t *testing.T) {
	encoded, err := encodeInferenceTool(ToolDefinition{Name: "probe_marker", Description: "Return marker", InputSchema: []byte(`{"type":"object","properties":{"marker":{"type":"string"}}}`)})
	require.NoError(t, err)
	var parameters structpb.Struct
	require.NoError(t, proto.Unmarshal(bytesField(encoded, 3), &parameters))
	root := parameters.AsMap()
	require.NotContains(t, root, "type")
	schema, ok := root["jsonSchema"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "object", schema["type"])
}

func TestEncodeInferenceRequestCarriesHistoryToolsAndRequestedModel(t *testing.T) {
	request := RunRequest{
		RuntimeProfile: runtimeSand, Model: "cursor-grok-4.6-medium", ConversationID: "conversation-1",
		SystemText: "system", UserText: "continue",
		HistoryMessages: [][]byte{
			[]byte(`{"role":"user","content":[{"type":"text","text":"hello"}]}`),
			[]byte(`{"role":"assistant","content":[{"type":"tool-call","toolCallId":"call-1","toolName":"lookup","args":{"q":"x"}}]}`),
			[]byte(`{"role":"tool","content":[{"type":"tool-result","toolCallId":"call-1","toolName":"lookup","result":"done"}]}`),
		},
		Tools: []ToolDefinition{{Name: "lookup", Description: "Lookup", InputSchema: []byte(`{"type":"object","properties":{"q":{"type":"string"}}}`)}},
	}
	payload, err := encodeInferenceRequest(request)
	require.NoError(t, err)
	require.Equal(t, 5, countBytesFields(payload, 1))
	require.Equal(t, 1, countBytesFields(payload, 2))
	requested := bytesField(payload, 7)
	require.Equal(t, "grok-4.6", decodeString(requested, 1))
	parameter := bytesField(requested, 3)
	require.Equal(t, "effort", decodeString(parameter, 1))
	require.Equal(t, "medium", decodeString(parameter, 2))
	require.True(t, decodeBool(requested, 4))
	require.Equal(t, "conversation-1", decodeString(payload, 8))
}

func TestConsumeInferenceStreamProjectsTextUsageAndTerminal(t *testing.T) {
	textPart := appendStringField(nil, 1, "OK")
	textPart = appendVarintField(textPart, 2, 1)
	extended := appendVarintField(nil, 1, 100)
	extended = appendVarintField(extended, 2, 20)
	extended = appendVarintField(extended, 3, 40)
	extended = appendVarintField(extended, 4, 5)
	wire := append(frameConnect(appendBytesField(nil, 1, textPart)), frameConnect(appendBytesField(nil, 5, extended))...)
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := []Event{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), RunRequest{}, func(event Event) { events = append(events, event) }, nil, nil, &inferenceHostedUseTracker{})
	require.NoError(t, err)
	require.Equal(t, EventText, events[0].Type)
	require.Equal(t, "OK", events[0].Text)
	require.Equal(t, EventUsageObservation, events[1].Type)
	require.Equal(t, countValue(100), events[1].Usage.Input)
	require.Equal(t, countValue(40), events[1].Usage.CacheRead)
	require.Equal(t, EventCheckpoint, events[2].Type)
	checkpoint, decodeErr := decodeInferenceCheckpoint(events[2].Checkpoint)
	require.NoError(t, decodeErr)
	require.Len(t, checkpoint.Messages, 1)
	require.Equal(t, EventDone, events[3].Type)
}

func TestConsumeInferenceStreamAggregatesToolArgumentsAndParks(t *testing.T) {
	first := appendStringField(nil, 1, "upstream-call")
	first = appendStringField(first, 2, "lookup")
	first = appendStringField(first, 3, `{"q":`)
	second := appendStringField(nil, 1, "upstream-call")
	second = appendStringField(second, 3, `"x"}`)
	second = appendVarintField(second, 4, 1)
	wire := append(frameConnect(appendBytesField(nil, 2, first)), frameConnect(appendBytesField(nil, 2, second))...)
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := []Event{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), RunRequest{HistoryRequestKey: "history", RequireToolCall: true, MaxParallelTools: 1, Tools: []ToolDefinition{{Name: "lookup"}}}, func(event Event) { events = append(events, event) }, nil, nil, &inferenceHostedUseTracker{})
	require.NoError(t, err)
	require.Equal(t, EventToolCall, events[0].Type)
	require.Equal(t, "lookup", events[0].ToolCall.Name)
	require.Equal(t, `{"q":"x"}`, events[0].ToolCall.Arguments)
	require.Equal(t, EventTurnEnded, events[1].Type)
	require.Equal(t, EventDone, events[2].Type)
}

func completeInferenceToolFrame(id, name, args string) []byte {
	body := appendStringField(nil, 1, id)
	body = appendStringField(body, 2, name)
	body = appendStringField(body, 3, args)
	body = appendVarintField(body, 4, 1)
	return frameConnect(appendBytesField(nil, 2, body))
}

func TestConsumeInferenceStreamReturnsTextWhenRequiredToolIsSkipped(t *testing.T) {
	wire := frameConnect(appendBytesField(nil, 1, appendStringField(nil, 1, "plain answer")))
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := []Event{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), RunRequest{
		HistoryRequestKey: "history", RequireToolCall: true, Tools: []ToolDefinition{{Name: "lookup"}},
	}, func(event Event) { events = append(events, event) }, nil, nil, &inferenceHostedUseTracker{})
	require.NoError(t, err)
	require.Equal(t, EventText, events[0].Type)
	require.Equal(t, "plain answer", events[0].Text)
}

func TestConsumeInferenceStreamAllowsParallelCallerTools(t *testing.T) {
	wire := completeInferenceToolFrame("call-1", "first", `{"q":"a"}`)
	wire = append(wire, completeInferenceToolFrame("call-2", "second", `{"q":"b"}`)...)
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := []Event{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), RunRequest{
		HistoryRequestKey: "history",
		Tools:             []ToolDefinition{{Name: "first"}, {Name: "second"}},
	}, func(event Event) { events = append(events, event) }, nil, nil, &inferenceHostedUseTracker{})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolCall, EventToolCall, EventTurnEnded, EventDone}, inferenceEventTypes(events))
	require.Equal(t, "first", events[0].ToolCall.Name)
	require.Equal(t, "second", events[1].ToolCall.Name)

	// The cap counts in-flight calls: a second call opened while the first is
	// still streaming arguments is rejected only when the caller disabled parallel.
	openFirst := frameConnect(appendBytesField(nil, 2, appendStringField(appendStringField(nil, 1, "call-1"), 2, "first")))
	interleaved := append(append([]byte(nil), openFirst...), completeInferenceToolFrame("call-2", "second", `{"q":"b"}`)...)
	serialErr := consumeInferenceStream(context.Background(), bytes.NewReader(interleaved), RunRequest{
		HistoryRequestKey: "history",
		MaxParallelTools:  1,
		Tools:             []ToolDefinition{{Name: "first"}, {Name: "second"}},
	}, func(Event) {}, nil, nil, &inferenceHostedUseTracker{})
	require.ErrorContains(t, serialErr, "parallel caller tools")
}

func TestMergeInferenceToolArgumentsAcceptsDeltasAndCumulativeSnapshots(t *testing.T) {
	require.Equal(t, `{"q":"x"}`, mergeInferenceToolArguments(`{"q":`, `"x"}`))
	require.Equal(t, `{"q":"x"}`, mergeInferenceToolArguments(`{"q":`, `{"q":"x"}`))
}

func TestPrepareInferenceRunRestoresCheckpointAndAppendsToolResult(t *testing.T) {
	checkpoint, err := encodeInferenceCheckpoint(RunRequest{SystemText: "system", UserText: "use tool"}, "", inferenceReasoning{}, []inferenceToolCall{{ID: "call-1", Name: "lookup", Arguments: `{"q":"x"}`}})
	require.NoError(t, err)
	results := make(chan ToolResult, 1)
	results <- ToolResult{ToolCallID: "call-1", Content: "done"}
	request, accepted, err := prepareInferenceRun(t.Context(), RunRequest{Model: "claude-sonnet-4-6", InitialCheckpoint: checkpoint, Resume: true, ToolResults: results})
	require.NoError(t, err)
	require.Equal(t, "call-1", accepted.ToolCallID)
	require.Equal(t, historyContinuationText, request.UserText)
	require.Len(t, request.HistoryMessages, 4)
	encoded, err := encodeInferenceRequest(request)
	require.NoError(t, err)
	require.Equal(t, 5, countBytesFields(encoded, 1))
}

func bytesField(payload []byte, target protowire.Number) []byte {
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return nil
		}
		payload = payload[tagLength:]
		if wireType == protowire.BytesType {
			value, length := protowire.ConsumeBytes(payload)
			if length < 0 {
				return nil
			}
			if number == target {
				return value
			}
			payload = payload[length:]
			continue
		}
		length := protowire.ConsumeFieldValue(number, wireType, payload)
		if length < 0 {
			return nil
		}
		payload = payload[length:]
	}
	return nil
}

func countBytesFields(payload []byte, target protowire.Number) int {
	count := 0
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return count
		}
		payload = payload[tagLength:]
		length := protowire.ConsumeFieldValue(number, wireType, payload)
		if length < 0 {
			return count
		}
		if number == target && wireType == protowire.BytesType {
			count++
		}
		payload = payload[length:]
	}
	return count
}

func allBytesFields(payload []byte, target protowire.Number) [][]byte {
	values := [][]byte{}
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return values
		}
		payload = payload[tagLength:]
		if wireType == protowire.BytesType {
			value, length := protowire.ConsumeBytes(payload)
			if length < 0 {
				return values
			}
			if number == target {
				values = append(values, value)
			}
			payload = payload[length:]
			continue
		}
		length := protowire.ConsumeFieldValue(number, wireType, payload)
		if length < 0 {
			return values
		}
		payload = payload[length:]
	}
	return values
}
