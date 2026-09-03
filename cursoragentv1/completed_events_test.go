package cursoragentv1

import (
	"math"
	"strings"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestCompletedSnapshotCompactionPreservesProjectionAndUsage(t *testing.T) {
	carried := testMeasuredTerminal(7)
	carried.Usage.Carried = true
	events := []Event{
		carried,
		{Type: EventThinking, Text: "first\n"}, {Type: EventThinking, Text: `"second"`},
		{Type: EventThinkingCompleted},
		{Type: EventText, Text: "hello"}, {Type: EventText, Text: " <世界>"},
		{Type: EventUsage, Tokens: 7}, {Type: EventUsage, Tokens: 3},
		{Type: EventUsage, Usage: &UsageObservation{Kind: UsageKindTokenDelta, TokenDelta: ObservedCount{Present: true}}},
		{Type: EventUsage, Tokens: 5, Usage: &UsageObservation{Kind: UsageKindTokenDelta, TokenDelta: ObservedCount{Present: true, Value: 5}}},
		{Type: EventUsage, Usage: &UsageObservation{Kind: UsageKindTokenDelta, TokenDelta: ObservedCount{}}},
		{Type: EventUsage, Usage: &UsageObservation{Kind: UsageKindTokenDelta, TokenDelta: ObservedCount{Present: true, Invalid: true, Value: -1}}},
		testMeasuredTerminal(11), {Type: EventDone},
	}
	var snapshot eventSnapshot
	for _, event := range events {
		require.NoError(t, snapshot.append(event))
	}
	require.Less(t, len(snapshot.events), len(events))
	persisted, err := persistEvents(events)
	require.NoError(t, err)
	encoded, err := jsonx.Marshal(persisted)
	require.NoError(t, err)
	require.Equal(t, len(encoded), snapshot.bytes, "the budget includes escaping, metadata and array separators")
	var restored []persistedEvent
	require.NoError(t, jsonx.Unmarshal(encoded, &restored))
	replayed, err := eventsFromPersisted(restored)
	require.NoError(t, err)
	require.True(t, replayed[0].Usage.Carried)
	require.Equal(t, 2, replayed[1].ProgressEvents)
	for _, metered := range []bool{false, true} {
		original, copy := usageFacts{measuredEnabled: metered}, usageFacts{measuredEnabled: metered}
		for _, event := range events {
			original.observe(event)
		}
		for _, event := range replayed {
			copy.observe(event)
		}
		require.Equal(t, original.privateDiagnosticFields(), copy.privateDiagnosticFields())
		require.Equal(t, original.publicOutputTokens(), copy.publicOutputTokens())
		require.Equal(t, original.visibleOutput.String(), copy.visibleOutput.String())
	}
	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		original, _ := runProjection(t, format, "snapshot-projection", events...)
		copy, _ := runProjection(t, format, "snapshot-projection", replayed...)
		original.respCreatedAt, copy.respCreatedAt = 1, 1
		first, err := original.nonStream()
		require.NoError(t, err)
		second, err := copy.nonStream()
		require.NoError(t, err)
		require.JSONEq(t, string(first), string(second), string(format))
	}
}

func TestCompletedSnapshotKeepsToolAndNativeTurnBoundaries(t *testing.T) {
	events := []Event{
		{Type: EventText, Text: "before"},
		{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "one", Name: "lookup", Arguments: `{}`}},
		{Type: EventTurnEnded},
		{Type: EventToolResultAccepted, ToolResult: &ToolResult{ToolCallID: "one", Content: "ok"}},
		{Type: EventText, Text: "after"},
		testMeasuredTerminal(7), testMeasuredTerminal(9),
		{Type: EventUsage, Tokens: math.MaxInt64}, {Type: EventUsage, Tokens: 1},
		{Type: EventDone},
	}
	persisted, err := persistEvents(events)
	require.NoError(t, err)
	replayed, err := eventsFromPersisted(persisted)
	require.NoError(t, err)
	require.Equal(t, events, replayed, "never collapse tool/turn observations or overflow an additive delta")
}

func TestCompletedSnapshotUsesSerializedByteLimit(t *testing.T) {
	var snapshot eventSnapshot
	// Two megabytes of quotes occupy four megabytes when encoded, before metadata.
	require.ErrorContains(t, snapshot.append(Event{Type: EventText, Text: strings.Repeat(`"`, maxCompletedEventBytes/2)}), "exceeds")
	require.Empty(t, snapshot.events)
	for i := 0; i < 1024; i++ {
		require.NoError(t, snapshot.append(Event{Type: EventText, Text: strings.Repeat("x", 1024)}))
	}
	require.Len(t, snapshot.events, 1)
	require.Len(t, snapshot.events[0].Text, 1<<20)
	require.ErrorContains(t, snapshot.append(Event{Type: EventText, Text: strings.Repeat("x", maxCompletedEventBytes)}), "exceeds")
	require.Len(t, snapshot.events[0].Text, 1<<20, "reject before mutating a retained snapshot")
}

func TestSnapshotReplayDoesNotApplyLiveQueueCapacity(t *testing.T) {
	events := make([]Event, 0, 601)
	for i := 0; i < 300; i++ {
		events = append(events, Event{Type: EventThinking, Text: "r"}, Event{Type: EventText, Text: "x"})
	}
	events = append(events, Event{Type: EventDone})
	output := emitCompletedReplay(CompletedReplay{Events: events, BillingRunID: "legacy-receipt"})
	require.Equal(t, events, collectManagedEvents(output.Events))
	require.NoError(t, <-output.Result)
	require.Equal(t, "legacy-receipt", output.BillingRunID)
	buffered := emitBufferedOutput(&ManagedOutput{BillingRunID: "mixed-receipt"}, events)
	require.Equal(t, events, collectManagedEvents(buffered.Events))
	require.NoError(t, <-buffered.Result)
	invalid := emitCompletedReplay(CompletedReplay{Events: []Event{{Type: EventText, Text: strings.Repeat("x", maxCompletedEventBytes)}}})
	require.Empty(t, collectManagedEvents(invalid.Events))
	require.ErrorContains(t, <-invalid.Result, "exceeds")
}
