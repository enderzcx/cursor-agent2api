package cursoragentv1

import (
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestUsageObservationSnapshotPreservesPresenceAndIsolation(t *testing.T) {
	events := []Event{
		{Type: EventUsage, Tokens: 7}, // Pre-upgrade snapshot: no typed Usage.
		{Type: EventUsageObservation, Usage: &UsageObservation{
			Kind: UsageKindTurnEnded, Input: ObservedCount{Present: true, Value: 1000},
			Output: ObservedCount{Present: true}, CacheRead: ObservedCount{},
			CacheWrite: ObservedCount{Present: true, Invalid: true, Value: -1},
			Reasoning:  ObservedCount{Present: true, Value: 3},
		}},
	}
	baseline := []Event{cloneEvent(events[0]), cloneEvent(events[1])}
	persisted, err := persistEvents(events)
	require.NoError(t, err)
	encoded, err := jsonx.Marshal(persisted)
	require.NoError(t, err)
	var restored []persistedEvent
	require.NoError(t, jsonx.Unmarshal(encoded, &restored))
	replayed, err := eventsFromPersisted(restored)
	require.NoError(t, err)
	require.Equal(t, events, replayed)
	events[1].Usage.Input.Value = 9999
	require.EqualValues(t, 1000, replayed[1].Usage.Input.Value)
	require.EqualValues(t, 1000, persisted[1].Usage.Input.Value)
	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			original, _ := runProjection(t, format, "usage-replay", append(baseline, Event{Type: EventText, Text: "answer"}, Event{Type: EventDone})...)
			copy, _ := runProjection(t, format, "usage-replay", append(replayed, Event{Type: EventText, Text: "answer"}, Event{Type: EventDone})...)
			original.respCreatedAt, copy.respCreatedAt = 1, 1
			first, err := original.nonStream()
			require.NoError(t, err)
			second, err := copy.nonStream()
			require.NoError(t, err)
			require.JSONEq(t, string(first), string(second))
		})
	}
}

func TestUsageObservationCarryIsNotRequestAttribution(t *testing.T) {
	first := &UsageObservation{Kind: UsageKindTurnEnded, Input: ObservedCount{Present: true, Value: 1000}}
	carry := usageEvidenceEvents([]Event{{Type: EventUsage, Tokens: 7}, {Type: EventUsageObservation, Usage: first}})
	require.Len(t, carry, 2)
	first.Input.Value = 9999
	require.EqualValues(t, 1000, carry[1].Usage.Input.Value)
	var facts usageFacts
	for _, event := range carry {
		facts.observe(event)
	}
	facts.observe(Event{Type: EventUsage, Tokens: 5})
	facts.observe(Event{Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded, Input: ObservedCount{Present: true, Value: 2000}}})
	require.EqualValues(t, 12, facts.publicOutputTokens(), "terminal counters must never be added to request output")
	require.EqualValues(t, 2000, facts.turnEnded.Input.Value, "only latest raw observation, NOT sum or per-request attribution")
	facts.observe(Event{Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded}})
	require.False(t, facts.turnEnded.Input.Present, "missing latest field must not inherit another turn's value")
	require.EqualValues(t, 12, facts.publicOutputTokens())
	var parked usageFacts
	parked.observe(Event{Type: EventTurnEnded})
	require.Nil(t, parked.turnEnded, "synthetic tool park is not an upstream counter observation")
}

func TestMeteredCarryMarkerSurvivesPersistedReplay(t *testing.T) {
	carry := meteredUsageCarry([]Event{testMeasuredTerminal(7)})
	persisted, err := persistEvents(carry)
	require.NoError(t, err)
	encoded, err := jsonx.Marshal(persisted)
	require.NoError(t, err)
	var restored []persistedEvent
	require.NoError(t, jsonx.Unmarshal(encoded, &restored))
	replayed, err := eventsFromPersisted(restored)
	require.NoError(t, err)
	require.True(t, replayed[0].Usage.Carried)
	facts := usageFacts{measuredEnabled: true}
	for _, event := range replayed {
		facts.observe(event)
	}
	_, complete := facts.measuredTurn()
	require.False(t, complete)
	require.Equal(t, false, facts.privateDiagnosticFields()["cursor_agent_v1_turn_ended_output_tokens_observed"])
}
