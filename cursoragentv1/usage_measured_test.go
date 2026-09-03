package cursoragentv1

import (
	"context"
	"math"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestMeasuredTurnUsageMapping(t *testing.T) {
	count := func(v int64) ObservedCount { return ObservedCount{Present: true, Value: v} }
	base := UsageObservation{Kind: UsageKindTurnEnded, Input: count(6638), Output: count(105), CacheRead: count(544), CacheWrite: count(0)}
	u, ok := base.measuredTurn()
	require.True(t, ok)
	require.Equal(t, measuredTurnUsage{Input: 6094, TotalInput: 6638, Output: 105, CacheRead: 544}, u)
	tests := []struct {
		name  string
		edit  func(*UsageObservation)
		valid bool
	}{
		{"zero output", func(o *UsageObservation) { o.Output = count(0) }, true},
		{"all cached", func(o *UsageObservation) { o.CacheRead = count(6638) }, true},
		{"explicit all zero", func(o *UsageObservation) { o.Input = count(0); o.Output = count(0); o.CacheRead = count(0) }, true},
		{"missing input", func(o *UsageObservation) { o.Input = ObservedCount{} }, false},
		{"missing cache", func(o *UsageObservation) { o.CacheRead = ObservedCount{} }, false},
		{"missing output", func(o *UsageObservation) { o.Output = ObservedCount{} }, false},
		{"bad cache", func(o *UsageObservation) { o.CacheWrite = count(7000) }, false},
		{"bad number", func(o *UsageObservation) { o.Input.Invalid = true }, false},
		{"negative", func(o *UsageObservation) { o.Output = count(-1) }, false},
		{"overflow", func(o *UsageObservation) { o.Output = count(math.MaxInt64) }, false},
		{"delta is not usage", func(o *UsageObservation) { o.Kind = UsageKindTokenDelta }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { o := base; tt.edit(&o); _, ok := o.measuredTurn(); require.Equal(t, tt.valid, ok) })
	}
}

func TestMeasuredTurnProjectionUsesTerminalNotDelta(t *testing.T) {
	count := func(v int64) ObservedCount { return ObservedCount{Present: true, Value: v} }
	events := []Event{{Type: EventUsage, Tokens: 2929}, {Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded, Input: count(66950), Output: count(223), CacheRead: count(31321), CacheWrite: count(35625)}}, {Type: EventDone}}
	claude := projectMeasuredUsage(t, sdktranslator.FormatClaude, events...)
	require.EqualValues(t, 4, claude["input_tokens"])
	require.EqualValues(t, 223, claude["output_tokens"])
	require.EqualValues(t, 31321, claude["cache_read_input_tokens"])
	require.EqualValues(t, 35625, claude["cache_creation_input_tokens"])
	responses := projectMeasuredUsage(t, sdktranslator.FormatOpenAIResponse, events...)
	require.EqualValues(t, 66950, responses["input_tokens"])
	require.EqualValues(t, 223, responses["output_tokens"])
	require.EqualValues(t, 67173, responses["total_tokens"])
	require.Equal(t, "agent_turn", responses["cursor_agent_v1_turn_ended_scope"])
	chat := projectMeasuredUsage(t, sdktranslator.FormatOpenAI, events...)
	require.EqualValues(t, 66950, chat["prompt_tokens"])
	require.EqualValues(t, 223, chat["completion_tokens"])
	require.EqualValues(t, 67173, chat["total_tokens"])
}

func TestMeasuredTurnStreamParity(t *testing.T) {
	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			p := newClaudeProjection(context.Background(), translatedTurn{publicModel: "claude-opus-5", responseFormat: format, managed: ManagedRequest{SessionID: "stream-measured", BillingRunID: meteredTurnID("stream")}})
			event := testMeasuredTerminal(0)
			event.Usage.Input.Value = 100
			event.Usage.CacheRead.Value = 100
			var chunks [][]byte
			for _, e := range []Event{{Type: EventText, Text: "done"}, event, {Type: EventDone}} {
				out, _, _, err := p.handle(e)
				require.NoError(t, err)
				chunks = append(chunks, out...)
			}
			var usage map[string]any
			if format == sdktranslator.FormatOpenAIResponse {
				usage = usageFromResponsesStream(t, chunks)
			} else {
				usage = usageFromStream(t, chunks)
			}
			if format == sdktranslator.FormatOpenAI {
				require.EqualValues(t, 100, usage["prompt_tokens"])
				require.Zero(t, usage["completion_tokens"])
			} else {
				want := 100
				if format == sdktranslator.FormatClaude {
					want = 0
				}
				require.EqualValues(t, want, usage["input_tokens"])
				require.Zero(t, usage["output_tokens"])
			}
		})
	}
}

func TestCarriedUsageAloneIsNotCompleteTurnUsage(t *testing.T) {
	p := newClaudeProjection(context.Background(), translatedTurn{publicModel: "claude-opus-5", responseFormat: sdktranslator.FormatClaude, managed: ManagedRequest{BillingRunID: meteredTurnID("carry-without-terminal")}})
	for _, event := range meteredUsageCarry([]Event{testMeasuredTerminal(7)}) {
		_, _, _, err := p.handle(event)
		require.NoError(t, err)
	}
	_, _, _, err := p.handle(Event{Type: EventText, Text: "new segment without terminal"})
	require.NoError(t, err)
	_, complete := p.facts.measuredTurn()
	require.False(t, complete, "a previous segment terminal cannot certify missing current-segment usage")
	require.Greater(t, p.outputTokens(), int64(7))
	require.Equal(t, false, p.facts.privateDiagnosticFields()["cursor_agent_v1_turn_ended_output_tokens_observed"])
	_, _, _, err = p.handle(testMeasuredTerminal(5))
	require.NoError(t, err)
	u, complete := p.facts.measuredTurn()
	require.True(t, complete)
	require.EqualValues(t, 12, u.Output)
	missing := newClaudeProjection(context.Background(), translatedTurn{managed: ManagedRequest{BillingRunID: meteredTurnID("missing-prior")}})
	for _, event := range meteredUsageCarry([]Event{{Type: EventText, Text: "prior run without counters"}}) {
		_, _, _, err = missing.handle(event)
		require.NoError(t, err)
	}
	_, _, _, err = missing.handle(testMeasuredTerminal(5))
	require.NoError(t, err)
	_, complete = missing.facts.measuredTurn()
	require.False(t, complete, "a missing prior snapshot cannot be replaced by the next run's complete usage")
}

func projectMeasuredUsage(t *testing.T, format sdktranslator.Format, events ...Event) map[string]any {
	p := newClaudeProjection(context.Background(), translatedTurn{publicModel: "claude-opus-5", responseFormat: format, managed: ManagedRequest{SessionID: "measured", BillingRunID: meteredTurnID("test")}})
	for _, event := range events {
		_, _, _, err := p.handle(event)
		require.NoError(t, err)
	}
	b, err := p.nonStream()
	require.NoError(t, err)
	return usageMap(t, b)
}

func testMeasuredTerminal(output int64) Event {
	count := func(v int64) ObservedCount { return ObservedCount{Present: true, Value: v} }
	return Event{Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded, Input: count(1), CacheRead: count(0), CacheWrite: count(0), Output: count(output)}}
}
