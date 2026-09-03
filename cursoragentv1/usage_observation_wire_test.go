package cursoragentv1

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func loadUsageFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "usage-observation", name))
	require.NoError(t, err)
	decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	return decoded
}

func TestOfficialTurnEndedUpdateDescriptorFieldTypes(t *testing.T) {
	desc := (&agentv1.TurnEndedUpdate{}).ProtoReflect().Descriptor()
	require.Equal(t, protoreflect.Name("TurnEndedUpdate"), desc.Name())
	want := []struct {
		number protoreflect.FieldNumber
		name   protoreflect.Name
		kind   protoreflect.Kind
	}{
		{1, "input_tokens", protoreflect.Int64Kind},
		{2, "output_tokens", protoreflect.Int64Kind},
		{3, "cache_read_tokens", protoreflect.Int64Kind},
		{4, "cache_write_tokens", protoreflect.Int64Kind},
		{5, "reasoning_tokens", protoreflect.Int64Kind},
	}
	require.Equal(t, len(want), desc.Fields().Len())
	for _, field := range want {
		got := desc.Fields().ByNumber(field.number)
		require.NotNil(t, got, "missing field %d", field.number)
		require.Equal(t, field.name, got.Name())
		require.Equal(t, field.kind, got.Kind())
		require.True(t, got.HasPresence(), "field %s must keep proto3 optional presence", field.name)
		require.True(t, got.HasOptionalKeyword())
	}

	delta := (&agentv1.TokenDeltaUpdate{}).ProtoReflect().Descriptor().Fields().ByNumber(1)
	require.NotNil(t, delta)
	require.Equal(t, protoreflect.Name("tokens"), delta.Name())
	require.Equal(t, protoreflect.Int32Kind, delta.Kind())
	require.False(t, delta.HasOptionalKeyword())
}

func TestGoldenTurnEndedUsagePresenceZeroInvalid(t *testing.T) {
	empty, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_empty.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, empty.Type)
	require.Equal(t, UsageKindTurnEnded, empty.Usage.Kind)
	require.False(t, empty.Usage.Input.Present)
	require.False(t, empty.Usage.Output.Present)
	require.False(t, empty.Usage.CacheRead.Present)
	require.False(t, empty.Usage.CacheWrite.Present)
	require.False(t, empty.Usage.Reasoning.Present)

	all, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_all_present.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, all.Type)
	require.Equal(t, ObservedCount{Present: true, Value: 10}, all.Usage.Input)
	require.Equal(t, ObservedCount{Present: true, Value: 20}, all.Usage.Output)
	require.Equal(t, ObservedCount{Present: true, Value: 3}, all.Usage.CacheRead)
	require.Equal(t, ObservedCount{Present: true, Value: 4}, all.Usage.CacheWrite)
	require.Equal(t, ObservedCount{Present: true, Value: 5}, all.Usage.Reasoning)

	zeros, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_explicit_zero.hex"))
	require.NoError(t, err)
	require.True(t, zeros.Usage.Input.Present)
	require.False(t, zeros.Usage.Input.Invalid)
	require.EqualValues(t, 0, zeros.Usage.Input.Value)
	require.True(t, zeros.Usage.Output.Present)
	require.EqualValues(t, 0, zeros.Usage.Output.Value)
	require.True(t, zeros.Usage.CacheRead.Present)
	require.True(t, zeros.Usage.CacheWrite.Present)
	require.True(t, zeros.Usage.Reasoning.Present)

	only, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_input_only.hex"))
	require.NoError(t, err)
	require.Equal(t, ObservedCount{Present: true, Value: 42}, only.Usage.Input)
	require.False(t, only.Usage.Output.Present)
	require.False(t, only.Usage.CacheRead.Present)

	neg, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_negative_input.hex"))
	require.NoError(t, err)
	require.True(t, neg.Usage.Input.Present)
	require.True(t, neg.Usage.Input.Invalid)
	require.EqualValues(t, -1, neg.Usage.Input.Value)
	require.False(t, neg.Usage.Output.Present)

	malformed, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_malformed_input.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, malformed.Type)
	require.True(t, malformed.Usage.Input.Invalid)
	require.True(t, malformed.Usage.Input.Present)
}

func TestGoldenTokenDeltaVersusTurnEnded(t *testing.T) {
	zero, err := decodeServerMessage(loadUsageFixture(t, "token_delta_zero.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTokenDelta, zero.Type)
	require.Equal(t, UsageKindTokenDelta, zero.Usage.Kind)
	require.Equal(t, ObservedCount{Present: true, Value: 0}, zero.Usage.TokenDelta)

	positive, err := decodeServerMessage(loadUsageFixture(t, "token_delta_positive.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTokenDelta, positive.Type)
	require.EqualValues(t, 9, positive.TokenDelta)
	require.Equal(t, ObservedCount{Present: true, Value: 9}, positive.Usage.TokenDelta)
	require.Equal(t, UsageKindTokenDelta, positive.Usage.Kind)
	require.False(t, positive.Usage.Output.Present)

	negative, err := decodeServerMessage(loadUsageFixture(t, "token_delta_negative.hex"))
	require.NoError(t, err)
	require.True(t, negative.Usage.TokenDelta.Invalid)
	require.EqualValues(t, -3, negative.Usage.TokenDelta.Value)
}

func TestGoldenUnknownAdvisoryVersusControl(t *testing.T) {
	advisory, err := decodeServerMessage(loadUsageFixture(t, "unknown_advisory_feedback_then_turn_ended.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, advisory.Type)
	require.Equal(t, ObservedCount{Present: true, Value: 8}, advisory.Usage.Output)
	require.False(t, advisory.Usage.Input.Present)

	leftover, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_unknown_advisory_field.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, leftover.Type)
	require.Equal(t, ObservedCount{Present: true, Value: 7}, leftover.Usage.Input)
	require.False(t, leftover.Usage.Output.Present)

	_, err = decodeServerMessage(loadUsageFixture(t, "unknown_control_field_42.hex"))
	require.ErrorContains(t, err, "unknown field(s) [42]")

	withKnown, err := decodeServerMessage(loadUsageFixture(t, "unknown_control_field_42_with_turn_ended.hex"))
	require.NoError(t, err)
	require.Equal(t, wireTurnEnded, withKnown.Type)
	require.False(t, withKnown.Usage.Input.Present)
}

func TestTurnEndedObservationDoesNotEnterPublicUsage(t *testing.T) {
	decoded, err := decodeServerMessage(loadUsageFixture(t, "turn_ended_all_present.hex"))
	require.NoError(t, err)
	usage := projectUsage(t, sdktranslator.FormatClaude,
		Event{Type: EventUsageObservation, Usage: decoded.Usage},
		Event{Type: EventText, Text: "hello"},
		Event{Type: EventDone},
	)
	require.EqualValues(t, 0, usage["output_tokens"])
	require.EqualValues(t, 0, usage["input_tokens"])
	require.Equal(t, true, usage["cursor_agent_v1_turn_ended_observed"])
	require.Equal(t, "unresolved", usage["cursor_agent_v1_turn_ended_scope"])
	require.Equal(t, "last_wire_turn_ended", usage["cursor_agent_v1_turn_ended_source"])
	require.Equal(t, true, usage["cursor_agent_v1_turn_ended_input_tokens_observed"])
	require.EqualValues(t, 10, usage["cursor_agent_v1_turn_ended_input_tokens"])
	require.EqualValues(t, 20, usage["cursor_agent_v1_turn_ended_output_tokens"])
	require.EqualValues(t, 3, usage["cursor_agent_v1_turn_ended_cache_read_tokens"])
	require.EqualValues(t, 4, usage["cursor_agent_v1_turn_ended_cache_write_tokens"])
	require.EqualValues(t, 5, usage["cursor_agent_v1_turn_ended_reasoning_tokens"])
	_, claimsMeasuredCache := usage["cursor_agent_v1_cached_tokens_observed"]
	require.False(t, claimsMeasuredCache)
	_, hasDelta := usage["cursor_agent_v1_output_token_delta_observed"]
	require.False(t, hasDelta)

	invalid := projectUsage(t, sdktranslator.FormatClaude,
		Event{Type: EventUsage, Tokens: -3, Usage: &UsageObservation{Kind: UsageKindTokenDelta, TokenDelta: ObservedCount{Present: true, Invalid: true, Value: -3}}},
		Event{Type: EventDone},
	)
	require.EqualValues(t, 0, invalid["output_tokens"])
	require.Equal(t, true, invalid["cursor_agent_v1_output_token_delta_invalid"])
	_, observed := invalid["cursor_agent_v1_output_token_delta_observed"]
	require.False(t, observed)
}

func TestAnchoredTranslateKeepsParsedUserTextForExecutor(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok"},{"type":"text","text":"continue"}]}]}`
	turn, err := translateExecutionRequest(anchoredClaudeExecutorRequest("mixed-native", payload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, "continue", turn.parsedUserText)
	require.Equal(t, "continue", turn.managed.FollowUpText)
	require.Len(t, turn.managed.ActiveResults, 1)
	require.Equal(t, "toolu_1", turn.managed.ActiveResults[0].ToolCallID)
}

func TestProjectionStillReadsTokenDeltaForPublicOutput(t *testing.T) {
	projection := newClaudeProjection(context.Background(), translatedTurn{
		publicModel:    "claude-opus-5",
		responseFormat: sdktranslator.FormatClaude,
		managed:        ManagedRequest{SessionID: "msg-delta-final"},
	})
	_, _, _, err := projection.handle(Event{Type: EventUsage, Tokens: 4, Usage: &UsageObservation{Kind: UsageKindTokenDelta, TokenDelta: ObservedCount{Present: true, Value: 4}}})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventUsageObservation, Usage: &UsageObservation{
		Kind:   UsageKindTurnEnded,
		Input:  ObservedCount{Present: true, Value: 100},
		Output: ObservedCount{Present: true, Value: 999},
	}})
	require.NoError(t, err)
	_, _, _, err = projection.handle(Event{Type: EventDone})
	require.NoError(t, err)
	payload, err := projection.nonStream()
	require.NoError(t, err)
	usage := usageMap(t, payload)
	require.EqualValues(t, 4, usage["output_tokens"])
	require.EqualValues(t, 0, usage["input_tokens"])
	require.EqualValues(t, 4, usage["cursor_agent_v1_output_token_delta"])
	require.Equal(t, "unresolved", usage["cursor_agent_v1_turn_ended_scope"])
	require.Equal(t, "last_wire_turn_ended", usage["cursor_agent_v1_turn_ended_source"])
	require.Equal(t, true, usage["cursor_agent_v1_turn_ended_input_tokens_observed"])
	require.EqualValues(t, 100, usage["cursor_agent_v1_turn_ended_input_tokens"])
	require.EqualValues(t, 999, usage["cursor_agent_v1_turn_ended_output_tokens"])
}
