package cursoragentv1

import (
	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"math"
	"strings"
)

// UsageKind distinguishes TokenDeltaUpdate from TurnEndedUpdate counters.
// TokenDelta is a heuristic progress counter. TurnEnded covers an Agent turn,
// which can span several client tool-result requests.
type UsageKind int

const (
	UsageKindUnknown UsageKind = iota
	UsageKindTokenDelta
	UsageKindTurnEnded
)

// ObservedCount records proto presence separately from the numeric value.
// Missing, explicit 0, and invalid/negative are distinct.
type ObservedCount struct {
	Present bool
	Invalid bool
	Value   int64
}

// UsageObservation is the typed decode of Agent v1 usage counters.
// It is the only facts source for those counters. Projection may read it;
// it must not invent per-request measured attribution.
type UsageObservation struct {
	Carried    bool // internal: a completed earlier native turn, not this run's terminal
	Kind       UsageKind
	TokenDelta ObservedCount
	Input      ObservedCount
	Output     ObservedCount
	CacheRead  ObservedCount
	CacheWrite ObservedCount
	Reasoning  ObservedCount
}

func observedOptionalInt64(present bool, value int64) ObservedCount {
	if !present {
		return ObservedCount{}
	}
	if value < 0 {
		return ObservedCount{Present: true, Invalid: true, Value: value}
	}
	return ObservedCount{Present: true, Value: value}
}

func observedInt32Tokens(value int32) ObservedCount {
	if value < 0 {
		return ObservedCount{Present: true, Invalid: true, Value: int64(value)}
	}
	return ObservedCount{Present: true, Value: int64(value)}
}

func observeTokenDeltaUpdate(update *agentv1.TokenDeltaUpdate) *UsageObservation {
	obs := &UsageObservation{Kind: UsageKindTokenDelta}
	if update == nil {
		obs.TokenDelta = ObservedCount{Invalid: true}
		return obs
	}
	obs.TokenDelta = observedInt32Tokens(update.GetTokens())
	markMalformedCount(&obs.TokenDelta, update, 1)
	return obs
}

func observeTurnEndedUpdate(update *agentv1.TurnEndedUpdate) *UsageObservation {
	obs := &UsageObservation{Kind: UsageKindTurnEnded}
	if update == nil {
		return obs
	}
	obs.Input = observedOptionalInt64(update.InputTokens != nil, update.GetInputTokens())
	obs.Output = observedOptionalInt64(update.OutputTokens != nil, update.GetOutputTokens())
	obs.CacheRead = observedOptionalInt64(update.CacheReadTokens != nil, update.GetCacheReadTokens())
	obs.CacheWrite = observedOptionalInt64(update.CacheWriteTokens != nil, update.GetCacheWriteTokens())
	obs.Reasoning = observedOptionalInt64(update.ReasoningTokens != nil, update.GetReasoningTokens())
	markMalformedTurnEnded(obs, update)
	return obs
}

func markMalformedTurnEnded(obs *UsageObservation, msg proto.Message) {
	if obs == nil {
		return
	}
	for _, number := range malformedVarintFields(msg) {
		switch number {
		case 1:
			obs.Input.Present, obs.Input.Invalid = true, true
		case 2:
			obs.Output.Present, obs.Output.Invalid = true, true
		case 3:
			obs.CacheRead.Present, obs.CacheRead.Invalid = true, true
		case 4:
			obs.CacheWrite.Present, obs.CacheWrite.Invalid = true, true
		case 5:
			obs.Reasoning.Present, obs.Reasoning.Invalid = true, true
		}
	}
}

func markMalformedCount(count *ObservedCount, msg proto.Message, number protowire.Number) {
	if count == nil {
		return
	}
	for _, found := range malformedVarintFields(msg) {
		if found == number {
			count.Present = true
			count.Invalid = true
			return
		}
	}
}

func malformedVarintFields(msg proto.Message) []protowire.Number {
	if msg == nil {
		return nil
	}
	unknown := msg.ProtoReflect().GetUnknown()
	var numbers []protowire.Number
	seen := map[protowire.Number]struct{}{}
	for len(unknown) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(unknown)
		if consumed < 0 {
			return numbers
		}
		unknown = unknown[consumed:]
		n := protowire.ConsumeFieldValue(number, wireType, unknown)
		if n < 0 {
			return numbers
		}
		unknown = unknown[n:]
		if wireType == protowire.VarintType {
			continue
		}
		if _, exists := seen[number]; exists {
			continue
		}
		seen[number] = struct{}{}
		numbers = append(numbers, number)
	}
	return numbers
}

func cloneUsageObservation(usage *UsageObservation) *UsageObservation {
	if usage == nil {
		return nil
	}
	cloned := *usage
	return &cloned
}

// measuredTurnUsage is one completed Agent turn, not one HTTP tool segment.
// Cursor input includes both cache buckets; Anthropic input excludes them.
type measuredTurnUsage struct {
	Input, TotalInput, Output, CacheRead, CacheWrite, Reasoning int64
}

func (o *UsageObservation) measuredTurn() (measuredTurnUsage, bool) {
	if o == nil || o.Kind != UsageKindTurnEnded {
		return measuredTurnUsage{}, false
	}
	for _, count := range []ObservedCount{o.Input, o.Output, o.CacheRead, o.CacheWrite} {
		if !count.Present || count.Invalid || count.Value < 0 {
			return measuredTurnUsage{}, false
		}
	}
	if o.CacheRead.Value > o.Input.Value || o.CacheWrite.Value > o.Input.Value-o.CacheRead.Value || o.Output.Value > math.MaxInt64-o.Input.Value {
		return measuredTurnUsage{}, false
	}
	usage := measuredTurnUsage{Input: o.Input.Value - o.CacheRead.Value - o.CacheWrite.Value,
		TotalInput: o.Input.Value, Output: o.Output.Value, CacheRead: o.CacheRead.Value, CacheWrite: o.CacheWrite.Value}
	if o.Reasoning.Present && !o.Reasoning.Invalid && o.Reasoning.Value >= 0 && o.Reasoning.Value <= usage.Output {
		usage.Reasoning = o.Reasoning.Value
	}
	return usage, true
}

// usageFacts is the single owner for decoded usage observation.
// Legacy sessions retain their old contract. Versioned metered turns use real
// terminal counts, including carried completed turns in a mixed follow-up.
type usageFacts struct {
	measuredEnabled          bool
	terminalCount            int
	freshTerminal            bool
	visibleOutput            strings.Builder
	outputTokenDelta         int64
	outputTokenDeltaObserved bool
	outputTokenDeltaInvalid  bool
	turnEnded                *UsageObservation
	hostedSearches           int
	completedSearchCount     int
	completedSearchObserved  bool
	progressEventCount       int
}

func (f *usageFacts) observe(event Event) {
	if f == nil {
		return
	}
	switch event.Type {
	case EventText:
		f.visibleOutput.WriteString(event.Text)
	case EventToolCall:
		if event.ToolCall != nil {
			f.visibleOutput.WriteString(event.ToolCall.Name)
			f.visibleOutput.WriteString(event.ToolCall.Arguments)
		}
	case EventUsage:
		f.observeTokenDelta(event)
	case EventUsageObservation:
		if event.Usage != nil && event.Usage.Kind == UsageKindTurnEnded {
			f.freshTerminal = f.freshTerminal || !event.Usage.Carried
			if f.measuredEnabled && f.turnEnded != nil {
				f.turnEnded = addTurnObservations(f.turnEnded, event.Usage)
			} else {
				f.turnEnded = cloneUsageObservation(event.Usage)
			}
			f.terminalCount++
		}
	case EventHostedSearchCall:
		f.hostedSearches++
	case EventHostedSearchStarted:
		f.progressEventCount++
	case EventHostedSearchCompleted:
		f.completedSearchCount++
		f.completedSearchObserved = true
		f.progressEventCount++
	case EventThinking:
		f.visibleOutput.WriteString(event.Text)
		if event.Text != "" {
			f.progressEventCount += max(1, event.ProgressEvents)
		}
	case EventThinkingCompleted:
		f.progressEventCount++
	}
}

func addTurnObservations(a, b *UsageObservation) *UsageObservation {
	add := func(x, y ObservedCount) ObservedCount {
		if !x.Present || !y.Present {
			return ObservedCount{}
		}
		if x.Invalid || y.Invalid || x.Value < 0 || y.Value < 0 || y.Value > math.MaxInt64-x.Value {
			return ObservedCount{Present: true, Invalid: true}
		}
		return ObservedCount{Present: true, Value: x.Value + y.Value}
	}
	return &UsageObservation{Kind: UsageKindTurnEnded, Input: add(a.Input, b.Input), Output: add(a.Output, b.Output), CacheRead: add(a.CacheRead, b.CacheRead), CacheWrite: add(a.CacheWrite, b.CacheWrite), Reasoning: add(a.Reasoning, b.Reasoning)}
}

func (f *usageFacts) observeTokenDelta(event Event) {
	count := ObservedCount{Present: true, Value: event.Tokens}
	if event.Usage != nil && event.Usage.Kind == UsageKindTokenDelta {
		count = event.Usage.TokenDelta
	} else if event.Tokens < 0 {
		count.Invalid = true
	}
	if count.Invalid {
		f.outputTokenDeltaInvalid = true
		return
	}
	if !count.Present {
		return
	}
	f.outputTokenDeltaObserved = true
	f.outputTokenDelta += count.Value
}

func (f *usageFacts) publicOutputTokens() int64 {
	if f == nil {
		return 0
	}
	if usage, ok := f.measuredTurn(); ok {
		return usage.Output
	}
	return f.outputTokenDelta
}

func (f *usageFacts) measuredTurn() (measuredTurnUsage, bool) {
	if f == nil || !f.measuredEnabled || !f.freshTerminal {
		return measuredTurnUsage{}, false
	}
	return f.turnEnded.measuredTurn()
}

func (f *usageFacts) privateDiagnosticFields() map[string]any {
	fields := map[string]any{}
	if f == nil {
		return fields
	}
	if f.hostedSearches > 0 {
		// Private approved-search diagnostic for EventHostedSearchCall.
		// Not Anthropic server_tool_use and must not enter web-search pricing.
		fields["cursor_agent_v1_hosted_search_requests"] = f.hostedSearches
	}
	if f.completedSearchObserved {
		fields["cursor_agent_v1_completed_search_count"] = f.completedSearchCount
	}
	if f.progressEventCount > 0 {
		fields["cursor_agent_v1_progress_event_count"] = f.progressEventCount
	}
	if f.outputTokenDeltaObserved {
		fields["cursor_agent_v1_output_token_delta_observed"] = true
		fields["cursor_agent_v1_output_token_delta"] = f.outputTokenDelta
	}
	if f.outputTokenDeltaInvalid {
		fields["cursor_agent_v1_output_token_delta_invalid"] = true
	}
	if f.turnEnded != nil {
		fields["cursor_agent_v1_turn_ended_observed"] = true
		fields["cursor_agent_v1_turn_ended_scope"] = "unresolved"
		if _, ok := f.measuredTurn(); ok {
			fields["cursor_agent_v1_turn_ended_scope"] = "agent_turn"
		}
		fields["cursor_agent_v1_turn_ended_source"] = "last_wire_turn_ended"
		if f.measuredEnabled && f.terminalCount > 1 {
			fields["cursor_agent_v1_turn_ended_source"] = "chained_wire_turns"
		}
		attachObservedCount(fields, "cursor_agent_v1_turn_ended_input_tokens", f.turnEnded.Input)
		attachObservedCount(fields, "cursor_agent_v1_turn_ended_output_tokens", f.turnEnded.Output)
		attachObservedCount(fields, "cursor_agent_v1_turn_ended_cache_read_tokens", f.turnEnded.CacheRead)
		attachObservedCount(fields, "cursor_agent_v1_turn_ended_cache_write_tokens", f.turnEnded.CacheWrite)
		attachObservedCount(fields, "cursor_agent_v1_turn_ended_reasoning_tokens", f.turnEnded.Reasoning)
		if f.measuredEnabled && !f.freshTerminal {
			// Previous-turn numbers are not a partial snapshot of this entire
			// operation. Preserve diagnostics without overriding its estimate.
			for _, name := range []string{"input", "output", "cache_read", "cache_write", "reasoning"} {
				fields["cursor_agent_v1_turn_ended_"+name+"_tokens_observed"] = false
			}
		}
	}
	return fields
}

func attachObservedCount(fields map[string]any, key string, count ObservedCount) {
	if count.Invalid {
		fields[key+"_invalid"] = true
		return
	}
	if !count.Present {
		return
	}
	fields[key+"_observed"] = true
	fields[key] = count.Value
}
