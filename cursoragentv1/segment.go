package cursoragentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/google/uuid"
	"sort"
	"strings"
)

const meteredTurnPrefix = "turn2:"
const headerMeteringMode = "X-CPA-Runtime-Cursor-Agent-V1-Metering"

func billingIDForRequest(request ManagedRequest) string {
	if request.TurnMetering {
		return meteredTurnID(request.BillingRunID)
	}
	return request.BillingRunID
}

func meteringVersionError() error {
	return &requestError{status: 503, text: "Cursor continuation requires an updated gateway; retry after gateway rollout completes"}
}

func meteredTurnID(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, meteredTurnPrefix) {
		return id
	}
	if id == "" {
		id = uuid.NewString()
	}
	return meteredTurnPrefix + id
}

func isMeteredTurn(id string) bool { return strings.HasPrefix(id, meteredTurnPrefix) }

func normalizeFollowUp(text string) string {
	return strings.TrimSpace(text)
}

func generationSegmentID(fingerprint string, originalBatch []string, accepted []CompletedToolResult, followUp string) string {
	batch := normalizeToolIDs(originalBatch)
	material := strings.TrimSpace(fingerprint) + "\x00" + strings.Join(batch, "\x00") + "\x00" + toolResultDigest(accepted) + "\x00" + normalizeFollowUp(followUp)
	digest := sha256.Sum256([]byte(material))
	return hex.EncodeToString(digest[:])
}

func usageEvidenceEvents(events []Event) []Event {
	out := make([]Event, 0, 4)
	for _, event := range events {
		switch event.Type {
		case EventUsage, EventUsageObservation, EventHostedSearchCall, EventHostedSearchStarted, EventHostedSearchCompleted, EventThinkingCompleted:
			out = append(out, cloneEvent(event))
		}
	}
	return out
}

func meteredUsageCarry(events []Event) []Event {
	carry := usageEvidenceEvents(events)
	found := false
	for i := range carry {
		if carry[i].Type == EventUsageObservation && carry[i].Usage != nil && carry[i].Usage.Kind == UsageKindTurnEnded {
			carry[i].Usage.Carried = true
			found = true
		}
	}
	if !found {
		// A completed native run without counters must not disappear from a
		// later aggregate just because the next run has a complete snapshot.
		carry = append(carry, Event{Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded, Carried: true}})
	}
	return carry
}

func completedResultsForBatch(results []CompletedToolResult, batchIDs []string) ([]CompletedToolResult, bool) {
	wanted := make(map[string]struct{}, len(batchIDs))
	for _, id := range normalizeToolIDs(batchIDs) {
		wanted[id] = struct{}{}
	}
	if len(wanted) == 0 {
		return nil, false
	}
	filtered := make([]CompletedToolResult, 0, len(wanted))
	for _, result := range results {
		if _, ok := wanted[result.ToolCallID]; !ok {
			continue
		}
		filtered = append(filtered, result)
		delete(wanted, result.ToolCallID)
	}
	if len(wanted) > 0 {
		return nil, false
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ToolCallID < filtered[j].ToolCallID })
	return filtered, true
}

func toolResultsForBatch(results []ToolResult, batchIDs []string) ([]ToolResult, bool) {
	canonical, err := canonicalToolResults(results)
	if err != nil {
		return nil, false
	}
	filtered, ok := completedResultsForBatch(canonical, batchIDs)
	if !ok {
		return nil, false
	}
	out := make([]ToolResult, 0, len(filtered))
	for _, result := range filtered {
		out = append(out, ToolResult{ToolCallID: result.ToolCallID, Content: result.Content, IsError: result.IsError, Media: cloneMedia(result.Media)})
	}
	return out, true
}

func batchSubsetOf(batchIDs, submittedIDs []string) bool {
	submitted := make(map[string]struct{}, len(submittedIDs))
	for _, id := range normalizeToolIDs(submittedIDs) {
		submitted[id] = struct{}{}
	}
	batch := normalizeToolIDs(batchIDs)
	if len(batch) == 0 {
		return false
	}
	for _, id := range batch {
		if _, ok := submitted[id]; !ok {
			return false
		}
	}
	return true
}
