package cursoragentv1

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

// eventSnapshot bounds the serialized snapshot, independently of wire chunk
// count. Only adjacent equivalent deltas combine; tools, thinking boundaries,
// terminal observations and carried native turns keep their original order.
type eventSnapshot struct {
	events   []Event
	bytes    int
	lastSize int
	text     strings.Builder
}

func (s *eventSnapshot) append(event Event) error {
	if s.bytes == 0 {
		s.bytes = 2 // JSON array brackets
	}
	if len(s.events) > 0 {
		last := &s.events[len(s.events)-1]
		if last.Type == event.Type && snapshotText(*last) && snapshotText(event) {
			encoded, err := jsonx.Marshal(event.Text)
			if err != nil {
				return err
			}
			delta := len(encoded) - 2 // append only the JSON string contents
			progress := last.ProgressEvents
			if event.Type == EventThinking {
				progress = max(1, progress) + max(1, event.ProgressEvents)
				delta += progressFieldSize(progress) - progressFieldSize(last.ProgressEvents)
			}
			if err := s.grow(delta); err != nil {
				return err
			}
			s.text.WriteString(event.Text)
			last.Text = s.text.String()
			last.ProgressEvents = progress
			s.lastSize += delta
			return nil
		}
		if merged, ok := mergeSnapshotUsage(*last, event); ok {
			size, err := snapshotEventSize(merged)
			if err != nil {
				return err
			}
			if err := s.grow(size - s.lastSize); err != nil {
				return err
			}
			*last, s.lastSize = merged, size
			return nil
		}
	}
	size, err := snapshotEventSize(event)
	if err != nil {
		return err
	}
	delta := size
	if len(s.events) > 0 {
		delta++ // comma
	}
	if err := s.grow(delta); err != nil {
		return err
	}
	s.events = append(s.events, cloneEvent(event))
	s.lastSize = size
	s.text.Reset()
	if snapshotText(event) {
		s.text.WriteString(event.Text)
	}
	return nil
}

func (s *eventSnapshot) grow(delta int) error {
	if delta > maxCompletedEventBytes-s.bytes {
		return fmt.Errorf("Cursor Agent v1 completed snapshot exceeds %d bytes", maxCompletedEventBytes)
	}
	s.bytes += delta
	return nil
}

func snapshotEventSize(event Event) (int, error) {
	encoded, err := jsonx.Marshal(persistedEventFrom(event))
	return len(encoded), err
}

func snapshotText(event Event) bool {
	return (event.Type == EventText || event.Type == EventThinking) && event.Text != "" &&
		event.Tokens == 0 && event.Usage == nil && event.ToolCall == nil && event.ToolResult == nil &&
		event.HostedSearch == nil && len(event.Checkpoint) == 0 && len(event.Blobs) == 0 &&
		(event.Type == EventThinking || event.ProgressEvents == 0)
}

func progressFieldSize(count int) int {
	if count == 0 {
		return 0
	}
	return len(`,"progress_events":`) + len(strconv.Itoa(count))
}

func mergeSnapshotUsage(a, b Event) (Event, bool) {
	if a.Type != EventUsage || b.Type != EventUsage || a.Tokens < 0 || b.Tokens < 0 || b.Tokens > math.MaxInt64-a.Tokens {
		return Event{}, false
	}
	merged := cloneEvent(a)
	merged.Tokens += b.Tokens
	if a.Usage == nil && b.Usage == nil {
		return merged, true
	}
	if a.Usage == nil || b.Usage == nil || a.Usage.Kind != UsageKindTokenDelta || b.Usage.Kind != UsageKindTokenDelta {
		return Event{}, false
	}
	x, y := a.Usage.TokenDelta, b.Usage.TokenDelta
	if !x.Present || !y.Present || x.Invalid || y.Invalid || x.Value < 0 || y.Value < 0 || y.Value > math.MaxInt64-x.Value {
		return Event{}, false
	}
	left, right := *a.Usage, *b.Usage
	left.TokenDelta, right.TokenDelta = ObservedCount{}, ObservedCount{}
	if left != right {
		return Event{}, false
	}
	merged.Usage.TokenDelta.Value = x.Value + y.Value
	return merged, true
}

// A completed snapshot is already finite and byte-bounded. Prebuffer it once
// instead of imposing the live producer's queue limit on a fast replay.
func newSnapshotOutput(events []Event) *managedOutput {
	var snapshot eventSnapshot
	for _, event := range events {
		if err := snapshot.append(event); err != nil {
			output := newManagedOutput()
			output.close(err)
			return output
		}
	}
	output := &managedOutput{events: make(chan Event, len(snapshot.events)), result: make(chan error, 1)}
	for _, event := range snapshot.events {
		output.events <- event
	}
	output.close(nil)
	return output
}
