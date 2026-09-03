package cursoragentv1

import (
	"context"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

// Cancel exactly at the next select after Park, independently of scheduling.
type cancelAfterParkContext struct {
	context.Context
	done  chan struct{}
	calls int
}

func (c *cancelAfterParkContext) Done() <-chan struct{} {
	c.calls++
	if c.calls == 2 {
		close(c.done)
	}
	return c.done
}

func (c *cancelAfterParkContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestMixedFollowUpCancelAfterParkPreservesPendingBatch(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	manager, err := newSessionManager(&scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "next-batch", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("mixed-park-cancel")
	t.Cleanup(func() { _ = manager.Cancel(request) })
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	turn := &translatedTurn{managed: request}
	turn.managed.FollowUpText = "continue after tools"
	events := make(chan Event, 1)
	events <- Event{Type: EventTurnEnded}
	// Keep the delivery channel open to model cancellation while draining.
	output := &ManagedOutput{Events: events, Result: make(chan error)}
	ctx := &cancelAfterParkContext{Context: context.Background(), done: make(chan struct{})}
	_, err = (&Executor{manager: manager}).maybeChainFollowUp(ctx, turn, output)
	require.ErrorIs(t, err, context.Canceled)
	resumed, err := manager.ResumeTool(request, ToolResult{ToolCallID: "next-batch", Content: "ok"})
	require.NoError(t, err, "HTTP cancellation after Park must not delete durable pending tools")
	for range resumed.Events {
	}
	require.NoError(t, <-resumed.Result)
}

func TestContinueStreamCancelAfterParkPreservesPending(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	manager, err := newSessionManager(&scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "next-batch", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("stream-park-cancel-review")
	t.Cleanup(func() { _ = manager.Cancel(request) })
	first, err := manager.Start(request)
	require.NoError(t, err)
	tool := <-first.Events
	require.Equal(t, EventToolCall, tool.Type)
	ctx := &cancelAfterParkContext{Context: context.Background(), done: make(chan struct{})}
	projector := newClaudeProjection(ctx, translatedTurn{managed: request, responseFormat: sdktranslator.FormatClaude})
	_, _, _, err = projector.handle(Event{Type: EventText, Text: "I will look that up."})
	require.NoError(t, err)
	_, _, _, err = projector.handle(tool)
	require.NoError(t, err)
	events := make(chan Event, 1)
	events <- Event{Type: EventTurnEnded}
	output := &ManagedOutput{Events: events, Result: make(chan error)}
	// Model a full downstream queue. The second select cancels only after Park.
	chunks := make(chan cliproxyexecutor.StreamChunk)
	(&Executor{manager: manager}).continueStream(ctx, request, output, projector, chunks)
	resumed, err := manager.ResumeTool(request, ToolResult{ToolCallID: "next-batch", Content: "ok"})
	t.Logf("resume after parked stream cancellation error=%v", err)
	require.NoError(t, err, "HTTP cancellation after Park must preserve pending tools")
	for range resumed.Events {
	}
	require.NoError(t, <-resumed.Result)
}
