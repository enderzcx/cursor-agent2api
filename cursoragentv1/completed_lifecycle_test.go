package cursoragentv1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLongToolContinuationKeepsHotAndRestartReplay(t *testing.T) {
	for _, metered := range []bool{false, true} {
		t.Run(fmt.Sprint("metered=", metered), func(t *testing.T) {
			root := t.TempDir()
			store, err := NewFileStateStore(root)
			require.NoError(t, err)
			var runs atomic.Int32
			consumed := make(chan struct{})
			engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
				runs.Add(1)
				emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "long-tool", Name: "lookup", Arguments: `{}`}})
				select {
				case result := <-request.ToolResults:
					emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				case <-ctx.Done():
					return ctx.Err()
				}
				for i := 0; i < 300; i++ {
					emit(Event{Type: EventText, Text: "x"})
					select {
					case <-consumed:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				emit(testMeasuredTerminal(17))
				emit(Event{Type: EventDone})
				return nil
			}}
			manager, err := newSessionManager(engine, store, 10*time.Second)
			require.NoError(t, err)
			request := managedTestRequest("long-continuation")
			request.TurnMetering = metered
			t.Cleanup(func() { _ = manager.Cancel(request) })
			first, err := manager.Start(request)
			require.NoError(t, err)
			require.Equal(t, EventToolCall, (<-first.Events).Type)
			require.NoError(t, manager.Park(request))
			for range first.Events {
			}
			require.NoError(t, <-first.Result)
			result := ToolResult{ToolCallID: "long-tool", Content: "ok"}
			second, err := manager.ResumeTool(request, result)
			require.NoError(t, err)
			live := []Event{}
			for event := range second.Events {
				live = append(live, event)
				if event.Type == EventText {
					consumed <- struct{}{}
				}
			}
			require.NoError(t, <-second.Result)
			require.Len(t, live, 303, "the first response still streams every original delta")
			require.Equal(t, EventDone, live[len(live)-1].Type)
			if metered {
				require.Equal(t, first.BillingRunID, second.BillingRunID)
			} else {
				require.NotEqual(t, first.BillingRunID, second.BillingRunID)
			}
			restartedStore, err := NewFileStateStore(root)
			require.NoError(t, err)
			restarted, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
				t.Error("completed replay must not run the provider")
				return errors.New("unexpected provider call")
			}}, restartedStore, time.Second)
			require.NoError(t, err)
			for _, replayManager := range []*SessionManager{manager, restarted} {
				replay, err := replayManager.ResumeTool(request, result)
				require.NoError(t, err)
				require.True(t, replay.Replay)
				require.Equal(t, second.BillingRunID, replay.BillingRunID)
				events := collectManagedEvents(replay.Events)
				require.NoError(t, <-replay.Result)
				require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventUsageObservation, EventDone}, eventTypes(events))
				require.Equal(t, strings.Repeat("x", 300), events[1].Text)
				require.Equal(t, live[len(live)-2].Usage, events[2].Usage)
			}
			require.EqualValues(t, 1, runs.Load())
		})
	}
}

type completedSaveStore struct {
	StateStore
	entered chan struct{}
	release chan struct{}
	err     error
}

func (s *completedSaveStore) SaveCompleted(owner OwnerLease, results []ToolResult, events []Event, ephemeral bool, followUp string) error {
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	if s.err != nil {
		return s.err
	}
	return s.StateStore.SaveCompleted(owner, results, events, ephemeral, followUp)
}

func TestCompletionWaitsForSnapshotPersistence(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("fail=", fail), func(t *testing.T) {
			store, err := NewFileStateStore(t.TempDir())
			require.NoError(t, err)
			gate := &completedSaveStore{StateStore: store, entered: make(chan struct{}), release: make(chan struct{})}
			if fail {
				gate.err = errors.New("snapshot disk failure")
			}
			engine := &scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
				emit(Event{Type: EventText, Text: "visible before storage"})
				emit(Event{Type: EventDone})
				return nil
			}}
			manager, err := newSessionManager(engine, gate, 10*time.Second)
			require.NoError(t, err)
			claim, err := store.Claim("persist-gate", "conversation", "credential", ClaimOptions{})
			require.NoError(t, err)
			output, err := manager.startClaimed(RunRequest{}, claim.Owner, nil, nil, []ToolResult{{ToolCallID: "tool", Content: "ok"}}, []string{"tool"}, nil, false)
			require.NoError(t, err)
			<-gate.entered
			require.Equal(t, EventText, (<-output.Events).Type)
			select {
			case event := <-output.Events:
				t.Fatalf("event %v escaped before persistence finished", event.Type)
			default:
			}
			close(gate.release)
			events := collectManagedEvents(output.Events)
			result := <-output.Result
			if fail {
				require.Empty(t, events)
				require.ErrorIs(t, result, gate.err)
			} else {
				require.Equal(t, []EventType{EventDone}, eventTypes(events))
				require.NoError(t, result)
			}
		})
	}
}

func TestAnchoredExecutorReportsCompletionPersistenceFailure(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint("stream=", streaming), func(t *testing.T) {
			executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
				emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "persist-tool", Name: "lookup", Arguments: `{}`}})
				emit(Event{Type: EventTurnEnded})
				select {
				case result := <-request.ToolResults:
					emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
					emit(Event{Type: EventText, Text: "partial-answer"})
					emit(Event{Type: EventDone})
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}})
			storageError := errors.New("snapshot disk failure")
			executor.manager.store = &completedSaveStore{StateStore: executor.manager.store, err: storageError}
			first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("persist-failure", `{
				"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
				"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
			}`), claudeExecutorOptions(false))
			require.NoError(t, err)
			ids := agentV1ToolCallIDs(first.Payload)
			require.Len(t, ids, 1)
			request := anchoredClaudeExecutorRequest("ignored", `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"`+ids[0]+`","content":"ok"}]}]}`)
			if !streaming {
				response, err := executor.Execute(context.Background(), testExecutorAuth(), request, claudeExecutorOptions(false))
				require.ErrorIs(t, err, storageError)
				require.Empty(t, response.Payload)
				return
			}
			stream, err := executor.ExecuteStream(context.Background(), testExecutorAuth(), request, claudeExecutorOptions(true))
			require.NoError(t, err)
			var payload strings.Builder
			var terminalError error
			for chunk := range stream.Chunks {
				payload.Write(chunk.Payload)
				if chunk.Err != nil {
					terminalError = chunk.Err
				}
			}
			require.ErrorIs(t, terminalError, storageError)
			require.Contains(t, payload.String(), "partial-answer")
			require.NotContains(t, payload.String(), "message_stop", "failed persistence cannot be followed by success")
			require.NotContains(t, payload.String(), `"stop_reason":"end_turn"`)
		})
	}
}
