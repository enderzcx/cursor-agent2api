package cursoragentv1

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/stretchr/testify/require"
)

func TestAnchoredMeteredCarrySurvivesAnotherToolAndColdResume(t *testing.T) {
	for _, cold := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "cold"}[cold], func(t *testing.T) {
			var runs atomic.Int32
			engine := &scriptedEngine{run: func(ctx context.Context, req RunRequest, emit func(Event)) error {
				n := runs.Add(1)
				if req.Resume {
					select {
					case r := <-req.ToolResults:
						emit(Event{Type: EventToolResultAccepted, ToolResult: &r})
					case <-ctx.Done():
						return ctx.Err()
					}
					emit(testMeasuredTerminal(5))
					emit(Event{Type: EventText, Text: "FINAL"})
					emit(Event{Type: EventDone})
					return nil
				}
				id := "first-tool"
				if n > 1 {
					id = "second-tool"
				}
				emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: id, Name: "lookup", Arguments: `{}`}})
				emit(Event{Type: EventTurnEnded})
				select {
				case r := <-req.ToolResults:
					emit(Event{Type: EventToolResultAccepted, ToolResult: &r})
				case <-ctx.Done():
					return ctx.Err()
				}
				out := int64(7)
				if n > 1 {
					out = 5
				}
				emit(testMeasuredTerminal(out))
				emit(Event{Type: EventText, Text: "FINAL"})
				emit(Event{Type: EventDone})
				return nil
			}}
			executor := testExecutor(t, engine)
			call := func(id, body string) ([]byte, string) {
				t.Helper()
				r, e := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest(id, body), claudeExecutorOptions(false))
				require.NoError(t, e)
				return r.Payload, r.Headers.Get(HeaderUsageReceipt)
			}
			first, receipt := call("carry-session", `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}]}`)
			firstID := agentV1ToolCallIDs(first)[0]
			mixedBody := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + firstID + `","content":"ok"},{"type":"text","text":"more"}]}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}]}`
			mixed, mixedReceipt := call("carry-mixed", mixedBody)
			require.Equal(t, receipt, mixedReceipt)
			var decoded map[string]any
			require.NoError(t, jsonx.Unmarshal(mixed, &decoded))
			require.Equal(t, true, decoded["usage"].(map[string]any)["cursor_agent_v1_usage_pending"])
			secondID := agentV1ToolCallIDs(mixed)[0]
			if cold {
				executor.manager.mu.Lock()
				active := executor.manager.active["carry-session"]
				executor.manager.mu.Unlock()
				require.NotNil(t, active)
				active.cancel()
				waitUntilInactive(t, executor.manager, "carry-session")
				manager, e := newSessionManager(engine, executor.manager.store, 3*time.Second)
				require.NoError(t, e)
				executor = newExecutorForTest(manager, nil)
			}
			body := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + secondID + `","content":"ok"}]}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}]}`
			final, finalReceipt := call("carry-final", body)
			require.Equal(t, receipt, finalReceipt)
			usage := usageMap(t, final)
			require.EqualValues(t, 12, usage["output_tokens"])
			require.EqualValues(t, 2, usage["input_tokens"])
			// A completed HTTP result must also replay after its in-flight
			// follower shortcut is gone and only durable state remains.
			waitUntilInactive(t, executor.manager, "carry-session")
			replay, replayReceipt := call("carry-replay", body)
			require.Equal(t, receipt, replayReceipt)
			require.JSONEq(t, string(final), string(replay))
			want := int32(2)
			if cold {
				want = 3
			}
			require.Equal(t, want, runs.Load())
		})
	}
}
