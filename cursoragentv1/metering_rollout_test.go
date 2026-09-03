package cursoragentv1

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnchoredMeteringGatewayRolloutNegotiation(t *testing.T) {
	for _, metered := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-gateway", true: "new-gateway"}[metered], func(t *testing.T) {
			var submitted atomic.Int32
			executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, req RunRequest, emit func(Event)) error {
				emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "rollout-tool", Name: "lookup", Arguments: `{}`}})
				emit(Event{Type: EventTurnEnded})
				select {
				case r := <-req.ToolResults:
					submitted.Add(1)
					emit(Event{Type: EventToolResultAccepted, ToolResult: &r})
				case <-ctx.Done():
					return ctx.Err()
				}
				emit(testMeasuredTerminal(7))
				emit(Event{Type: EventText, Text: "ok"})
				emit(Event{Type: EventDone})
				return nil
			}})
			old := claudeExecutorOptions(false)
			old.Headers = nil
			opts := old
			if metered {
				opts = claudeExecutorOptions(false)
			}
			first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("rollout", `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}]}`), opts)
			require.NoError(t, err)
			require.Equal(t, metered, strings.HasPrefix(first.Headers.Get(HeaderUsageReceipt), "cursor-agent-v1:turn:"))
			id := agentV1ToolCallIDs(first.Payload)[0]
			body := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + id + `","content":"ok"}]}]}`
			if metered {
				_, err = executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("old-resume", body), old)
				require.Equal(t, 503, errorStatusCode(err))
				require.Zero(t, submitted.Load())
			}
			last, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("new-resume", body), opts)
			require.NoError(t, err)
			require.Equal(t, int32(1), submitted.Load())
			if metered {
				require.Equal(t, first.Headers.Get(HeaderUsageReceipt), last.Headers.Get(HeaderUsageReceipt))
				_, err = executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("old-replay", body), old)
				require.Equal(t, 503, errorStatusCode(err))
				require.Equal(t, int32(1), submitted.Load())
			}
		})
	}
}
