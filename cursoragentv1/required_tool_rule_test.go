package cursoragentv1

import (
	"context"
	"testing"
	"time"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRunnerTransmitsRequiredToolChoiceOnlyBeforeFirstInvocation(t *testing.T) {
	for _, tc := range []struct {
		name             string
		required, resume bool
	}{
		{"required", true, false}, {"auto", false, false}, {"resume", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := newFakeStream()
			runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
			req := baseRunRequest()
			req.RequireToolCall, req.Resume = tc.required, tc.resume
			req.Tools = []ToolDefinition{{Name: "session_title", InputSchema: []byte(`{"type":"object"}`)}}
			req.ToolResults = make(chan ToolResult)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			called := make(chan struct{}, 1)
			go func() {
				result <- runner.Run(ctx, req, func(e Event) {
					if e.Type == EventToolCall {
						called <- struct{}{}
					}
				})
			}()
			assertRunWasWritten(t, upstream)
			readContext := func() *agentv1.RequestContext {
				upstream.data <- frameConnect(encodeServerRequestContext(7, "ctx"))
				select {
				case frame := <-upstream.writes:
					_, payload, _, complete, err := parseConnectFrame(frame)
					require.NoError(t, err)
					require.True(t, complete)
					message := &agentv1.AgentClientMessage{}
					require.NoError(t, proto.Unmarshal(payload, message))
					return message.GetExecClientMessage().GetRequestContextResult().GetSuccess().GetRequestContext()
				case <-time.After(time.Second):
					t.Fatal("context response missing")
					return nil
				}
			}
			rc := readContext()
			require.Equal(t, "be concise", rc.GetCloudRule(), "existing system transport is unchanged")
			if tc.required && !tc.resume {
				require.Len(t, rc.Rules, 1)
				rule := rc.Rules[0]
				require.NotNil(t, rule.Type.GetGlobal())
				require.EqualValues(t, agentv1.CursorRuleSource_CURSOR_RULE_SOURCE_USER, rule.Source)
				require.Contains(t, rule.Content, "session_title")
				require.Contains(t, rule.Content, "not a substitute")
			} else {
				require.Empty(t, rc.Rules)
			}
			upstream.data <- frameConnect(encodeServerMCP(t, 9, "exec", "call", "session_title", map[string]any{}))
			select {
			case <-called:
			case <-time.After(time.Second):
				t.Fatal("tool callback missing")
			}
			require.Empty(t, readContext().Rules, "do not request a second tool after a real invocation")
			cancel()
			require.ErrorIs(t, <-result, context.Canceled)
		})
	}
}
