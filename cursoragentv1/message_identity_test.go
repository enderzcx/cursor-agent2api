package cursoragentv1

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// The merged shape is emitted by Claude Code when older type64 responses reuse
// one message ID for a tool-use response and its completed assistant answer.
func TestMessagesCompletedToolNewUserHistoryWithCatalogEvolution(t *testing.T) {
	for _, merged := range []bool{false, true} {
		for _, catalogChanged := range []bool{false, true} {
			t.Run(fmt.Sprintf("merged=%t/catalog_changed=%t", merged, catalogChanged), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var runs atomic.Int32
				executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
					switch runs.Add(1) {
					case 1:
						emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "local-probe-call", Name: "Bash", Arguments: `{}`}})
					case 2:
						emit(Event{Type: EventText, Text: "FIRST_DONE"})
					default:
						require.Len(t, request.Tools, 1)
						require.Equal(t, catalogChanged, strings.Contains(request.Tools[0].Description, "updated help"))
						emit(Event{Type: EventText, Text: "FOLLOWUP_OK"})
					}
					emit(Event{Type: EventDone})
					return nil
				}})
				tools := `[{"name":"Bash","description":"Run a harmless command","input_schema":{"type":"object"}}]`
				prefix := `{"model":"claude-sonnet-4-6","system":"x-anthropic-billing-header: cc_version=2.1.251; cc_entrypoint=sdk-cli;","messages":`
				first, err := executor.Execute(ctx, testExecutorAuth(), messageIdentityRequest("local-first", prefix+`[{"role":"user","content":"do local probe"}],"tools":`+tools+`}`), claudeExecutorOptions(false))
				require.NoError(t, err)
				ids := agentV1ToolCallIDs(first.Payload)
				require.Len(t, ids, 1)
				toolBlock := `{"type":"tool_use","id":"` + ids[0] + `","name":"Bash","input":{}}`
				resultBlock := `{"type":"tool_result","tool_use_id":"` + ids[0] + `","content":"LOCAL_TOOL_OK"}`
				resultMessages := `[{"role":"user","content":"do local probe"},` + string(first.Payload) + `,{"role":"user","content":[` + resultBlock + `]}]`
				second, err := executor.Execute(ctx, testExecutorAuth(), messageIdentityRequest("local-result", prefix+resultMessages+`,"tools":`+tools+`}`), claudeExecutorOptions(false))
				require.NoError(t, err)
				require.NotEqual(t, gjson.GetBytes(first.Payload, "id").String(), gjson.GetBytes(second.Payload, "id").String(), "different assistant responses must not merge in Claude Code")
				replay, err := executor.Execute(ctx, testExecutorAuth(), messageIdentityRequest("local-result-retry", prefix+resultMessages+`,"tools":`+tools+`}`), claudeExecutorOptions(false))
				require.NoError(t, err)
				require.Equal(t, gjson.GetBytes(second.Payload, "id").String(), gjson.GetBytes(replay.Payload, "id").String())
				require.Equal(t, second.Headers.Get(HeaderUsageReceipt), replay.Headers.Get(HeaderUsageReceipt))
				require.Equal(t, "1", replay.Headers.Get(HeaderUsageReplay))
				require.Eventually(t, func() bool {
					executor.manager.mu.Lock()
					defer executor.manager.mu.Unlock()
					return len(executor.manager.active)+len(executor.manager.starting) == 0
				}, time.Second, time.Millisecond)
				messages := `[{"role":"user","content":"do local probe"},`
				if merged {
					messages += `{"role":"assistant","content":[{"type":"text","text":"FIRST_DONE"},` + toolBlock + `]},`
					messages += `{"role":"user","content":[` + resultBlock + `,{"type":"text","text":"NEW_QUESTION"}]}`
				} else {
					messages += `{"role":"assistant","content":[` + toolBlock + `]},`
					messages += `{"role":"user","content":[` + resultBlock + `]},`
					messages += `{"role":"assistant","content":[{"type":"text","text":"FIRST_DONE"}]},`
					messages += `{"role":"user","content":[{"type":"text","text":"NEW_QUESTION"}]}`
				}
				messages += `,{"role":"system","content":"Available local tools"}]`
				if catalogChanged {
					tools = strings.Replace(tools, "Run a harmless command", "Run a harmless command with updated help", 1)
				}
				third, err := executor.Execute(ctx, testExecutorAuth(), messageIdentityRequest("local-next-user", prefix+messages+`,"tools":`+tools+`}`), claudeExecutorOptions(false))
				require.NoError(t, err)
				require.Contains(t, string(third.Payload), "FOLLOWUP_OK")
				require.Equal(t, int32(3), runs.Load())
				require.NotEqual(t, gjson.GetBytes(second.Payload, "id").String(), gjson.GetBytes(third.Payload, "id").String())
				require.NotEqual(t, second.Headers.Get(HeaderUsageReceipt), third.Headers.Get(HeaderUsageReceipt))
				if merged {
					retry, err := executor.Execute(ctx, testExecutorAuth(), messageIdentityRequest("local-next-user-retry", prefix+messages+`,"tools":`+tools+`}`), claudeExecutorOptions(false))
					require.NoError(t, err)
					require.Equal(t, gjson.GetBytes(third.Payload, "id").String(), gjson.GetBytes(retry.Payload, "id").String())
					require.Equal(t, third.Headers.Get(HeaderUsageReceipt), retry.Headers.Get(HeaderUsageReceipt))
					require.Equal(t, "1", retry.Headers.Get(HeaderUsageReplay))
					require.Equal(t, int32(3), runs.Load(), "retry must not rerun tools or the new question")
				}
			})
		}
	}
}

func messageIdentityRequest(requestID, payload string) cliproxyexecutor.Request {
	request := claudeExecutorRequest(requestID, payload)
	request.Metadata["beefapi_request_id"] = requestID
	return request
}

func TestMessagesIdentityUsesSegmentNotConversationOrBillingTurn(t *testing.T) {
	turn := translatedTurn{responseFormat: sdktranslator.FormatClaude, managed: ManagedRequest{
		SessionID: "conversation", BillingRunID: "turn2:one-turn", RequestSegmentID: "segment-a",
	}}
	first := newClaudeProjection(context.Background(), turn)
	turn.managed.RequestSegmentID = "segment-b"
	second := newClaudeProjection(context.Background(), turn)
	replay := newClaudeProjection(context.Background(), turn)
	require.NotEqual(t, first.messageID, second.messageID)
	require.Equal(t, second.messageID, replay.messageID)
	require.Contains(t, string(bytes.Join(second.start(), nil)), `"id":"`+second.messageID+`"`)
	turn.responseFormat = sdktranslator.FormatOpenAIResponse
	response := newClaudeProjection(context.Background(), turn)
	require.Equal(t, "conversation", response.messageID, "Responses must retain its continuation anchor")
}

func TestFollowUpCatalogEvolutionPreservesOwnership(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			store, err := NewFileStateStore(t.TempDir())
			require.NoError(t, err)
			request := managedTestRequest("catalog-owner")
			fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
			require.NoError(t, err)
			claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
				ToolCatalogDigest: request.ToolCatalogDigest, ToolCatalog: request.Run.Tools,
			})
			require.NoError(t, err)
			if pending {
				require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "still-pending", Name: "lookup", Arguments: `{}`}}))
			}
			require.NoError(t, store.Release(claim.Owner))
			manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
				t.Error("must not invoke provider for another owner or while tools are pending")
				return nil
			}}, store, time.Second)
			require.NoError(t, err)
			request.FollowUpText = "next question"
			request.ToolCatalogDigest = "sha256:updated-catalog"
			if !pending {
				request.AccountKey = "different-account"
			}
			_, err = manager.StartFollowUp(request, nil)
			if pending {
				require.ErrorIs(t, err, ErrSessionPending)
			} else {
				require.ErrorIs(t, err, ErrCredentialMismatch)
			}
			path, _ := store.paths(request.SessionID)
			snapshot, err := readState(path)
			require.NoError(t, err)
			require.Equal(t, "sha256:test-tools", snapshot.ToolCatalogDigest)
			require.Equal(t, fingerprint, snapshot.CredentialFingerprint)
		})
	}
}

func TestFollowUpNoneThenOmittedRetainsCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	var catalogs [][]ToolDefinition
	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
		catalogs = append(catalogs, cloneToolDefinitions(request.Tools))
		emit(Event{Type: EventText, Text: "done"})
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("none-then-omitted")
	request.Run.Tools, err = normalizeToolDefinitions(request.Run.Tools)
	require.NoError(t, err)
	first, err := manager.Start(request)
	require.NoError(t, err)
	collectManagedEvents(first.Events)
	require.NoError(t, <-first.Result)
	waitUntilInactive(t, manager, request.SessionID)
	request.Run.Tools = nil
	request.ToolCatalogDigest = ""
	request.DisableCallerTools = true
	request.FollowUpText = "without tools"
	none, err := manager.StartFollowUp(request, nil)
	require.NoError(t, err)
	collectManagedEvents(none.Events)
	require.NoError(t, <-none.Result)
	waitUntilInactive(t, manager, request.SessionID)
	request.DisableCallerTools = false
	request.FollowUpText = "tools omitted"
	omitted, err := manager.StartFollowUp(request, nil)
	require.NoError(t, err)
	collectManagedEvents(omitted.Events)
	require.NoError(t, <-omitted.Result)
	waitUntilInactive(t, manager, request.SessionID)
	require.Len(t, catalogs, 3)
	require.Empty(t, catalogs[1], "none only disables this request")
	require.Equal(t, catalogs[0], catalogs[2], "omitted tools inherit the original catalog")
	path, _ := store.paths(request.SessionID)
	snapshot, err := readState(path)
	require.NoError(t, err)
	require.Equal(t, "sha256:test-tools", snapshot.ToolCatalogDigest)
}
