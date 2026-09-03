package cursoragentv1

import (
	"context"
	"testing"
	"time"

	agentv1 "github.com/enderzcx/cursor-agent2api/cursoragentv1/gen"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const claudeCodeChildSearchPayload = `{
	"model":"claude-sonnet-4-6",
	"system":["You are an assistant for performing a web search tool use"],
	"messages":[{"role":"user","content":"Perform a web search for the query: Grok Bot use cases"}],
	"tools":[{"type":"web_search_20250305","name":"web_search","max_uses":8,"allowed_domains":[],"blocked_domains":[]}],
	"tool_choice":{"type":"tool","name":"web_search"},
	"thinking":{"type":"disabled"}
}`

func TestTranslateClaudeCodeChildTypedSearch(t *testing.T) {
	req := claudeExecutorRequest("cc-child-search", claudeCodeChildSearchPayload)
	turn, err := translateExecutionRequest(req, withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)
	require.False(t, turn.managed.Run.AllowHostedFetch)
	require.True(t, turn.managed.Run.RequireHostedSearch)
	require.False(t, turn.managed.Run.RequireToolCall)
	require.Empty(t, turn.managed.Run.Tools)
	require.NotNil(t, turn.managed.Run.HostedSearchMaxUses)
	require.Equal(t, 8, *turn.managed.Run.HostedSearchMaxUses)
	require.Contains(t, turn.managed.Run.SystemText, "assistant for performing a web search tool use")
	require.Contains(t, turn.managed.Run.UserText, "Perform a web search for the query:")
}

func TestTranslateStandaloneTypedFetchDoesNotEnableSearch(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"fetch"}],"tools":[{"type":"web_fetch_20250305","name":"web_fetch","max_uses":2}]}`
	turn, err := translateExecutionRequest(claudeExecutorRequest("typed-fetch", payload), withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.AllowHostedFetch)
	require.False(t, turn.managed.Run.RequireHostedFetch)
	require.NotNil(t, turn.managed.Run.HostedFetchMaxUses)
	require.Equal(t, 2, *turn.managed.Run.HostedFetchMaxUses)
}

func TestTranslateCustomFunctionNamedWebSearchStaysCallerTool(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"search"}],"tool_choice":{"type":"tool","name":"web_search"},"tools":[{"name":"web_search","input_schema":{"type":"object"}},{"type":"web_search_20250305","name":"web_search"}]}`
	turn, err := translateExecutionRequest(claudeExecutorRequest("custom-web-search", payload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.RequireToolCall)
	require.Len(t, turn.managed.Run.Tools, 1)
	require.Equal(t, "web_search", turn.managed.Run.Tools[0].Name)
}

func TestTranslateNewerServerWebVersionsMapByFamily(t *testing.T) {
	// Anthropic ships dated versions of the same capability; Kiro and the
	// Anthropic relay match by prefix so a Claude Code bump must not 422 here.
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"search"}],"tools":[{"type":"web_search_20991231","name":"web_search"},{"type":"web_fetch_20991231","name":"web_fetch","max_content_tokens":5000,"citations":{"enabled":true}}]}`
	turn, err := translateExecutionRequest(claudeExecutorRequest("newer-search-version", payload), withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.AllowHostedFetch)
}

func TestTranslateSearchDomainsRequireSandAndReachRunRequest(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"search"}],"tools":[{"type":"web_search_20250305","name":"web_search","allowed_domains":["example.com","docs.python.org/3"]},{"type":"web_fetch_20250305","name":"web_fetch","blocked_domains":["evil.test"]}]}`
	_, err := translateExecutionRequest(claudeExecutorRequest("restricted-search-agent", payload), withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.ErrorContains(t, err, "require the Sand runtime profile")

	sandOpts := withHostedSearchHeader(claudeExecutorOptions(false))
	sandOpts.Metadata = map[string]any{"beefapi_cursor_runtime_profile": runtimeSand}
	turn, err := translateExecutionRequest(claudeExecutorRequest("restricted-search-sand", payload), sandOpts, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, []string{"example.com", "docs.python.org/3"}, turn.managed.Run.HostedSearchDomains.Allowed)
	require.Equal(t, []string{"evil.test"}, turn.managed.Run.HostedFetchDomains.Blocked)

	both := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"search"}],"tools":[{"type":"web_search_20250305","name":"web_search","allowed_domains":["a.test"],"blocked_domains":["b.test"]}]}`
	_, err = translateExecutionRequest(claudeExecutorRequest("restricted-search-both", both), sandOpts, "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.ErrorContains(t, err, "cannot combine")
}

func TestHostedDomainPolicyMatchesAnthropicSemantics(t *testing.T) {
	allowed := hostedDomainPolicy{Allowed: []string{"example.com", "docs.python.org/3"}}
	require.True(t, allowed.permits("https://example.com/page"))
	require.True(t, allowed.permits("https://www.example.com/page"))
	require.True(t, allowed.permits("https://blog.example.com/x"))
	require.False(t, allowed.permits("https://notexample.com/"))
	require.True(t, allowed.permits("https://docs.python.org/3/library/os.html"))
	require.False(t, allowed.permits("https://docs.python.org/2/library/os.html"))
	require.False(t, allowed.permits("not a url"))

	blocked := hostedDomainPolicy{Blocked: []string{"evil.test"}}
	require.False(t, blocked.permits("https://cdn.evil.test/a"))
	require.True(t, blocked.permits("https://good.test/a"))
	require.True(t, hostedDomainPolicy{}.permits("anything"))
}

func TestTranslateAnyWithOnlyTypedSearchRequiresHostedEvidence(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"latest"}],"tool_choice":{"type":"any"},"tools":[{"type":"web_search_20250305","name":"web_search"}]}`
	turn, err := translateExecutionRequest(claudeExecutorRequest("any-hosted-only", payload), withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)
	require.True(t, turn.managed.Run.RequireHostedSearch)
	require.False(t, turn.managed.Run.RequireToolCall)
	require.Empty(t, turn.managed.Run.Tools)
}

func TestTranslateAnyWithCallerAndTypedSearchRetainsUnsupportedBoundary(t *testing.T) {
	payload := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"latest"}],"tool_choice":{"type":"any"},"tools":[{"type":"web_search_20250305","name":"web_search"},{"name":"Bash","input_schema":{"type":"object"}}]}`
	_, err := translateExecutionRequest(claudeExecutorRequest("any-mixed", payload), withHostedSearchHeader(claudeExecutorOptions(false)), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.ErrorContains(t, err, "server-tool-only")
}

func TestTranslateForcedTypedSearchWorksWithoutChannelHeader(t *testing.T) {
	// Hosted search is an account capability; no per-channel header is needed.
	turn, err := translateExecutionRequest(claudeExecutorRequest("child-no-cap", claudeCodeChildSearchPayload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.True(t, turn.managed.Run.AllowHostedSearch)
}

func TestTranslateResponsesRequiredSearchStillUnsupported(t *testing.T) {
	payload := []byte(`{"model":"glm-5.2","input":"hello","tools":[{"type":"web_search"}],"tool_choice":"required"}`)
	req := cliproxyexecutor.Request{Model: "glm-5.2-high", Payload: payload, Format: sdktranslator.FormatOpenAIResponse, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "responses-required-search"}}
	_, err := translateExecutionRequest(req, withHostedSearchHeader(cliproxyexecutor.Options{SourceFormat: req.Format, ResponseFormat: req.Format, OriginalRequest: payload}), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.ErrorContains(t, err, "required web_search is unsupported")
}

func TestDecodeTypedWebSearchQueryCarriesToolCallID(t *testing.T) {
	payload, err := proto.Marshal(&agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionQuery{
			InteractionQuery: &agentv1.InteractionQuery{
				Id: 41,
				Query: &agentv1.InteractionQuery_WebSearchRequestQuery{
					WebSearchRequestQuery: &agentv1.WebSearchRequestQuery{
						Args: &agentv1.WebSearchArgs{SearchTerm: "Grok Bot", ToolCallId: "ws-child-1"},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	decoded, err := decodeServerMessage(payload)
	require.NoError(t, err)
	require.Equal(t, wireInteractionQuery, decoded.Type)
	require.Equal(t, protowire.Number(2), decoded.InteractionKind)
	require.Equal(t, "ws-child-1", decoded.ToolCallID)
}

func TestRunnerSearchOnlyDoesNotEnableFetchNativeTool(t *testing.T) {
	upstream := newFakeStream()
	opener := &fakeOpener{stream: upstream}
	runner := newRunnerForTest(opener, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, nil) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, []string{"mcp_tool_call", "web_search_tool_call"}, opener.config.AllowedNativeTools)
}

func TestRunnerFetchOnlyDoesNotEnableSearchNativeTool(t *testing.T) {
	upstream := newFakeStream()
	opener := &fakeOpener{stream: upstream}
	runner := newRunnerForTest(opener, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedFetch = true
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, nil) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, []string{"mcp_tool_call", "web_fetch_tool_call"}, opener.config.AllowedNativeTools)
}

func TestRunnerRequiredHostedSearchNeedsNativeExecution(t *testing.T) {
	t.Run("approval is not execution", func(t *testing.T) {
		upstream := newFakeStream()
		runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
		request := baseRunRequest()
		request.AllowHostedSearch = true
		request.RequireHostedSearch = true
		result := make(chan error, 1)
		go func() { result <- runner.Run(context.Background(), request, nil) }()
		assertRunWasWritten(t, upstream)
		upstream.data <- frameConnect(mustMarshal(t, webSearchQuery(24, "ws-1")))
		upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
		require.EqualError(t, <-result, "Cursor Agent v1 completed without the required hosted search")
	})
	t.Run("native completion satisfies", func(t *testing.T) {
		upstream := newFakeStream()
		runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
		request := baseRunRequest()
		request.AllowHostedSearch = true
		request.RequireHostedSearch = true
		result := make(chan error, 1)
		go func() { result <- runner.Run(context.Background(), request, nil) }()
		assertRunWasWritten(t, upstream)
		done, err := proto.Marshal(webSearchCompleted("ws-1", "q", "https://example.com", "Example"))
		require.NoError(t, err)
		upstream.data <- frameConnect(done)
		upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
		require.NoError(t, <-result)
	})
}

func TestRunnerEnforcesHostedSearchMaxUsesAndDedupesIDs(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	maxUses := 1
	request.HostedSearchMaxUses = &maxUses
	result := make(chan error, 1)
	approvedCalls := 0
	go func() {
		result <- runner.Run(context.Background(), request, func(e Event) {
			if e.Type == EventHostedSearchCall {
				approvedCalls++
			}
		})
	}()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(mustMarshal(t, webSearchQuery(31, "ws-1")))
	require.True(t, interactionApproved(t, readInteraction(t, upstream), 2))
	upstream.data <- frameConnect(mustMarshal(t, webSearchQuery(32, "ws-1")))
	require.True(t, interactionApproved(t, readInteraction(t, upstream), 2), "duplicate tool id must not consume another use")
	upstream.data <- frameConnect(mustMarshal(t, webSearchQuery(33, "ws-2")))
	second := readInteraction(t, upstream)
	require.False(t, interactionApproved(t, second, 2))
	require.Equal(t, "hosted search exceeded max_uses", interactionRejectReason(t, second, 2))
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, 1, approvedCalls, "duplicate approvals must not inflate usage evidence")
}

func TestRunnerSearchApprovalDoesNotApproveFetch(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, nil) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerInteractionQuery(41, 2))
	require.True(t, interactionApproved(t, readInteraction(t, upstream), 2))
	upstream.data <- frameConnect(encodeServerInteractionQuery(42, 9))
	require.False(t, interactionApproved(t, readInteraction(t, upstream), 9))
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func webSearchQuery(id uint32, toolCallID string) *agentv1.AgentServerMessage {
	return &agentv1.AgentServerMessage{
		Message: &agentv1.AgentServerMessage_InteractionQuery{
			InteractionQuery: &agentv1.InteractionQuery{
				Id: id,
				Query: &agentv1.InteractionQuery_WebSearchRequestQuery{
					WebSearchRequestQuery: &agentv1.WebSearchRequestQuery{
						Args: &agentv1.WebSearchArgs{SearchTerm: "q", ToolCallId: toolCallID},
					},
				},
			},
		},
	}
}

func mustMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	payload, err := proto.Marshal(message)
	require.NoError(t, err)
	return payload
}

func readInteraction(t *testing.T, upstream *fakeStream) []byte {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case write := <-upstream.writes:
			if topLevelField(t, write) == agentClientInteractionResponse {
				return write[connectHeaderSize:]
			}
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatal("missing interaction response")
	return nil
}

func interactionApproved(t *testing.T, payload []byte, kind protowire.Number) bool {
	t.Helper()
	response := decodeBytes(payload, agentClientInteractionResponse)
	result := decodeBytes(response, kind)
	return len(decodeBytes(result, 1)) == 0 && len(result) > 0 && len(decodeBytes(result, 2)) == 0
}

func interactionRejectReason(t *testing.T, payload []byte, kind protowire.Number) string {
	t.Helper()
	response := decodeBytes(payload, agentClientInteractionResponse)
	result := decodeBytes(response, kind)
	return decodeString(decodeBytes(result, 2), 1)
}
