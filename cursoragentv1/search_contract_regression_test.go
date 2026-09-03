package cursoragentv1

import (
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

// Lead-owned acceptance boundary: splitting Messages child search must not
// remove the existing web-fetch capability from Responses/Chat search.
func TestSearchChildChangePreservesResponsesAndChatFetch(t *testing.T) {
	for _, tc := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatOpenAIResponse, `{"model":"claude-opus-5","input":"search and open a source","tools":[{"type":"web_search"}]}`},
		{sdktranslator.FormatOpenAI, `{"model":"claude-opus-5","messages":[{"role":"user","content":"search and open a source"}],"web_search_options":{}}`},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			req := cliproxyexecutor.Request{Model: "claude-opus-5", Payload: []byte(tc.body), Format: tc.format, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "legacy-web-contract"}}
			opts := withHostedSearchHeader(cliproxyexecutor.Options{SourceFormat: tc.format, ResponseFormat: tc.format, OriginalRequest: req.Payload})
			turn, err := translateExecutionRequest(req, opts, "scope", "account", "https://agent.example", "test-access", "test-cli")
			require.NoError(t, err)
			require.True(t, turn.managed.Run.AllowHostedSearch)
			require.True(t, turn.managed.Run.AllowHostedFetch, "existing Responses/Chat search included native page opening")
		})
	}
}

func TestSearchChildChangePreservesMainNativeFetch(t *testing.T) {
	body := `{"model":"claude-opus-5","system":"You are Claude Code, Anthropic's official CLI for Claude.","messages":[{"role":"user","content":"search"}],"tools":[{"name":"WebSearch","input_schema":{"type":"object"}},{"name":"WebFetch","input_schema":{"type":"object"}}]}`
	turn, err := translateExecutionRequest(claudeExecutorRequest("cc-main-web", body), withHostedSearchHeader(claudeExecutorOptions(false)), "scope", "account", "https://agent.example", "test-access", "test-cli")
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedSearch, "main search belongs to the caller wrapper")
	require.True(t, turn.managed.Run.AllowHostedFetch, "approved change preserves existing hosted WebFetch independently")
	require.Len(t, turn.managed.Run.Tools, 1)
	require.Equal(t, "WebSearch", turn.managed.Run.Tools[0].Name)
}

func TestSearchChildDoesNotEnableUnselectedNativeFetch(t *testing.T) {
	body := `{"model":"claude-opus-5","system":"You are Claude Code, Anthropic's official CLI for Claude.","messages":[{"role":"user","content":"search"}],"tools":[{"name":"WebSearch","input_schema":{"type":"object"}},{"name":"WebFetch","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"WebSearch"}}`
	turn, err := translateExecutionRequest(claudeExecutorRequest("cc-force-web", body), withHostedSearchHeader(claudeExecutorOptions(false)), "scope", "account", "https://agent.example", "test-access", "test-cli")
	require.NoError(t, err)
	require.False(t, turn.managed.Run.AllowHostedFetch)
	require.True(t, turn.managed.Run.RequireToolCall)
}

func TestSearchChildCapsCannotLeakAcrossParkedCallerRuns(t *testing.T) {
	body := `{"model":"claude-opus-5","messages":[{"role":"user","content":"search"}],"tools":[{"name":"Bash","input_schema":{"type":"object"}},{"type":"web_search_20250305","name":"web_search","max_uses":8}]}`
	_, err := translateExecutionRequest(claudeExecutorRequest("mixed-cap", body), withHostedSearchHeader(claudeExecutorOptions(false)), "scope", "account", "https://agent.example", "test-access", "test-cli")
	require.ErrorContains(t, err, "server-tool-only")
}
