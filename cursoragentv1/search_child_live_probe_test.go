package cursoragentv1

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/stretchr/testify/require"
)

// Opt-in provider evidence, not gateway/ledger or interactive client proof.
func TestAuthorizedClaudeSearchChildLive(t *testing.T) {
	if os.Getenv("CURSOR_SEARCH_CHILD_PROBE") != "one-run" {
		t.Skip("dedicated native search probe not authorized")
	}
	metadata := liveProbeMetadata(t)
	executor, err := NewExecutor(t.TempDir())
	require.NoError(t, err)
	auth := &cliproxyauth.Auth{ID: "dedicated-search-child-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// Production's CPA auth manager performs this exchange. A direct Executor
	// probe must not reuse a DB snapshot's already-expired access token.
	auth, err = executor.Refresh(ctx, auth)
	if err != nil {
		t.Fatal("dedicated probe credential exchange failed")
	}
	metadata = auth.Metadata
	body := `{"model":"claude-opus-5","max_tokens":1024,"system":[{"type":"text","text":"You are an assistant for performing a web search tool use"}],"tools":[{"type":"web_search_20250305","name":"web_search","max_uses":1}],"tool_choice":{"type":"tool","name":"web_search"},"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"Perform a web search for the query: Cursor CLI official documentation. Return a concise answer with a source URL."}]}`
	request := claudeExecutorRequest("search-child-live-"+uuid.NewString(), body)
	// Same native intent selected by the gateway's agentV1ModelIntents table.
	request.Model = "claude-opus-5-high"
	options := claudeExecutorOptions(false)
	options.Headers.Set(headerCursorAgentV1NativeWebSearch, "true")
	response, err := executor.Execute(ctx, auth, request, options)
	if err != nil {
		message := err.Error()
		for _, value := range metadata {
			if secret, ok := value.(string); ok && secret != "" {
				message = strings.ReplaceAll(message, secret, "[redacted]")
			}
		}
		t.Fatal(message)
	}
	var output map[string]any
	require.NoError(t, jsonx.Unmarshal(response.Payload, &output))
	require.Equal(t, "end_turn", output["stop_reason"])
	blocks, _ := output["content"].([]any)
	calls, results, urls := 0, 0, 0
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		switch block["type"] {
		case "tool_use":
			t.Fatal("typed child incorrectly emitted a caller tool")
		case "server_tool_use":
			if block["name"] == "web_search" {
				calls++
			}
		case "web_search_tool_result":
			results++
			items, _ := block["content"].([]any)
			for _, item := range items {
				result, _ := item.(map[string]any)
				url, _ := result["url"].(string)
				if strings.HasPrefix(url, "https://") {
					urls++
				}
			}
		}
	}
	require.Equal(t, 1, calls, "forced typed search must run and respect max_uses=1")
	require.Equal(t, calls, results)
	require.Positive(t, urls, "real search must return source URLs")
	t.Logf("native typed child completed: search_calls=%d search_results=%d source_urls=%d; no caller tool", calls, results, urls)
}
