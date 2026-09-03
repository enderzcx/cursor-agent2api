package cursoragentv1

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/stretchr/testify/require"
)

// Exercise the reported mixed continuation against the real provider, only
// when the dedicated account is explicitly supplied by the release operator.
func TestLiveReleaseMixedNoneProbe(t *testing.T) {
	metadata := liveProbeMetadata(t)
	executor, err := NewExecutor(t.TempDir())
	require.NoError(t, err)
	auth := &cliproxyauth.Auth{ID: "dedicated-live-policy-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	call := func(body map[string]any) map[string]any {
		payload, encodeErr := jsonx.Marshal(body)
		require.NoError(t, encodeErr)
		request := claudeExecutorRequest("live-mixed-policy-probe", string(payload))
		request.Model = "composer-2.5-fast"
		response, callErr := executor.Execute(ctx, auth, request, claudeExecutorOptions(false))
		if callErr != nil {
			message := callErr.Error()
			for _, value := range metadata {
				if secret, ok := value.(string); ok && secret != "" {
					message = strings.ReplaceAll(message, secret, "[redacted]")
				}
			}
			t.Fatal(message)
		}
		var output map[string]any
		require.NoError(t, jsonx.Unmarshal(response.Payload, &output))
		return output
	}
	user := map[string]any{"role": "user", "content": "Call probe_marker exactly once. After its result reply exactly BEEFAPI_MIXED_NONE_OK."}
	tools := []any{map[string]any{"name": "probe_marker", "description": "Return the marker", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}}}
	first := call(map[string]any{"model": "composer-2.5-fast", "messages": []any{user}, "tools": tools,
		"tool_choice": map[string]any{"type": "tool", "name": "probe_marker"}})
	var toolID string
	blocks, _ := first["content"].([]any)
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		if block["type"] == "tool_use" {
			toolID, _ = block["id"].(string)
		}
	}
	if toolID == "" {
		t.Fatal("provider did not request the declared caller tool")
	}
	second := call(map[string]any{"model": "composer-2.5-fast", "tools": tools,
		"tool_choice": map[string]any{"type": "none"}, "messages": []any{user,
			map[string]any{"role": "assistant", "content": first["content"]},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": toolID, "content": "BEEFAPI_MIXED_NONE_OK"},
				map[string]any{"type": "text", "text": "Consume the tool result and reply exactly BEEFAPI_MIXED_NONE_OK."},
			}}}})
	if second["stop_reason"] != "end_turn" {
		t.Fatal("explicit none did not finish with end_turn")
	}
	var text strings.Builder
	blocks, _ = second["content"].([]any)
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		if block["type"] == "tool_use" {
			t.Fatal("explicit none emitted a caller tool")
		}
		if block["type"] == "text" {
			value, _ := block["text"].(string)
			text.WriteString(value)
		}
	}
	require.Contains(t, text.String(), "BEEFAPI_MIXED_NONE_OK")
	waitUntilInactive(t, executor.manager, "live-mixed-policy-probe")
	t.Log("live mixed caller-tool continuation obeyed explicit none")
}
