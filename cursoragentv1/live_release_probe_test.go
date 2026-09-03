package cursoragentv1

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

// Explicitly opt-in, dedicated-account transport probe. Credentials exist only
// in the child environment/memory; output contains no auth or response payload.
func TestLiveReleaseProtocolProbe(t *testing.T) {
	metadata := liveProbeMetadata(t)
	model := os.Getenv("CURSOR_AGENT_V1_LIVE_MODEL")
	if model == "" {
		model = "composer-2.5-fast"
	}
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-live-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	for _, format := range []sdktranslator.Format{sdktranslator.FormatClaude, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			body := map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "Reply exactly BEEFAPI_LIVE_PROTOCOL_OK."}}}
			if format == sdktranslator.FormatOpenAIResponse {
				delete(body, "messages")
				body["input"] = "Reply exactly BEEFAPI_LIVE_PROTOCOL_OK."
			}
			payload, err := jsonx.Marshal(body)
			if err != nil {
				t.Fatal("could not encode probe")
			}
			sessionID := "isolated-live-probe-" + string(format)
			request := claudeExecutorRequest(sessionID, string(payload))
			request.Model = model
			request.Format = format
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			response, err := executor.Execute(ctx, auth, request, cliproxyexecutor.Options{SourceFormat: format, ResponseFormat: format})
			if err != nil {
				message := err.Error()
				for _, value := range metadata {
					if secret, ok := value.(string); ok && secret != "" {
						message = strings.ReplaceAll(message, secret, "[redacted]")
					}
				}
				t.Fatal(message)
			}
			waitUntilInactive(t, executor.manager, sessionID)
			if !strings.Contains(string(response.Payload), "BEEFAPI_LIVE_PROTOCOL_OK") {
				t.Fatal("probe completed without expected text")
			}
			t.Log("live native text and terminal verified")
		})
	}
}

func liveProbeMetadata(t *testing.T) map[string]any {
	t.Helper()
	raw := os.Getenv("CURSOR_AGENT_V1_LIVE_AUTH_JSON")
	if raw == "" {
		t.Skip("dedicated live credential not supplied")
	}
	var metadata map[string]any
	if jsonx.Unmarshal([]byte(raw), &metadata) != nil {
		t.Fatal("invalid live credential shape")
	}
	return metadata
}

func TestLiveReleaseCallerToolProbe(t *testing.T) {
	metadata := liveProbeMetadata(t)
	executor, err := NewExecutor(t.TempDir())
	require.NoError(t, err)
	auth := &cliproxyauth.Auth{ID: "dedicated-live-tool-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	call := func(session string, body map[string]any) cliproxyexecutor.Response {
		payload, encodeErr := jsonx.Marshal(body)
		require.NoError(t, encodeErr)
		request := claudeExecutorRequest(session, string(payload))
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
		return response
	}
	user := map[string]any{"role": "user", "content": "Call probe_marker exactly once. After its result, reply exactly BEEFAPI_TOOL_PROBE_OK."}
	first := call("live-tool-probe", map[string]any{"model": "composer-2.5-fast", "messages": []any{user},
		"tools":       []any{map[string]any{"name": "probe_marker", "description": "Return the probe marker", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}}},
		"tool_choice": map[string]any{"type": "tool", "name": "probe_marker"}})
	var assistant map[string]any
	require.NoError(t, jsonx.Unmarshal(first.Payload, &assistant))
	var ids []string
	blocks, _ := assistant["content"].([]any)
	for _, block := range blocks {
		item, _ := block.(map[string]any)
		if item["type"] == "tool_use" && item["name"] == "probe_marker" {
			if id, ok := item["id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
	}
	if len(ids) != 1 {
		t.Fatal("live probe did not return exactly one requested caller tool")
	}
	continuation := map[string]any{"model": "composer-2.5-fast", "messages": []any{user,
		map[string]any{"role": "assistant", "content": assistant["content"]},
		map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": ids[0], "content": "BEEFAPI_TOOL_PROBE_OK"}}}}}
	second := call("live-tool-result", continuation)
	require.Contains(t, string(second.Payload), "BEEFAPI_TOOL_PROBE_OK")
	require.NotEmpty(t, first.Headers.Get(HeaderUsageReceipt))
	require.NotEmpty(t, second.Headers.Get(HeaderUsageReceipt))
	if first.Headers.Get(HeaderUsageReceipt) == second.Headers.Get(HeaderUsageReceipt) {
		t.Fatal("new tool-result generation reused the already settled initial receipt")
	}
	waitUntilInactive(t, executor.manager, "live-tool-probe")
	replay := call("live-tool-replay", continuation)
	require.Equal(t, "1", replay.Headers.Get(HeaderUsageReplay))
	require.Equal(t, second.Headers.Get(HeaderUsageReceipt), replay.Headers.Get(HeaderUsageReceipt))
	require.JSONEq(t, string(second.Payload), string(replay.Payload))
	t.Log("live caller tool, result continuation and exact replay verified")
}
