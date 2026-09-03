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

func TestAuthorizedHostedHistoryReplay(t *testing.T) {
	if os.Getenv("CURSOR_HOSTED_HISTORY") != "search-and-fetch" {
		t.Skip("explicit bounded live acceptance only")
	}
	executor, err := NewExecutor(t.TempDir())
	require.NoError(t, err)
	auth := &cliproxyauth.Auth{ID: "hosted-history", Provider: providerID, Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: liveProbeMetadata(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	auth, err = executor.Refresh(ctx, auth)
	if err != nil {
		t.Fatal("auth exchange failed")
	}
	for _, kind := range []string{"web_search", "web_fetch"} {
		prompt := "Search Cursor CLI official documentation and give one official source URL."
		if kind == "web_fetch" {
			prompt = "Fetch https://example.com and tell me the page title."
		}
		messages := []any{map[string]any{"role": "user", "content": prompt}}
		var sources []string
		for i := 0; i < 2; i++ {
			body := map[string]any{"model": "claude-opus-5", "max_tokens": 1024, "messages": messages, "tools": []any{map[string]any{"type": kind + "_20250305", "name": kind, "max_uses": 1}}}
			if i == 0 {
				body["tool_choice"] = map[string]any{"type": "tool", "name": kind}
			} else {
				body["tool_choice"] = map[string]any{"type": "none"}
			}
			raw, err := jsonx.Marshal(body)
			require.NoError(t, err)
			req := claudeExecutorRequest("hosted-history-"+uuid.NewString(), string(raw))
			req.Model = "claude-opus-5-high"
			opts := claudeExecutorOptions(false)
			opts.Headers.Set(headerCursorAgentV1NativeWebSearch, "true")
			response, err := executor.Execute(ctx, auth, req, opts)
			require.NoError(t, err)
			var output map[string]any
			require.NoError(t, jsonx.Unmarshal(response.Payload, &output))
			blocks := output["content"].([]any)
			require.NotEmpty(t, blocks)
			hosted, texts := 0, 0
			text := ""
			for _, b := range blocks {
				block := b.(map[string]any)
				if block["type"] == kind+"_tool_result" {
					urls := hostedHistoryURLs(block["content"])
					if len(urls) > 0 {
						hosted++
					}
					sources = append(sources, urls...)
				}
				if block["type"] == "text" && stringValue(block["text"]) != "" {
					texts++
					text += stringValue(block["text"])
				}
			}
			if i == 0 {
				require.Equal(t, 1, hosted)
				require.NotEmpty(t, sources)
			} else {
				require.Positive(t, texts)
				require.Zero(t, hosted)
				found := false
				for _, url := range sources {
					found = found || strings.Contains(text, url)
				}
				require.True(t, found, "follow-up must recall an actual result URL")
			}
			t.Logf("kind=%s request=%d hosted_results=%d text_blocks=%d stop=%v", kind, i, hosted, texts, output["stop_reason"])
			messages = append(messages, map[string]any{"role": "assistant", "content": blocks}, map[string]any{"role": "user", "content": "Which official URL did you just find? Answer from the previous results without another search."})
		}
	}
}

func hostedHistoryURLs(value any) []string {
	var urls []string
	switch v := value.(type) {
	case map[string]any:
		for k, x := range v {
			if k == "url" {
				if s, ok := x.(string); ok && strings.HasPrefix(s, "https://") {
					urls = append(urls, s)
				}
			} else {
				urls = append(urls, hostedHistoryURLs(x)...)
			}
		}
	case []any:
		for _, x := range v {
			urls = append(urls, hostedHistoryURLs(x)...)
		}
	}
	return urls
}
