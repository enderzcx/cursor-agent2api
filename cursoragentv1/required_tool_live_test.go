package cursoragentv1

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
)

// One explicitly authorized upstream call. Unlike the older marker probes,
// the user's text does not ask to invoke a tool. No caller tool is executed.
func TestAuthorizedRequiredToolRuleProbe(t *testing.T) {
	if os.Getenv("CURSOR_REQUIRED_TOOL_RULE_PROBE") != "one-run" {
		t.Skip("bounded live title probe not authorized")
	}
	meta := liveProbeMetadata(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req := RunRequest{
		BaseURL: stringValue(meta["base_url"]), AccessToken: stringValue(meta["access_token"]), ClientVersion: stringValue(meta["client_version"]),
		Model: "composer-2.5-fast", ConversationID: uuid.NewString(),
		SystemText:      "Generate a short, descriptive session title for the user query enclosed in user_query tags. Return only the session_title and nothing else.",
		UserText:        "<user_query>Read a local marker file and report its contents.</user_query>",
		RequireToolCall: true, MaxParallelTools: 1, ToolResults: make(chan ToolResult),
		Tools: []ToolDefinition{{Name: "session_title", Description: "Return a short descriptive session title", InputSchema: []byte(`{"type":"object","properties":{"session_title":{"type":"string"}},"required":["session_title"],"additionalProperties":false}`)}},
	}
	valid := false
	err := NewRunner().Run(ctx, req, func(e Event) {
		if e.Type != EventToolCall || e.ToolCall == nil {
			return
		}
		var args map[string]any
		valid = e.ToolCall.Name == "session_title" && jsonx.Unmarshal([]byte(e.ToolCall.Arguments), &args) == nil && strings.TrimSpace(stringValue(args["session_title"])) != ""
		cancel()
	})
	if !valid {
		message := "required MCP call was not observed"
		if err != nil {
			message = err.Error()
		}
		for _, v := range meta {
			if secret, ok := v.(string); ok && secret != "" {
				message = strings.ReplaceAll(message, secret, "[redacted]")
			}
		}
		t.Fatal(message)
	}
	t.Log("actual session_title MCP exec and schema-shaped arguments verified; canceled before caller execution")
}
