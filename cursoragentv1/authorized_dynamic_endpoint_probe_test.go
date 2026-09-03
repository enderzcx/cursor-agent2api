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
)

func TestAuthorizedDynamicEndpointCLIProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_DYNAMIC_ENDPOINT_PROBE") != "1-request" {
		t.Skip("explicit bounded dynamic endpoint authorization required")
	}
	metadata := liveProbeMetadata(t)
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{
		ID:         "dedicated-dynamic-endpoint-probe",
		Provider:   providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"},
		Metadata:   metadata,
	}
	payload, err := jsonx.Marshal(map[string]any{
		"model": "composer-2.5-fast",
		"input": "Reply exactly TYPE64_DYNAMIC_ENDPOINT_OK.",
	})
	if err != nil {
		t.Fatal("could not encode probe")
	}
	request := claudeExecutorRequest("isolated-dynamic-endpoint-probe", string(payload))
	request.Model = "composer-2.5-fast"
	request.Format = sdktranslator.FormatOpenAIResponse
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	response, err := executor.Execute(ctx, auth, request, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAIResponse,
		ResponseFormat: sdktranslator.FormatOpenAIResponse,
	})
	if err != nil {
		message := err.Error()
		for _, value := range metadata {
			if secret, ok := value.(string); ok && secret != "" {
				message = strings.ReplaceAll(message, secret, "[redacted]")
			}
		}
		t.Fatal(message)
	}
	if !strings.Contains(string(response.Payload), "TYPE64_DYNAMIC_ENDPOINT_OK") {
		t.Fatal("dynamic endpoint probe completed without the expected marker")
	}
	t.Log("dynamic endpoint and CLI Agent Run completed")
}
