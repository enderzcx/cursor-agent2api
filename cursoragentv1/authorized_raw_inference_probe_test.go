package cursoragentv1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestAuthorizedRawSandFrameProbe posts one encoded Sand request and prints the
// decoded response part kinds (field numbers and sizes) so wire-level questions
// such as "did the upstream emit thinking_part/signature at all" are answered
// from evidence rather than from the projection layer.
//
//	CURSOR_AGENT_V1_RAW_SAND_PROBE=1-request
//	CURSOR_AGENT_V1_RAW_SAND_PARAMS=thinking=true,effort=medium,context=200k
func TestAuthorizedRawSandFrameProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_RAW_SAND_PROBE") != "1-request" {
		t.Skip("explicit bounded raw Sand probe authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-raw-sand-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	refreshed, err := executor.Refresh(ctx, auth)
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	accessToken := metadataValue(refreshed, "access_token")
	clientVersion := metadataValue(refreshed, "client_version")
	model := mediaProbeModel()
	selection, err := inferenceRequestedModel(model)
	if err != nil {
		t.Fatal(err)
	}
	if params := strings.TrimSpace(os.Getenv("CURSOR_AGENT_V1_RAW_SAND_PARAMS")); params != "" {
		selection.Parameters = nil
		for _, pair := range strings.Split(params, ",") {
			key, value, _ := strings.Cut(strings.TrimSpace(pair), "=")
			selection.Parameters = append(selection.Parameters, inferenceModelParameter{ID: key, Value: value})
		}
	}
	payload := appendBytesField(nil, 1, encodeInferenceTextMessage(1, "Think step by step about why 2+2=4, then reply exactly RAW_PROBE_OK."))
	requested := appendStringField(nil, 1, selection.ID)
	if selection.MaxMode {
		requested = appendVarintField(requested, 2, 1)
	}
	for _, value := range selection.Parameters {
		parameter := appendStringField(nil, 1, value.ID)
		parameter = appendStringField(parameter, 2, value.Value)
		requested = appendBytesField(requested, 3, parameter)
	}
	if selection.BuiltIn {
		requested = appendVarintField(requested, 4, 1)
	}
	payload = appendStringField(payload, 6, "raw-probe-"+fmt.Sprint(time.Now().Unix()))
	payload = appendBytesField(payload, 7, requested)
	payload = appendStringField(payload, 8, "raw-probe-conversation")
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, defaultInferenceBaseURL+inferenceStreamPath, bytes.NewReader(frameConnect(payload)))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+accessToken)
	httpRequest.Header.Set("Content-Type", "application/connect+proto")
	httpRequest.Header.Set("Connect-Protocol-Version", "1")
	httpRequest.Header.Set("X-Cursor-Client-Type", runtimeSand)
	httpRequest.Header.Set("X-Cursor-Client-Version", clientVersion)
	httpRequest.Header.Set("X-Cursor-Streaming", "true")
	httpRequest.Header.Set("X-Ghost-Mode", "true")
	httpRequest.Header.Set("User-Agent", "connect-es/1.6.1")
	httpRequest.Header.Set("Cookie", cursorRequestCookie(accessToken))
	transport, err := cursorHTTP2Transport("", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Do(httpRequest)
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	t.Logf("HTTP %d, %d bytes, params=%v", response.StatusCode, len(body), selection.Parameters)
	buffer := body
	for len(buffer) > 0 {
		flags, frame, consumed, ok, parseErr := parseConnectFrame(buffer)
		if parseErr != nil || !ok {
			t.Logf("frame parse stopped: %v", parseErr)
			break
		}
		buffer = buffer[consumed:]
		if flags&connectEndStreamFlag != 0 {
			t.Logf("end-stream: %s", strings.TrimSpace(string(frame)))
			continue
		}
		if flags&connectCompressionFlag != 0 {
			frame, _ = decompressConnectPayload(frame)
		}
		number, value, _ := firstBytesField(frame)
		describe := fmt.Sprintf("part field=%d bytes=%d", number, len(value))
		switch number {
		case 1:
			describe += fmt.Sprintf(" text=%q final=%v", decodeString(value, 1), decodeBool(value, 2))
		case 9:
			describe += fmt.Sprintf(" thinking_len=%d signature_len=%d final=%v", len(decodeString(value, 1)), len(decodeString(value, 2)), decodeBool(value, 3))
		case 8:
			describe += fmt.Sprintf(" error=%q code=%q", decodeString(value, 1), decodeString(value, 2))
		case 4, 6:
			describe += " " + describeSubfields(value)
		}
		t.Log(describe)
	}
}

func describeSubfields(data []byte) string {
	parts := []string{}
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			break
		}
		data = data[n:]
		length := protowire.ConsumeFieldValue(number, wireType, data)
		if length < 0 {
			break
		}
		parts = append(parts, fmt.Sprintf("f%d(%d)", number, length))
		data = data[length:]
	}
	return strings.Join(parts, ",")
}
