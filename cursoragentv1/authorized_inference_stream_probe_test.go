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

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestAuthorizedSandExecutorProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_EXECUTOR_PROBE") != "1-request" {
		t.Skip("explicit bounded Sand executor authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	model := authorizedSandProbeModel()
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-sand-executor-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	body, err := jsonx.Marshal(map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "Reply exactly TYPE64_SAND_EXECUTOR_OK."}}})
	if err != nil {
		t.Fatal("could not encode Sand executor probe")
	}
	request := claudeExecutorRequest("type64-sand-executor-probe", string(body))
	request.Model = model
	request.Format = sdktranslator.FormatClaude
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	response, err := executor.Execute(ctx, auth, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, ResponseFormat: sdktranslator.FormatClaude})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	if !strings.Contains(string(response.Payload), "TYPE64_SAND_EXECUTOR_OK") {
		t.Fatal("Sand executor completed without expected marker")
	}
	if strings.TrimSpace(response.Headers.Get(HeaderUsageReceipt)) == "" {
		t.Fatal("Sand executor completed without usage receipt")
	}
	t.Logf("Sand executor model=%q returned TYPE64_SAND_EXECUTOR_OK with a usage receipt", model)
}

func TestAuthorizedSandExecutorToolProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_TOOL_PROBE") != "2-requests" {
		t.Skip("explicit bounded Sand tool authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-sand-tool-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	model := authorizedSandProbeModel()
	call := func(session string, body map[string]any) cliproxyexecutor.Response {
		payload, encodeErr := jsonx.Marshal(body)
		if encodeErr != nil {
			t.Fatal("could not encode Sand tool probe")
		}
		request := claudeExecutorRequest(session, string(payload))
		request.Model = model
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		response, callErr := executor.Execute(ctx, auth, request, claudeExecutorOptions(false))
		if callErr != nil {
			t.Fatal(redactProbeError(callErr, metadata))
		}
		return response
	}
	user := map[string]any{"role": "user", "content": "Call probe_marker exactly once. After its result, reply exactly TYPE64_SAND_TOOL_OK."}
	tools := []any{map[string]any{"name": "probe_marker", "description": "Return the supplied marker", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"marker": map[string]any{"type": "string"}}, "required": []any{"marker"}}}}
	first := call("type64-sand-tool-call", map[string]any{"model": model, "messages": []any{user},
		"tools":       tools,
		"tool_choice": map[string]any{"type": "tool", "name": "probe_marker"}})
	var assistant map[string]any
	if jsonx.Unmarshal(first.Payload, &assistant) != nil {
		t.Fatal("could not decode Sand tool call")
	}
	blocks, _ := assistant["content"].([]any)
	toolID := ""
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		if block["type"] == "tool_use" && block["name"] == "probe_marker" {
			toolID, _ = block["id"].(string)
		}
	}
	if toolID == "" {
		t.Fatal("Sand tool probe did not return probe_marker")
	}
	continuation := map[string]any{
		"model": model, "tools": tools,
		"messages": []any{
			user,
			map[string]any{"role": "assistant", "content": blocks},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": toolID, "content": "TYPE64_SAND_TOOL_OK"},
			}},
		},
	}
	second := call("type64-sand-tool-result", continuation)
	if !strings.Contains(string(second.Payload), "TYPE64_SAND_TOOL_OK") {
		t.Fatal("Sand tool continuation completed without expected marker")
	}
	if first.Headers.Get(HeaderUsageReceipt) == "" || second.Headers.Get(HeaderUsageReceipt) == "" {
		t.Fatal("Sand tool probe completed without both usage receipts")
	}
	t.Log("Sand caller tool and full-history tool_result continuation returned TYPE64_SAND_TOOL_OK with receipts")
}

func TestAuthorizedSandHostedWebProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_HOSTED_WEB_PROBE") != "2-requests" {
		t.Skip("explicit bounded Sand hosted web authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-sand-hosted-web-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	model := authorizedSandProbeModel()
	call := func(session string, body map[string]any) cliproxyexecutor.Response {
		payload, encodeErr := jsonx.Marshal(body)
		if encodeErr != nil {
			t.Fatal("could not encode Sand hosted web probe")
		}
		request := claudeExecutorRequest(session, string(payload))
		request.Model = model
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		response, callErr := executor.Execute(ctx, auth, request, withHostedSearchHeader(claudeExecutorOptions(false)))
		if callErr != nil {
			t.Fatal(redactProbeError(callErr, metadata))
		}
		if response.Headers.Get(HeaderUsageReceipt) == "" {
			t.Fatal("Sand hosted web probe completed without a usage receipt")
		}
		return response
	}
	search := call("type64-sand-hosted-search", map[string]any{
		"model":       model,
		"messages":    []any{map[string]any{"role": "user", "content": "Search the live web once for the official Cursor documentation. Reply with exactly one cursor.com URL."}},
		"tools":       []any{map[string]any{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}},
		"tool_choice": map[string]any{"type": "tool", "name": "web_search"},
	})
	if !strings.Contains(string(search.Payload), `"web_search_tool_result"`) || !strings.Contains(string(search.Payload), "cursor.com") {
		t.Fatal("Sand hosted search did not return the typed official-search result")
	}
	fetch := call("type64-sand-hosted-fetch", map[string]any{
		"model":       model,
		"messages":    []any{map[string]any{"role": "user", "content": "Fetch https://cursor.com/docs once. After reading it, reply exactly TYPE64_SAND_WEB_FETCH_OK."}},
		"tools":       []any{map[string]any{"type": "web_fetch_20250305", "name": "web_fetch", "max_uses": 1}},
		"tool_choice": map[string]any{"type": "tool", "name": "web_fetch"},
	})
	if !strings.Contains(string(fetch.Payload), `"web_fetch_tool_result"`) || !strings.Contains(string(fetch.Payload), "TYPE64_SAND_WEB_FETCH_OK") {
		t.Fatal("Sand hosted fetch did not return the typed official-fetch result")
	}
	t.Log("Sand official WebSearch and WebFetch completed inside the private inference loop with typed results and receipts")
}

func TestAuthorizedSandResponsesHostedWebProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_RESPONSES_HOSTED_WEB_PROBE") != "1-request" {
		t.Skip("explicit bounded Sand Responses hosted web authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-sand-responses-hosted-web-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	model := authorizedSandProbeModel()
	responsesBody, encodeErr := jsonx.Marshal(map[string]any{
		"model":       model,
		"input":       "Search the live web once for the official Cursor documentation. Reply with exactly one cursor.com URL.",
		"tools":       []any{map[string]any{"type": "web_search", "external_web_access": true}},
		"tool_choice": "required",
	})
	if encodeErr != nil {
		t.Fatal("could not encode Sand Responses hosted search probe")
	}
	responsesRequest := claudeExecutorRequest("type64-sand-responses-hosted-search", string(responsesBody))
	responsesRequest.Model = model
	responsesRequest.Format = sdktranslator.FormatOpenAIResponse
	responsesOptions := withHostedSearchHeader(cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: responsesBody,
	})
	responsesCtx, responsesCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer responsesCancel()
	responses, responsesErr := executor.Execute(responsesCtx, auth, responsesRequest, responsesOptions)
	if responsesErr != nil {
		t.Fatal(redactProbeError(responsesErr, metadata))
	}
	if responses.Headers.Get(HeaderUsageReceipt) == "" || !strings.Contains(string(responses.Payload), `"type":"web_search_call"`) || !strings.Contains(string(responses.Payload), "cursor.com") {
		t.Fatal("Sand Responses hosted search did not return the typed search item and receipt")
	}
	t.Log("Sand required Responses web_search completed inside the private inference loop with a typed result and receipt")
}

func TestAuthorizedSandResponsesContinuationProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_RESPONSES_PROBE") != "2-requests" {
		t.Skip("explicit bounded Sand Responses authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-sand-responses-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	model := authorizedSandProbeModel()
	call := func(session string, body map[string]any) cliproxyexecutor.Response {
		payload, encodeErr := jsonx.Marshal(body)
		if encodeErr != nil {
			t.Fatal("could not encode Sand Responses probe")
		}
		request := claudeExecutorRequest(session, string(payload))
		request.Model = model
		request.Format = sdktranslator.FormatOpenAIResponse
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		response, callErr := executor.Execute(ctx, auth, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse})
		if callErr != nil {
			t.Fatal(redactProbeError(callErr, metadata))
		}
		return response
	}
	first := call("type64-sand-responses-first", map[string]any{"model": model, "input": "Remember the private marker ORBITAL_731 for the next turn. Reply exactly RESPONSES_FIRST_OK."})
	responseID := gjson.GetBytes(first.Payload, "id").String()
	if responseID == "" || !strings.Contains(string(first.Payload), "RESPONSES_FIRST_OK") {
		t.Fatal("Sand Responses first turn did not complete")
	}
	second := call(responseID, map[string]any{"model": model, "previous_response_id": responseID, "input": "Return only the private marker from the prior turn."})
	if !strings.Contains(string(second.Payload), "ORBITAL_731") {
		t.Fatal("Sand Responses continuation did not restore prior transcript")
	}
	if first.Headers.Get(HeaderUsageReceipt) == "" || second.Headers.Get(HeaderUsageReceipt) == "" {
		t.Fatal("Sand Responses continuation completed without both usage receipts")
	}
	t.Log("Sand Responses previous_response_id restored ORBITAL_731 with receipts")
}

func TestAuthorizedSandResponsesToolProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_RESPONSES_TOOL_PROBE") != "2-requests" {
		t.Skip("explicit bounded Sand Responses tool authorization required")
	}
	metadata := liveProbeMetadata(t)
	metadata["runtime_profile"] = runtimeSand
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-sand-responses-tool-probe", Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	model := authorizedSandProbeModel()
	call := func(session string, body map[string]any) cliproxyexecutor.Response {
		payload, encodeErr := jsonx.Marshal(body)
		if encodeErr != nil {
			t.Fatal("could not encode Sand Responses tool probe")
		}
		request := claudeExecutorRequest(session, string(payload))
		request.Model = model
		request.Format = sdktranslator.FormatOpenAIResponse
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		response, callErr := executor.Execute(ctx, auth, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse})
		if callErr != nil {
			t.Fatal(redactProbeError(callErr, metadata))
		}
		return response
	}
	first := call("type64-sand-responses-tool-call", map[string]any{
		"model":       model,
		"input":       "Call probe_marker exactly once. After its output, reply exactly RESPONSES_TOOL_OK.",
		"tools":       []any{map[string]any{"type": "function", "name": "probe_marker", "description": "Return the supplied marker", "parameters": map[string]any{"type": "object", "properties": map[string]any{"marker": map[string]any{"type": "string"}}, "required": []any{"marker"}}}},
		"tool_choice": map[string]any{"type": "function", "name": "probe_marker"},
	})
	var firstBody map[string]any
	if jsonx.Unmarshal(first.Payload, &firstBody) != nil {
		t.Fatal("could not decode Sand Responses tool call")
	}
	responseID := strings.TrimSpace(fmt.Sprint(firstBody["id"]))
	callID := ""
	output, _ := firstBody["output"].([]any)
	for _, raw := range output {
		item, _ := raw.(map[string]any)
		if item["type"] == "function_call" && item["name"] == "probe_marker" {
			callID = strings.TrimSpace(fmt.Sprint(item["call_id"]))
		}
	}
	if responseID == "" || callID == "" {
		t.Fatal("Sand Responses did not return probe_marker")
	}
	fingerprint, fingerprintErr := CredentialFingerprint("tenant:1:user:1:channel:301", strings.TrimSpace(fmt.Sprint(metadata["account_id"])))
	if fingerprintErr != nil {
		t.Fatal("could not build Sand Responses tool fingerprint")
	}
	resolvedSession, resolveErr := executor.manager.store.ResolvePending(fingerprint, []string{callID})
	if resolveErr != nil || resolvedSession != responseID {
		t.Fatal("Sand Responses pending tool was not durably indexed")
	}
	if store, ok := executor.manager.store.(*FileStateStore); ok {
		statePath, _ := store.paths(responseID)
		state, stateErr := readState(statePath)
		if stateErr != nil || state.SessionID != responseID || len(state.PendingTools) != 1 {
			t.Fatal("Sand Responses pending state was not persisted")
		}
	}
	second := call(responseID, map[string]any{
		"model": model, "previous_response_id": responseID,
		"input": []any{map[string]any{"type": "function_call_output", "call_id": callID, "output": "RESPONSES_TOOL_OK"}},
	})
	if !strings.Contains(string(second.Payload), "RESPONSES_TOOL_OK") {
		t.Fatal("Sand Responses function_call_output did not resume checkpoint")
	}
	if first.Headers.Get(HeaderUsageReceipt) == "" || second.Headers.Get(HeaderUsageReceipt) == "" {
		t.Fatal("Sand Responses tool probe completed without both usage receipts")
	}
	t.Log("Sand Responses function_call_output resumed checkpoint and returned RESPONSES_TOOL_OK with receipts")
}

func authorizedSandProbeModel() string {
	model := strings.TrimSpace(os.Getenv("CURSOR_AGENT_V1_SAND_MODEL"))
	if model == "" || model == "grok-4.6" {
		return "cursor-grok-4.6-medium"
	}
	return model
}

func TestAuthorizedSandInferenceStreamProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_SAND_INFERENCE_PROBE") != "1-request" {
		t.Skip("explicit bounded Sand inference authorization required")
	}
	metadata := liveProbeMetadata(t)
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{
		ID:         "dedicated-sand-inference-probe",
		Provider:   providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"},
		Metadata:   metadata,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	refreshed, err := executor.Refresh(ctx, auth)
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}

	requestedModelID := strings.TrimSpace(os.Getenv("CURSOR_AGENT_V1_SAND_MODEL"))
	if requestedModelID == "" {
		requestedModelID = "grok-4.6"
	}
	message := appendVarintField(nil, 1, 1) // InferenceMessageRole.USER
	message = appendStringField(message, 2, "Reply exactly TYPE64_SAND_INFERENCE_OK.")
	parameter := appendStringField(nil, 1, "effort")
	parameter = appendStringField(parameter, 2, "medium")
	requestedModel := appendStringField(nil, 1, requestedModelID)
	requestedModel = appendBytesField(requestedModel, 3, parameter)
	requestedModel = appendVarintField(requestedModel, 4, 1) // built_in_model
	requestPayload := appendBytesField(nil, 1, message)
	requestPayload = appendStringField(requestPayload, 6, "type64-sand-inference-probe")
	requestPayload = appendBytesField(requestPayload, 7, requestedModel)
	requestPayload = appendStringField(requestPayload, 8, "type64-sand-inference-probe")

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api2.cursor.sh"+inferenceStreamPath, bytes.NewReader(frameConnect(requestPayload)))
	if err != nil {
		t.Fatal("could not build Sand inference request")
	}
	accessToken := metadataValue(refreshed, "access_token")
	beforeUsage, beforePlan := fetchSandUsageProbe(t, ctx, accessToken)
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set("Content-Type", "application/connect+proto")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("Connect-Accept-Encoding", "gzip,br")
	request.Header.Set("X-Cursor-Client-Type", "sand")
	request.Header.Set("X-Cursor-Client-Version", metadataValue(refreshed, "client_version"))
	request.Header.Set("X-Cursor-Streaming", "true")
	request.Header.Set("X-Ghost-Mode", "true")
	request.Header.Set("User-Agent", "connect-es/1.6.1")
	request.Header.Set("Cookie", cursorRequestCookie(accessToken))

	response, err := (&http.Client{Transport: &http2.Transport{}}).Do(request)
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Fatalf("Sand inference HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxConnectPayloadSize+1))
	if err != nil {
		t.Fatal("could not read Sand inference response")
	}
	if len(body) > maxConnectPayloadSize {
		t.Fatal("Sand inference response exceeded probe bound")
	}
	text, streamErr := decodeInferenceProbeFrames(body)
	if streamErr != nil {
		t.Fatal(streamErr)
	}
	if !strings.Contains(text, "TYPE64_SAND_INFERENCE_OK") {
		t.Fatalf("Sand inference completed without expected marker: %q", text)
	}
	afterUsage, afterPlan := fetchSandUsageProbe(t, ctx, accessToken)
	t.Logf("Sand InferenceService/Stream model=%q returned TYPE64_SAND_INFERENCE_OK; Grok Bot plan=%q usage %.6f%% -> %.6f%%", requestedModelID, firstNonEmpty(afterPlan, beforePlan), beforeUsage, afterUsage)
}

func fetchSandUsageProbe(t *testing.T, ctx context.Context, accessToken string) (float64, string) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api2.cursor.sh/aiserver.v1.DashboardService/GetSandUsageStatus", strings.NewReader("{}"))
	if err != nil {
		t.Fatal("could not build Sand usage request")
	}
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("X-Cursor-Client-Type", "sand")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("could not fetch Sand usage")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		t.Fatal("could not read Sand usage")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		t.Fatalf("Sand usage HTTP %d", response.StatusCode)
	}
	if !gjson.GetBytes(body, "hasAvailableUsage").Bool() {
		t.Fatal("Sand usage reports no available Grok Bot quota")
	}
	return gjson.GetBytes(body, "usagePercent").Float(), gjson.GetBytes(body, "grokPlanLabel").String()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func redactProbeError(err error, metadata map[string]any) string {
	message := err.Error()
	for _, value := range metadata {
		if secret, ok := value.(string); ok && secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	return message
}

func decodeInferenceProbeFrames(buffer []byte) (string, error) {
	var text strings.Builder
	for len(buffer) > 0 {
		flags, payload, consumed, ok, err := parseConnectFrame(buffer)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", fmt.Errorf("incomplete Sand inference Connect frame")
		}
		buffer = buffer[consumed:]
		if flags&connectEndStreamFlag != 0 {
			if err := parseConnectEndStream(payload); err != nil {
				return "", err
			}
			continue
		}
		responseField, responsePayload := firstInferenceResponseField(payload)
		switch responseField {
		case 1: // InferenceTextStreamPart
			text.WriteString(decodeString(responsePayload, 1))
		case 8: // InferenceStreamError
			return "", fmt.Errorf("Sand inference error %s: %s", decodeString(responsePayload, 2), decodeString(responsePayload, 1))
		}
	}
	return text.String(), nil
}

func firstInferenceResponseField(payload []byte) (protowire.Number, []byte) {
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return 0, nil
		}
		payload = payload[tagLength:]
		if wireType != protowire.BytesType {
			valueLength := protowire.ConsumeFieldValue(number, wireType, payload)
			if valueLength < 0 {
				return 0, nil
			}
			payload = payload[valueLength:]
			continue
		}
		value, valueLength := protowire.ConsumeBytes(payload)
		if valueLength < 0 {
			return 0, nil
		}
		return number, value
	}
	return 0, nil
}
