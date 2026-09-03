package cursoragentv1

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestConsumeInferenceStreamClosesThinkingOnSignatureFrame(t *testing.T) {
	// Observed Cursor 3.18.9 shape (TestAuthorizedRawSandFrameProbe): thinking
	// deltas, then a thinking_part carrying only the signature, is_final never
	// set, then text. The signed block must be completed exactly once and the
	// signature must ride on EventThinkingCompleted.
	thinkA := appendStringField(nil, 1, "plan ")
	thinkB := appendStringField(nil, 1, "the answer")
	signature := appendStringField(nil, 2, "SIGNATURE-1")
	text := appendStringField(nil, 1, "OK")
	wire := frameConnect(appendBytesField(nil, 9, thinkA))
	wire = append(wire, frameConnect(appendBytesField(nil, 9, thinkB))...)
	wire = append(wire, frameConnect(appendBytesField(nil, 9, signature))...)
	wire = append(wire, frameConnect(appendBytesField(nil, 1, text))...)
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := []Event{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), RunRequest{}, func(event Event) { events = append(events, event) }, nil, nil, &inferenceHostedUseTracker{})
	require.NoError(t, err)
	completed := 0
	for _, event := range events {
		if event.Type == EventThinkingCompleted {
			completed++
			require.Equal(t, "SIGNATURE-1", event.Signature)
		}
	}
	require.Equal(t, 1, completed)
	// Checkpoint replays the signed reasoning so a same-provider continuation
	// keeps its signature.
	var checkpoint []byte
	for _, event := range events {
		if event.Type == EventCheckpoint {
			checkpoint = event.Checkpoint
		}
	}
	decoded, err := decodeInferenceCheckpoint(checkpoint)
	require.NoError(t, err)
	joined := string(bytes.Join(decoded.Messages, nil))
	require.Contains(t, joined, `"signature":"SIGNATURE-1"`)
	require.Contains(t, joined, `"type":"reasoning"`)
}

func TestConsumeInferenceStreamClosesUnsignedThinkingAtStreamEnd(t *testing.T) {
	think := appendStringField(nil, 1, "only thinking")
	wire := frameConnect(appendBytesField(nil, 9, think))
	wire = append(wire, frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := []Event{}
	err := consumeInferenceStream(context.Background(), bytes.NewReader(wire), RunRequest{}, func(event Event) { events = append(events, event) }, nil, nil, &inferenceHostedUseTracker{})
	require.NoError(t, err)
	completed := 0
	for _, event := range events {
		if event.Type == EventThinkingCompleted {
			completed++
			require.Empty(t, event.Signature)
		}
	}
	require.Equal(t, 1, completed)
}

func TestClaudeProjectionEmitsSignedThinkingBlockOnlyWithSignature(t *testing.T) {
	turn := translatedTurn{responseFormat: sdktranslator.FormatClaude, managed: ManagedRequest{SessionID: "s", Run: RunRequest{Model: "claude-sonnet-4-6"}}}
	signed := newClaudeProjection(context.Background(), turn)
	chunks := [][]byte{}
	for _, event := range []Event{{Type: EventThinking, Text: "why"}, {Type: EventThinkingCompleted, Signature: "SIG"}, {Type: EventText, Text: "OK"}, {Type: EventDone}} {
		out, _, _, err := signed.handle(event)
		require.NoError(t, err)
		chunks = append(chunks, out...)
	}
	sse := string(bytes.Join(chunks, nil))
	require.Contains(t, sse, `"delta":{"thinking":"why","type":"thinking_delta"}`)
	require.Contains(t, sse, `"delta":{"signature":"SIG","type":"signature_delta"}`)
	body, err := signed.nonStream()
	require.NoError(t, err)
	require.Contains(t, string(body), `"signature":"SIG"`)

	unsigned := newClaudeProjection(context.Background(), turn)
	chunks = chunks[:0]
	for _, event := range []Event{{Type: EventThinking, Text: "why"}, {Type: EventThinkingCompleted}, {Type: EventText, Text: "OK"}, {Type: EventDone}} {
		out, _, _, err := unsigned.handle(event)
		require.NoError(t, err)
		chunks = append(chunks, out...)
	}
	require.NotContains(t, string(bytes.Join(chunks, nil)), `"type":"thinking"`)
}

func TestInferenceRunnerRetriesForeignSignatureRejectionOnce(t *testing.T) {
	history := [][]byte{
		[]byte(`{"role":"user","content":[{"type":"text","text":"one"}]}`),
		[]byte(`{"role":"assistant","content":[{"type":"reasoning","text":"private","signature":"FOREIGN"},{"type":"text","text":"ONE"}]}`),
	}
	require.Equal(t, []string{"FOREIGN"}, historySignatures(history))
	require.True(t, isForeignReasoningSignatureRejection(&connectError{Code: "resource_exhausted", Message: "Error"}))
	require.False(t, isForeignReasoningSignatureRejection(&connectError{Code: "not_found"}))
	request := RunRequest{RuntimeProfile: runtimeSand, Model: "claude-sonnet-4-6", ConversationID: "c", UserText: "two", HistoryMessages: history}
	// A signature Cursor never minted through this sidecar is not forwarded at
	// all, so migrated sessions pay no failed round trip.
	require.False(t, historyCarriesForwardedSignature(history))
	unknown, err := encodeInferenceRequestWithPolicy(request, false)
	require.NoError(t, err)
	require.NotContains(t, string(unknown), "FOREIGN")
	require.Contains(t, string(unknown), "private")
	// Once Cursor mints it, the same signature is forwarded; a rejection then
	// demotes it and the retry encoder strips it.
	rememberMintedSignature("FOREIGN")
	require.True(t, historyCarriesForwardedSignature(history))
	withSignature, err := encodeInferenceRequestWithPolicy(request, false)
	require.NoError(t, err)
	require.Contains(t, string(withSignature), "FOREIGN")
	withoutSignature, err := encodeInferenceRequestWithPolicy(request, true)
	require.NoError(t, err)
	require.NotContains(t, string(withoutSignature), "FOREIGN")
	markHistorySignaturesRejected(history)
	require.False(t, historyCarriesForwardedSignature(history))
}

func TestEncodeInferenceUserMessageUsesNativeImageParts(t *testing.T) {
	media := []MediaPart{{Kind: mediaKindImage, MimeType: "image/png", Data: tinyPNGBase64}, {Kind: mediaKindDocument, MimeType: "application/pdf", Data: "JVBERi0=", Filename: "spec.pdf"}}
	message := encodeInferenceUserMessage("look", media)
	require.Equal(t, uint64(1), decodeVarint(message, 1))
	require.Empty(t, decodeString(message, 2), "media turns must use parts(3), the content oneof forbids text(2)")
	parts := protoRepeated(t, bytesField(message, 3), 1)
	require.Len(t, parts, 3)
	require.Equal(t, "look", decodeString(bytesField(parts[0], 1), 1))
	image := bytesField(parts[1], 2)
	require.Equal(t, tinyPNGBase64, decodeString(image, 1))
	require.Equal(t, "image/png", decodeString(image, 2))
	file := bytesField(parts[2], 3)
	require.Equal(t, "application/pdf", decodeString(file, 2))
	require.Equal(t, "spec.pdf", decodeString(file, 3))
}

func TestAgentV1RunRequestCarriesSelectedImages(t *testing.T) {
	raw, _ := base64.StdEncoding.DecodeString(tinyPNGBase64)
	payload, err := encodeRunRequest(&runWireRequest{Model: "composer-2.5-fast", ConversationID: "conv", UserText: "what color",
		UserMedia: []MediaPart{{Kind: mediaKindImage, MimeType: "image/png", Data: tinyPNGBase64}}, BlobStore: map[string][]byte{}})
	require.NoError(t, err)
	agentRun := bytesField(payload, agentClientRunRequest)
	action := bytesField(agentRun, 2)
	userAction := bytesField(action, 1)
	userMessage := bytesField(userAction, 1)
	require.Equal(t, "what color", decodeString(userMessage, 1))
	selectedContext := bytesField(userMessage, 3)
	images := protoRepeated(t, selectedContext, 1)
	require.Len(t, images, 1)
	require.Equal(t, "image/png", decodeString(images[0], 7))
	require.Equal(t, raw, decodeBytes(images[0], 8))
	dimension := bytesField(images[0], 4)
	require.Equal(t, uint64(1), decodeVarint(dimension, 1))
	require.Equal(t, uint64(1), decodeVarint(dimension, 2))
}

func TestAgentV1MCPResultCarriesImageItems(t *testing.T) {
	raw, _ := base64.StdEncoding.DecodeString(tinyPNGBase64)
	payload := encodeMCPResult(7, "exec-1", ToolResult{ToolCallID: "call", Content: "captured", Media: []MediaPart{{Kind: mediaKindImage, MimeType: "image/png", Data: tinyPNGBase64}, {Kind: mediaKindDocument, MimeType: "application/pdf", Data: "JVBERi0=", Filename: "a.pdf"}}})
	execMessage := bytesField(payload, agentClientExecMessage)
	mcpResult := bytesField(execMessage, 11)
	success := bytesField(mcpResult, 1)
	items := protoRepeated(t, success, 1)
	require.Len(t, items, 3)
	require.Equal(t, "captured", decodeString(bytesField(items[0], 1), 1))
	image := bytesField(items[1], 2)
	require.Equal(t, raw, decodeBytes(image, 1))
	require.Equal(t, "image/png", decodeString(image, 2))
	require.Contains(t, decodeString(bytesField(items[2], 1), 1), "a.pdf")
}

func TestAgentV1LiftToolResultMediaKeepsPairingAndOrder(t *testing.T) {
	history := [][]byte{
		[]byte(`{"role":"assistant","content":[{"type":"tool-call","toolCallId":"c1","toolName":"shot","args":{}}]}`),
		[]byte(`{"role":"tool","id":"c1","content":[{"type":"tool-result","toolCallId":"c1","toolName":"shot","result":[{"type":"text","text":"captured"}],"experimental_content":[{"type":"text","text":"captured"},{"type":"image","image":"` + tinyPNGBase64 + `","mimeType":"image/png"}]}]}`),
		[]byte(`{"role":"assistant","content":[{"type":"text","text":"it is red"}]}`),
	}
	lifted, err := agentV1LiftToolResultMedia(history)
	require.NoError(t, err)
	require.Len(t, lifted, 4)
	require.NotContains(t, string(lifted[1]), "experimental_content")
	require.Contains(t, string(lifted[1]), `"toolCallId":"c1"`)
	var inserted map[string]any
	require.NoError(t, jsonx.Unmarshal(lifted[2], &inserted))
	require.Equal(t, "user", inserted["role"])
	parts := inserted["content"].([]any)
	require.Contains(t, parts[0].(map[string]any)["text"], "shot")
	require.Equal(t, "image", parts[1].(map[string]any)["type"])
	require.Contains(t, string(lifted[3]), "it is red")
}

func TestParseActiveUserTurnSplitsTextMediaAndToolResults(t *testing.T) {
	active, err := parseActiveUserTurn([]any{
		map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": []any{map[string]any{"type": "text", "text": "shot"}, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": tinyPNGBase64}}}},
		map[string]any{"type": "text", "text": "and this"},
		map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/jpeg", "data": tinyPNGBase64}},
	})
	require.NoError(t, err)
	require.Equal(t, "and this", active.Text)
	require.Len(t, active.Results, 1)
	require.Equal(t, "shot", active.Results[0].Content)
	require.Len(t, active.Results[0].Media, 1)
	require.Len(t, active.Media, 1)
	require.Equal(t, "image/jpeg", active.Media[0].MimeType)

	_, err = parseActiveUserTurn([]any{map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "not base64!"}}})
	require.ErrorContains(t, err, "base64 is invalid")
}

func TestCompletedToolResultsPersistMedia(t *testing.T) {
	canonical, err := canonicalToolResults([]ToolResult{{ToolCallID: "a", Content: "x", Media: []MediaPart{{Kind: mediaKindImage, MimeType: "image/png", Data: tinyPNGBase64}}}})
	require.NoError(t, err)
	require.Len(t, canonical[0].Media, 1)
	restored := toolResultsFromCanonical(canonical)
	require.Len(t, restored[0].Media, 1)
	encoded, err := jsonx.Marshal(canonical)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"media"`)
}

func TestParseAnthropicMediaUsesConnectFrameLimit(t *testing.T) {
	// Claude Code PDFs between the old 5 MiB local cap and the Connect frame
	// must not 422 here; Anthropic/Kiro forward them. Cursor may still reject.
	require.Equal(t, maxConnectPayloadSize, maxMediaPartBytes)
	overFive := base64.StdEncoding.EncodeToString(make([]byte, (5<<20)+1))
	part, err := parseAnthropicMediaBlock(map[string]any{
		"type":  "document",
		"title": "notes.pdf",
		"source": map[string]any{
			"type":       "base64",
			"media_type": "application/pdf",
			"data":       overFive,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, mediaKindDocument, part.Kind)

	// DecodedLen is n/4*3; a short oversize fixture avoids allocating a 16 MiB body.
	overFrame := strings.Repeat("A", ((maxMediaPartBytes/3)+1)*4)
	_, err = parseAnthropicMediaBlock(map[string]any{
		"type": "document",
		"source": map[string]any{
			"type":       "base64",
			"media_type": "application/pdf",
			"data":       overFrame,
		},
	})
	require.ErrorContains(t, err, fmt.Sprintf("media attachment exceeds %d MiB", maxMediaPartBytes>>20))
}
