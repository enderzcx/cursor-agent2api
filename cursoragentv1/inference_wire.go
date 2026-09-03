package cursoragentv1

import (
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const defaultInferenceBaseURL = "https://api2.cursor.sh"

type inferenceToolCall struct {
	ID        string
	Name      string
	Arguments string
}

type inferenceUsage struct {
	Input      int64
	Output     int64
	CacheRead  int64
	CacheWrite int64
	Extended   bool
}

type inferenceResponse struct {
	Text           string
	TextFinal      bool
	Thinking       string
	ThinkSignature string
	ThinkFinal     bool
	Tool           *inferenceToolCall
	ToolFinal      bool
	Usage          *inferenceUsage
	ErrorCode      string
	ErrorText      string
}

func prepareInferenceTurn(turn *translatedTurn, req cliproxyexecutor.Request) error {
	if turn == nil {
		return &requestError{status: http.StatusBadRequest, text: "Cursor Sand request is missing"}
	}
	run := &turn.managed.Run
	if err := attachInferenceHostedTools(run); err != nil {
		return err
	}
	var root map[string]any
	if err := jsonx.Unmarshal(turn.claudeRequest, &root); err != nil {
		return &requestError{status: http.StatusBadRequest, text: "Cursor Sand could not rebuild request history"}
	}
	messages, _ := root["messages"].([]any)
	history := [][]byte(nil)
	var err error
	if req.Format != sdktranslator.FormatOpenAIResponse || len(turn.managed.ActiveResults) == 0 {
		history, err = structuredHistory(messages, true)
		if err != nil {
			return err
		}
	}
	run.HistoryMessages = history
	if req.Format != sdktranslator.FormatOpenAIResponse {
		run.HistoryRequestKey, err = historyRequestKey(*turn, req.Metadata["beefapi_token_id"], len(turn.managed.ActiveResults) > 0)
		if err != nil {
			return err
		}
	} else {
		run.HistoryRequestKey = ""
	}
	run.InitialCheckpoint = nil
	run.InitialBlobs = nil
	run.Resume = false
	turn.managed.FollowUpText = ""
	return nil
}

func encodeInferenceRequest(request RunRequest) ([]byte, error) {
	return encodeInferenceRequestWithPolicy(request, request.dropReasoningSignatures)
}

func encodeInferenceRequestWithPolicy(request RunRequest, dropSignatures bool) ([]byte, error) {
	selection, err := inferenceRequestedModel(request.Model)
	if err != nil {
		return nil, err
	}
	if request.ThinkingEnabled && !selection.hasParameter("thinking") && inferenceModelSupportsThinkingParameter(selection.ID) {
		// Mirror the caller's Anthropic `thinking.type=enabled` onto the Cursor
		// model parameter so Sand returns native thinking parts (and their
		// signatures) instead of silently running without extended thinking.
		selection.Parameters = append(selection.Parameters, inferenceModelParameter{ID: "thinking", Value: "true"})
	}
	payload := []byte{}
	if strings.TrimSpace(request.SystemText) != "" {
		payload = appendBytesField(payload, 1, encodeInferenceTextMessage(4, request.SystemText))
	}
	for _, raw := range request.HistoryMessages {
		message, err := encodeInferenceHistoryMessage(raw, dropSignatures)
		if err != nil {
			return nil, err
		}
		if len(message) > 0 {
			payload = appendBytesField(payload, 1, message)
		}
	}
	if strings.TrimSpace(request.UserText) != "" || len(request.UserMedia) > 0 {
		payload = appendBytesField(payload, 1, encodeInferenceUserMessage(request.UserText, request.UserMedia))
	}
	for _, tool := range request.Tools {
		encoded, err := encodeInferenceTool(tool)
		if err != nil {
			return nil, err
		}
		payload = appendBytesField(payload, 2, encoded)
	}
	requestedModel := appendStringField(nil, 1, selection.ID)
	if selection.MaxMode {
		requestedModel = appendVarintField(requestedModel, 2, 1)
	}
	for _, value := range selection.Parameters {
		parameter := appendStringField(nil, 1, value.ID)
		parameter = appendStringField(parameter, 2, value.Value)
		requestedModel = appendBytesField(requestedModel, 3, parameter)
	}
	if selection.BuiltIn {
		requestedModel = appendVarintField(requestedModel, 4, 1)
	}
	payload = appendStringField(payload, 6, uuid.NewString())
	payload = appendBytesField(payload, 7, requestedModel)
	payload = appendStringField(payload, 8, request.ConversationID)
	if len(payload) > maxConnectPayloadSize {
		return nil, errExceedsConnectLimit("Cursor Sand request")
	}
	return payload, nil
}

type inferenceModelParameter struct {
	ID    string
	Value string
}

type inferenceModelSpec struct {
	ID         string
	Parameters []inferenceModelParameter
	BuiltIn    bool
	MaxMode    bool
}

func (spec inferenceModelSpec) hasParameter(id string) bool {
	for _, parameter := range spec.Parameters {
		if parameter.ID == id {
			return true
		}
	}
	return false
}

// Cursor's model catalog exposes a boolean `thinking` parameter for Claude
// models; Grok/Composer/GLM/Kimi variants are addressed through effort only.
func inferenceModelSupportsThinkingParameter(modelID string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(modelID)), "claude-")
}

func inferenceRequestedModel(model string) (inferenceModelSpec, error) {
	modelID, effort, err := inferenceModelSelection(model)
	if err != nil {
		return inferenceModelSpec{}, err
	}
	if modelID == "claude-sonnet-5" {
		return inferenceModelSpec{
			ID: modelID, MaxMode: true,
			Parameters: []inferenceModelParameter{{ID: "thinking", Value: "true"}, {ID: "context", Value: "1m"}, {ID: "effort", Value: "high"}},
		}, nil
	}
	spec := inferenceModelSpec{ID: modelID, BuiltIn: true}
	if effort != "" {
		spec.Parameters = []inferenceModelParameter{{ID: "effort", Value: effort}}
	}
	return spec, nil
}

func inferenceModelSelection(model string) (string, string, error) {
	value := strings.ToLower(strings.TrimSpace(model))
	for _, prefix := range []string{"cursor-grok-4.6-", "cursor-grok-4.5-"} {
		if strings.HasPrefix(value, prefix) {
			effort := strings.TrimPrefix(value, prefix)
			if effort != "low" && effort != "medium" && effort != "high" && effort != "xhigh" {
				return "", "", &requestError{status: http.StatusBadRequest, text: "Cursor Sand Grok effort is invalid"}
			}
			return strings.TrimSuffix(strings.TrimPrefix(prefix, "cursor-"), "-"), effort, nil
		}
	}
	if value == "grok-4.6" {
		return value, "medium", nil
	}
	if value == "" {
		return "", "", &requestError{status: http.StatusBadRequest, text: "Cursor Sand model is required"}
	}
	return value, "", nil
}

func encodeInferenceTextMessage(role uint64, text string) []byte {
	message := appendVarintField(nil, 1, role)
	return appendStringField(message, 2, text)
}

func encodeInferenceHistoryMessage(raw []byte, dropSignatures bool) ([]byte, error) {
	var message map[string]any
	if err := jsonx.Unmarshal(raw, &message); err != nil {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand history is invalid"}
	}
	roleName := strings.ToLower(strings.TrimSpace(fmt.Sprint(message["role"])))
	roles := map[string]uint64{"user": 1, "assistant": 2, "tool": 3, "system": 4}
	role, ok := roles[roleName]
	if !ok {
		return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Sand history role is unsupported"}
	}
	out := appendVarintField(nil, 1, role)
	parts, _ := message["content"].([]any)
	texts := make([]string, 0, len(parts))
	contentParts := []byte{}
	hasMedia := false
	toolResults := []byte{}
	reasoning := []byte{}
	for _, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok {
			return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand history part is invalid"}
		}
		switch strings.TrimSpace(fmt.Sprint(part["type"])) {
		case "text":
			text := fmt.Sprint(part["text"])
			texts = append(texts, text)
			contentParts = appendBytesField(contentParts, 1, encodeInferenceContentPart(ContentPart{Text: text}))
		case "image", "file":
			media, ok := mediaPartFromHistory(part)
			if !ok {
				return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand history media part is invalid"}
			}
			hasMedia = true
			contentParts = appendBytesField(contentParts, 1, encodeInferenceContentPart(ContentPart{Media: &media}))
		case "reasoning":
			// InferenceReasoningPart{is_redacted=1, text=2, signature=3}. The
			// signature is forwarded verbatim; whether the upstream provider
			// accepts a signature minted elsewhere is decided by the Sand A/B
			// policy in inferenceReasoningSignaturePolicy.
			text, _ := part["text"].(string)
			signature, _ := part["signature"].(string)
			encoded := appendStringField(nil, 2, text)
			if !dropSignatures && sandForwardsReasoningSignature(signature) {
				encoded = appendStringField(encoded, 3, signature)
			}
			if len(encoded) > 0 {
				reasoning = appendBytesField(reasoning, 7, encoded)
			}
		case "redacted-reasoning":
			// InferenceReasoningPart{is_redacted=1, redacted_data=4}: opaque
			// ciphertext is only meaningful on the provider that produced it.
			data, _ := part["data"].(string)
			if sandForwardsRedactedReasoning() && data != "" {
				encoded := appendVarintField(nil, 1, 1)
				encoded = appendStringField(encoded, 4, data)
				reasoning = appendBytesField(reasoning, 7, encoded)
			}
		case "tool-call":
			call, err := encodeInferenceHistoricalToolCall(part)
			if err != nil {
				return nil, err
			}
			out = appendBytesField(out, 4, call)
		case "tool-result":
			result, err := encodeInferenceHistoricalToolResult(part)
			if err != nil {
				return nil, err
			}
			toolResults = appendBytesField(toolResults, 1, result)
		default:
			return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Sand history part is unsupported"}
		}
	}
	// InferenceCoreMessage.content is a oneof: text(2) | parts(3) | tool_content(6).
	switch {
	case len(toolResults) > 0:
		out = appendBytesField(out, 6, toolResults)
	case hasMedia:
		out = appendBytesField(out, 3, contentParts)
	default:
		if text := strings.TrimSpace(strings.Join(texts, "\n\n")); text != "" {
			out = appendStringField(out, 2, text)
		}
	}
	if len(reasoning) > 0 {
		out = append(out, reasoning...)
	}
	return out, nil
}

// encodeInferenceUserMessage builds the active user InferenceCoreMessage. Text-only
// turns keep the historical text(2) shape; attached media switches to parts(3)
// with native InferenceImagePart/InferenceFilePart entries.
func encodeInferenceUserMessage(text string, media []MediaPart) []byte {
	if len(media) == 0 {
		return encodeInferenceTextMessage(1, text)
	}
	parts := []byte{}
	if strings.TrimSpace(text) != "" {
		parts = appendBytesField(parts, 1, encodeInferenceContentPart(ContentPart{Text: text}))
	}
	for index := range media {
		parts = appendBytesField(parts, 1, encodeInferenceContentPart(ContentPart{Media: &media[index]}))
	}
	message := appendVarintField(nil, 1, 1)
	return appendBytesField(message, 3, parts)
}

// encodeInferenceContentPart encodes InferenceContentPart{text=1 | image=2 | file=3}.
func encodeInferenceContentPart(part ContentPart) []byte {
	if part.Media == nil {
		return appendBytesField(nil, 1, appendStringField(nil, 1, part.Text))
	}
	media := part.Media
	if media.Kind == mediaKindImage {
		// InferenceImagePart{data=1 (base64 string), mime_type=2}
		image := appendStringField(nil, 1, media.Data)
		image = appendStringField(image, 2, media.MimeType)
		return appendBytesField(nil, 2, image)
	}
	// InferenceFilePart{data=1, media_type=2, filename=3}
	file := appendStringField(nil, 1, media.Data)
	file = appendStringField(file, 2, media.MimeType)
	file = appendStringField(file, 3, media.Filename)
	return appendBytesField(nil, 3, file)
}

func encodeInferenceHistoricalToolCall(part map[string]any) ([]byte, error) {
	id := strings.TrimSpace(fmt.Sprint(part["toolCallId"]))
	name := strings.TrimSpace(fmt.Sprint(part["toolName"]))
	args, ok := part["args"].(map[string]any)
	if id == "" || name == "" || !ok {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand historical tool call is invalid"}
	}
	argsProto, err := marshalStruct(args)
	if err != nil {
		return nil, err
	}
	out := appendStringField(nil, 1, id)
	out = appendStringField(out, 2, name)
	out = appendBytesField(out, 3, argsProto)
	if rawArgs, marshalErr := jsonx.Marshal(args); marshalErr == nil {
		out = appendStringField(out, 4, string(rawArgs))
	}
	return out, nil
}

func encodeInferenceHistoricalToolResult(part map[string]any) ([]byte, error) {
	id := strings.TrimSpace(fmt.Sprint(part["toolCallId"]))
	name := strings.TrimSpace(fmt.Sprint(part["toolName"]))
	if id == "" || name == "" {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand historical tool result is invalid"}
	}
	value := part["result"]
	isError := false
	if wrapper, ok := value.(map[string]any); ok {
		if raw, exists := wrapper["is_error"]; exists {
			isError, _ = raw.(bool)
			value = wrapper["content"]
		}
	}
	valueProto, err := structpb.NewValue(value)
	if err != nil {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand historical tool result is invalid"}
	}
	encodedValue, err := proto.Marshal(valueProto)
	if err != nil {
		return nil, err
	}
	out := appendStringField(nil, 1, id)
	out = appendStringField(out, 2, name)
	out = appendBytesField(out, 3, encodedValue)
	if isError {
		out = appendVarintField(out, 4, 1)
	}
	// InferenceToolResultPart.experimental_content(5): ordered text+media parts.
	// Present only when the tool returned media, so text-only results keep the
	// exact historical wire shape.
	if experimental, ok := part["experimental_content"].([]any); ok {
		for _, rawContent := range experimental {
			content, ok := rawContent.(map[string]any)
			if !ok {
				return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand historical tool result media is invalid"}
			}
			if media, isMedia := mediaPartFromHistory(content); isMedia {
				out = appendBytesField(out, 5, encodeInferenceContentPart(ContentPart{Media: &media}))
				continue
			}
			if strings.TrimSpace(fmt.Sprint(content["type"])) == "text" {
				text, _ := content["text"].(string)
				out = appendBytesField(out, 5, encodeInferenceContentPart(ContentPart{Text: text}))
				continue
			}
			return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand historical tool result media is invalid"}
		}
	}
	return out, nil
}

func encodeInferenceTool(tool ToolDefinition) ([]byte, error) {
	if strings.TrimSpace(tool.Name) == "" {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand tool name is required"}
	}
	schema := map[string]any{}
	if len(tool.InputSchema) > 0 {
		if err := jsonx.Unmarshal(tool.InputSchema, &schema); err != nil {
			return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand tool schema is invalid"}
		}
	}
	// Cursor's prompt compiler wraps JSON Schema in the ToolParameters
	// jsonSchema envelope before serializing the protobuf Struct. Sending the
	// schema directly is accepted for text-only turns but makes tool-bearing
	// Sand requests fail upstream as resource_exhausted.
	parameters, err := marshalStruct(map[string]any{"jsonSchema": schema})
	if err != nil {
		return nil, err
	}
	out := appendStringField(nil, 1, tool.Name)
	out = appendStringField(out, 2, tool.Description)
	out = appendBytesField(out, 3, parameters)
	return out, nil
}

func marshalStruct(value map[string]any) ([]byte, error) {
	message, err := structpb.NewStruct(value)
	if err != nil {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Sand protobuf Struct is invalid"}
	}
	return proto.Marshal(message)
}

func decodeInferenceResponse(payload []byte) (inferenceResponse, error) {
	number, value, err := firstBytesField(payload)
	if err != nil {
		return inferenceResponse{}, err
	}
	switch number {
	case 1:
		return inferenceResponse{Text: decodeString(value, 1), TextFinal: decodeBool(value, 2)}, nil
	case 2:
		return inferenceResponse{Tool: &inferenceToolCall{ID: decodeString(value, 1), Name: decodeString(value, 2), Arguments: decodeString(value, 3)}, ToolFinal: decodeBool(value, 4)}, nil
	case 3:
		return inferenceResponse{Usage: &inferenceUsage{Input: decodeInt64(value, 1), Output: decodeInt64(value, 2)}}, nil
	case 5:
		return inferenceResponse{Usage: &inferenceUsage{Input: decodeInt64(value, 1), Output: decodeInt64(value, 2), CacheRead: decodeInt64(value, 3), CacheWrite: decodeInt64(value, 4), Extended: true}}, nil
	case 8:
		return inferenceResponse{ErrorText: decodeString(value, 1), ErrorCode: decodeString(value, 2)}, nil
	case 9:
		return inferenceResponse{Thinking: decodeString(value, 1), ThinkSignature: decodeString(value, 2), ThinkFinal: decodeBool(value, 3)}, nil
	default:
		return inferenceResponse{}, nil
	}
}

func firstBytesField(payload []byte) (protowire.Number, []byte, error) {
	for len(payload) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(payload)
		if tagLength < 0 {
			return 0, nil, fmt.Errorf("Cursor Sand response tag is invalid")
		}
		payload = payload[tagLength:]
		if wireType == protowire.BytesType {
			value, length := protowire.ConsumeBytes(payload)
			if length < 0 {
				return 0, nil, fmt.Errorf("Cursor Sand response field is invalid")
			}
			return number, value, nil
		}
		length := protowire.ConsumeFieldValue(number, wireType, payload)
		if length < 0 {
			return 0, nil, fmt.Errorf("Cursor Sand response field is invalid")
		}
		payload = payload[length:]
	}
	return 0, nil, nil
}

func decodeBool(data []byte, target protowire.Number) bool { return decodeInt64(data, target) != 0 }

func decodeInt64(data []byte, target protowire.Number) int64 {
	for len(data) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(data)
		if tagLength < 0 {
			return 0
		}
		data = data[tagLength:]
		if number == target && wireType == protowire.VarintType {
			value, length := protowire.ConsumeVarint(data)
			if length > 0 {
				if value > uint64(math.MaxInt64) {
					return math.MaxInt64
				}
				return int64(value)
			}
			return 0
		}
		length := protowire.ConsumeFieldValue(number, wireType, data)
		if length < 0 {
			return 0
		}
		data = data[length:]
	}
	return 0
}
