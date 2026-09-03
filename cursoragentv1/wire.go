package cursoragentv1

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	agentClientRunRequest          protowire.Number = 1
	agentClientExecMessage         protowire.Number = 2
	agentClientKVMessage           protowire.Number = 3
	agentClientInteractionResponse protowire.Number = 6
	agentClientHeartbeat           protowire.Number = 7

	agentServerInteractionUpdate protowire.Number = 1
	agentServerExecMessage       protowire.Number = 2
	agentServerCheckpoint        protowire.Number = 3
	agentServerKVMessage         protowire.Number = 4
	agentServerInteractionQuery  protowire.Number = 7
)

type wireMessageType int

const (
	wireUnknown wireMessageType = iota
	wireTextDelta
	wireThinkingDelta
	wireTokenDelta
	wireTurnEnded
	wireCheckpoint
	wireKVGet
	wireKVSet
	wireRequestContext
	wireMCP
	wireShell
	wireShellStream
	wireBackgroundShell
	wireRead
	wireWrite
	wireDelete
	wireGrep
	wireList
	wireFetch
	wireDiagnostics
	wireInteractionQuery
	wireUnsupportedExec
	wireIgnored
	wireStreamMCP
	wireToolCallStarted
	wireToolCallDelta
	wireToolCallCompleted
	wireThinkingCompleted
	wireAllowlistPrecheck
	wireExecAbort
)

type decodedWireMessage struct {
	Type wireMessageType
	Text string

	TokenDelta int64
	Usage      *UsageObservation
	Checkpoint []byte

	ID       uint32
	ExecID   string
	BlobID   []byte
	BlobData []byte

	InteractionKind protowire.Number
	ToolCallID      string
	ToolName        string
	ToolArguments   string
	CallID          string
	AllowlistKind   string
	HostedSearch    *HostedSearch
}

type runWireRequest struct {
	Model           string
	ConversationID  string
	UserText        string
	UserMedia       []MediaPart
	SystemText      string
	Tools           []ToolDefinition
	HistoryMessages [][]byte
	BlobStore       map[string][]byte
	Checkpoint      []byte
	Resume          bool
}

func appendBytesField(dst []byte, number protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, number, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

func appendStringField(dst []byte, number protowire.Number, value string) []byte {
	if value == "" {
		return dst
	}
	return appendBytesField(dst, number, []byte(value))
}

func appendVarintField(dst []byte, number protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, number, protowire.VarintType)
	return protowire.AppendVarint(dst, value)
}

func encodeHeartbeat() []byte {
	return appendBytesField(nil, agentClientHeartbeat, nil)
}

func encodeRunRequest(request *runWireRequest) ([]byte, error) {
	if request == nil {
		return nil, fmt.Errorf("cursor Agent v1 run request is nil")
	}
	if request.Model == "" || request.ConversationID == "" || (!request.Resume && request.UserText == "") {
		return nil, fmt.Errorf("cursor Agent v1 run requires model, conversation id, and a user or resume action")
	}
	if request.BlobStore == nil {
		request.BlobStore = make(map[string][]byte)
	}

	conversationState := append([]byte(nil), request.Checkpoint...)
	if len(conversationState) == 0 {
		history, err := agentV1LiftToolResultMedia(request.HistoryMessages)
		if err != nil {
			return nil, err
		}
		for index, message := range history {
			if len(message) == 0 || !jsonx.Valid(message) {
				return nil, fmt.Errorf("cursor Agent v1 history message %d must be valid JSON", index)
			}
			rootID := sha256.Sum256(message)
			request.BlobStore[hex.EncodeToString(rootID[:])] = append([]byte(nil), message...)
			conversationState = appendBytesField(conversationState, 1, rootID[:])
		}
	}

	var action []byte
	if request.Resume {
		action = appendBytesField(nil, 2, nil)
	} else {
		userMessage := appendStringField(nil, 1, request.UserText)
		userMessage = appendStringField(userMessage, 2, uuid.NewString())
		if selected := encodeSelectedContextMedia(request.UserMedia); len(selected) > 0 {
			userMessage = appendBytesField(userMessage, 3, selected)
		}
		userAction := appendBytesField(nil, 1, userMessage)
		action = appendBytesField(nil, 1, userAction)
	}

	modelDetails := appendStringField(nil, 1, request.Model)
	modelDetails = appendStringField(modelDetails, 3, request.Model)
	modelDetails = appendStringField(modelDetails, 4, request.Model)
	requestedModel := appendStringField(nil, 1, request.Model)

	agentRun := appendBytesField(nil, 1, conversationState)
	agentRun = appendBytesField(agentRun, 2, action)
	agentRun = appendBytesField(agentRun, 3, modelDetails)
	if len(request.Tools) > 0 {
		agentRun = appendBytesField(agentRun, 4, encodeMCPTools(request.Tools))
	}
	agentRun = appendStringField(agentRun, 5, request.ConversationID)
	agentRun = appendBytesField(agentRun, 9, requestedModel)
	return appendBytesField(nil, agentClientRunRequest, agentRun), nil
}

// agentV1LiftToolResultMedia adapts structured history for the Agent v1 blob
// contract. Live probes (TestAuthorizedHistoryToolResultImageProbe, profile
// agent_v1) showed the Agent v1 server renders image parts inside user
// messages but ignores tool-result experimental_content, while Sand renders
// both. So for Agent v1 the media returned by a tool is re-attached as a user
// message immediately after its tool message, labelled with the originating
// tool call, and the tool-result keeps its text-only result. Nothing is
// dropped and the tool_use/tool_result pairing is unchanged.
func agentV1LiftToolResultMedia(messages [][]byte) ([][]byte, error) {
	out := make([][]byte, 0, len(messages))
	for _, raw := range messages {
		var message map[string]any
		if jsonx.Unmarshal(raw, &message) != nil || stringValue(message["role"]) != "tool" {
			out = append(out, raw)
			continue
		}
		parts, _ := message["content"].([]any)
		lifted := []any{}
		rewritten := make([]any, 0, len(parts))
		for _, rawPart := range parts {
			part, ok := rawPart.(map[string]any)
			if !ok {
				rewritten = append(rewritten, rawPart)
				continue
			}
			experimental, hasMedia := part["experimental_content"].([]any)
			if !hasMedia {
				rewritten = append(rewritten, part)
				continue
			}
			copyPart := make(map[string]any, len(part))
			for key, value := range part {
				if key != "experimental_content" {
					copyPart[key] = value
				}
			}
			rewritten = append(rewritten, copyPart)
			for _, rawContent := range experimental {
				content, ok := rawContent.(map[string]any)
				if !ok {
					continue
				}
				if _, isMedia := mediaPartFromHistory(content); isMedia {
					if len(lifted) == 0 {
						lifted = append(lifted, map[string]any{"type": "text", "text": "Attachment returned by tool " + stringValue(part["toolName"]) + " (call " + stringValue(part["toolCallId"]) + "):"})
					}
					lifted = append(lifted, content)
				}
			}
		}
		message["content"] = rewritten
		encoded, err := jsonx.Marshal(message)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
		if len(lifted) > 0 {
			liftedMessage, err := jsonx.Marshal(map[string]any{"role": "user", "content": lifted})
			if err != nil {
				return nil, err
			}
			out = append(out, liftedMessage)
		}
	}
	return out, nil
}

func encodeKVGetResult(id uint32, blob []byte) []byte {
	result := []byte(nil)
	if blob != nil {
		result = appendBytesField(result, 1, blob)
	}
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 2, result)
	return appendBytesField(nil, agentClientKVMessage, message)
}

func encodeKVSetResult(id uint32) []byte {
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 3, nil)
	return appendBytesField(nil, agentClientKVMessage, message)
}

func encodeMCPTool(tool ToolDefinition) []byte {
	definition := appendStringField(nil, 1, tool.Name)
	definition = appendStringField(definition, 2, tool.Description)
	// The live CLI contract carries JSON Schema in input_schema_json (field 6).
	// Field 3 is an opaque protobuf-encoded schema, not raw JSON; populating it
	// with JSON bytes makes Agent v1 fail while parsing the run request.
	definition = appendStringField(definition, 4, "beefapi")
	definition = appendStringField(definition, 5, tool.Name)
	definition = appendStringField(definition, 6, string(tool.InputSchema))
	return definition
}

func encodeMCPTools(tools []ToolDefinition) []byte {
	var payload []byte
	for _, tool := range tools {
		payload = appendBytesField(payload, 1, encodeMCPTool(tool))
	}
	return payload
}

type requestContextPolicy struct {
	AllowHostedSearch   bool
	AllowHostedFetch    bool
	RequireToolCall     bool
	RequireHostedSearch bool
	RequireHostedFetch  bool
	RequireHostedTool   bool
}

func encodeRequestContextResult(id uint32, execID, systemText string, tools []ToolDefinition, policy requestContextPolicy) []byte {
	requestContextEnv := appendStringField(nil, 1, "linux")
	var requestContext []byte
	requestContext = appendBytesField(requestContext, 4, requestContextEnv)
	for _, tool := range tools {
		requestContext = appendBytesField(requestContext, 7, encodeMCPTool(tool))
	}
	if systemText != "" {
		requestContext = appendStringField(requestContext, 16, systemText)
	}
	if policy.RequireToolCall && len(tools) > 0 {
		names := make([]string, 0, len(tools))
		for _, tool := range tools {
			names = append(names, tool.Name)
		}
		// Agent v1 has no native tool_choice field. Transmit the caller's
		// choice as an always-apply rule, but keep the real MCP/terminal checks.
		// This is not a promise that the upstream model obeys a hard constraint.
		content := "Tool choice for this turn: invoke at least one of the provided MCP tools (" + strings.Join(names, ", ") + "). Invoke the actual tool with its declared arguments; a plain-text or JSON answer is not a substitute. Once the tool has been invoked, continue using its result normally."
		requestContext = appendBytesField(requestContext, 2, encodeCallerRule("/caller/tool-choice.mdc", content))
	}
	if policy.RequireHostedSearch {
		requestContext = appendBytesField(requestContext, 2, encodeCallerRule("/caller/hosted-search.mdc", "This turn requires an actual native web search. A plain-text or JSON answer is not a substitute. Once search results are available, continue using them normally. Upstream has no native hard tool_choice; this is guidance only."))
	}
	if policy.RequireHostedFetch {
		requestContext = appendBytesField(requestContext, 2, encodeCallerRule("/caller/hosted-fetch.mdc", "This turn requires an actual native web fetch. A plain-text or JSON answer is not a substitute. Once the fetch result is available, continue using it normally. Upstream has no native hard tool_choice; this is guidance only."))
	}
	if policy.RequireHostedTool && !policy.RequireHostedSearch && !policy.RequireHostedFetch {
		requestContext = appendBytesField(requestContext, 2, encodeCallerRule("/caller/hosted-web.mdc", "This turn requires an actual native web search or web fetch. A plain-text or JSON answer is not a substitute. Once results are available, continue using them normally. Upstream has no native hard tool_choice; this is guidance only."))
	}
	if policy.AllowHostedSearch {
		requestContext = appendVarintField(requestContext, 17, 1)
	}
	if policy.AllowHostedFetch {
		requestContext = appendVarintField(requestContext, 24, 1)
	}
	requestContextSuccess := appendBytesField(nil, 1, requestContext)
	requestContextResult := appendBytesField(nil, 1, requestContextSuccess)
	execMessage := appendVarintField(nil, 1, uint64(id))
	execMessage = appendBytesField(execMessage, 10, requestContextResult)
	execMessage = appendStringField(execMessage, 15, execID)
	return appendBytesField(nil, agentClientExecMessage, execMessage)
}

func encodeCallerRule(path, content string) []byte {
	rule := appendStringField(nil, 1, path)
	rule = appendStringField(rule, 2, content)
	rule = appendBytesField(rule, 3, appendBytesField(nil, 1, nil)) // CursorRuleType.global
	rule = appendVarintField(rule, 4, 2)                            // CursorRuleSource.USER
	return rule
}

func encodeExecRejected(id uint32, execID string, resultField, rejectedField, rejectedReasonField protowire.Number, reason string) []byte {
	rejected := appendStringField(nil, rejectedReasonField, reason)
	result := appendBytesField(nil, rejectedField, rejected)
	execMessage := appendVarintField(nil, 1, uint64(id))
	execMessage = appendBytesField(execMessage, resultField, result)
	execMessage = appendStringField(execMessage, 15, execID)
	return appendBytesField(nil, agentClientExecMessage, execMessage)
}

func encodeExecError(id uint32, execID string, resultField protowire.Number, message string) []byte {
	errorMessage := appendStringField(nil, 1, message)
	result := appendBytesField(nil, 2, errorMessage)
	execMessage := appendVarintField(nil, 1, uint64(id))
	execMessage = appendBytesField(execMessage, resultField, result)
	execMessage = appendStringField(execMessage, 15, execID)
	return appendBytesField(nil, agentClientExecMessage, execMessage)
}

func encodeFetchError(id uint32, execID string, message string) []byte {
	fetchError := appendStringField(nil, 2, message)
	result := appendBytesField(nil, 2, fetchError)
	execMessage := appendVarintField(nil, 1, uint64(id))
	execMessage = appendBytesField(execMessage, 20, result)
	execMessage = appendStringField(execMessage, 15, execID)
	return appendBytesField(nil, agentClientExecMessage, execMessage)
}

// encodeSelectedContextMedia encodes UserMessage.selected_context(3) with
// SelectedImage entries {uuid=2, path=3, dimension=4{width=1,height=2},
// mime_type=7, data=8}. Documents have no native Agent v1 user attachment and
// are surfaced as an explicit text note by the caller instead.
func encodeSelectedContextMedia(media []MediaPart) []byte {
	var selected []byte
	for _, part := range media {
		if part.Kind != mediaKindImage {
			continue
		}
		raw, err := part.rawBytes()
		if err != nil || len(raw) == 0 {
			continue
		}
		id := uuid.NewString()
		image := appendStringField(nil, 2, id)
		image = appendStringField(image, 3, "pasted-"+id+imageExtension(part.MimeType))
		if width, height := part.imageDimensions(); width > 0 && height > 0 {
			dimension := appendVarintField(nil, 1, uint64(width))
			dimension = appendVarintField(dimension, 2, uint64(height))
			image = appendBytesField(image, 4, dimension)
		}
		image = appendStringField(image, 7, part.MimeType)
		image = appendBytesField(image, 8, raw)
		selected = appendBytesField(selected, 1, image)
	}
	return selected
}

func imageExtension(mimeType string) string {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		return ".png"
	}
}

// encodeMCPResult encodes McpSuccess{content=1[McpToolResultContentItem{text=1 |
// image=2{data=1,mime_type=2}}], is_error=2}. Media returned by the caller
// tool is delivered as native image items so the model actually sees it.
func encodeMCPResult(id uint32, execID string, result ToolResult) []byte {
	var success []byte
	if result.Content != "" || len(result.Media) == 0 {
		text := appendStringField(nil, 1, result.Content)
		success = appendBytesField(success, 1, appendBytesField(nil, 1, text))
	}
	for _, media := range result.Media {
		raw, err := media.rawBytes()
		if err != nil || len(raw) == 0 {
			continue
		}
		if media.Kind != mediaKindImage {
			// McpToolResultContentItem has no document member; keep a bounded,
			// explicit note instead of silently dropping the attachment.
			note := appendStringField(nil, 1, "[attachment "+media.MimeType+" \""+media.Filename+"\" omitted: Cursor Agent v1 tool results carry text and images only]")
			success = appendBytesField(success, 1, appendBytesField(nil, 1, note))
			continue
		}
		image := appendBytesField(nil, 1, raw)
		image = appendStringField(image, 2, media.MimeType)
		success = appendBytesField(success, 1, appendBytesField(nil, 2, image))
	}
	if result.IsError {
		success = appendVarintField(success, 2, 1)
	}
	mcpResult := appendBytesField(nil, 1, success)
	execMessage := appendVarintField(nil, 1, uint64(id))
	execMessage = appendBytesField(execMessage, 11, mcpResult)
	execMessage = appendStringField(execMessage, 15, execID)
	return appendBytesField(nil, agentClientExecMessage, execMessage)
}

func encodeInteractionDecision(id uint32, kind protowire.Number, approve bool, reason string) ([]byte, error) {
	if kind != 2 && kind != 4 && kind != 5 && kind != 6 && kind != 9 {
		return nil, fmt.Errorf("unsupported Cursor Agent v1 interaction query kind %d", kind)
	}
	result := []byte(nil)
	if approve {
		result = appendBytesField(result, 1, nil)
	} else {
		rejected := appendStringField(nil, 1, reason)
		result = appendBytesField(result, 2, rejected)
	}
	response := appendVarintField(nil, 1, uint64(id))
	response = appendBytesField(response, kind, result)
	return appendBytesField(nil, agentClientInteractionResponse, response), nil
}

func encodeAskQuestionRejected(id uint32, reason string) []byte {
	rejected := appendStringField(nil, 1, reason)
	result := appendBytesField(nil, 3, rejected)
	interactionResult := appendBytesField(nil, 1, result)
	response := appendVarintField(nil, 1, uint64(id))
	response = appendBytesField(response, 3, interactionResult)
	return appendBytesField(nil, agentClientInteractionResponse, response)
}

func encodeCreatePlanError(id uint32, message string) []byte {
	planError := appendStringField(nil, 1, message)
	planResult := appendBytesField(nil, 2, planError)
	planResponse := appendBytesField(nil, 1, planResult)
	response := appendVarintField(nil, 1, uint64(id))
	response = appendBytesField(response, 7, planResponse)
	return appendBytesField(nil, agentClientInteractionResponse, response)
}

func wireReplyRequired(messageType wireMessageType) bool {
	switch messageType {
	case wireKVGet, wireKVSet, wireRequestContext, wireMCP, wireShell,
		wireShellStream, wireBackgroundShell, wireRead, wireWrite, wireDelete,
		wireGrep, wireList, wireFetch, wireDiagnostics, wireInteractionQuery,
		wireUnsupportedExec, wireAllowlistPrecheck, wireExecAbort:
		return true
	default:
		return false
	}
}

func decodeProtoValue(data []byte) any {
	if len(data) == 0 {
		return nil
	}
	value := &structpb.Value{}
	if err := proto.Unmarshal(data, value); err != nil {
		if utf8.Valid(data) {
			return string(data)
		}
		return base64.RawURLEncoding.EncodeToString(data)
	}
	return value.AsInterface()
}

func decodeString(data []byte, target protowire.Number) string {
	return string(decodeBytes(data, target))
}

func decodeBytes(data []byte, target protowire.Number) []byte {
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return nil
		}
		data = data[consumed:]
		if wireType == protowire.BytesType {
			value, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil
			}
			data = data[n:]
			if number == target {
				return append([]byte(nil), value...)
			}
			continue
		}
		n := protowire.ConsumeFieldValue(number, wireType, data)
		if n < 0 {
			return nil
		}
		data = data[n:]
	}
	return nil
}

func decodeVarint(data []byte, target protowire.Number) uint64 {
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return 0
		}
		data = data[consumed:]
		if wireType == protowire.VarintType {
			value, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return 0
			}
			data = data[n:]
			if number == target {
				return value
			}
			continue
		}
		n := protowire.ConsumeFieldValue(number, wireType, data)
		if n < 0 {
			return 0
		}
		data = data[n:]
	}
	return 0
}
