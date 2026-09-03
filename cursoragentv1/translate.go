package cursoragentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

var responsesRequestTranslateMu sync.Mutex

const (
	// headerCursorAgentV1NativeWebSearch is still sent by the relay for older
	// sidecar images; hosted search no longer depends on it here.
	headerCursorAgentV1NativeWebSearch = "X-CPA-Runtime-Cursor-Agent-V1-Native-Web-Search"
	claudeCodeNoVisibleOutputRecovery  = "[Your previous response had no visible output. Please continue and produce a user-visible response.]"
	// continueTurnUserText matches Kiro's fallback when the last message is
	// assistant or the active user text is empty after stripping trailing system.
	continueTurnUserText = "."
)

type requestError struct {
	status int
	text   string
}

func (e *requestError) Error() string         { return e.text }
func (e *requestError) StatusCode() int       { return e.status }
func (e *requestError) IsRequestScoped() bool { return true }

type translatedTurn struct {
	managed        ManagedRequest
	publicModel    string
	responseFormat sdktranslator.Format
	original       []byte
	claudeRequest  []byte
	parsedUserText string
	replay         bool
}

func translateExecutionRequest(req cliproxyexecutor.Request, opts cliproxyexecutor.Options, credentialScope, accountKey, baseURL, accessToken, clientVersion string) (translatedTurn, error) {
	claudeBody := append([]byte(nil), req.Payload...)
	hosted := hostedRunPolicy{}
	switch req.Format {
	case sdktranslator.FormatOpenAI:
		claudeBody = sdktranslator.TranslateRequest(sdktranslator.FormatOpenAI, sdktranslator.FormatClaude, req.Model, req.Payload, false)
	case sdktranslator.FormatOpenAIResponse:
		normalized, responsesHosted, normalizeErr := normalizeAgentV1ResponsesRequest(req.Payload, metadataString(opts.Metadata, "beefapi_cursor_runtime_profile") == runtimeSand)
		if normalizeErr != nil {
			return translatedTurn{}, normalizeErr
		}
		hosted = responsesHosted
		if hosted.AllowSearch {
			// Existing Responses search also opened native pages.
			hosted.AllowFetch = true
		}
		responsesRequestTranslateMu.Lock()
		claudeBody = sdktranslator.TranslateRequest(sdktranslator.FormatOpenAIResponse, sdktranslator.FormatClaude, req.Model, normalized, false)
		responsesRequestTranslateMu.Unlock()
	case sdktranslator.FormatClaude:
	default:
		claudeBody = sdktranslator.TranslateRequest(req.Format, sdktranslator.FormatClaude, req.Model, req.Payload, false)
	}
	var root map[string]any
	if err := jsonx.Unmarshal(claudeBody, &root); err != nil {
		return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 could not translate the request to Messages"}
	}
	messages, _ := root["messages"].([]any)
	dynamicSystemText := ""
	if req.Format == sdktranslator.FormatClaude {
		originalMessageCount := len(messages)
		var normalizeErr error
		messages, dynamicSystemText, normalizeErr = normalizeTrailingContext(messages)
		if normalizeErr != nil {
			return translatedTurn{}, normalizeErr
		}
		if len(messages) != originalMessageCount {
			root["messages"] = messages
			normalizedBody, marshalErr := jsonx.Marshal(root)
			if marshalErr != nil {
				return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 could not normalize Claude Code context"}
			}
			claudeBody = normalizedBody
		}
	}

	if len(messages) == 0 {
		return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 requires at least one message"}
	}
	last, ok := messages[len(messages)-1].(map[string]any)
	if !ok {
		return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 requires at least one message"}
	}
	continueFromAssistant := false
	switch stringValue(last["role"]) {
	case "user":
	case "assistant":
		// Anthropic prefills and Kiro both accept a trailing assistant turn.
		// Synthesize a continue user message instead of 400.
		continueFromAssistant = true
	default:
		return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 requires a trailing user message"}
	}
	var userText string
	var results []ToolResult
	var userMedia []MediaPart
	var err error
	if continueFromAssistant {
		userText = continueTurnUserText
	} else {
		userText, results, err = parseLiveToolContinuation(messages)
		if err != nil {
			return translatedTurn{}, err
		}
		activeTurn, turnErr := parseActiveUserTurn(last["content"])
		if turnErr != nil {
			return translatedTurn{}, turnErr
		}
		userMedia = cloneMedia(activeTurn.Media)
		if strings.TrimSpace(userText) == "" && len(results) == 0 && len(userMedia) > 0 {
			// Both data planes require a user text; an image-only turn gets a
			// neutral pointer rather than a 400.
			userText = "See the attached media."
		}
		if strings.TrimSpace(userText) == "" && len(results) == 0 && len(userMedia) == 0 {
			userText = continueTurnUserText
		}
	}
	parsedUserText := userText
	if len(results) > 0 && strings.TrimSpace(userText) != "" {
		if req.Format == sdktranslator.FormatClaude && isClaudeCodeSystem(root["system"]) && isClaudeCodeToolRecoveryText(userText) {
			userText = ""
		}
	}
	policy, err := parseCallerToolPolicy(req)
	if err != nil {
		return translatedTurn{}, err
	}
	if req.Format == sdktranslator.FormatOpenAIResponse && hosted.RequireSearch {
		policy.required = false
		policy.name = ""
	}
	// Live Agent v1 hands control to the client on each MCP call and does not
	// emit a pre-result turnEnded batch delimiter. Keep that wire serial.
	// Sand already correlates parallel caller tools on the inference stream
	// (MaxParallelTools is a cap, not an immediate park).
	if metadataString(opts.Metadata, "beefapi_cursor_runtime_profile") != runtimeSand {
		policy.maxParallel = 1
	}
	if req.Format == sdktranslator.FormatOpenAI {
		chatSearch, chatErr := chatHostedSearchPolicy(req.Payload)
		if chatErr != nil {
			return translatedTurn{}, chatErr
		}
		if chatSearch {
			hosted.AllowSearch = true
			hosted.AllowFetch = true
		}
	}
	toolValue := root["tools"]
	if req.Format == sdktranslator.FormatClaude && isClaudeCodeSystem(root["system"]) {
		var enableFetch bool
		toolValue, enableFetch, err = normalizeClaudeCodeNativeSearchTools(toolValue)
		if err != nil {
			return translatedTurn{}, err
		}
		if enableFetch && !policy.required {
			hosted.AllowFetch = true
		}
	}
	tools, digest, serverHosted, err := parseClaudeTools(toolValue, policy)
	if err != nil {
		return translatedTurn{}, err
	}
	if serverHosted.AllowSearch {
		hosted.AllowSearch = true
	}
	if serverHosted.AllowFetch {
		hosted.AllowFetch = true
	}
	if serverHosted.RequireSearch {
		hosted.RequireSearch = true
	}
	if serverHosted.RequireFetch {
		hosted.RequireFetch = true
	}
	if serverHosted.RequireAny {
		hosted.RequireAny = true
	}
	if serverHosted.SearchMaxUses != nil {
		hosted.SearchMaxUses = serverHosted.SearchMaxUses
	}
	if serverHosted.FetchMaxUses != nil {
		hosted.FetchMaxUses = serverHosted.FetchMaxUses
	}
	hosted.SearchDomains = hosted.SearchDomains.merge(serverHosted.SearchDomains)
	hosted.FetchDomains = hosted.FetchDomains.merge(serverHosted.FetchDomains)
	if policy.none {
		hosted = hostedRunPolicy{}
	}
	if hosted.restrictsDomains() && metadataString(opts.Metadata, "beefapi_cursor_runtime_profile") != runtimeSand {
		// Agent v1 runs hosted search inside Cursor's server loop; results reach
		// the model before the sidecar sees them, so the restriction cannot be
		// honored there. Sand filters results locally before the model reads them.
		return translatedTurn{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 hosted search domain restrictions require the Sand runtime profile"}
	}
	historySource := messages[:len(messages)-1]
	if continueFromAssistant {
		historySource = messages
	}
	history := make([][]byte, 0, len(historySource))
	for _, message := range historySource {
		historyMessage, normalizeErr := normalizeHistoryMessage(message)
		if normalizeErr != nil {
			return translatedTurn{}, normalizeErr
		}
		encoded, marshalErr := jsonx.Marshal(historyMessage)
		if marshalErr != nil {
			return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 history is invalid"}
		}
		history = append(history, encoded)
	}
	if len(results) > 0 {
		if historyToolMessage, ok := toolResultHistoryMessage(last, results); ok {
			encoded, marshalErr := jsonx.Marshal(historyToolMessage)
			if marshalErr != nil {
				return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 history is invalid"}
			}
			history = append(history, encoded)
		}
	}
	sessionID := strings.TrimSpace(metadataString(req.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey))
	if len(results) > 0 {
		sessionID = ""
	} else if sessionID == "" {
		return translatedTurn{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 execution session id is required"}
	}
	requestSystemText := systemText(root["system"])
	if dynamicSystemText != "" {
		if requestSystemText != "" {
			requestSystemText += "\n\n"
		}
		requestSystemText += dynamicSystemText
	}
	managed := ManagedRequest{
		TurnMetering: opts.Headers.Get(headerMeteringMode) == "turn-v2",
		SessionID:    sessionID, BillingRunID: metadataString(req.Metadata, "beefapi_request_id"), CredentialScope: credentialScope, AccountKey: accountKey, ToolCatalogDigest: digest,
		RequireExisting: responsesRequiresExisting(req), DisableCallerTools: policy.none,
		Run: RunRequest{
			BaseURL: baseURL, AccessToken: accessToken, ClientVersion: clientVersion, Model: req.Model,
			UserText: userText, UserMedia: userMedia, ThinkingEnabled: claudeThinkingEnabled(root["thinking"]),
			SystemText: requestSystemText, Tools: tools, HistoryMessages: history,
			RouteChannelID:       routeChannelID(credentialScope),
			RouteUserID:          routeUserID(credentialScope),
			AllowHostedSearch:    hosted.AllowSearch,
			AllowHostedFetch:     hosted.AllowFetch,
			RequireHostedSearch:  hosted.RequireSearch,
			RequireHostedFetch:   hosted.RequireFetch,
			RequireHostedTool:    hosted.RequireAny,
			HostedSearchMaxUses:  hosted.SearchMaxUses,
			HostedFetchMaxUses:   hosted.FetchMaxUses,
			HostedSearchDomains:  hosted.SearchDomains,
			HostedFetchDomains:   hosted.FetchDomains,
			RequireToolCall:      policy.required && len(results) == 0 && len(tools) > 0,
			MaxParallelTools:     policy.maxParallel,
			DeleteCompletedState: req.Format != sdktranslator.FormatOpenAIResponse,
		},
	}
	if len(results) > 0 {
		managed.FollowUpText = strings.TrimSpace(userText)
		managed.ActiveResults = results
		managed.Run.UserText = ""
	}
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	if responseFormat == "" {
		responseFormat = req.Format
	}
	publicModel := metadataString(opts.Metadata, cliproxyexecutor.RequestedModelMetadataKey)
	if publicModel == "" {
		publicModel = req.Model
	}
	projectionOriginal, err := requestWithModel(opts.OriginalRequest, publicModel)
	if err != nil {
		return translatedTurn{}, err
	}
	turn := translatedTurn{managed: managed, publicModel: publicModel, responseFormat: responseFormat, original: projectionOriginal, claudeRequest: claudeBody, parsedUserText: parsedUserText}
	if req.Format == sdktranslator.FormatClaude || req.Format == sdktranslator.FormatOpenAI {
		turn.managed.Run.HistoryMessages, err = structuredHistory(messages, !continueFromAssistant)
		if err != nil {
			return translatedTurn{}, err
		}
		turn.managed.Run.UserText = userText
		if len(results) > 0 && strings.TrimSpace(userText) == "" {
			turn.managed.Run.UserText = historyContinuationText
		}
		turn.managed.FollowUpText = ""
		turn.managed.Run.HistoryRequestKey, err = historyRequestKey(turn, req.Metadata["beefapi_token_id"], len(results) > 0)
		if err != nil {
			return translatedTurn{}, err
		}
	}
	return turn, nil
}

func isClaudeCodeToolRecoveryText(text string) bool {
	return strings.TrimSpace(text) == claudeCodeNoVisibleOutputRecovery
}

func normalizeClaudeCodeNativeSearchTools(value any) (any, bool, error) {
	tools, ok := value.([]any)
	if !ok {
		return value, false, nil
	}
	filtered := make([]any, 0, len(tools))
	enableFetch := false
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok {
			filtered = append(filtered, raw)
			continue
		}
		typeName := strings.ToLower(strings.TrimSpace(stringValue(tool["type"])))
		if knownServerWebKind(typeName) != "" {
			filtered = append(filtered, raw)
			continue
		}
		name := strings.ToLower(strings.TrimSpace(stringValue(tool["name"])))
		if name == "webfetch" {
			// Keep hosted fetch independently. Do not restore the Claude Code
			// catalog WebFetch as a caller tool (domain safety / preflight).
			enableFetch = true
			continue
		}
		filtered = append(filtered, raw)
	}
	return filtered, enableFetch, nil
}

func normalizeTrailingContext(messages []any) ([]any, string, error) {
	if len(messages) == 0 {
		return messages, "", nil
	}
	start := len(messages)
	for start > 0 {
		message, ok := messages[start-1].(map[string]any)
		if !ok {
			break
		}
		role := stringValue(message["role"])
		if role != "system" && role != "developer" {
			break
		}
		start--
	}
	if start == len(messages) {
		return messages, "", nil
	}
	if start == 0 {
		return nil, "", &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 Claude Code context is missing its user message"}
	}
	active, ok := messages[start-1].(map[string]any)
	if !ok {
		return nil, "", &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 Claude Code context is missing its user message"}
	}
	switch stringValue(active["role"]) {
	case "user", "assistant":
	default:
		return nil, "", &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 Claude Code context must follow a user message"}
	}
	parts := make([]string, 0, len(messages)-start)
	for _, raw := range messages[start:] {
		message, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		text, err := lenientTrailingSystemText(message["content"])
		if err != nil {
			continue
		}
		if strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	return append([]any(nil), messages[:start]...), strings.Join(parts, "\n\n"), nil
}

func isClaudeCodeSystem(value any) bool {
	text := strings.TrimSpace(systemText(value))
	if strings.HasPrefix(text, "You are Claude Code") {
		return true
	}
	if !strings.Contains(text, "x-anthropic-billing-header: cc_version=") {
		return false
	}
	return strings.Contains(text, "cc_entrypoint=sdk-cli;") ||
		strings.Contains(text, "cc_entrypoint=cli;")
}

func lenientTrailingSystemText(value any) (string, error) {
	if text, ok := value.(string); ok {
		return text, nil
	}
	blocks, ok := value.([]any)
	if !ok {
		return "", nil
	}
	parts := make([]string, 0, len(blocks))
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if !ok || stringValue(block["type"]) != "text" {
			continue
		}
		if text, ok := block["text"].(string); ok && strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n"), nil
}

func normalizeHistoryMessage(value any) (any, error) {
	message, ok := value.(map[string]any)
	if !ok {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 history is invalid"}
	}
	blocks, ok := message["content"].([]any)
	if !ok {
		return message, nil
	}
	normalized := make([]any, 0, len(blocks))
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if !ok {
			normalized = append(normalized, raw)
			continue
		}
		switch stringValue(block["type"]) {
		case "tool_use":
			id, name := stringValue(block["id"]), stringValue(block["name"])
			if id == "" || name == "" {
				return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_use is invalid"}
			}
			input := block["input"]
			if input == nil {
				input = map[string]any{}
			}
			if _, ok := input.(map[string]any); !ok {
				return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_use input must be an object"}
			}
			event, err := jsonx.Marshal(map[string]any{"type": "tool_use", "id": id, "name": name, "input": input})
			if err != nil {
				return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_use input is invalid"}
			}
			normalized = append(normalized, map[string]any{"type": "text", "text": "BEEFAPI_TOOL_EVENT " + string(event)})
		case "tool_result":
			id := stringValue(block["tool_use_id"])
			if id == "" {
				return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_result is invalid"}
			}
			content, contentErr := normalizedHistoryToolResultContent(block["content"])
			if contentErr != nil {
				return nil, contentErr
			}
			isError := false
			if rawIsError, exists := block["is_error"]; exists {
				var ok bool
				isError, ok = rawIsError.(bool)
				if !ok {
					return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_result is_error must be a boolean"}
				}
			}
			event, err := jsonx.Marshal(map[string]any{"type": "tool_result", "tool_use_id": id, "is_error": isError, "content": content})
			if err != nil {
				return nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_result content is invalid"}
			}
			normalized = append(normalized, map[string]any{"type": "text", "text": "BEEFAPI_TOOL_EVENT " + string(event)})
		case "image", "document", "search_result":
			converted, _, convertErr := parseAnthropicContentBlock(block)
			if convertErr != nil {
				return nil, convertErr
			}
			normalized = append(normalized, historyContentParts(converted)...)
		case "thinking", "redacted_thinking":
			// Responses-lane history is text-event encoded; provider reasoning
			// has no faithful place there and is never rendered as text.
			continue
		default:
			normalized = append(normalized, raw)
		}
	}
	copy := make(map[string]any, len(message))
	for key, item := range message {
		copy[key] = item
	}
	copy["content"] = normalized
	return copy, nil
}

func normalizedHistoryToolResultContent(content any) (any, error) {
	// Responses-lane text-event history cannot carry media; keep the text facts
	// and let the structured-history path (Messages/Chat, Sand checkpoint) own
	// native media replay.
	normalized, _, err := normalizedHistoryToolResultParts(content)
	return normalized, err
}

func toolResultHistoryMessage(last map[string]any, results []ToolResult) (map[string]any, bool) {
	if last == nil || len(results) == 0 {
		return nil, false
	}
	blocks, ok := last["content"].([]any)
	if !ok {
		return nil, false
	}
	filtered := make([]any, 0, len(blocks))
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if ok && stringValue(block["type"]) == "tool_result" {
			filtered = append(filtered, block)
		}
	}
	if len(filtered) == 0 {
		return nil, false
	}
	message := make(map[string]any, len(last))
	for key, value := range last {
		message[key] = value
	}
	message["content"] = filtered
	normalized, err := normalizeHistoryMessage(message)
	if err != nil {
		return nil, false
	}
	typed, ok := normalized.(map[string]any)
	return typed, ok
}

func parseLiveToolContinuation(messages []any) (string, []ToolResult, error) {
	if len(messages) == 0 {
		return "", nil, nil
	}
	last, _ := messages[len(messages)-1].(map[string]any)
	userText, results, err := parseActiveUserContent(last["content"])
	if err != nil {
		return "", nil, err
	}
	if len(results) > 0 {
		return userText, results, nil
	}
	if strings.TrimSpace(userText) == "" || len(messages) < 2 {
		return userText, results, nil
	}
	previous, ok := messages[len(messages)-2].(map[string]any)
	if !ok || stringValue(previous["role"]) != "user" {
		return userText, results, nil
	}
	_, previousResults, err := parseActiveUserContent(previous["content"])
	if err != nil {
		return "", nil, err
	}
	return userText, previousResults, nil
}

func requestWithModel(payload []byte, model string) ([]byte, error) {
	if len(payload) == 0 || strings.TrimSpace(model) == "" {
		return append([]byte(nil), payload...), nil
	}
	var root map[string]any
	if err := jsonx.Unmarshal(payload, &root); err != nil {
		return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 original request is invalid"}
	}
	root["model"] = strings.TrimSpace(model)
	encoded, err := jsonx.Marshal(root)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func responsesRequiresExisting(req cliproxyexecutor.Request) bool {
	if req.Format != sdktranslator.FormatOpenAIResponse {
		return false
	}
	var request struct {
		PreviousResponseID string `json:"previous_response_id"`
	}
	return jsonx.Unmarshal(req.Payload, &request) == nil && strings.TrimSpace(request.PreviousResponseID) != ""
}

func routeChannelID(credentialScope string) int {
	parts := strings.Split(strings.TrimSpace(credentialScope), ":")
	if len(parts) != 6 || parts[4] != "channel" {
		return 0
	}
	channelID, err := strconv.Atoi(parts[5])
	if err != nil || channelID <= 0 {
		return 0
	}
	return channelID
}

func routeUserID(credentialScope string) int {
	parts := strings.Split(strings.TrimSpace(credentialScope), ":")
	if len(parts) != 6 || parts[2] != "user" {
		return 0
	}
	userID, err := strconv.Atoi(parts[3])
	if err != nil || userID <= 0 {
		return 0
	}
	return userID
}

func normalizeAgentV1ResponsesRequest(payload []byte, allowRequiredHosted bool) ([]byte, hostedRunPolicy, error) {
	hosted := hostedRunPolicy{}
	var root map[string]any
	if jsonx.Unmarshal(payload, &root) != nil {
		return payload, hosted, nil
	}
	choiceMode := "auto"
	if choice, exists := root["tool_choice"]; exists && choice != nil {
		switch typed := choice.(type) {
		case string:
			choiceMode = strings.ToLower(strings.TrimSpace(typed))
		case map[string]any:
			choiceMode = strings.ToLower(strings.TrimSpace(stringValue(typed["type"])))
		default:
			choiceMode = "invalid"
		}
	}
	optional := choiceMode == "" || choiceMode == "auto" || choiceMode == "function" || choiceMode == "custom" || choiceMode == "tool"
	disabled := choiceMode == "none"
	namedCaller := choiceMode == "function" || choiceMode == "custom" || choiceMode == "tool"
	allowHostedSearch := false
	sawHostedSearch := false
	requiredHostedChoice := choiceMode == "required" || choiceMode == "web_search" || choiceMode == "web_search_preview"
	if tools, ok := root["tools"].([]any); ok {
		filtered := make([]any, 0, len(tools))
		for _, raw := range tools {
			tool, ok := raw.(map[string]any)
			if !ok {
				filtered = append(filtered, raw)
				continue
			}
			typeName := strings.ToLower(strings.TrimSpace(stringValue(tool["type"])))
			switch typeName {
			case "x_search":
				if optional || disabled {
					continue
				}
				return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required x_search must use a Grok/xAI channel"}
			case "web_search", "web_search_preview":
				sawHostedSearch = true
				if requiredHostedChoice && !allowRequiredHosted {
					return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required web_search is unsupported"}
				}
				for key, value := range tool {
					switch key {
					case "type", "external_web_access", "user_location", "search_context_size":
					case "filters":
						filters, _ := value.(map[string]any)
						allowed, ok := domainListFromJSON(filters["allowed_domains"])
						if filters == nil || !ok {
							return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 native web_search filters.allowed_domains must be a list of domains"}
						}
						hosted.SearchDomains = hosted.SearchDomains.merge(hostedDomainPolicy{Allowed: allowed})
					default:
						return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 native web_search cannot faithfully apply " + key}
					}
				}
				if !optional && !disabled && !requiredHostedChoice {
					return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required web_search is unsupported"}
				}
				if disabled {
					continue
				}
				if namedCaller {
					continue
				}
				if rawAccess, exists := tool["external_web_access"]; exists {
					access, ok := rawAccess.(bool)
					if !ok {
						return nil, hosted, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 web_search.external_web_access must be a boolean"}
					}
					if !access {
						if requiredHostedChoice {
							return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required web_search needs external_web_access=true"}
						}
						continue
					}
				}
				allowHostedSearch = true
				continue
			}
			filtered = append(filtered, raw)
		}
		root["tools"] = filtered
		if requiredHostedChoice && sawHostedSearch {
			if !allowHostedSearch || len(filtered) > 0 {
				return nil, hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required web_search needs a hosted-search-only request"}
			}
			hosted.RequireSearch = true
			root["tool_choice"] = "auto"
		}
	}
	hosted.AllowSearch = allowHostedSearch
	if text, ok := root["input"].(string); ok {
		root["input"] = []any{map[string]any{
			"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": text}},
		}}
	}
	encoded, err := jsonx.Marshal(root)
	if err != nil {
		return nil, hosted, err
	}
	return encoded, hosted, nil
}

// activeUserContent is the active user turn split into the fields the run
// needs: prompt text, caller tool results (with any media they returned), and
// media the user attached directly (Claude Code pasted screenshots, PDFs).
type activeUserContent struct {
	Text    string
	Results []ToolResult
	Media   []MediaPart
}

func parseActiveUserContent(content any) (string, []ToolResult, error) {
	active, err := parseActiveUserTurn(content)
	if err != nil {
		return "", nil, err
	}
	return active.Text, active.Results, nil
}

func parseActiveUserTurn(content any) (activeUserContent, error) {
	if text, ok := content.(string); ok {
		return activeUserContent{Text: text}, nil
	}
	blocks, ok := content.([]any)
	if !ok {
		return activeUserContent{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 user content is invalid"}
	}
	var textParts []string
	active := activeUserContent{}
	for _, value := range blocks {
		block, ok := value.(map[string]any)
		if !ok {
			continue
		}
		switch stringValue(block["type"]) {
		case "text":
			if text := stringValue(block["text"]); text != "" {
				textParts = append(textParts, text)
			}
		case "tool_result":
			id := stringValue(block["tool_use_id"])
			if id == "" {
				return activeUserContent{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 tool_result is missing tool_use_id"}
			}
			result, err := parseActiveToolResult(id, block)
			if err != nil {
				return activeUserContent{}, err
			}
			active.Results = append(active.Results, result)
		case "image", "document", "search_result":
			converted, _, err := parseAnthropicContentBlock(block)
			if err != nil {
				return activeUserContent{}, err
			}
			text, media := splitContentParts(converted)
			if text != "" {
				textParts = append(textParts, text)
			}
			active.Media = append(active.Media, media...)
		}
	}
	active.Text = strings.Join(textParts, "\n")
	return active, nil
}

// parseActiveToolResult keeps the legacy text contract (all text blocks joined)
// and additionally collects returned media so screenshots reach the model.
func parseActiveToolResult(id string, block map[string]any) (ToolResult, error) {
	result := ToolResult{ToolCallID: id, IsError: boolValue(block["is_error"])}
	switch content := block["content"].(type) {
	case nil:
	case string:
		result.Content = content
	case []any:
		var texts []string
		for _, raw := range content {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			converted, known, err := parseAnthropicContentBlock(part)
			if err != nil {
				return ToolResult{}, err
			}
			if !known {
				// Live path historically ignored unknown result blocks; keep that
				// tolerance for the active turn but never for rebuilt history.
				continue
			}
			text, media := splitContentParts(converted)
			if text != "" {
				texts = append(texts, text)
			}
			result.Media = append(result.Media, media...)
		}
		result.Content = strings.Join(texts, "\n")
	default:
		// Matches the historical live-path tolerance (contentText returned "").
	}
	return result, nil
}

type callerToolPolicy struct {
	none        bool
	required    bool
	name        string
	maxParallel int
}

func parseCallerToolPolicy(req cliproxyexecutor.Request) (callerToolPolicy, error) {
	var root map[string]any
	if err := jsonx.Unmarshal(req.Payload, &root); err != nil {
		return callerToolPolicy{}, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 tool policy is invalid"}
	}
	policy := callerToolPolicy{}
	if parallel, ok := root["parallel_tool_calls"].(bool); ok && !parallel {
		policy.maxParallel = 1
	}
	choice := root["tool_choice"]
	if choice == nil {
		return policy, nil
	}
	if value, ok := choice.(string); ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", "auto":
		case "none":
			policy.none = true
		case "required", "any", "web_search", "web_search_preview":
			policy.required = true
		default:
			return callerToolPolicy{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 tool_choice is unsupported"}
		}
		return policy, nil
	}
	object, ok := choice.(map[string]any)
	if !ok {
		return callerToolPolicy{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 tool_choice is invalid"}
	}
	if disabled, ok := object["disable_parallel_tool_use"].(bool); ok && disabled {
		policy.maxParallel = 1
	}
	switch strings.ToLower(strings.TrimSpace(stringValue(object["type"]))) {
	case "", "auto":
	case "none":
		policy.none = true
	case "any", "required", "web_search", "web_search_preview":
		policy.required = true
	case "tool", "function", "custom":
		policy.required = true
		policy.name = strings.TrimSpace(stringValue(object["name"]))
		if policy.name == "" {
			if function, ok := object["function"].(map[string]any); ok {
				policy.name = strings.TrimSpace(stringValue(function["name"]))
			}
		}
		if policy.name == "" {
			return callerToolPolicy{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 named tool_choice is missing its name"}
		}
	default:
		return callerToolPolicy{}, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 tool_choice type is unsupported"}
	}
	return policy, nil
}

func chatHostedSearchPolicy(payload []byte) (bool, error) {
	var root map[string]any
	if jsonx.Unmarshal(payload, &root) != nil {
		return false, nil
	}
	raw, exists := root["web_search_options"]
	if !exists || raw == nil {
		return false, nil
	}
	if _, ok := raw.(map[string]any); !ok {
		return false, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 web_search_options must be an object"}
	}
	// user_location and search_context_size are ranking hints; every other
	// BeefAPI Claude pathway forwards them without effect, so do not fail here.
	return true, nil
}

func parseClaudeTools(value any, policy callerToolPolicy) ([]ToolDefinition, string, hostedRunPolicy, error) {
	var hosted hostedRunPolicy
	if policy.none {
		return nil, "", hosted, nil
	}
	if value == nil {
		if policy.required {
			return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required tool_choice has no tools"}
		}
		return nil, "", hosted, nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, "", hosted, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 tools must be an array"}
	}
	tools := make([]ToolDefinition, 0, len(list))
	hostedTools := make([]hostedDeclaredTool, 0)
	for _, raw := range list {
		tool, ok := raw.(map[string]any)
		if !ok {
			return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 supports caller function tools only"}
		}
		typeName := strings.ToLower(strings.TrimSpace(stringValue(tool["type"])))
		if kind := knownServerWebKind(typeName); kind != "" {
			declared, err := parseHostedDeclaredTool(tool, kind)
			if err != nil {
				return nil, "", hosted, err
			}
			hostedTools = append(hostedTools, declared)
			continue
		}
		if stringValue(tool["name"]) == "" {
			return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 supports caller function tools only"}
		}
		schema, err := jsonx.Marshal(tool["input_schema"])
		if err != nil {
			return nil, "", hosted, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 tool input schema is invalid"}
		}
		tools = append(tools, ToolDefinition{Name: stringValue(tool["name"]), Description: stringValue(tool["description"]), InputSchema: schema})
	}
	normalized, err := normalizeToolDefinitions(tools)
	if err != nil {
		return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: err.Error()}
	}
	requireHosted := false
	if policy.name != "" {
		selected := normalized[:0]
		for _, tool := range normalized {
			if tool.Name == policy.name {
				selected = append(selected, tool)
			}
		}
		if len(selected) > 0 {
			normalized = selected
			hostedTools = nil
		} else {
			matched := hostedTools[:0]
			for _, tool := range hostedTools {
				if tool.Name == policy.name {
					matched = append(matched, tool)
				}
			}
			if len(matched) == 0 {
				return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 named tool_choice does not match a declared tool"}
			}
			normalized = nil
			hostedTools = matched
			requireHosted = true
		}
	} else if policy.required && len(normalized) == 0 {
		if len(hostedTools) == 0 {
			return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required tool_choice has no tools"}
		}
		requireHosted = true
	}
	hosted = summarizeHostedPolicy(hostedTools, requireHosted)
	// Capped hosted execution is a one-shot child contract. A caller MCP run
	// may park across HTTP requests, so its run-local counter cannot represent
	// the client's per-request cap. Such mixed caps were unsupported before.
	if len(normalized) > 0 && (hosted.SearchMaxUses != nil || hosted.FetchMaxUses != nil || (policy.required && hosted.enabled())) {
		return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 bounded or required hosted search needs a server-tool-only request"}
	}
	if policy.required && len(normalized) == 0 && !hosted.enabled() {
		return nil, "", hosted, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 required tool_choice has no tools"}
	}
	canonical := make([]map[string]any, 0, len(normalized))
	for _, tool := range normalized {
		canonical = append(canonical, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": string(tool.InputSchema)})
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i]["name"].(string) < canonical[j]["name"].(string) })
	encoded, _ := jsonx.Marshal(canonical)
	digest := sha256.Sum256(encoded)
	return normalized, "sha256:" + hex.EncodeToString(digest[:]), hosted, nil
}

func systemText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	blocks, _ := value.([]any)
	parts := make([]string, 0, len(blocks))
	for _, raw := range blocks {
		if text, ok := raw.(string); ok {
			if strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
			continue
		}
		if block, ok := raw.(map[string]any); ok && stringValue(block["type"]) == "text" {
			parts = append(parts, stringValue(block["text"]))
		}
	}
	return strings.Join(parts, "\n\n")
}

func contentText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	blocks, _ := value.([]any)
	parts := make([]string, 0, len(blocks))
	for _, raw := range blocks {
		if block, ok := raw.(map[string]any); ok && stringValue(block["type"]) == "text" {
			parts = append(parts, stringValue(block["text"]))
		}
	}
	return strings.Join(parts, "\n")
}

func claudeThinkingEnabled(value any) bool {
	thinking, ok := value.(map[string]any)
	if !ok {
		return false
	}
	kind := strings.ToLower(stringValue(thinking["type"]))
	return kind == "enabled" || kind == "adaptive"
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil || metadata[key] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(metadata[key]))
}
