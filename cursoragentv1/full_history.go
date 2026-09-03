package cursoragentv1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

const historyContinuationText = "Continue, using the tool results above."

// Anthropic server-side tools whose completed use/result pairs may appear in
// assistant history. They are replayed as generic tool-call/tool-result facts;
// nothing here re-executes them or registers them as caller tools.
var anthropicServerToolResultTypes = map[string]bool{
	"web_search_tool_result":                  true,
	"web_fetch_tool_result":                   true,
	"code_execution_tool_result":              true,
	"bash_code_execution_tool_result":         true,
	"text_editor_code_execution_tool_result":  true,
	"tool_search_tool_result":                 true,
	"advisor_tool_result":                     true,
	"mcp_tool_result":                         true,
	"computer_tool_result":                    true,
	"browser_tool_result":                     true,
	"memory_tool_result":                      true,
	"programmatic_tool_call_tool_result":      true,
	"container_upload_tool_result":            true,
	"web_search_tool_result_error":            true,
	"code_execution_tool_result_error":        true,
	"bash_code_execution_tool_result_error":   true,
	"text_editor_code_execution_result_error": true,
}

// Native history uses tool-call/tool-result parts, as independently exercised
// against AstroQore/cursor2api@23544e4. Caller results are facts in a new Run,
// never instructions to resume a possibly terminated upstream connection.
// Completed hosted results use this same generic conversation tool-part shape;
// this does not register a caller/MCP tool or execute the historical call again.
// TestAuthorizedHostedHistoryReplay covers real search/fetch next-turn recall.
//
// Media (user images/documents, tool-result screenshots) is kept as native
// image/file parts so both Cursor data planes can carry it; provider reasoning
// is kept as reasoning parts and each encoder decides whether the signature is
// meaningful on its target. Unknown blocks remain fail-closed.
func structuredHistory(messages []any, omitLastText bool) ([][]byte, error) {
	out := make([][]byte, 0, len(messages))
	names := map[string]string{}
	serverCalls := map[string]bool{}
	resolved := map[string]bool{}
	appendMessage := func(role string, parts []any, id string) error {
		if len(parts) == 0 {
			return nil
		}
		message := map[string]any{"role": role, "content": parts}
		if id != "" {
			message["id"] = id
		}
		encoded, err := jsonx.Marshal(message)
		if err != nil {
			return err
		}
		out = append(out, encoded)
		return nil
	}
	// A caller tool_use the client never answered (interrupted turn, client-side
	// truncation) is closed with a placeholder result, the way Kiro tolerates
	// it, instead of failing the whole conversation. Server tool loops keep the
	// pause_turn conflict below because fabricating their result would lie.
	flushUnansweredCallerCalls := func(answeredNow map[string]bool) error {
		for _, id := range sortedKeys(names) {
			if resolved[id] || serverCalls[id] || answeredNow[id] {
				continue
			}
			toolPart := map[string]any{"type": "tool-result", "toolCallId": id, "toolName": names[id], "result": map[string]any{"is_error": true, "content": orphanToolResultText}}
			if err := appendMessage("tool", []any{toolPart}, id); err != nil {
				return err
			}
			resolved[id] = true
		}
		return nil
	}
	for index, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			return nil, invalidHistory("message must be an object")
		}
		role := stringValue(message["role"])
		omitText := omitLastText && index == len(messages)-1
		content := message["content"]
		if role == "user" {
			if err := flushUnansweredCallerCalls(toolResultIDs(content)); err != nil {
				return nil, err
			}
		}
		if text, ok := content.(string); ok {
			if !omitText {
				if err := appendMessage(role, []any{map[string]any{"type": "text", "text": text}}, ""); err != nil {
					return nil, err
				}
			}
			continue
		}
		blocks, ok := content.([]any)
		if !ok {
			return nil, invalidHistory("content must be text or content blocks")
		}
		parts := []any{}
		for _, rawBlock := range blocks {
			block, ok := rawBlock.(map[string]any)
			if !ok {
				return nil, invalidHistory("content block must be an object")
			}
			blockType := stringValue(block["type"])
			switch {
			case blockType == "text":
				text, ok := block["text"].(string)
				if !ok {
					return nil, invalidHistory("text block has no text")
				}
				if !omitText {
					parts = append(parts, map[string]any{"type": "text", "text": text})
					if citations, ok := block["citations"].([]any); ok && len(citations) > 0 {
						// Native text parts have no Anthropic citations field. Keep
						// returned excerpts/URLs as historical text evidence rather
						// than dropping them or inventing another tool execution.
						encoded, err := jsonx.Marshal(citations)
						if err != nil {
							return nil, err
						}
						parts = append(parts, map[string]any{"type": "text", "text": "Source citations for the preceding text: " + string(encoded)})
					}
				}
			case blockType == "image" || blockType == "document" || blockType == "search_result":
				if omitText {
					// The active turn's media travels through RunRequest.UserMedia.
					continue
				}
				converted, _, err := parseAnthropicContentBlock(block)
				if err != nil {
					return nil, err
				}
				parts = append(parts, historyContentParts(converted)...)
			case blockType == "thinking":
				if role != "assistant" {
					return nil, invalidHistory("thinking block must belong to assistant")
				}
				text, _ := block["thinking"].(string)
				part := map[string]any{"type": "reasoning", "text": text}
				if signature := stringValue(block["signature"]); signature != "" {
					part["signature"] = signature
				}
				parts = append(parts, part)
			case blockType == "redacted_thinking":
				if role != "assistant" {
					return nil, invalidHistory("redacted_thinking block must belong to assistant")
				}
				// Opaque provider ciphertext: never decoded or rendered as text.
				// Encoders only forward it when the target is the same provider
				// protocol; otherwise it is dropped there.
				parts = append(parts, map[string]any{"type": "redacted-reasoning", "data": stringValue(block["data"])})
			case blockType == "compaction":
				if role != "assistant" {
					return nil, invalidHistory("compaction block must belong to assistant")
				}
				summary, ok := block["content"].(string)
				if !ok || strings.TrimSpace(summary) == "" {
					// Only a trusted plaintext summary can cross providers.
					return nil, invalidHistory("compaction block must carry a plaintext summary")
				}
				// Anthropic semantics: everything before the compaction block is
				// dropped and the conversation continues from the summary. Any
				// tool pairing that was open before it is intentionally forgotten.
				out = out[:0]
				parts = nil
				names = map[string]string{}
				serverCalls = map[string]bool{}
				resolved = map[string]bool{}
				if err := appendMessage("user", []any{map[string]any{"type": "text", "text": "[Compacted conversation summary from the previous provider]\n" + summary}}, ""); err != nil {
					return nil, err
				}
			case blockType == "container_upload":
				return nil, invalidHistory("container_upload references an Anthropic Files API container and cannot be replayed on Cursor")
			case blockType == "tool_use" || blockType == "server_tool_use" || blockType == "mcp_tool_use":
				id, name := stringValue(block["id"]), stringValue(block["name"])
				if role != "assistant" || id == "" || name == "" || names[id] != "" {
					return nil, invalidHistory("tool call identity is invalid or duplicated")
				}
				input := block["input"]
				if input == nil {
					input = map[string]any{}
				}
				if _, ok := input.(map[string]any); !ok {
					return nil, invalidHistory("tool arguments must be an object")
				}
				names[id] = name
				if blockType != "tool_use" {
					serverCalls[id] = true
				}
				// caller/toolset metadata (programmatic tool calling, browser or
				// computer toolsets) has no native Cursor field; pairing by id is
				// what both data planes need and is preserved exactly.
				parts = append(parts, map[string]any{"type": "tool-call", "toolCallId": id, "toolName": name, "args": input})
			case blockType == "tool_result" || anthropicServerToolResultTypes[blockType]:
				id := stringValue(block["tool_use_id"])
				if id == "" || names[id] == "" || resolved[id] {
					return nil, invalidHistory("tool result has no unique matching call")
				}
				if blockType == "tool_result" && serverCalls[id] {
					return nil, invalidHistory("tool_result cannot answer a server_tool_use")
				}
				if blockType != "tool_result" && !serverCalls[id] {
					return nil, invalidHistory(blockType + " must answer a server_tool_use")
				}
				if err := appendMessage(role, parts, ""); err != nil {
					return nil, err
				}
				parts = nil
				result := block["content"]
				if result == nil {
					result = ""
				}
				toolPart := map[string]any{"type": "tool-result", "toolCallId": id, "toolName": names[id]}
				if blockType == "tool_result" {
					normalized, media, err := normalizedHistoryToolResultParts(result)
					if err != nil {
						return nil, err
					}
					result = normalized
					if rawError, exists := block["is_error"]; exists {
						isError, ok := rawError.(bool)
						if !ok {
							return nil, invalidHistory("tool result error flag must be boolean")
						}
						if isError {
							result = map[string]any{"is_error": true, "content": result}
						}
					}
					if len(media) > 0 {
						toolPart["experimental_content"] = media
					}
				}
				if blockType != "tool_result" {
					result = withoutProviderCiphertext(result)
				}
				toolPart["result"] = result
				if err := appendMessage("tool", []any{toolPart}, id); err != nil {
					return nil, err
				}
				resolved[id] = true
			default:
				return nil, invalidHistory("unsupported content block " + blockType)
			}
		}
		if err := appendMessage(role, parts, ""); err != nil {
			return nil, err
		}
	}
	for id := range names {
		if resolved[id] || !serverCalls[id] {
			continue
		}
		// Anthropic pause_turn: the server-side loop is unfinished. A cold
		// rebuild here would fabricate a result the original provider never
		// produced, so make the caller resume on the channel that owns it.
		return nil, &requestError{status: http.StatusConflict, text: "Cursor Agent v1 cannot continue an unfinished Anthropic server tool loop (pause_turn); resume this conversation on its original channel"}
	}
	if err := flushUnansweredCallerCalls(nil); err != nil {
		return nil, err
	}
	return out, nil
}

const orphanToolResultText = "[No result was returned for this tool call.]"

func toolResultIDs(content any) map[string]bool {
	blocks, ok := content.([]any)
	if !ok {
		return nil
	}
	ids := map[string]bool{}
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id := stringValue(block["tool_use_id"]); id != "" {
			ids[id] = true
		}
	}
	return ids
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// normalizedHistoryToolResultParts returns the text-only result value plus, when
// the tool returned media (chrome-devtools screenshots, PDFs), the complete
// ordered parts list for experimental_content so the model sees both.
func normalizedHistoryToolResultParts(content any) (any, []any, error) {
	if content == nil {
		return "", nil, nil
	}
	if text, ok := content.(string); ok {
		return text, nil, nil
	}
	blocks, ok := content.([]any)
	if !ok {
		return nil, nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_result content must be text"}
	}
	textParts := make([]any, 0, len(blocks))
	allParts := make([]ContentPart, 0, len(blocks))
	hasMedia := false
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if !ok {
			return nil, nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_result content block must be an object"}
		}
		converted, known, err := parseAnthropicContentBlock(block)
		if err != nil {
			return nil, nil, err
		}
		if !known {
			return nil, nil, &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 historical tool_result content contains an unsupported block"}
		}
		for _, part := range converted {
			allParts = append(allParts, part)
			if part.Media != nil {
				hasMedia = true
				continue
			}
			textParts = append(textParts, map[string]any{"type": "text", "text": part.Text})
		}
	}
	if !hasMedia {
		return textParts, nil, nil
	}
	return textParts, historyContentParts(allParts), nil
}

// withoutProviderCiphertext removes opaque provider-encrypted payloads (for
// example advisor_redacted_result.encrypted_content) from a completed server
// tool result before it is replayed on another provider. The executor already
// consumed that advice on the original channel; the ciphertext cannot be
// decrypted or verified here, so only a labelled placeholder crosses over.
func withoutProviderCiphertext(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		redacted := false
		for key, item := range typed {
			if key == "encrypted_content" || key == "encrypted_index" {
				redacted = true
				continue
			}
			out[key] = withoutProviderCiphertext(item)
		}
		if redacted {
			out["beefapi_note"] = "encrypted provider content omitted: it is only readable by the channel that produced it"
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, withoutProviderCiphertext(item))
		}
		return out
	default:
		return value
	}
}

func invalidHistory(detail string) error {
	return &requestError{status: http.StatusUnprocessableEntity, text: "Cursor Agent v1 cannot rebuild history: " + detail}
}

// Key only the effective generation contract. Stream framing and new HTTP IDs
// do not turn an identical tool-result retry into another billable generation.
func historyRequestKey(turn translatedTurn, tokenID any, continuation bool) (string, error) {
	r := turn.managed.Run
	contract := map[string]any{
		"format": string(turn.responseFormat), "model": r.Model, "public_model": turn.publicModel,
		"token_id": tokenID, "system": r.SystemText, "history": r.HistoryMessages,
		"user": r.UserText, "tools": r.Tools, "require_tool": r.RequireToolCall,
		"search": r.AllowHostedSearch, "fetch": r.AllowHostedFetch,
		"require_search": r.RequireHostedSearch, "require_fetch": r.RequireHostedFetch, "require_hosted": r.RequireHostedTool,
		"search_max": r.HostedSearchMaxUses, "fetch_max": r.HostedFetchMaxUses,
	}
	if r.HostedSearchDomains.restricts() {
		contract["search_domains"] = r.HostedSearchDomains
	}
	if r.HostedFetchDomains.restricts() {
		contract["fetch_domains"] = r.HostedFetchDomains
	}
	if len(r.UserMedia) > 0 {
		contract["user_media"] = r.UserMedia
	}
	if normalizeRuntimeProfile(r.RuntimeProfile) == runtimeSand {
		contract["runtime_profile"] = runtimeSand
	}
	if !continuation {
		contract["new_request"] = turn.managed.SessionID
	}
	encoded, err := jsonx.Marshal(contract)
	if err != nil {
		return "", fmt.Errorf("encode full-history request identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "history:" + hex.EncodeToString(digest[:]), nil
}

func visibleHistoryOutput(events []Event) bool {
	for _, event := range events {
		if event.Type == EventText && strings.TrimSpace(event.Text) != "" {
			return true
		}
		if event.Type == EventToolCall && event.ToolCall != nil {
			return true
		}
		if event.Type == EventHostedSearchCompleted && event.HostedSearch != nil {
			return true
		}
	}
	return false
}
