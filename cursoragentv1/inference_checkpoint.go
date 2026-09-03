package cursoragentv1

import (
	"context"
	"fmt"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

const inferenceCheckpointVersion = 1

type inferenceHostedToolResult struct {
	CallID  string
	Name    string
	Content any
	IsError bool
}

type inferenceCheckpoint struct {
	Version  int      `json:"version"`
	Profile  string   `json:"profile"`
	Messages [][]byte `json:"messages"`
}

func prepareInferenceRun(ctx context.Context, request RunRequest) (RunRequest, *ToolResult, error) {
	if len(request.InitialCheckpoint) > 0 {
		checkpoint, err := decodeInferenceCheckpoint(request.InitialCheckpoint)
		if err != nil {
			return RunRequest{}, nil, err
		}
		request.HistoryMessages = cloneByteSlices(checkpoint.Messages)
		request.SystemText = ""
	}
	if !request.Resume {
		return request, nil, nil
	}
	if request.ToolResults == nil {
		return RunRequest{}, nil, fmt.Errorf("Cursor Sand tool continuation has no result channel")
	}
	var result ToolResult
	select {
	case <-ctx.Done():
		return RunRequest{}, nil, ctx.Err()
	case received, ok := <-request.ToolResults:
		if !ok {
			return RunRequest{}, nil, fmt.Errorf("Cursor Sand tool continuation closed before its result")
		}
		result = received
	}
	name := inferenceCheckpointToolName(request.HistoryMessages, result.ToolCallID)
	if name == "" {
		return RunRequest{}, nil, fmt.Errorf("Cursor Sand tool continuation does not match checkpoint")
	}
	value := any(result.Content)
	if result.IsError {
		value = map[string]any{"is_error": true, "content": result.Content}
	}
	toolPart := map[string]any{"type": "tool-result", "toolCallId": result.ToolCallID, "toolName": name, "result": value}
	if len(result.Media) > 0 {
		experimental := []ContentPart{}
		if result.Content != "" {
			experimental = append(experimental, ContentPart{Text: result.Content})
		}
		for index := range result.Media {
			experimental = append(experimental, ContentPart{Media: &result.Media[index]})
		}
		toolPart["experimental_content"] = historyContentParts(experimental)
	}
	message := map[string]any{"role": "tool", "content": []any{toolPart}}
	encoded, err := jsonx.Marshal(message)
	if err != nil {
		return RunRequest{}, nil, err
	}
	request.HistoryMessages = append(request.HistoryMessages, encoded)
	request.UserText = historyContinuationText
	request.RequireToolCall = false
	request.Resume = false
	return request, &result, nil
}

type inferenceReasoning struct {
	Text      string
	Signature string
}

func encodeInferenceCheckpoint(request RunRequest, assistantText string, reasoning inferenceReasoning, tools []inferenceToolCall, hostedResults ...inferenceHostedToolResult) ([]byte, error) {
	messages := cloneByteSlices(request.HistoryMessages)
	if strings.TrimSpace(request.SystemText) != "" {
		system, err := jsonx.Marshal(map[string]any{"role": "system", "content": []any{map[string]any{"type": "text", "text": request.SystemText}}})
		if err != nil {
			return nil, err
		}
		messages = append([][]byte{system}, messages...)
	}
	if strings.TrimSpace(request.UserText) != "" || len(request.UserMedia) > 0 {
		userParts := []any{}
		if strings.TrimSpace(request.UserText) != "" {
			userParts = append(userParts, map[string]any{"type": "text", "text": request.UserText})
		}
		for _, media := range request.UserMedia {
			userParts = append(userParts, historyMediaPart(media))
		}
		user, err := jsonx.Marshal(map[string]any{"role": "user", "content": userParts})
		if err != nil {
			return nil, err
		}
		messages = append(messages, user)
	}
	parts := make([]any, 0, len(tools)+2)
	if strings.TrimSpace(reasoning.Text) != "" {
		// Same-provider continuation: Cursor produced this reasoning and its
		// signature, so the next Sand segment may replay it natively.
		part := map[string]any{"type": "reasoning", "text": reasoning.Text}
		if reasoning.Signature != "" {
			part["signature"] = reasoning.Signature
		}
		parts = append(parts, part)
	}
	if strings.TrimSpace(assistantText) != "" {
		parts = append(parts, map[string]any{"type": "text", "text": assistantText})
	}
	for _, tool := range tools {
		var args map[string]any
		if jsonx.Unmarshal([]byte(tool.Arguments), &args) != nil {
			return nil, fmt.Errorf("Cursor Sand returned invalid tool arguments")
		}
		parts = append(parts, map[string]any{"type": "tool-call", "toolCallId": tool.ID, "toolName": tool.Name, "args": args})
	}
	if len(parts) > 0 {
		assistant, err := jsonx.Marshal(map[string]any{"role": "assistant", "content": parts})
		if err != nil {
			return nil, err
		}
		messages = append(messages, assistant)
	}
	for _, result := range hostedResults {
		value := result.Content
		if result.IsError {
			value = map[string]any{"is_error": true, "content": value}
		}
		toolMessage, err := jsonx.Marshal(map[string]any{"role": "tool", "content": []any{map[string]any{
			"type": "tool-result", "toolCallId": result.CallID, "toolName": result.Name, "result": value,
		}}})
		if err != nil {
			return nil, err
		}
		messages = append(messages, toolMessage)
	}
	return jsonx.Marshal(inferenceCheckpoint{Version: inferenceCheckpointVersion, Profile: runtimeSand, Messages: messages})
}

func decodeInferenceCheckpoint(raw []byte) (*inferenceCheckpoint, error) {
	var checkpoint inferenceCheckpoint
	if jsonx.Unmarshal(raw, &checkpoint) != nil || checkpoint.Version != inferenceCheckpointVersion || checkpoint.Profile != runtimeSand {
		return nil, fmt.Errorf("Cursor Sand checkpoint is invalid")
	}
	return &checkpoint, nil
}

func inferenceCheckpointToolName(messages [][]byte, toolCallID string) string {
	toolCallID = strings.TrimSpace(toolCallID)
	for index := len(messages) - 1; index >= 0; index-- {
		var message map[string]any
		if jsonx.Unmarshal(messages[index], &message) != nil || strings.TrimSpace(fmt.Sprint(message["role"])) != "assistant" {
			continue
		}
		parts, _ := message["content"].([]any)
		for _, raw := range parts {
			part, _ := raw.(map[string]any)
			if strings.TrimSpace(fmt.Sprint(part["type"])) == "tool-call" && strings.TrimSpace(fmt.Sprint(part["toolCallId"])) == toolCallID {
				return strings.TrimSpace(fmt.Sprint(part["toolName"]))
			}
		}
	}
	return ""
}
