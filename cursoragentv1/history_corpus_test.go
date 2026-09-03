package cursoragentv1

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// The captured-history conformance corpus lives in testdata/history-corpus.
// Each case is a real client request shape (Claude Code, Kiro-origin, Cursor
// origin, Anthropic server tools) plus the expected structured history and the
// fields that must be present on the Sand wire / Agent v1 blobs. New client
// block types must be added here before they are accepted by the translator.
type historyCorpusCase struct {
	Direction       string         `json:"direction"`
	SourceRequestID string         `json:"source_request_id"`
	Request         map[string]any `json:"request"`
	Expect          struct {
		ErrorStatus        int      `json:"error_status"`
		ErrorContains      string   `json:"error_contains"`
		HistoryLen         *int     `json:"history_len"`
		HistoryContains    []string `json:"history_contains"`
		HistoryNotContains []string `json:"history_not_contains"`
		UserMedia          *int     `json:"user_media"`
		UserText           *string  `json:"user_text"`
		ActiveResults      *int     `json:"active_results"`
		ActiveResultMedia  *int     `json:"active_result_media"`
		ThinkingEnabled    *bool    `json:"thinking_enabled"`
		Sand               *struct {
			ToolResultMediaParts         *int     `json:"tool_result_media_parts"`
			UserMediaParts               *int     `json:"user_media_parts"`
			HistoryImageParts            *int     `json:"history_image_parts"`
			HistoryFileParts             *int     `json:"history_file_parts"`
			ReasoningParts               *int     `json:"reasoning_parts"`
			ReasoningSignatures          *int     `json:"reasoning_signatures"`
			ReasoningSignaturesAfterDrop *int     `json:"reasoning_signatures_after_retry"`
			RedactedReasoningParts       *int     `json:"redacted_reasoning_parts"`
			ToolCalls                    *int     `json:"tool_calls"`
			ToolResults                  *int     `json:"tool_results"`
			ModelParameters              []string `json:"model_parameters"`
			MintedSignatures             []string `json:"minted_signatures"`
		} `json:"sand"`
		AgentV1 *struct {
			LiftedUserMediaMessages *int `json:"lifted_user_media_messages"`
		} `json:"agent_v1"`
	} `json:"expect"`
}

type sandWireSummary struct {
	toolResultMediaParts   int
	userMediaParts         int
	historyImageParts      int
	historyFileParts       int
	reasoningParts         int
	reasoningSignatures    int
	redactedReasoningParts int
	toolCalls              int
	toolResults            int
	modelParameters        []string
}

func TestHistoryConformanceCorpus(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "history-corpus", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		file := file
		t.Run(strings.TrimSuffix(filepath.Base(file), ".json"), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			require.NoError(t, err)
			var testCase historyCorpusCase
			require.NoError(t, jsonx.Unmarshal(raw, &testCase))
			payload, err := jsonx.Marshal(testCase.Request)
			require.NoError(t, err)
			request := claudeExecutorRequest("corpus-"+filepath.Base(file), string(payload))
			request.Model = stringValue(testCase.Request["model"])
			turn, err := translateExecutionRequest(request, claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
			expect := testCase.Expect
			if expect.ErrorStatus != 0 {
				require.Error(t, err)
				var scoped *requestError
				require.True(t, errors.As(err, &scoped), "error must be request scoped: %v", err)
				require.Equal(t, expect.ErrorStatus, scoped.status)
				require.Contains(t, err.Error(), expect.ErrorContains)
				return
			}
			require.NoError(t, err)
			run := turn.managed.Run
			history := string(bytes.Join(run.HistoryMessages, nil))
			if expect.HistoryLen != nil {
				require.Len(t, run.HistoryMessages, *expect.HistoryLen, history)
			}
			for _, want := range expect.HistoryContains {
				require.Contains(t, history, want)
			}
			for _, unwanted := range expect.HistoryNotContains {
				require.NotContains(t, history, unwanted)
			}
			if expect.UserMedia != nil {
				require.Len(t, run.UserMedia, *expect.UserMedia)
			}
			if expect.UserText != nil {
				require.Equal(t, *expect.UserText, run.UserText)
			}
			if expect.ActiveResults != nil {
				require.Len(t, turn.managed.ActiveResults, *expect.ActiveResults)
			}
			if expect.ActiveResultMedia != nil {
				media := 0
				for _, result := range turn.managed.ActiveResults {
					media += len(result.Media)
				}
				require.Equal(t, *expect.ActiveResultMedia, media)
			}
			if expect.ThinkingEnabled != nil {
				require.Equal(t, *expect.ThinkingEnabled, run.ThinkingEnabled)
			}
			if expect.Sand != nil {
				sandRun := run
				sandRun.RuntimeProfile = runtimeSand
				sandRun.ConversationID = "corpus"
				for _, signature := range expect.Sand.MintedSignatures {
					rememberMintedSignature(signature)
				}
				encoded, err := encodeInferenceRequest(sandRun)
				require.NoError(t, err)
				summary := summarizeSandWire(t, encoded)
				want := expect.Sand
				if want.ToolResultMediaParts != nil {
					require.Equal(t, *want.ToolResultMediaParts, summary.toolResultMediaParts, "tool result media parts")
				}
				if want.UserMediaParts != nil {
					require.Equal(t, *want.UserMediaParts, summary.userMediaParts, "user media parts")
				}
				if want.HistoryImageParts != nil {
					require.Equal(t, *want.HistoryImageParts, summary.historyImageParts, "history image parts")
				}
				if want.HistoryFileParts != nil {
					require.Equal(t, *want.HistoryFileParts, summary.historyFileParts, "history file parts")
				}
				if want.ReasoningParts != nil {
					require.Equal(t, *want.ReasoningParts, summary.reasoningParts, "reasoning parts")
				}
				if want.ReasoningSignatures != nil {
					require.Equal(t, *want.ReasoningSignatures, summary.reasoningSignatures, "reasoning signatures")
				}
				if want.RedactedReasoningParts != nil {
					require.Equal(t, *want.RedactedReasoningParts, summary.redactedReasoningParts, "redacted reasoning parts")
				}
				if want.ToolCalls != nil {
					require.Equal(t, *want.ToolCalls, summary.toolCalls, "tool calls")
				}
				if want.ToolResults != nil {
					require.Equal(t, *want.ToolResults, summary.toolResults, "tool results")
				}
				for _, parameter := range want.ModelParameters {
					require.Contains(t, summary.modelParameters, parameter)
				}
				if want.ReasoningSignaturesAfterDrop != nil {
					dropped, err := encodeInferenceRequestWithPolicy(sandRun, true)
					require.NoError(t, err)
					require.Equal(t, *want.ReasoningSignaturesAfterDrop, summarizeSandWire(t, dropped).reasoningSignatures, "signatures after retry")
				}
			}
			if expect.AgentV1 != nil {
				lifted, err := agentV1LiftToolResultMedia(run.HistoryMessages)
				require.NoError(t, err)
				count := 0
				for _, raw := range lifted {
					var message map[string]any
					require.NoError(t, jsonx.Unmarshal(raw, &message))
					if stringValue(message["role"]) != "user" {
						continue
					}
					parts, _ := message["content"].([]any)
					for _, rawPart := range parts {
						part, _ := rawPart.(map[string]any)
						if _, ok := mediaPartFromHistory(part); ok {
							count++
							break
						}
					}
				}
				if expect.AgentV1.LiftedUserMediaMessages != nil {
					require.Equal(t, *expect.AgentV1.LiftedUserMediaMessages, count)
					require.NotContains(t, string(bytes.Join(lifted, nil)), `"experimental_content"`)
				}
				payload, err := encodeRunRequest(&runWireRequest{Model: "m", ConversationID: "c", UserText: "u", HistoryMessages: run.HistoryMessages, UserMedia: run.UserMedia, BlobStore: map[string][]byte{}})
				require.NoError(t, err)
				require.NotEmpty(t, payload)
			}
			_ = sdktranslator.FormatClaude
			_ = cliproxyexecutor.Request{}
		})
	}
}

// summarizeSandWire decodes an InferenceStreamRequest using the field layout
// recovered from Cursor 3.18.9 and counts the parts relevant to the corpus.
func summarizeSandWire(t *testing.T, payload []byte) sandWireSummary {
	t.Helper()
	summary := sandWireSummary{}
	messages := protoRepeated(t, payload, 1)
	for index, message := range messages {
		lastUser := index == len(messages)-1 && protoVarint(message, 1) == 1
		for _, contentParts := range protoRepeated(t, message, 3) {
			for _, part := range protoRepeated(t, contentParts, 1) {
				if len(protoRepeated(t, part, 2)) > 0 {
					if lastUser {
						summary.userMediaParts++
					} else {
						summary.historyImageParts++
					}
				}
				if len(protoRepeated(t, part, 3)) > 0 {
					summary.historyFileParts++
				}
			}
		}
		summary.toolCalls += len(protoRepeated(t, message, 4))
		for _, toolContent := range protoRepeated(t, message, 6) {
			for _, result := range protoRepeated(t, toolContent, 1) {
				summary.toolResults++
				for _, experimental := range protoRepeated(t, result, 5) {
					if len(protoRepeated(t, experimental, 2)) > 0 || len(protoRepeated(t, experimental, 3)) > 0 {
						summary.toolResultMediaParts++
					}
				}
			}
		}
		for _, reasoning := range protoRepeated(t, message, 7) {
			if protoVarint(reasoning, 1) == 1 {
				summary.redactedReasoningParts++
				continue
			}
			summary.reasoningParts++
			if decodeString(reasoning, 3) != "" {
				summary.reasoningSignatures++
			}
		}
	}
	for _, requested := range protoRepeated(t, payload, 7) {
		for _, parameter := range protoRepeated(t, requested, 3) {
			summary.modelParameters = append(summary.modelParameters, decodeString(parameter, 1)+"="+decodeString(parameter, 2))
		}
	}
	return summary
}

func protoRepeated(t *testing.T, data []byte, target protowire.Number) [][]byte {
	t.Helper()
	var out [][]byte
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		require.GreaterOrEqual(t, n, 0)
		data = data[n:]
		if wireType == protowire.BytesType {
			value, length := protowire.ConsumeBytes(data)
			require.GreaterOrEqual(t, length, 0)
			if number == target {
				out = append(out, value)
			}
			data = data[length:]
			continue
		}
		length := protowire.ConsumeFieldValue(number, wireType, data)
		require.GreaterOrEqual(t, length, 0)
		data = data[length:]
	}
	return out
}

func protoVarint(data []byte, target protowire.Number) uint64 {
	return decodeVarint(data, target)
}
