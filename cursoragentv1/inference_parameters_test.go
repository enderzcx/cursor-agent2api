package cursoragentv1

import (
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSandFableParametersSurviveSourceProtocols(t *testing.T) {
	for _, tc := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatClaude, `{"output_config":{"effort":"medium"},"cursor_context":"1m"}`},
		{sdktranslator.FormatOpenAI, `{"reasoning_effort":"medium","cursor_context":"1m"}`},
		{sdktranslator.FormatOpenAIResponse, `{"reasoning":{"effort":"medium"},"cursor_context":"1m"}`},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			turn := translatedTurn{claudeRequest: []byte(`{"messages":[{"role":"user","content":"hi"}]}`), managed: ManagedRequest{Run: RunRequest{Model: "claude-fable-5-1", RuntimeProfile: runtimeSand, ThinkingEnabled: true}}}
			require.NoError(t, prepareInferenceTurn(&turn, cliproxyexecutor.Request{Format: tc.format, Payload: []byte(tc.body)}))
			payload, err := encodeInferenceRequest(turn.managed.Run)
			require.NoError(t, err)
			requested := bytesField(payload, 7)
			require.Equal(t, "claude-fable-5-1", decodeString(requested, 1))
			require.True(t, decodeBool(requested, 2))
			require.False(t, decodeBool(requested, 4))
			params := map[string]string{}
			for _, value := range allBytesFields(requested, 3) {
				params[decodeString(value, 1)] = decodeString(value, 2)
			}
			require.Equal(t, map[string]string{"context": "1m", "effort": "medium", "thinking": "true"}, params)
		})
	}
}

func TestSandExplicitParametersValidationAndDefaults(t *testing.T) {
	for _, run := range []RunRequest{
		{Model: "claude-fable-5-1", InferenceContext: "2m"},
		{Model: "grok-4.6", InferenceContext: "1m"},
		{Model: "claude-fable-5-1", ReasoningEffort: "invalid"},
	} {
		_, err := encodeInferenceRequest(run)
		require.Error(t, err)
	}
	payload, err := encodeInferenceRequest(RunRequest{Model: "claude-fable-5-1"})
	require.NoError(t, err)
	require.True(t, decodeBool(bytesField(payload, 7), 4))
	require.Empty(t, allBytesFields(bytesField(payload, 7), 3))
	payload, err = encodeInferenceRequest(RunRequest{Model: "claude-fable-5-1", InferenceContext: "300k"})
	require.NoError(t, err)
	require.False(t, decodeBool(bytesField(payload, 7), 2))
}

func TestSandHistoryIdentitySeparatesInferenceParameters(t *testing.T) {
	turn := translatedTurn{managed: ManagedRequest{Run: RunRequest{Model: "claude-fable-5-1", RuntimeProfile: runtimeSand}}}
	base, err := historyRequestKey(turn, 1, false)
	require.NoError(t, err)
	for _, field := range []string{"effort", "context", "thinking"} {
		other := turn
		switch field {
		case "effort":
			other.managed.Run.ReasoningEffort = "medium"
		case "context":
			other.managed.Run.InferenceContext = "1m"
		case "thinking":
			other.managed.Run.ThinkingEnabled = true
		}
		key, err := historyRequestKey(other, 1, false)
		require.NoError(t, err)
		require.NotEqual(t, base, key)
	}
}
