package cursoragentv1

import (
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestAllRegisteredClaudeParameterCombinations(t *testing.T) {
	count := 0
	for _, model := range DefaultModels {
		if !strings.HasPrefix(model, "claude-") {
			continue
		}
		catalog, ok := claudeInferenceParameters[model]
		require.True(t, ok, "missing catalog for %s", model)
		count++
		for _, context := range strings.Fields(catalog.contexts) {
			for _, effort := range strings.Fields(catalog.efforts) {
				for _, thinking := range []bool{false, true} {
					t.Run(model+"/"+context+"/"+effort+"/"+map[bool]string{true: "on", false: "off"}[thinking], func(t *testing.T) {
						payload, err := encodeInferenceRequest(RunRequest{Model: model, InferenceContext: context, ReasoningEffort: effort, InferenceThinking: &thinking})
						if strings.HasPrefix(model, "claude-fable-") && !thinking {
							require.Error(t, err)
							return
						}
						require.NoError(t, err)
						requested := bytesField(payload, 7)
						require.Equal(t, model, decodeString(requested, 1))
						require.Equal(t, context == "1m", decodeBool(requested, 2))
						require.False(t, decodeBool(requested, 4))
						params := map[string]string{}
						for _, value := range allBytesFields(requested, 3) {
							params[decodeString(value, 1)] = decodeString(value, 2)
						}
						require.Equal(t, context, params["context"])
						require.Equal(t, effort, params["effort"])
						require.Equal(t, map[bool]string{true: "true", false: "false"}[thinking], params["thinking"])
					})
				}
			}
		}
	}
	require.Equal(t, 8, count)
}

func TestClaudeCatalogRejectsUnsupportedTiers(t *testing.T) {
	for _, run := range []RunRequest{
		{Model: "claude-opus-4-6", InferenceContext: "300k"},
		{Model: "claude-sonnet-4-6", ReasoningEffort: "xhigh"},
		{Model: "claude-fable-5", InferenceContext: "200k"},
	} {
		_, err := encodeInferenceRequest(run)
		require.Error(t, err)
	}
	for _, kind := range []string{"enabled", "adaptive", "disabled", "Enabled", "Adaptive", "Disabled", " DISABLED "} {
		v := originalInferenceThinking(cliproxyexecutor.Request{Payload: []byte(`{"thinking":{"type":"` + kind + `"}}`)})
		require.NotNil(t, v)
		require.Equal(t, strings.ToLower(strings.TrimSpace(kind)) != "disabled", *v)
	}
	require.Nil(t, originalInferenceThinking(cliproxyexecutor.Request{Payload: []byte(`{}`)}))
}

func TestSandGrokMaxCompatibilityAlias(t *testing.T) {
	alias, err := encodeInferenceRequest(RunRequest{Model: "grok-4.6", ReasoningEffort: "max"})
	require.NoError(t, err)
	explicit, err := encodeInferenceRequest(RunRequest{Model: "grok-4.6", ReasoningEffort: "xhigh"})
	require.NoError(t, err)
	require.Equal(t, bytesField(explicit, 7), bytesField(alias, 7))
}

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
	require.Equal(t, "thinking", decodeString(bytesField(bytesField(payload, 7), 3), 1))
	require.Equal(t, "true", decodeString(bytesField(bytesField(payload, 7), 3), 2))
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

func TestSandHistoryIdentityDistinguishesExplicitThinking(t *testing.T) {
	turn := translatedTurn{managed: ManagedRequest{Run: RunRequest{Model: "claude-sonnet-5", RuntimeProfile: runtimeSand}}}
	omitted, err := historyRequestKey(turn, 1, false)
	require.NoError(t, err)
	keys := []string{omitted}
	for _, value := range []bool{false, true} {
		turn.managed.Run.InferenceThinking = &value
		key, err := historyRequestKey(turn, 1, false)
		require.NoError(t, err)
		for _, prior := range keys {
			require.NotEqual(t, prior, key)
		}
		keys = append(keys, key)
	}
}
