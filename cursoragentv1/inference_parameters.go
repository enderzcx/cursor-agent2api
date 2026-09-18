package cursoragentv1

import (
	"net/http"
	"strings"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

// Read before protocol translation, which may discard provider-specific options.
func originalReasoningEffort(req cliproxyexecutor.Request) string {
	path := "output_config.effort"
	switch req.Format {
	case sdktranslator.FormatOpenAI:
		if value := strings.TrimSpace(gjson.GetBytes(req.Payload, "reasoning_effort").String()); value != "" {
			return strings.ToLower(value)
		}
		path = "reasoning.effort"
	case sdktranslator.FormatOpenAIResponse:
		path = "reasoning.effort"
	}
	return strings.ToLower(strings.TrimSpace(gjson.GetBytes(req.Payload, path).String()))
}

func originalInferenceContext(req cliproxyexecutor.Request) string {
	return strings.ToLower(strings.TrimSpace(gjson.GetBytes(req.Payload, "cursor_context").String()))
}

func setInferenceParameter(selection *inferenceModelSpec, key, value string) {
	for i := range selection.Parameters {
		if selection.Parameters[i].ID == key {
			selection.Parameters[i].Value = value
			return
		}
	}
	selection.Parameters = append(selection.Parameters, inferenceModelParameter{ID: key, Value: value})
}

// Cursor catalog observed 2026-09-19; all Claude entries in DefaultModels.
var claudeInferenceParameters = map[string]struct{ contexts, efforts string }{
	"claude-fable-5-1":  {"300k 1m", "low medium high xhigh max"},
	"claude-fable-5":    {"300k 1m", "low medium high xhigh max"},
	"claude-opus-5":     {"300k 1m", "low medium high xhigh max"},
	"claude-opus-4-8":   {"300k 1m", "low medium high xhigh max"},
	"claude-opus-4-7":   {"300k 1m", "low medium high xhigh max"},
	"claude-opus-4-6":   {"200k 1m", "low medium high max"},
	"claude-sonnet-5":   {"300k 1m", "low medium high xhigh max"},
	"claude-sonnet-4-6": {"200k 1m", "low medium high max"},
}

func containsInferenceValue(values, value string) bool {
	for _, candidate := range strings.Fields(values) {
		if candidate == value {
			return true
		}
	}
	return false
}

func originalInferenceThinking(req cliproxyexecutor.Request) *bool {
	switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(req.Payload, "thinking.type").String())) {
	case "enabled", "adaptive":
		value := true
		return &value
	case "disabled":
		value := false
		return &value
	}
	return nil
}

func applyInferenceParameters(selection *inferenceModelSpec, request RunRequest) error {
	catalog, knownClaude := claudeInferenceParameters[selection.ID]
	// Claude Code's Fable contract is thinking-always-on. Do not silently
	// downgrade it to Cursor's optional no-thinking variant when omitted.
	if selection.ID == "claude-fable-5-1" || selection.ID == "claude-fable-5" {
		if request.InferenceThinking != nil && !*request.InferenceThinking {
			return &requestError{status: http.StatusBadRequest, text: "Fable thinking cannot be disabled; use effort to control reasoning depth"}
		}
		setInferenceParameter(selection, "thinking", "true")
	}
	if request.InferenceThinking != nil && knownClaude {
		value := "false"
		if *request.InferenceThinking {
			value = "true"
		}
		setInferenceParameter(selection, "thinking", value)
	}
	if request.InferenceContext != "" {
		if !knownClaude || !containsInferenceValue(catalog.contexts, request.InferenceContext) {
			return &requestError{status: http.StatusBadRequest, text: "unsupported cursor_context for this Claude model"}
		}
		selection.BuiltIn = false
		selection.MaxMode = request.InferenceContext == "1m"
		setInferenceParameter(selection, "context", request.InferenceContext)
	}
	effort := strings.ToLower(strings.TrimSpace(request.ReasoningEffort))
	if effort == "" {
		return nil
	}
	if knownClaude && !containsInferenceValue(catalog.efforts, effort) {
		return &requestError{status: http.StatusBadRequest, text: "unsupported reasoning effort for this Claude model"}
	}
	// Preserve unrelated models rather than sending unverified provider parameters.
	if !strings.HasPrefix(selection.ID, "claude-") && !strings.HasPrefix(selection.ID, "grok-") {
		return &requestError{status: http.StatusBadRequest, text: "explicit Sand effort is currently supported for Claude and Grok"}
	}
	if effort != "low" && effort != "medium" && effort != "high" && effort != "xhigh" && effort != "max" {
		return &requestError{status: http.StatusBadRequest, text: "unsupported Cursor Sand reasoning effort"}
	}
	if strings.HasPrefix(selection.ID, "grok-") && effort == "max" {
		// Public compatibility alias: Cursor Grok calls its highest tier xhigh.
		effort = "xhigh"
	}
	setInferenceParameter(selection, "effort", effort)
	return nil
}
