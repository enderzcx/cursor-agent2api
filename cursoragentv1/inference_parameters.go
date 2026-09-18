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

func applyInferenceParameters(selection *inferenceModelSpec, request RunRequest) error {
	// Only this catalog contract has been verified for explicit context selection.
	if request.InferenceContext != "" {
		if selection.ID != "claude-fable-5-1" || (request.InferenceContext != "1m" && request.InferenceContext != "300k") {
			return &requestError{status: http.StatusBadRequest, text: "cursor_context requires claude-fable-5-1 and 1m or 300k"}
		}
		selection.BuiltIn = false
		selection.MaxMode = request.InferenceContext == "1m"
		setInferenceParameter(selection, "context", request.InferenceContext)
	}
	effort := strings.ToLower(strings.TrimSpace(request.ReasoningEffort))
	if effort == "" {
		return nil
	}
	// Preserve unrelated models rather than sending unverified provider parameters.
	if !strings.HasPrefix(selection.ID, "claude-") && !strings.HasPrefix(selection.ID, "grok-") {
		return &requestError{status: http.StatusBadRequest, text: "explicit Sand effort is currently supported for Claude and Grok"}
	}
	if effort != "low" && effort != "medium" && effort != "high" && effort != "xhigh" && effort != "max" {
		return &requestError{status: http.StatusBadRequest, text: "unsupported Cursor Sand reasoning effort"}
	}
	if strings.HasPrefix(selection.ID, "grok-") && effort == "max" {
		effort = "xhigh"
	}
	setInferenceParameter(selection, "effort", effort)
	return nil
}
