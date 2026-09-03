package cursoragentv1

import (
	"bytes"
	"context"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// Opt-in live probes for media and reasoning replay on both type64 data
// planes. Each is bounded to the request count in its gate variable and uses
// the dedicated channel credential supplied in-process only.
//
//	CURSOR_AGENT_V1_MEDIA_PROBE=agent_v1|sand   selects the data plane
//	CURSOR_AGENT_V1_MEDIA_MODEL=<model>          defaults to claude-sonnet-4-6

func mediaProbeProfile(t *testing.T) string {
	profile := strings.TrimSpace(os.Getenv("CURSOR_AGENT_V1_MEDIA_PROBE"))
	if profile != runtimeSand && profile != "agent_v1" {
		t.Skip("explicit bounded media probe authorization required")
	}
	return profile
}

func mediaProbeModel() string {
	if model := strings.TrimSpace(os.Getenv("CURSOR_AGENT_V1_MEDIA_MODEL")); model != "" {
		return model
	}
	return "claude-sonnet-4-6"
}

// solidPNG renders a 96x96 image of one color so the model's answer is
// unambiguous evidence that pixels, not a placeholder, reached it.
func solidPNG(t *testing.T, c color.RGBA) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 96, 96))
	for y := 0; y < 96; y++ {
		for x := 0; x < 96; x++ {
			img.SetRGBA(x, y, c)
		}
	}
	var buffer bytes.Buffer
	if err := png.Encode(&buffer, img); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes())
}

func mediaProbeExecutor(t *testing.T, profile string) (*Executor, *cliproxyauth.Auth, map[string]any) {
	t.Helper()
	metadata := liveProbeMetadata(t)
	if profile == runtimeSand {
		metadata["runtime_profile"] = runtimeSand
	}
	executor, err := NewExecutor(t.TempDir())
	if err != nil {
		t.Fatal("could not initialize isolated executor")
	}
	auth := &cliproxyauth.Auth{ID: "dedicated-media-probe-" + profile, Provider: providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	return executor, auth, metadata
}

func mediaProbeCall(t *testing.T, executor *Executor, auth *cliproxyauth.Auth, metadata map[string]any, session string, body map[string]any) (cliproxyexecutor.Response, error) {
	t.Helper()
	payload, err := jsonx.Marshal(body)
	if err != nil {
		t.Fatal("could not encode probe body")
	}
	request := claudeExecutorRequest(session, string(payload))
	request.Model = mediaProbeModel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	response, callErr := executor.Execute(ctx, auth, request, claudeExecutorOptions(false))
	if callErr != nil {
		return response, callErr
	}
	return response, nil
}

func imageBlock(data string) map[string]any {
	return map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": data}}
}

func TestAuthorizedUserImageProbe(t *testing.T) {
	profile := mediaProbeProfile(t)
	executor, auth, metadata := mediaProbeExecutor(t, profile)
	response, err := mediaProbeCall(t, executor, auth, metadata, "media-user-image-"+profile, map[string]any{
		"model": mediaProbeModel(),
		"messages": []any{map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "text", "text": "The attached image is a single solid color. Reply with exactly one word naming that color in lowercase English, nothing else."},
			imageBlock(solidPNG(t, color.RGBA{R: 0, G: 0, B: 255, A: 255})),
		}}},
	})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	text := strings.ToLower(string(response.Payload))
	t.Logf("profile=%s user image answer: %s", profile, gjsonText(response.Payload))
	if !strings.Contains(text, "blue") {
		t.Fatalf("model did not see the blue image: %s", gjsonText(response.Payload))
	}
	if response.Headers.Get(HeaderUsageReceipt) == "" {
		t.Fatal("user image probe completed without usage receipt")
	}
}

func TestAuthorizedHistoryToolResultImageProbe(t *testing.T) {
	profile := mediaProbeProfile(t)
	executor, auth, metadata := mediaProbeExecutor(t, profile)
	tools := []any{map[string]any{"name": "take_screenshot", "description": "Capture the current page", "input_schema": map[string]any{"type": "object", "properties": map[string]any{}}}}
	response, err := mediaProbeCall(t, executor, auth, metadata, "media-history-tool-image-"+profile, map[string]any{
		"model": mediaProbeModel(), "tools": tools,
		"messages": []any{
			map[string]any{"role": "user", "content": "Take a screenshot of the page."},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "text", "text": "Capturing."},
				map[string]any{"type": "tool_use", "id": "toolu_shot_1", "name": "take_screenshot", "input": map[string]any{}},
			}},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "toolu_shot_1", "content": []any{
					map[string]any{"type": "text", "text": "Screenshot captured."},
					imageBlock(solidPNG(t, color.RGBA{R: 255, G: 0, B: 0, A: 255})),
				}},
				map[string]any{"type": "text", "text": "The screenshot you received is one solid color. Reply with exactly one lowercase English word naming that color, nothing else."},
			}},
		},
	})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	text := strings.ToLower(string(response.Payload))
	t.Logf("profile=%s tool-result image answer: %s", profile, gjsonText(response.Payload))
	if !strings.Contains(text, "red") {
		t.Fatalf("model did not see the red screenshot in tool_result history: %s", gjsonText(response.Payload))
	}
}

func TestAuthorizedHistoryUserImageProbe(t *testing.T) {
	profile := mediaProbeProfile(t)
	executor, auth, metadata := mediaProbeExecutor(t, profile)
	response, err := mediaProbeCall(t, executor, auth, metadata, "media-history-user-image-"+profile, map[string]any{
		"model": mediaProbeModel(),
		"messages": []any{
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "text", "text": "Remember this image."},
				imageBlock(solidPNG(t, color.RGBA{R: 0, G: 160, B: 0, A: 255})),
			}},
			map[string]any{"role": "assistant", "content": "Noted."},
			map[string]any{"role": "user", "content": "The image I sent earlier was one solid color. Reply with exactly one lowercase English word naming that color, nothing else."},
		},
	})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	text := strings.ToLower(string(response.Payload))
	t.Logf("profile=%s history user image answer: %s", profile, gjsonText(response.Payload))
	if !strings.Contains(text, "green") {
		t.Fatalf("model did not see the green image in user history: %s", gjsonText(response.Payload))
	}
}

// TestAuthorizedSandReasoningReplayProbe is the A/B that fixes the Sand
// reasoning policy: (1) a thinking-enabled turn to obtain a real Cursor
// signature, (2) replay with that signature, (3) replay with a foreign
// signature, (4) replay text-only. Set CURSOR_AGENT_V1_REASONING_PROBE=4-requests.
func TestAuthorizedSandReasoningReplayProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_REASONING_PROBE") != "4-requests" {
		t.Skip("explicit bounded reasoning probe authorization required")
	}
	executor, auth, metadata := mediaProbeExecutor(t, runtimeSand)
	first, err := mediaProbeCall(t, executor, auth, metadata, "reasoning-probe-1", map[string]any{
		"model":    mediaProbeModel(),
		"thinking": map[string]any{"type": "enabled", "budget_tokens": 1024},
		"messages": []any{map[string]any{"role": "user", "content": "Think briefly, then reply exactly REASONING_PROBE_ONE."}},
	})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	var assistant map[string]any
	if jsonx.Unmarshal(first.Payload, &assistant) != nil {
		t.Fatal("could not decode first reasoning response")
	}
	blocks, _ := assistant["content"].([]any)
	signature, thinking := "", ""
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		if block["type"] == "thinking" {
			signature, _ = block["signature"].(string)
			thinking, _ = block["thinking"].(string)
		}
	}
	t.Logf("first turn returned thinking=%d chars signature=%d chars", len(thinking), len(signature))
	replay := func(session string, blocksForHistory []any, forward bool) (string, error) {
		previous := sandReasoningSignatureForwarding
		sandReasoningSignatureForwarding = forward
		defer func() { sandReasoningSignatureForwarding = previous }()
		response, callErr := mediaProbeCall(t, executor, auth, metadata, session, map[string]any{
			"model":    mediaProbeModel(),
			"thinking": map[string]any{"type": "enabled", "budget_tokens": 1024},
			"messages": []any{
				map[string]any{"role": "user", "content": "Think briefly, then reply exactly REASONING_PROBE_ONE."},
				map[string]any{"role": "assistant", "content": blocksForHistory},
				map[string]any{"role": "user", "content": "Now reply exactly REASONING_PROBE_TWO."},
			},
		})
		if callErr != nil {
			return "", callErr
		}
		return gjsonText(response.Payload), nil
	}
	if signature != "" {
		answer, replayErr := replay("reasoning-probe-2-own", blocks, true)
		t.Logf("A own-signature forwarded: err=%v answer=%q", redactErr(replayErr, metadata), answer)
	} else {
		t.Log("A skipped: Sand returned no signature")
	}
	foreign := []any{map[string]any{"type": "thinking", "thinking": "I should answer with the second marker.", "signature": "ErUBCkYIBxgCKkD" + strings.Repeat("A", 80)}, map[string]any{"type": "text", "text": "REASONING_PROBE_ONE"}}
	answer, replayErr := replay("reasoning-probe-3-foreign", foreign, true)
	t.Logf("B foreign-signature forwarded (expects one text-only retry): err=%v answer=%q", redactErr(replayErr, metadata), answer)
	if replayErr != nil {
		t.Fatal("foreign-signature replay must recover via the text-only retry: " + redactProbeError(replayErr, metadata))
	}
	answer, replayErr = replay("reasoning-probe-4-textonly", foreign, false)
	t.Logf("C text-only reasoning: err=%v answer=%q", redactErr(replayErr, metadata), answer)
	if replayErr != nil {
		t.Fatal("text-only reasoning replay must succeed: " + redactProbeError(replayErr, metadata))
	}
}

func redactErr(err error, metadata map[string]any) string {
	if err == nil {
		return "<nil>"
	}
	return redactProbeError(err, metadata)
}

func gjsonText(payload []byte) string {
	var message map[string]any
	if jsonx.Unmarshal(payload, &message) != nil {
		return string(payload)
	}
	blocks, _ := message["content"].([]any)
	parts := []string{}
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		if block["type"] == "text" {
			parts = append(parts, stringValue(block["text"]))
		}
	}
	return strings.Join(parts, " ")
}

// TestAuthorizedTextBaselineProbe separates "model/route unavailable" from
// "media broke the request" by sending the same model a text-only turn.
func TestAuthorizedTextBaselineProbe(t *testing.T) {
	profile := mediaProbeProfile(t)
	executor, auth, metadata := mediaProbeExecutor(t, profile)
	response, err := mediaProbeCall(t, executor, auth, metadata, "media-text-baseline-"+profile, map[string]any{
		"model":    mediaProbeModel(),
		"messages": []any{map[string]any{"role": "user", "content": "Reply exactly BASELINE_OK."}},
	})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	t.Logf("profile=%s baseline answer: %s", profile, gjsonText(response.Payload))
}

// TestAuthorizedCorpusReplayProbe replays the corpus cases captured from the
// two production Request IDs (chrome-devtools screenshot history, VIP->Max
// thinking history) through the real Sand executor. Set
// CURSOR_AGENT_V1_CORPUS_REPLAY=2-requests.
func TestAuthorizedCorpusReplayProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_CORPUS_REPLAY") != "2-requests" {
		t.Skip("explicit bounded corpus replay authorization required")
	}
	executor, auth, metadata := mediaProbeExecutor(t, runtimeSand)
	for _, name := range []string{"claude-to-cursor-chrome-devtools-screenshot", "claude-to-cursor-vip-to-max-thinking"} {
		raw, err := os.ReadFile("testdata/history-corpus/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		var testCase historyCorpusCase
		if jsonx.Unmarshal(raw, &testCase) != nil {
			t.Fatal("corpus case is invalid")
		}
		request := testCase.Request
		request["model"] = mediaProbeModel()
		// Make the model's view of the media observable: the corpus screenshot is
		// a 1x1 PNG, so swap in a solid color and ask for it.
		if name == "claude-to-cursor-chrome-devtools-screenshot" {
			messages := request["messages"].([]any)
			toolTurn := messages[2].(map[string]any)["content"].([]any)[0].(map[string]any)
			toolTurn["content"].([]any)[1] = imageBlock(solidPNG(t, color.RGBA{R: 255, G: 0, B: 0, A: 255}))
			messages[len(messages)-1] = map[string]any{"role": "user", "content": "The screenshot returned by the tool earlier was one solid color. Reply with exactly one lowercase English word naming that color, nothing else."}
		} else {
			messages := request["messages"].([]any)
			messages[len(messages)-1] = map[string]any{"role": "user", "content": "Reply exactly HISTORY_SWITCH_OK."}
		}
		response, err := mediaProbeCall(t, executor, auth, metadata, "corpus-replay-"+name, request)
		if err != nil {
			t.Fatalf("%s (source request %s): %s", name, testCase.SourceRequestID, redactProbeError(err, metadata))
		}
		answer := gjsonText(response.Payload)
		t.Logf("%s (source request %s): receipt=%v answer=%q", name, testCase.SourceRequestID, response.Headers.Get(HeaderUsageReceipt) != "", answer)
		if name == "claude-to-cursor-chrome-devtools-screenshot" && !strings.Contains(strings.ToLower(answer), "red") {
			t.Fatalf("model did not see the replayed chrome-devtools screenshot: %q", answer)
		}
		if name == "claude-to-cursor-vip-to-max-thinking" && !strings.Contains(answer, "HISTORY_SWITCH_OK") {
			t.Fatalf("VIP->Max history replay did not complete: %q", answer)
		}
	}
}

// TestAuthorizedSandCrossModelSignatureTimingProbe seeds signed thinking from
// one model and replays it to another, timing the replay so the cost of the
// foreign-signature retry is measured rather than assumed.
// CURSOR_AGENT_V1_CROSS_MODEL_PROBE=2-requests, seed model via
// CURSOR_AGENT_V1_SEED_MODEL (default claude-opus-4-6), target via CURSOR_AGENT_V1_MEDIA_MODEL.
func TestAuthorizedSandCrossModelSignatureTimingProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_CROSS_MODEL_PROBE") != "2-requests" {
		t.Skip("explicit bounded cross-model probe authorization required")
	}
	seedModel := strings.TrimSpace(os.Getenv("CURSOR_AGENT_V1_SEED_MODEL"))
	if seedModel == "" {
		seedModel = "claude-opus-4-6"
	}
	executor, auth, metadata := mediaProbeExecutor(t, runtimeSand)
	call := func(session, model string, body map[string]any) (cliproxyexecutor.Response, time.Duration, error) {
		payload, _ := jsonx.Marshal(body)
		request := claudeExecutorRequest(session, string(payload))
		request.Model = model
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		started := time.Now()
		response, err := executor.Execute(ctx, auth, request, claudeExecutorOptions(false))
		return response, time.Since(started), err
	}
	user := map[string]any{"role": "user", "content": "Think briefly, then reply exactly SEED_OK."}
	seed, seedTook, err := call("cross-model-seed", seedModel, map[string]any{"model": seedModel, "thinking": map[string]any{"type": "enabled", "budget_tokens": 1024}, "messages": []any{user}})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	var assistant map[string]any
	_ = jsonx.Unmarshal(seed.Payload, &assistant)
	blocks, _ := assistant["content"].([]any)
	signed := 0
	for _, raw := range blocks {
		block, _ := raw.(map[string]any)
		if block["type"] == "thinking" && stringValue(block["signature"]) != "" {
			signed++
		}
	}
	t.Logf("seed %s took %s signed_thinking_blocks=%d", seedModel, seedTook.Round(time.Millisecond), signed)
	if signed == 0 {
		t.Fatal("seed produced no signed thinking")
	}
	target := mediaProbeModel()
	_, replayTook, err := call("cross-model-replay", target, map[string]any{"model": target, "thinking": map[string]any{"type": "enabled", "budget_tokens": 1024},
		"messages": []any{user, map[string]any{"role": "assistant", "content": blocks}, map[string]any{"role": "user", "content": "Reply exactly REPLAY_OK."}}})
	if err != nil {
		t.Fatal(redactProbeError(err, metadata))
	}
	t.Logf("replay %s -> %s took %s signature_retries=%d", seedModel, target, replayTook.Round(time.Millisecond), SandSignatureRetryCount())
}

// TestAuthorizedSandForeignSignatureCostProbe times a turn whose history carries
// a foreign signature (first attempt rejected, text-only retry) against the
// same turn sent text-only, so the per-turn penalty is measured.
// CURSOR_AGENT_V1_FOREIGN_COST_PROBE=3-requests
func TestAuthorizedSandForeignSignatureCostProbe(t *testing.T) {
	if os.Getenv("CURSOR_AGENT_V1_FOREIGN_COST_PROBE") != "3-requests" {
		t.Skip("explicit bounded foreign-signature cost probe authorization required")
	}
	executor, auth, metadata := mediaProbeExecutor(t, runtimeSand)
	model := mediaProbeModel()
	foreign := "EqQBCkYIBxgCKkD" + strings.Repeat("Zm9yZWlnbg", 40)
	history := []any{
		map[string]any{"role": "user", "content": "remember GULL-7"},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "thinking", "thinking": "Note the marker.", "signature": foreign}, map[string]any{"type": "text", "text": "Noted."}}},
		map[string]any{"role": "user", "content": "Reply exactly COST_OK."},
	}
	run := func(session string) (time.Duration, int64) {
		before := SandSignatureRetryCount()
		payload, _ := jsonx.Marshal(map[string]any{"model": model, "thinking": map[string]any{"type": "enabled", "budget_tokens": 1024}, "messages": history})
		request := claudeExecutorRequest(session, string(payload))
		request.Model = model
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		started := time.Now()
		if _, err := executor.Execute(ctx, auth, request, claudeExecutorOptions(false)); err != nil {
			t.Fatal(redactProbeError(err, metadata))
		}
		return time.Since(started), SandSignatureRetryCount() - before
	}
	took1, retries1 := run("foreign-cost-1")
	took2, retries2 := run("foreign-cost-2")
	t.Logf("turn1 took=%s retries=%d ; turn2 (same history) took=%s retries=%d", took1.Round(time.Millisecond), retries1, took2.Round(time.Millisecond), retries2)
	previous := sandReasoningSignatureForwarding
	sandReasoningSignatureForwarding = false
	defer func() { sandReasoningSignatureForwarding = previous }()
	took3, retries3 := run("foreign-cost-3-textonly")
	t.Logf("text-only baseline took=%s retries=%d", took3.Round(time.Millisecond), retries3)
}
