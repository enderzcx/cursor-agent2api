package cursoragentv1

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

const toolBoundaryParkObserve = 8 * time.Second

func toolBoundaryProbeAuthorized() bool {
	return os.Getenv("CURSOR_AUTHORIZED_PROBE") == "40-requests-20-cny" &&
		os.Getenv("CURSOR_TOOL_BOUNDARY_PROBE") == "1"
}

// oneStreamAttribution reuses attributionProbe decode/accounting but refuses a
// second HTTP/2 open. Resume must stay on the parked stream.
type oneStreamAttribution struct {
	*attributionProbe
	opened atomic.Bool
}

func (p *oneStreamAttribution) Open(ctx context.Context, cfg streamConfig) (stream, error) {
	if p == nil || p.attributionProbe == nil {
		return nil, fmt.Errorf("authorized upstream request limit")
	}
	if !p.opened.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("authorized upstream request limit")
	}
	return p.attributionProbe.Open(ctx, cfg)
}

var _ streamOpener = (*oneStreamAttribution)(nil)

func TestToolBoundaryProbeOfflineSkip(t *testing.T) {
	t.Setenv("CURSOR_AUTHORIZED_PROBE", "")
	t.Setenv("CURSOR_TOOL_BOUNDARY_PROBE", "")
	if toolBoundaryProbeAuthorized() {
		t.Fatal("empty env must not authorize the tool-boundary probe")
	}
	t.Setenv("CURSOR_AUTHORIZED_PROBE", "40-requests-20-cny")
	t.Setenv("CURSOR_TOOL_BOUNDARY_PROBE", "")
	if toolBoundaryProbeAuthorized() {
		t.Fatal("authorized probe env alone must not authorize the tool-boundary probe")
	}
	t.Setenv("CURSOR_AUTHORIZED_PROBE", "")
	t.Setenv("CURSOR_TOOL_BOUNDARY_PROBE", "1")
	if toolBoundaryProbeAuthorized() {
		t.Fatal("tool-boundary env alone must not authorize the live probe")
	}
}

// Opt-in discriminating instrument: A park, 8s watch of the still-open stream,
// then B. An 8s gap without TurnEnded is not evidence that the upstream never
// emits a tool-boundary terminal. No billing, no extra model calls after B.
func TestAuthorizedToolBoundaryProbe(t *testing.T) {
	if !toolBoundaryProbeAuthorized() {
		t.Skip("explicit tool-boundary authorization not supplied")
	}
	supplied := liveProbeMetadata(t)
	metadata := make(map[string]any)
	for _, key := range []string{"access_token", "account_id", "base_url", "client_version"} {
		if value, ok := supplied[key]; ok {
			metadata[key] = value
		}
	}
	inner := &attributionProbe{}
	p := &oneStreamAttribution{attributionProbe: inner}
	defer func() {
		inner.mu.Lock()
		defer inner.mu.Unlock()
		for _, cancel := range inner.cancels {
			cancel()
		}
	}()
	store, err := NewFileStateStore(t.TempDir())
	if err != nil {
		t.Fatal("isolated store failed")
	}
	manager, err := NewSessionManager(newRunnerForTest(p, defaultHeartbeatInterval, defaultFrameIdleTimeout), store)
	if err != nil {
		t.Fatal("isolated manager failed")
	}
	executor := newExecutorForTest(manager, nil)
	auth := &cliproxyauth.Auth{
		ID:         "authorized-tool-boundary-probe",
		Provider:   providerID,
		Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"},
		Metadata:   metadata,
	}
	redact := func(msg string) string {
		for _, v := range metadata {
			if s, ok := v.(string); ok && s != "" {
				msg = strings.ReplaceAll(msg, s, "[redacted]")
			}
		}
		return msg
	}
	call := func(label string, body map[string]any) map[string]any {
		payload, _ := jsonx.Marshal(body)
		req := claudeExecutorRequest("authorized-tool-boundary", string(payload))
		req.Model = "composer-2.5-fast"
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		resp, callErr := executor.Execute(ctx, auth, req, claudeExecutorOptions(false))
		if callErr != nil {
			t.Fatalf("bounded live probe failed at %s: %s", label, redact(callErr.Error()))
		}
		var result map[string]any
		if jsonx.Unmarshal(resp.Payload, &result) != nil {
			t.Fatal("response decode failed")
		}
		return result
	}

	tools := []any{map[string]any{
		"name":        "probe_marker",
		"description": "Return a fixed verification marker.",
		"input_schema": map[string]any{
			"type":       "object",
			"properties": map[string]any{"k": map[string]any{"type": "string"}},
		},
	}}
	user := map[string]any{"role": "user", "content": "Call probe_marker once with k=1. After the tool result reply exactly T64_BOUNDARY_OK. Do not call any other tool."}
	a := call("tool-a", map[string]any{
		"model": "composer-2.5-fast", "max_tokens": 128, "tools": tools,
		"tool_choice": map[string]any{"type": "tool", "name": "probe_marker"},
		"messages":    []any{user},
	})
	aReturn := snapshotProbeRows(inner)
	var id string
	if blocks, ok := a["content"].([]any); ok {
		for _, value := range blocks {
			block, _ := value.(map[string]any)
			if block["type"] == "tool_use" {
				id, _ = block["id"].(string)
			}
		}
	}
	if id == "" {
		t.Fatal("upstream did not call marker tool")
	}

	lateFirstMs := 0
	deadline := time.Now().Add(toolBoundaryParkObserve)
	for time.Now().Before(deadline) {
		cur := snapshotProbeRows(inner)
		if cur.terminals > aReturn.terminals && lateFirstMs == 0 {
			lateFirstMs = int(time.Since(aReturn.at) / time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
	}
	preB := snapshotProbeRows(inner)

	b := call("tool-b", map[string]any{
		"model": "composer-2.5-fast", "max_tokens": 128, "tools": tools,
		"tool_choice": map[string]any{"type": "none"},
		"messages": []any{
			user,
			map[string]any{"role": "assistant", "content": a["content"]},
			map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": id, "content": "ok"},
			}},
		},
	})
	_ = b
	afterB := snapshotProbeRows(inner)
	inner.mu.Lock()
	opens := inner.opens
	totalDelta := inner.tokens
	inner.mu.Unlock()
	if opens != 1 {
		t.Fatalf("probe required one http2 stream, opens=%d", opens)
	}
	report := map[string]any{
		"a_return_terminal_count":      aReturn.terminals,
		"a_return_latest_terminal":     aReturn.latestTerminal,
		"a_return_delta_sum":           aReturn.delta,
		"late_pre_b_terminal_count":    preB.terminals - aReturn.terminals,
		"late_pre_b_delta_sum":         preB.delta - aReturn.delta,
		"late_pre_b_first_terminal_ms": lateFirstMs,
		"pre_b_terminal_count":         preB.terminals,
		"pre_b_latest_terminal":        preB.latestTerminal,
		"after_b_terminal_count":       afterB.terminals - preB.terminals,
		"after_b_delta_sum":            afterB.delta - preB.delta,
		"after_b_latest_terminal":      latestTerminalSince(inner, preB.seq),
		"total_raw_delta":              totalDelta,
		"opens":                        opens,
		"park_observe_seconds":         int(toolBoundaryParkObserve / time.Second),
		"note":                         "8s absence is not a universal bound",
	}
	encoded, _ := jsonx.Marshal(report)
	t.Log("PROBE " + string(encoded))
}

type probeRowSnapshot struct {
	at             time.Time
	seq            int
	terminals      int
	delta          int64
	latestTerminal map[string]any
}

func snapshotProbeRows(p *attributionProbe) probeRowSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := probeRowSnapshot{at: time.Now(), seq: len(p.rows)}
	for _, row := range p.rows {
		switch probeRowType(row) {
		case "terminal":
			out.terminals++
			if fields := probeTerminalNumericFields(row); fields != nil {
				out.latestTerminal = fields
			}
		case "delta":
			out.delta += probeRowDelta(row)
		}
	}
	return out
}

func probeRowType(row map[string]any) string {
	value, _ := row["type"].(string)
	return value
}

func probeRowDelta(row map[string]any) int64 {
	switch typed := row["tokens"].(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func probeTerminalNumericFields(row map[string]any) map[string]any {
	usage, ok := row["usage"].(*UsageObservation)
	if !ok || usage == nil {
		return nil
	}
	field := func(count ObservedCount) map[string]any {
		item := map[string]any{"present": count.Present, "invalid": count.Invalid}
		if count.Present && !count.Invalid {
			item["value"] = count.Value
		}
		return item
	}
	return map[string]any{
		"input":       field(usage.Input),
		"output":      field(usage.Output),
		"cache_read":  field(usage.CacheRead),
		"cache_write": field(usage.CacheWrite),
		"reasoning":   field(usage.Reasoning),
	}
}

func latestTerminalSince(p *attributionProbe, from int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if from < 0 {
		from = 0
	}
	if from > len(p.rows) {
		return nil
	}
	var latest map[string]any
	for _, row := range p.rows[from:] {
		if probeRowType(row) != "terminal" {
			continue
		}
		if fields := probeTerminalNumericFields(row); fields != nil {
			latest = fields
		}
	}
	return latest
}
