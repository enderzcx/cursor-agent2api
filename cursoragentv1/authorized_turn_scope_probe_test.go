package cursoragentv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
)

// Isolated attribution experiment, not an end-user acceptance test. One caller
// tool invocation, one result continuation, one new user turn; no billing writes.
func TestAuthorizedTurnScopeProbe(t *testing.T) {
	if os.Getenv("CURSOR_TURN_SCOPE_PROBE") != "3-calls-1-cny" {
		t.Skip("explicit bounded experiment authorization required")
	}
	metadata := liveProbeMetadata(t)
	value := func(k string) string { v, _ := metadata[k].(string); return v }
	token := value("access_token")
	base := value("base_url")
	if base == "" {
		base = "https://api2.cursor.sh"
	}
	p := &attributionProbe{}
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, cancel := range p.cancels {
			cancel()
		}
	}()
	store, err := NewFileStateStore(t.TempDir())
	if err != nil {
		t.Fatal("isolated store unavailable")
	}
	m, err := NewSessionManager(newRunnerForTest(p, defaultHeartbeatInterval, defaultFrameIdleTimeout), store)
	if err != nil {
		t.Fatal("isolated manager unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cid := uuid.NewString()
	started := time.Now().Add(-time.Second).UnixMilli()
	request := ManagedRequest{TurnMetering: true, SessionID: uuid.NewString(), BillingRunID: uuid.NewString(),
		CredentialScope: "tenant:1:user:1:channel:301", AccountKey: token, Context: ctx,
		Run: RunRequest{BaseURL: base, AccessToken: token, ClientVersion: value("client_version"),
			Model: "composer-2.5-fast", ConversationID: cid, MaxParallelTools: 1,
			UserText:        "Call probe_marker once. After its result reply exactly SCOPE_TOOL_FINISHED. Do not call any other tool.",
			RequireToolCall: true, Tools: []ToolDefinition{{Name: "probe_marker", Description: "Return a fixed test marker", InputSchema: []byte(`{"type":"object","properties":{}}`)}},
		},
	}
	log := func(label string, data any) {
		b, _ := jsonx.Marshal(map[string]any{"label": label, "data": data})
		t.Log("SCOPE " + string(b))
	}
	log("identity", map[string]any{"conversation_hash": fmt.Sprintf("%x", sha256.Sum256([]byte(cid))), "started_ms": started})
	// Keep only records whose raw conversation ID exactly matches this probe.
	dashboard := func(label string) int {
		payload, _ := jsonx.Marshal(map[string]any{"teamId": 0, "startDate": fmt.Sprint(started), "endDate": fmt.Sprint(time.Now().Add(time.Second).UnixMilli()), "page": 1, "pageSize": 100})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api2.cursor.sh/aiserver.v1.DashboardService/GetFilteredUsageEvents", bytes.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Connect-Protocol-Version", "1")
		resp, e := (&http.Client{Timeout: 6 * time.Second}).Do(req)
		if e != nil {
			log(label, map[string]any{"query_failed": true})
			return -1
		}
		defer resp.Body.Close()
		var root struct {
			Total int `json:"totalUsageEventsCount"`
			Rows  []struct {
				CID       string         `json:"conversationId"`
				Model     string         `json:"model"`
				Timestamp any            `json:"timestamp"`
				Usage     map[string]any `json:"tokenUsage"`
			} `json:"usageEventsDisplay"`
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 500000))
		if jsonx.Unmarshal(body, &root) != nil {
			log(label, map[string]any{"status": resp.StatusCode, "decode_failed": true})
			return -1
		}
		rows := []map[string]any{}
		for _, r := range root.Rows {
			if r.CID == cid {
				safe := map[string]any{}
				for _, k := range []string{"inputTokens", "outputTokens", "cacheReadTokens", "cacheWriteTokens", "totalCents"} {
					if v, ok := r.Usage[k]; ok {
						safe[k] = v
					}
				}
				rows = append(rows, map[string]any{"model": r.Model, "timestamp": r.Timestamp, "usage": safe})
			}
		}
		log(label, map[string]any{"status": resp.StatusCode, "matching_rows": rows, "window_total": root.Total, "window_complete": root.Total <= len(root.Rows)})
		return len(rows)
	}
	drain := func(label string, out *ManagedOutput, park bool) string {
		// Live ResumeTools public output omits ConversationID. Absence is not
		// a changed ID; verify the active session's actual upstream identity.
		if out.ConversationID != "" && out.ConversationID != cid {
			t.Fatal("conversation identity changed")
		}
		if out.SessionID != request.SessionID {
			t.Fatal("session identity changed")
		}
		m.mu.Lock()
		active := m.active[out.SessionID]
		actualCID := ""
		if active != nil {
			actualCID = active.request.ConversationID
		}
		m.mu.Unlock()
		if actualCID != "" && actualCID != cid {
			t.Fatal("upstream conversation identity changed")
		}
		var tool string
		for {
			select {
			case <-ctx.Done():
				t.Fatal("bounded experiment timed out")
				return ""
			case e, ok := <-out.Events:
				if !ok {
					if e := <-out.Result; e != nil {
						t.Fatal("upstream experiment failed")
					}
					s := snapshotProbeRows(p)
					log(label, map[string]any{"terminal_count": s.terminals, "latest_terminal": s.latestTerminal, "delta_sum": s.delta})
					return tool
				}
				if e.Type == EventToolCall && e.ToolCall != nil {
					if !park || tool != "" {
						_ = m.Cancel(request)
						t.Fatal("unexpected additional tool; no execution permitted")
					}
					tool = e.ToolCall.ToolCallID
					if err := m.Park(request); err != nil {
						t.Fatal("park failed")
					}
				}
			}
		}
	}
	a, err := m.Start(request)
	if err != nil {
		t.Fatal("A start failed")
	}
	id := drain("A-tool-paused", a, true)
	if id == "" {
		t.Fatal("marker tool missing")
	}
	dashboard("A-dashboard")
	b, err := m.ResumeTool(request, ToolResult{ToolCallID: id, Content: "ok"})
	if err != nil {
		t.Fatal("B resume failed")
	}
	drain("B-tool-finished", b, false)
	dashboard("B-dashboard")
	request.BillingRunID = uuid.NewString()
	request.RequireExisting = true
	request.Run.UserText = "Reply exactly OK. Do not call any tool."
	request.Run.RequireToolCall = false
	request.DisableCallerTools = true
	request.Run.Tools = nil
	c, err := m.Start(request)
	if err != nil {
		t.Fatal("C next turn start failed")
	}
	drain("C-next-user-turn", c, false)
	for i := 0; i < 3; i++ {
		if dashboard(fmt.Sprintf("C-dashboard-%d", i)) >= 2 {
			break
		}
		if i < 2 {
			select {
			case <-ctx.Done():
				t.Fatal("dashboard observation timed out")
			case <-time.After(4 * time.Second):
			}
		}
	}
	p.mu.Lock()
	opens := p.opens
	p.mu.Unlock()
	log("completed", map[string]any{"application_calls": 3, "http2_streams": opens, "same_conversation": true})
}
