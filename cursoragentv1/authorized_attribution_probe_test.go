package cursoragentv1

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"google.golang.org/protobuf/encoding/protowire"
)

// Opt-in live evidence only. No credentials, prompts, full tool results, or
// request ids are written. The caller must reserve the entire batch budget;
// this opt-in test enforces transport/time bounds, not supplier currency fees.
type attributionProbe struct {
	mu        sync.Mutex
	rows      []map[string]any
	opens     int
	cancels   []context.CancelFunc
	tokens    int64
	textBytes int
}

type attributionStream struct {
	stream
	data chan []byte
}

func (s *attributionStream) Data() <-chan []byte { return s.data }

func (p *attributionProbe) Open(ctx context.Context, cfg streamConfig) (stream, error) {
	p.mu.Lock()
	p.opens++
	limit := 3
	if os.Getenv("CURSOR_PROBE_MODE") == "search-only" {
		limit = 1
	}
	if p.opens > limit {
		p.mu.Unlock()
		return nil, fmt.Errorf("authorized upstream request limit")
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	p.cancels = append(p.cancels, cancel)
	p.mu.Unlock()
	inner, err := (http2Opener{}).Open(ctx, cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &attributionStream{stream: inner, data: make(chan []byte)}
	go func() {
		defer close(s.data)
		var buf []byte
		for chunk := range inner.Data() {
			buf = append(buf, chunk...)
			for {
				flags, payload, n, ok, err := parseConnectFrame(buf)
				if err != nil {
					buf = nil
					break
				}
				if !ok {
					break
				}
				buf = buf[n:]
				if flags&connectCompressionFlag != 0 {
					payload, err = decompressConnectPayload(payload)
				}
				if err == nil && flags&connectEndStreamFlag == 0 {
					p.record(payload)
				}
			}
			select {
			case s.data <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()
	return s, nil
}

func (p *attributionProbe) record(payload []byte) {
	m, err := decodeServerMessage(payload)
	if err != nil {
		return
	}
	p.mu.Lock()
	if m.Type == wireTokenDelta {
		p.tokens += m.TokenDelta
	}
	if m.Type == wireTextDelta || m.Type == wireThinkingDelta {
		p.textBytes += len(m.Text)
	}
	if p.tokens > 4000 || p.textBytes > 16000 {
		for _, cancel := range p.cancels {
			cancel()
		}
	}
	p.mu.Unlock()
	r := map[string]any{}
	switch m.Type {
	case wireTokenDelta:
		r["type"], r["tokens"] = "delta", m.TokenDelta
	case wireTurnEnded:
		r["type"], r["usage"] = "terminal", m.Usage
		// Preserve exact numeric terminal bytes only, not the server envelope.
		if b := probeBytesField(probeBytesField(payload, agentServerInteractionUpdate), 14); b != nil && probeNumericOnly(b) {
			r["terminal_hex"] = hex.EncodeToString(b)
		}
	case wireToolCallStarted, wireToolCallDelta, wireToolCallCompleted:
		if m.HostedSearch == nil {
			return
		}
		s := m.HostedSearch
		r["type"], r["kind"], r["query_present"], r["url_present"] = "hosted", s.Kind, s.Query != "", s.URL != ""
		r["completed"] = m.Type == wireToolCallCompleted
		r["wire_type"] = int(m.Type)
		refs := []map[string]any{}
		for _, ref := range s.References {
			urls := regexp.MustCompile(`https?://[^\s<>"\]\)]+`).FindAllString(ref.Chunk, 8)
			entry := map[string]any{"url": ref.URL, "title": ref.Title, "chunk_chars": len(ref.Chunk), "chunk_urls": urls}
			if os.Getenv("CURSOR_PROBE_MODE") == "search-only" {
				prefix := ref.Chunk
				if len(prefix) > 240 {
					prefix = prefix[:240]
				}
				entry["format_prefix"] = prefix
			}
			refs = append(refs, entry)
		}
		r["references"] = refs
	default:
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	r["sequence"] = len(p.rows) + 1
	p.rows = append(p.rows, r)
}

func probeBytesField(data []byte, want protowire.Number) []byte {
	for len(data) > 0 {
		n, typ, tag := protowire.ConsumeTag(data)
		if tag < 0 {
			return nil
		}
		data = data[tag:]
		used := protowire.ConsumeFieldValue(n, typ, data)
		if used < 0 {
			return nil
		}
		if n == want && typ == protowire.BytesType {
			b, _ := protowire.ConsumeBytes(data)
			return b
		}
		data = data[used:]
	}
	return nil
}

func probeNumericOnly(data []byte) bool {
	for len(data) > 0 {
		n, typ, tag := protowire.ConsumeTag(data)
		if tag < 0 || n < 1 || n > 5 || typ != protowire.VarintType {
			return false
		}
		data = data[tag:]
		_, used := protowire.ConsumeVarint(data)
		if used < 0 {
			return false
		}
		data = data[used:]
	}
	return true
}

func TestAuthorizedAttributionProbe(t *testing.T) {
	if os.Getenv("CURSOR_AUTHORIZED_PROBE") != "40-requests-20-cny" {
		t.Skip("explicit bounded authorization not supplied")
	}
	supplied := liveProbeMetadata(t)
	metadata := make(map[string]any)
	// Never rotate a production credential or inherit unrelated auth options.
	for _, key := range []string{"access_token", "account_id", "base_url", "client_version"} {
		if value, ok := supplied[key]; ok {
			metadata[key] = value
		}
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
		t.Fatal("isolated store failed")
	}
	manager, err := NewSessionManager(newRunnerForTest(p, defaultHeartbeatInterval, defaultFrameIdleTimeout), store)
	if err != nil {
		t.Fatal("isolated manager failed")
	}
	executor := newExecutorForTest(manager, nil)
	auth := &cliproxyauth.Auth{ID: "authorized-channel301-probe", Provider: providerID, Attributes: map[string]string{"credential_scope": "tenant:1:user:1:channel:301"}, Metadata: metadata}
	probeModel := os.Getenv("CURSOR_AGENT_V1_LIVE_MODEL")
	if probeModel == "" {
		probeModel = "composer-2.5-fast"
	}
	call := func(label string, body map[string]any, hosted bool) map[string]any {
		p.mu.Lock()
		before := len(p.rows)
		p.mu.Unlock()
		payload, _ := jsonx.Marshal(body)
		req := claudeExecutorRequest("authorized-"+label, string(payload))
		req.Model = probeModel
		opts := claudeExecutorOptions(false)
		if hosted {
			opts = withHostedSearchHeader(opts)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		resp, callErr := executor.Execute(ctx, auth, req, opts)
		p.mu.Lock()
		rows := append([]map[string]any(nil), p.rows[before:]...)
		p.mu.Unlock()
		report := map[string]any{"label": label, "events": rows, "replay": resp.Headers.Get(HeaderUsageReplay) == "1"}
		if callErr != nil {
			msg := callErr.Error()
			for _, v := range metadata {
				if s, ok := v.(string); ok && s != "" {
					msg = strings.ReplaceAll(msg, s, "[redacted]")
				}
			}
			report["error"] = msg
			b, _ := jsonx.Marshal(report)
			t.Log("PROBE " + string(b))
			t.Fatalf("bounded live probe failed at %s", label)
		}
		var result map[string]any
		if jsonx.Unmarshal(resp.Payload, &result) != nil {
			t.Fatal("response decode failed")
		}
		report["stop_reason"], report["usage"] = result["stop_reason"], result["usage"]
		queryReady, sources := false, []string{}
		if blocks, ok := result["content"].([]any); ok {
			for _, value := range blocks {
				block, _ := value.(map[string]any)
				if block["type"] == "server_tool_use" && block["name"] == "web_search" {
					input, _ := block["input"].(map[string]any)
					q, _ := input["query"].(string)
					queryReady = queryReady || q != ""
				}
				if block["type"] == "web_search_tool_result" {
					if refs, ok := block["content"].([]any); ok {
						for _, value := range refs {
							ref, _ := value.(map[string]any)
							u, _ := ref["url"].(string)
							if u != "" {
								sources = append(sources, u)
							}
						}
					}
				}
			}
		}
		report["projected_search_query_present"], report["projected_search_sources"] = queryReady, sources
		b, _ := jsonx.Marshal(report)
		t.Log("PROBE " + string(b))
		if label == "tool-c-replay" && (resp.Headers.Get(HeaderUsageReplay) != "1" || len(rows) != 0) {
			t.Fatal("exact replay must be marked and have no new observed wire events")
		}
		if os.Getenv("CURSOR_VERIFY_SEARCH_CONTRACT") == "1" && hosted && (!queryReady || len(sources) == 0) {
			t.Fatal("real search projection lacks query or source URLs")
		}
		return result
	}
	user := func(text string) map[string]any { return map[string]any{"role": "user", "content": text} }
	if os.Getenv("CURSOR_PROBE_MODE") == "search-only" {
		call("opus-search", map[string]any{"model": probeModel, "max_tokens": 256, "tools": []any{map[string]any{"type": "web_search_20250305", "name": "web_search"}}, "messages": []any{user("Search official Cursor CLI docs. Return only one official URL and one sentence. Do not fetch additional pages.")}}, true)
		return
	}
	plain := call("plain", map[string]any{"model": "composer-2.5-fast", "max_tokens": 256, "messages": []any{user("Reply exactly S3_PLAIN_OK.")}}, false)
	_ = plain
	tools := []any{map[string]any{"name": "probe_marker", "description": "Return a fixed verification marker.", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"padding": map[string]any{"type": "string"}}, "required": []string{"padding"}}}}
	firstUser := user("Call probe_marker once with padding exactly: " + strings.Repeat("abcde", 80) + ". After receiving its result reply exactly S3_TOOL_OK. Do not call any other tool.")
	a := call("tool-a", map[string]any{"model": "composer-2.5-fast", "max_tokens": 512, "tools": tools, "tool_choice": map[string]any{"type": "tool", "name": "probe_marker"}, "messages": []any{firstUser}}, false)
	var id string
	for _, v := range a["content"].([]any) {
		b, _ := v.(map[string]any)
		if b["type"] == "tool_use" {
			id, _ = b["id"].(string)
		}
	}
	if id == "" {
		t.Fatal("upstream did not call marker tool")
	}
	resume := map[string]any{"model": "composer-2.5-fast", "max_tokens": 128, "tools": tools, "tool_choice": map[string]any{"type": "none"}, "messages": []any{firstUser, map[string]any{"role": "assistant", "content": a["content"]}, map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": id, "content": "S3_TOOL_OK"}}}}}
	call("tool-b", resume, false)
	call("tool-c-replay", resume, false)
	call("search", map[string]any{"model": "composer-2.5-fast", "max_tokens": 512, "tools": []any{map[string]any{"type": "web_search_20250305", "name": "web_search"}}, "messages": []any{user("Search official Cursor CLI docs. Name one feature and cite its official URL. Keep the answer short.")}}, true)
}
