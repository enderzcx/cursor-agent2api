package cursoragentv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"github.com/google/uuid"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

var hostedSearchMarkdownLink = regexp.MustCompile(`^(\d+)\.\s+\[([^\]]+)\]\(([^)]+)\)\s*$`)

type claudeProjection struct {
	historyTurn        bool
	ctx                context.Context
	model              string
	messageID          string
	responseFormat     sdktranslator.Format
	original           []byte
	request            []byte
	started            bool
	textOpen           bool
	textIndex          int
	thinkingOpen       bool
	thinkingIndex      int
	blockIndex         int
	text               strings.Builder
	thinking           strings.Builder
	toolCalls          []ToolCall
	blocks             []any
	citations          []any
	hostedStarted      map[string]bool
	param              any
	claudeSSE          bytes.Buffer
	facts              usageFacts
	semantic           bool
	responsesSeq       int
	responsesOut       int
	respSpan           int
	respStarted        bool
	respCreatedAt      int64
	respReasoningOpen  bool
	respReasoningIndex int
	respReasoningID    string
	respReasoning      strings.Builder
	respMessageOpen    bool
	respMessageIndex   int
	respMessageID      string
	respText           strings.Builder
	respByIndex        map[int]responsesOutputItem
	respSearchIndex    map[string]int
}

func newClaudeProjection(ctx context.Context, turn translatedTurn) *claudeProjection {
	messageID := strings.TrimSpace(turn.managed.SessionID)
	if turn.responseFormat == sdktranslator.FormatClaude {
		// A durable conversation is not a Messages response. Claude Code merges
		// replies with the same ID, so each generation segment needs its own ID.
		// Use the persisted segment identity to keep exact replays stable without
		// changing Responses IDs, which are continuation anchors.
		segmentID := strings.TrimSpace(turn.managed.RequestSegmentID)
		if segmentID == "" {
			segmentID = strings.TrimSpace(turn.managed.BillingRunID)
		}
		if segmentID != "" {
			digest := sha256.Sum256([]byte(messageID + "\x00" + segmentID))
			messageID = "msg_" + hex.EncodeToString(digest[:])
		} else {
			messageID = ""
		}
	}
	if messageID == "" {
		messageID = "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	model := strings.TrimSpace(turn.publicModel)
	if model == "" {
		model = turn.managed.Run.Model
	}
	return &claudeProjection{historyTurn: turn.managed.Run.HistoryRequestKey != "", ctx: ctx, model: model, messageID: messageID, responseFormat: turn.responseFormat, original: turn.original, request: turn.claudeRequest, hostedStarted: map[string]bool{}, facts: usageFacts{measuredEnabled: isMeteredTurn(turn.managed.BillingRunID)}}
}

func (p *claudeProjection) observeUsageEvidence(event Event) {
	p.facts.observe(event)
}

func (p *claudeProjection) handle(event Event) ([][]byte, bool, bool, error) {
	p.observeUsageEvidence(event)
	if p.responseFormat == sdktranslator.FormatOpenAIResponse {
		return p.handleResponses(event)
	}
	switch event.Type {
	case EventCheckpoint, EventToolResultAccepted, EventUsage, EventUsageObservation, EventHostedSearchCall:
		return nil, false, false, nil
	case EventHostedSearchStarted:
		p.semantic = true
		return p.projectHostedSearchStarted(event.HostedSearch), false, false, nil
	case EventHostedSearchCompleted:
		p.semantic = true
		return p.projectHostedSearchCompleted(event.HostedSearch), false, false, nil
	case EventToolCall:
		if event.ToolCall != nil {
			p.semantic = true
			p.toolCalls = append(p.toolCalls, *event.ToolCall)
		}
		return nil, false, false, nil
	case EventText:
		p.semantic = true
		p.text.WriteString(event.Text)
		chunks := p.start()
		chunks = append(chunks, p.closeThinking()...)
		if !p.textOpen {
			p.textIndex = p.nextBlock()
			chunks = append(chunks, p.project(map[string]any{"type": "content_block_start", "index": p.textIndex, "content_block": map[string]any{"type": "text", "text": ""}})...)
			p.textOpen = true
			chunks = append(chunks, p.projectCitations(p.textIndex)...)
		}
		chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": p.textIndex, "delta": map[string]any{"type": "text_delta", "text": event.Text}})...)
		return chunks, false, false, nil
	case EventThinking:
		if event.Text == "" {
			return nil, false, false, nil
		}
		p.thinking.WriteString(event.Text)
		if p.responseFormat == sdktranslator.FormatClaude {
			// Official Messages thinking blocks require an Anthropic signature,
			// which only arrives with the completed Sand thinking part. Buffer
			// the text and emit the whole signed block on completion; Agent v1
			// never signs, so its Claude clients keep the progress-only omission.
			return nil, false, false, nil
		}
		p.semantic = true
		chunks := p.start()
		chunks = append(chunks, p.closeText()...)
		if !p.thinkingOpen {
			p.thinkingIndex = p.nextBlock()
			chunks = append(chunks, p.project(map[string]any{"type": "content_block_start", "index": p.thinkingIndex, "content_block": map[string]any{"type": "thinking", "thinking": ""}})...)
			p.thinkingOpen = true
		}
		chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": p.thinkingIndex, "delta": map[string]any{"type": "thinking_delta", "thinking": event.Text}})...)
		return chunks, false, false, nil
	case EventThinkingCompleted:
		if p.responseFormat == sdktranslator.FormatClaude {
			return p.emitSignedThinking(event.Signature), false, false, nil
		}
		return p.closeThinking(), false, false, nil
	case EventTurnEnded:
		return p.toolHandoff(true)
	case EventDone:
		if p.historyTurn && len(p.toolCalls) > 0 {
			return p.toolHandoff(false)
		}
		chunks := p.start()
		chunks = append(chunks, p.closeThinking()...)
		chunks = append(chunks, p.closeText()...)
		chunks = append(chunks, p.chatAnnotationChunks()...)
		chunks = append(chunks, p.finish("end_turn")...)
		return chunks, true, false, nil
	default:
		return nil, false, false, nil
	}
}

func (p *claudeProjection) toolHandoff(park bool) ([][]byte, bool, bool, error) {
	if len(p.toolCalls) == 0 {
		return nil, false, false, &requestError{status: http.StatusBadGateway, text: "Cursor Agent v1 turnEnded carried no pending tool batch"}
	}
	chunks := p.start()
	chunks = append(chunks, p.closeThinking()...)
	chunks = append(chunks, p.closeText()...)
	for _, call := range p.toolCalls {
		var input any = map[string]any{}
		if strings.TrimSpace(call.Arguments) != "" {
			if err := jsonx.Unmarshal([]byte(call.Arguments), &input); err != nil {
				return nil, false, false, &requestError{status: http.StatusBadGateway, text: "Cursor Agent v1 returned invalid tool arguments"}
			}
		}
		blockIndex := p.nextBlock()
		arguments, _ := jsonx.Marshal(input)
		p.blocks = append(p.blocks, map[string]any{"type": "tool_use", "id": call.ToolCallID, "name": call.Name, "input": input})
		chunks = append(chunks, p.project(map[string]any{"type": "content_block_start", "index": blockIndex, "content_block": map[string]any{"type": "tool_use", "id": call.ToolCallID, "name": call.Name, "input": map[string]any{}}})...)
		chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": blockIndex, "delta": map[string]any{"type": "input_json_delta", "partial_json": string(arguments)}})...)
		chunks = append(chunks, p.project(map[string]any{"type": "content_block_stop", "index": blockIndex})...)
	}
	chunks = append(chunks, p.finish("tool_use")...)
	return chunks, true, park, nil
}

func (p *claudeProjection) nonStream() ([]byte, error) {
	content := p.blocks
	if content == nil {
		content = []any{}
	}
	reason := "end_turn"
	if len(p.toolCalls) > 0 {
		reason = "tool_use"
	}
	body, err := jsonx.Marshal(map[string]any{
		"id": p.messageID, "type": "message", "role": "assistant", "content": content, "model": p.model,
		"stop_reason": reason, "stop_sequence": nil, "usage": p.usagePayload(true),
	})
	if err != nil {
		return nil, err
	}
	if p.responseFormat == sdktranslator.FormatClaude {
		return body, nil
	}
	if p.responseFormat == sdktranslator.FormatOpenAIResponse {
		return p.responsesNonStream()
	}
	translated := sdktranslator.TranslateNonStream(p.ctx, sdktranslator.FormatClaude, p.responseFormat, p.model, p.original, p.request, p.claudeSSE.Bytes(), &p.param)
	annotated, err := p.attachChatAnnotations(translated)
	if err != nil {
		return nil, err
	}
	return p.attachPrivateUsageToPayload(annotated, true), nil
}

func (p *claudeProjection) start() [][]byte {
	if p.started {
		return nil
	}
	p.started = true
	return p.project(map[string]any{"type": "message_start", "message": map[string]any{"id": p.messageID, "type": "message", "role": "assistant", "content": []any{}, "model": p.model, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}})
}

func (p *claudeProjection) closeText() [][]byte {
	if !p.textOpen {
		return nil
	}
	p.textOpen = false
	textBlock := map[string]any{"type": "text", "text": p.text.String()}
	if len(p.citations) > 0 {
		textBlock["citations"] = append([]any(nil), p.citations...)
	}
	p.text.Reset()
	p.blocks = append(p.blocks, textBlock)
	return p.project(map[string]any{"type": "content_block_stop", "index": p.textIndex})
}

func (p *claudeProjection) closeThinking() [][]byte {
	if !p.thinkingOpen {
		return nil
	}
	p.thinkingOpen = false
	thinking := p.thinking.String()
	p.thinking.Reset()
	p.blocks = append(p.blocks, map[string]any{"type": "thinking", "thinking": thinking})
	return p.project(map[string]any{"type": "content_block_stop", "index": p.thinkingIndex})
}

// emitSignedThinking projects a complete Messages thinking block once the
// provider signature is known. Unsigned reasoning is dropped for Claude
// clients because Anthropic-compatible callers would otherwise replay an
// unverifiable block on their next turn.
func (p *claudeProjection) emitSignedThinking(signature string) [][]byte {
	thinking := p.thinking.String()
	p.thinking.Reset()
	if strings.TrimSpace(signature) == "" || strings.TrimSpace(thinking) == "" {
		return nil
	}
	p.semantic = true
	chunks := p.start()
	chunks = append(chunks, p.closeText()...)
	index := p.nextBlock()
	p.blocks = append(p.blocks, map[string]any{"type": "thinking", "thinking": thinking, "signature": signature})
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_start", "index": index, "content_block": map[string]any{"type": "thinking", "thinking": "", "signature": ""}})...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]any{"type": "thinking_delta", "thinking": thinking}})...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]any{"type": "signature_delta", "signature": signature}})...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_stop", "index": index})...)
	return chunks
}

func (p *claudeProjection) nextBlock() int {
	index := p.blockIndex
	p.blockIndex++
	return index
}

func (p *claudeProjection) finish(reason string) [][]byte {
	chunks := p.project(map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": reason, "stop_sequence": nil}, "usage": p.usagePayload(false)})
	return append(chunks, p.project(map[string]any{"type": "message_stop"})...)
}

func (p *claudeProjection) projectHostedSearchStarted(search *HostedSearch) [][]byte {
	if search == nil {
		return nil
	}
	id := hostedSearchToolID(search)
	if p.hostedStarted == nil {
		p.hostedStarted = map[string]bool{}
	}
	if p.hostedStarted[id] {
		return nil
	}
	p.hostedStarted[id] = true
	chunks := p.start()
	chunks = append(chunks, p.closeThinking()...)
	chunks = append(chunks, p.closeText()...)
	name := hostedSearchToolName(search.Kind)
	input := hostedSearchInput(search)
	index := p.nextBlock()
	p.blocks = append(p.blocks, map[string]any{"type": "server_tool_use", "id": id, "name": name, "input": input})
	partial, _ := jsonx.Marshal(input)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_start", "index": index, "content_block": map[string]any{"type": "server_tool_use", "id": id, "name": name, "input": map[string]any{}}})...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]any{"type": "input_json_delta", "partial_json": string(partial)}})...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_stop", "index": index})...)
	return chunks
}

func (p *claudeProjection) projectHostedSearchCompleted(search *HostedSearch) [][]byte {
	if search == nil {
		return nil
	}
	chunks := p.projectHostedSearchStarted(search)
	id := hostedSearchToolID(search)
	index := p.nextBlock()
	resultType, resultContent := hostedSearchResult(search)
	p.blocks = append(p.blocks, map[string]any{"type": resultType, "tool_use_id": id, "content": resultContent})
	p.citations = append(p.citations, hostedSearchCitations(search)...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_start", "index": index, "content_block": map[string]any{"type": resultType, "tool_use_id": id, "content": resultContent}})...)
	chunks = append(chunks, p.project(map[string]any{"type": "content_block_stop", "index": index})...)
	return chunks
}

func (p *claudeProjection) projectCitations(index int) [][]byte {
	if len(p.citations) == 0 {
		return nil
	}
	var chunks [][]byte
	for _, citation := range p.citations {
		chunks = append(chunks, p.project(map[string]any{"type": "content_block_delta", "index": index, "delta": map[string]any{"type": "citations_delta", "citation": citation}})...)
	}
	return chunks
}

func hostedSearchToolID(search *HostedSearch) string {
	id := strings.TrimSpace(search.CallID)
	if id == "" {
		id = "search"
	}
	if strings.HasPrefix(id, "srvtoolu_") || strings.HasPrefix(id, "ws_") {
		return id
	}
	return "srvtoolu_" + id
}

func hostedSearchToolName(kind HostedSearchKind) string {
	switch kind {
	case HostedSearchKindWebFetch, HostedSearchKindExaFetch:
		return "web_fetch"
	default:
		return "web_search"
	}
}

func hostedSearchInput(search *HostedSearch) map[string]any {
	if search.Kind == HostedSearchKindWebFetch || search.Kind == HostedSearchKindExaFetch {
		url := search.URL
		if url == "" {
			url = search.Query
		}
		return map[string]any{"url": url}
	}
	return map[string]any{"query": search.Query}
}

func hostedSearchResult(search *HostedSearch) (string, any) {
	if search.Kind == HostedSearchKindWebFetch || search.Kind == HostedSearchKindExaFetch {
		if search.Error != "" {
			return "web_fetch_tool_result", map[string]any{"type": "web_fetch_tool_result_error", "error_code": "unavailable", "error_message": search.Error}
		}
		fetchURL := search.URL
		if fetchURL == "" {
			fetchURL = search.Query
		}
		return "web_fetch_tool_result", map[string]any{
			"type": "web_fetch_result",
			"url":  fetchURL,
			"content": map[string]any{
				"type": "document",
				"source": map[string]any{
					"type": "text", "media_type": "text/plain", "data": search.Content,
				},
			},
		}
	}
	if search.Error != "" {
		return "web_search_tool_result", map[string]any{"type": "web_search_tool_result_error", "error_code": "unavailable", "error_message": search.Error}
	}
	usable := usableHostedSearchReferences(search)
	if len(search.References) > 0 && len(usable) == 0 {
		return "web_search_tool_result", map[string]any{"type": "web_search_tool_result_error", "error_code": "unavailable"}
	}
	results := make([]any, 0, len(usable))
	for _, ref := range usable {
		results = append(results, map[string]any{
			"type": "web_search_result", "title": ref.Title, "url": ref.URL, "page_age": nil,
		})
	}
	return "web_search_tool_result", results
}

func hostedSearchCitations(search *HostedSearch) []any {
	usable := usableHostedSearchReferences(search)
	citations := make([]any, 0, len(usable))
	for _, ref := range usable {
		citations = append(citations, map[string]any{
			"type": "web_search_result_location", "url": ref.URL, "title": ref.Title, "cited_text": ref.Chunk,
		})
	}
	return citations
}

// usableHTTPSourceURL is the shared rule for hosted search results, citations,
// and Responses action sources. Only already-returned HTTP(S) links with a
// valid hostname and optional 0-65535 port are projectable. This is not an
// SSRF policy and does not fetch or resolve DNS.
func usableHTTPSourceURL(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return false
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	if !validSourceHostname(parsed.Hostname()) {
		return false
	}
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 0 || port > 65535 {
			return false
		}
	}
	return true
}

func validSourceHostname(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" || host == "." || strings.ContainsAny(host, " \t\r\n") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return true
	}
	name := strings.TrimSuffix(host, ".")
	if name == "" {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" {
			return false
		}
	}
	return true
}

func usableHostedSearchReferences(search *HostedSearch) []HostedSearchReference {
	if search == nil {
		return nil
	}
	usable := make([]HostedSearchReference, 0, len(search.References))
	for _, ref := range search.References {
		if usableHTTPSourceURL(ref.URL) {
			usable = append(usable, ref)
		}
	}
	return usable
}

const hostedSearchWrapperTitle = "Web search results"

func expandHostedSearchLinkList(refs []HostedSearchReference) []HostedSearchReference {
	out := make([]HostedSearchReference, 0, len(refs))
	for _, ref := range refs {
		if strings.TrimSpace(ref.URL) != "" || strings.TrimSpace(ref.Title) != hostedSearchWrapperTitle {
			out = append(out, ref)
			continue
		}
		extracted := parseLeadingHostedSearchLinks(ref.Chunk)
		if len(extracted) == 0 {
			out = append(out, ref)
			continue
		}
		out = append(out, extracted...)
	}
	return out
}

func parseLeadingHostedSearchLinks(chunk string) []HostedSearchReference {
	text := strings.TrimLeft(chunk, "\ufeff \t")
	if !strings.HasPrefix(text, "Links:") {
		return nil
	}
	rest := strings.TrimPrefix(text, "Links:")
	rest = strings.TrimLeft(rest, " \t")
	if strings.HasPrefix(rest, "\r\n") {
		rest = rest[2:]
	} else if strings.HasPrefix(rest, "\n") {
		rest = rest[1:]
	} else {
		return nil
	}
	var refs []HostedSearchReference
	expected := 1
	for _, raw := range strings.Split(rest, "\n") {
		line := strings.TrimRight(raw, "\r")
		if strings.TrimSpace(line) == "" {
			break
		}
		match := hostedSearchMarkdownLink.FindStringSubmatch(line)
		if match == nil {
			break
		}
		number, err := strconv.Atoi(match[1])
		if err != nil || number != expected {
			break
		}
		expected++
		title := strings.TrimSpace(match[2])
		href := strings.TrimSpace(match[3])
		if !usableHTTPSourceURL(href) {
			continue
		}
		refs = append(refs, HostedSearchReference{Title: title, URL: href})
	}
	if expected == 1 {
		return nil
	}
	return refs
}

func (p *claudeProjection) chatAnnotationChunks() [][]byte {
	if p.responseFormat != sdktranslator.FormatOpenAI {
		return nil
	}
	annotations := p.chatAnnotations()
	if len(annotations) == 0 {
		return nil
	}
	payload, err := jsonx.Marshal(map[string]any{
		"id": p.messageID, "object": "chat.completion.chunk", "model": p.model,
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{"annotations": annotations}, "finish_reason": nil}},
	})
	if err != nil {
		return nil
	}
	return [][]byte{append(append([]byte("data: "), payload...), []byte("\n\n")...)}
}

func (p *claudeProjection) chatAnnotations() []any {
	annotations := make([]any, 0, len(p.citations))
	for _, citation := range p.citations {
		block, ok := citation.(map[string]any)
		if !ok {
			continue
		}
		url := stringValue(block["url"])
		if !usableHTTPSourceURL(url) {
			continue
		}
		annotations = append(annotations, map[string]any{
			"type":         "url_citation",
			"url_citation": map[string]any{"url": url, "title": stringValue(block["title"]), "start_index": 0, "end_index": 0},
		})
	}
	return annotations
}

func (p *claudeProjection) attachChatAnnotations(payload []byte) ([]byte, error) {
	if p.responseFormat != sdktranslator.FormatOpenAI {
		return payload, nil
	}
	annotations := p.chatAnnotations()
	if len(annotations) == 0 || len(payload) == 0 {
		return payload, nil
	}
	var root map[string]any
	if err := jsonx.Unmarshal(payload, &root); err != nil {
		return payload, nil
	}
	choices, _ := root["choices"].([]any)
	if len(choices) == 0 {
		return payload, nil
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return payload, nil
	}
	message, ok := choice["message"].(map[string]any)
	if !ok {
		return payload, nil
	}
	message["annotations"] = annotations
	choice["message"] = message
	choices[0] = choice
	root["choices"] = choices
	encoded, err := jsonx.Marshal(root)
	if err != nil {
		return payload, err
	}
	return encoded, nil
}

func hostedSearchResponsesAction(search *HostedSearch) map[string]any {
	if search.Kind == HostedSearchKindWebFetch || search.Kind == HostedSearchKindExaFetch {
		fetchURL := search.URL
		if fetchURL == "" {
			fetchURL = search.Query
		}
		return map[string]any{"type": "open_page", "url": fetchURL}
	}
	action := map[string]any{"type": "search", "query": search.Query}
	if sources := hostedSearchActionSources(search); len(sources) > 0 {
		action["sources"] = sources
	}
	return action
}

func hostedSearchActionSources(search *HostedSearch) []any {
	usable := usableHostedSearchReferences(search)
	if len(usable) == 0 {
		return nil
	}
	sources := make([]any, 0, len(usable))
	for _, ref := range usable {
		sources = append(sources, map[string]any{"type": "url", "url": ref.URL})
	}
	return sources
}

func responsesSSE(event string, payload []byte) []byte {
	var builder strings.Builder
	builder.WriteString("event: ")
	builder.WriteString(event)
	builder.WriteString("\ndata: ")
	builder.Write(payload)
	builder.WriteString("\n\n")
	return []byte(builder.String())
}

func (p *claudeProjection) nextResponsesSeq() int {
	p.responsesSeq++
	return p.responsesSeq
}

func (p *claudeProjection) nextResponsesOutput() int {
	index := p.responsesOut
	p.responsesOut++
	return index
}

func (p *claudeProjection) observeSequence(chunks [][]byte) {
	for _, chunk := range chunks {
		data := chunk
		if idx := bytes.Index(data, []byte("data:")); idx >= 0 {
			data = bytes.TrimSpace(data[idx+5:])
		}
		var root map[string]any
		if jsonx.Unmarshal(data, &root) != nil {
			continue
		}
		switch seq := root["sequence_number"].(type) {
		case float64:
			if int(seq) > p.responsesSeq {
				p.responsesSeq = int(seq)
			}
		}
	}
}

func (p *claudeProjection) usagePayload(includeInput bool) map[string]any {
	usage := map[string]any{"output_tokens": p.outputTokens()}
	if includeInput {
		usage["input_tokens"] = 0
	}
	if measured, ok := p.facts.measuredTurn(); ok {
		// message_start cannot know these counters. The final message_delta
		// carries the full usage snapshot, including an explicit net-input zero.
		usage["input_tokens"] = measured.Input
		usage["cache_read_input_tokens"] = measured.CacheRead
		usage["cache_creation_input_tokens"] = measured.CacheWrite
	} else if p.facts.measuredEnabled {
		usage["input_tokens"] = p.estimatedInputTokens()
	}
	p.attachPrivateUsage(usage)
	return usage
}

func (p *claudeProjection) privateUsageFields() map[string]any {
	fields := p.facts.privateDiagnosticFields()
	if p.historyTurn {
		// A caller handoff is final for this finite native generation. The next
		// tool-result request funds a new Run, not a settlement of this receipt.
		fields["cursor_agent_v1_usage_pending"] = false
	} else if p.facts.measuredEnabled && len(p.toolCalls) > 0 {
		fields["cursor_agent_v1_usage_pending"] = true
	}
	if p.facts.completedSearchObserved {
		fields["cursor_agent_v1_citation_count"] = len(p.citations)
	}
	return fields
}

func (p *claudeProjection) attachPrivateUsage(usage map[string]any) {
	if usage == nil {
		return
	}
	for key, value := range p.privateUsageFields() {
		usage[key] = value
	}
}

func (p *claudeProjection) attachPrivateUsageToPayload(payload []byte, createUsage bool) []byte {
	fields := p.privateUsageFields()
	if len(fields) == 0 || len(payload) == 0 {
		return payload
	}
	prefix, body, suffix := splitSSEJSONPayload(payload)
	var root map[string]any
	if jsonx.Unmarshal(body, &root) != nil {
		return payload
	}
	usage, _ := root["usage"].(map[string]any)
	if usage == nil {
		if !createUsage {
			return payload
		}
		usage = map[string]any{}
		root["usage"] = usage
	}
	for key, value := range fields {
		usage[key] = value
	}
	encoded, err := jsonx.Marshal(root)
	if err != nil {
		return payload
	}
	out := append(append([]byte{}, prefix...), encoded...)
	return append(out, suffix...)
}

func splitSSEJSONPayload(payload []byte) (prefix, body, suffix []byte) {
	suffixLen := 0
	for i := len(payload) - 1; i >= 0; i-- {
		if payload[i] != '\n' && payload[i] != '\r' {
			break
		}
		suffixLen++
	}
	if suffixLen > 0 {
		suffix = payload[len(payload)-suffixLen:]
	}
	core := payload[:len(payload)-suffixLen]
	if bytes.HasPrefix(core, []byte("data:")) {
		rest := bytes.TrimSpace(core[5:])
		return core[:len(core)-len(rest)], rest, suffix
	}
	return nil, core, suffix
}

func (p *claudeProjection) project(event map[string]any) [][]byte {
	data, _ := jsonx.Marshal(event)
	line := append([]byte("data: "), data...)
	p.claudeSSE.Write(line)
	p.claudeSSE.WriteString("\n\n")
	if p.responseFormat == sdktranslator.FormatClaude {
		return [][]byte{append(line, []byte("\n\n")...)}
	}
	chunks := sdktranslator.TranslateStream(p.ctx, sdktranslator.FormatClaude, p.responseFormat, p.model, p.original, p.request, line, &p.param)
	if p.responseFormat == sdktranslator.FormatOpenAI {
		for i := range chunks {
			chunks[i] = p.attachPrivateUsageToPayload(chunks[i], false)
		}
	}
	p.observeSequence(chunks)
	return chunks
}
