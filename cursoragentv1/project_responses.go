package cursoragentv1

import (
	"fmt"
	"net/http"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

type responsesOutputItem struct {
	Index       int
	ID          string
	Type        string
	Status      string
	CallID      string
	Name        string
	Text        string
	Arguments   string
	Action      map[string]any
	Annotations []any
	Role        string
}

func (p *claudeProjection) handleResponses(event Event) ([][]byte, bool, bool, error) {
	switch event.Type {
	case EventCheckpoint, EventToolResultAccepted, EventUsage, EventUsageObservation, EventHostedSearchCall:
		return nil, false, false, nil
	case EventHostedSearchStarted:
		p.semantic = true
		return p.responsesSearchStarted(event.HostedSearch), false, false, nil
	case EventHostedSearchCompleted:
		p.semantic = true
		return p.responsesSearchCompleted(event.HostedSearch), false, false, nil
	case EventToolCall:
		if event.ToolCall != nil {
			p.semantic = true
			p.toolCalls = append(p.toolCalls, *event.ToolCall)
		}
		return nil, false, false, nil
	case EventText:
		p.semantic = true
		p.respText.WriteString(event.Text)
		chunks := p.responsesStart()
		chunks = append(chunks, p.responsesCloseReasoning()...)
		chunks = append(chunks, p.responsesEnsureMessage()...)
		chunks = append(chunks, p.responsesEvent("response.output_text.delta", map[string]any{
			"type": "response.output_text.delta", "sequence_number": p.nextResponsesSeq(),
			"item_id": p.respMessageID, "output_index": p.respMessageIndex, "content_index": 0, "delta": event.Text,
		})...)
		return chunks, false, false, nil
	case EventThinking:
		if event.Text == "" {
			return nil, false, false, nil
		}
		p.semantic = true
		p.respReasoning.WriteString(event.Text)
		chunks := p.responsesStart()
		chunks = append(chunks, p.responsesCloseMessage()...)
		chunks = append(chunks, p.responsesEnsureReasoning()...)
		chunks = append(chunks, p.responsesEvent("response.reasoning_summary_text.delta", map[string]any{
			"type": "response.reasoning_summary_text.delta", "sequence_number": p.nextResponsesSeq(),
			"item_id": p.respReasoningID, "output_index": p.respReasoningIndex, "summary_index": 0, "delta": event.Text,
		})...)
		return chunks, false, false, nil
	case EventThinkingCompleted:
		return p.responsesCloseReasoning(), false, false, nil
	case EventTurnEnded:
		if len(p.toolCalls) == 0 {
			return nil, false, false, &requestError{status: http.StatusBadGateway, text: "Cursor Agent v1 turnEnded carried no pending tool batch"}
		}
		chunks := p.responsesStart()
		chunks = append(chunks, p.responsesCloseReasoning()...)
		chunks = append(chunks, p.responsesCloseMessage()...)
		for _, call := range p.toolCalls {
			chunks = append(chunks, p.responsesFunctionCall(call)...)
		}
		chunks = append(chunks, p.responsesFinish("completed", "")...)
		return chunks, true, true, nil
	case EventDone:
		chunks := p.responsesStart()
		chunks = append(chunks, p.responsesCloseReasoning()...)
		chunks = append(chunks, p.responsesCloseMessage()...)
		chunks = append(chunks, p.responsesFinish("completed", "")...)
		return chunks, true, false, nil
	default:
		return nil, false, false, nil
	}
}

func (p *claudeProjection) responsesStart() [][]byte {
	if p.respStarted {
		return nil
	}
	p.respStarted = true
	p.respCreatedAt = time.Now().Unix()
	if p.respByIndex == nil {
		p.respByIndex = map[int]responsesOutputItem{}
	}
	response := p.responsesEnvelope("in_progress", nil)
	created, _ := jsonx.Marshal(map[string]any{"type": "response.created", "sequence_number": p.nextResponsesSeq(), "response": response})
	inProgress, _ := jsonx.Marshal(map[string]any{"type": "response.in_progress", "sequence_number": p.nextResponsesSeq(), "response": response})
	return [][]byte{responsesSSE("response.created", created), responsesSSE("response.in_progress", inProgress)}
}

func (p *claudeProjection) responsesEnsureReasoning() [][]byte {
	if p.respReasoningOpen {
		return nil
	}
	p.respReasoningOpen = true
	p.respReasoningIndex = p.nextResponsesOutput()
	p.respSpan++
	p.respReasoningID = fmt.Sprintf("rs_%s_%d", p.messageID, p.respSpan)
	item := map[string]any{"id": p.respReasoningID, "type": "reasoning", "status": "in_progress", "summary": []any{}}
	added, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.added", "sequence_number": p.nextResponsesSeq(),
		"output_index": p.respReasoningIndex, "item": item,
	})
	part, _ := jsonx.Marshal(map[string]any{
		"type": "response.reasoning_summary_part.added", "sequence_number": p.nextResponsesSeq(),
		"item_id": p.respReasoningID, "output_index": p.respReasoningIndex, "summary_index": 0,
		"part": map[string]any{"type": "summary_text", "text": ""},
	})
	return [][]byte{responsesSSE("response.output_item.added", added), responsesSSE("response.reasoning_summary_part.added", part)}
}

func (p *claudeProjection) responsesCloseReasoning() [][]byte {
	if !p.respReasoningOpen {
		return nil
	}
	p.respReasoningOpen = false
	text := p.respReasoning.String()
	p.respReasoning.Reset()
	item := responsesOutputItem{
		Index: p.respReasoningIndex, ID: p.respReasoningID, Type: "reasoning", Status: "completed", Text: text,
	}
	p.storeResponsesItem(item)
	textDone, _ := jsonx.Marshal(map[string]any{
		"type": "response.reasoning_summary_text.done", "sequence_number": p.nextResponsesSeq(),
		"item_id": p.respReasoningID, "output_index": p.respReasoningIndex, "summary_index": 0, "text": text,
	})
	partDone, _ := jsonx.Marshal(map[string]any{
		"type": "response.reasoning_summary_part.done", "sequence_number": p.nextResponsesSeq(),
		"item_id": p.respReasoningID, "output_index": p.respReasoningIndex, "summary_index": 0,
		"part": map[string]any{"type": "summary_text", "text": text},
	})
	done, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.done", "sequence_number": p.nextResponsesSeq(),
		"output_index": p.respReasoningIndex, "item": p.responsesItemJSON(item),
	})
	return [][]byte{
		responsesSSE("response.reasoning_summary_text.done", textDone),
		responsesSSE("response.reasoning_summary_part.done", partDone),
		responsesSSE("response.output_item.done", done),
	}
}

func (p *claudeProjection) responsesEnsureMessage() [][]byte {
	if p.respMessageOpen {
		return nil
	}
	p.respMessageOpen = true
	p.respMessageIndex = p.nextResponsesOutput()
	p.respSpan++
	p.respMessageID = fmt.Sprintf("msg_%s_%d", p.messageID, p.respSpan)
	added, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.added", "sequence_number": p.nextResponsesSeq(),
		"output_index": p.respMessageIndex,
		"item":         map[string]any{"id": p.respMessageID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}},
	})
	part, _ := jsonx.Marshal(map[string]any{
		"type": "response.content_part.added", "sequence_number": p.nextResponsesSeq(),
		"item_id": p.respMessageID, "output_index": p.respMessageIndex, "content_index": 0,
		"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
	})
	return [][]byte{responsesSSE("response.output_item.added", added), responsesSSE("response.content_part.added", part)}
}

func (p *claudeProjection) responsesCloseMessage() [][]byte {
	if !p.respMessageOpen {
		return nil
	}
	p.respMessageOpen = false
	text := p.respText.String()
	p.respText.Reset()
	annotations := p.responsesTextAnnotations()
	item := responsesOutputItem{
		Index: p.respMessageIndex, ID: p.respMessageID, Type: "message", Status: "completed", Role: "assistant",
		Text: text, Annotations: annotations,
	}
	p.storeResponsesItem(item)
	textDone, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_text.done", "sequence_number": p.nextResponsesSeq(),
		"item_id": p.respMessageID, "output_index": p.respMessageIndex, "content_index": 0, "text": text,
	})
	part := map[string]any{"type": "output_text", "text": text, "annotations": annotations}
	if annotations == nil {
		part["annotations"] = []any{}
	}
	partDone, _ := jsonx.Marshal(map[string]any{
		"type": "response.content_part.done", "sequence_number": p.nextResponsesSeq(),
		"item_id": p.respMessageID, "output_index": p.respMessageIndex, "content_index": 0, "part": part,
	})
	done, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.done", "sequence_number": p.nextResponsesSeq(),
		"output_index": p.respMessageIndex, "item": p.responsesItemJSON(item),
	})
	return [][]byte{
		responsesSSE("response.output_text.done", textDone),
		responsesSSE("response.content_part.done", partDone),
		responsesSSE("response.output_item.done", done),
	}
}

func (p *claudeProjection) responsesSearchStarted(search *HostedSearch) [][]byte {
	if search == nil {
		return nil
	}
	id := "ws_" + hostedSearchToolID(search)
	if p.respSearchIndex == nil {
		p.respSearchIndex = map[string]int{}
	}
	if _, exists := p.respSearchIndex[id]; exists {
		return nil
	}
	chunks := p.responsesStart()
	chunks = append(chunks, p.responsesCloseReasoning()...)
	chunks = append(chunks, p.responsesCloseMessage()...)
	index := p.nextResponsesOutput()
	p.respSearchIndex[id] = index
	item := map[string]any{
		"id": id, "type": "web_search_call", "status": "in_progress",
		"action": hostedSearchResponsesAction(search),
	}
	added, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.added", "sequence_number": p.nextResponsesSeq(),
		"output_index": index, "item": item,
	})
	inProgress, _ := jsonx.Marshal(map[string]any{
		"type": "response.web_search_call.in_progress", "sequence_number": p.nextResponsesSeq(),
		"item_id": id, "output_index": index,
	})
	searching, _ := jsonx.Marshal(map[string]any{
		"type": "response.web_search_call.searching", "sequence_number": p.nextResponsesSeq(),
		"item_id": id, "output_index": index,
	})
	return append(chunks,
		responsesSSE("response.output_item.added", added),
		responsesSSE("response.web_search_call.in_progress", inProgress),
		responsesSSE("response.web_search_call.searching", searching),
	)
}

func (p *claudeProjection) responsesSearchCompleted(search *HostedSearch) [][]byte {
	chunks := p.responsesSearchStarted(search)
	if search == nil {
		return chunks
	}
	id := "ws_" + hostedSearchToolID(search)
	index := p.respSearchIndex[id]
	item := responsesOutputItem{
		Index: index, ID: id, Type: "web_search_call", Status: "completed", Action: hostedSearchResponsesAction(search),
	}
	p.storeResponsesItem(item)
	p.citations = append(p.citations, hostedSearchCitations(search)...)
	completed, _ := jsonx.Marshal(map[string]any{
		"type": "response.web_search_call.completed", "sequence_number": p.nextResponsesSeq(),
		"item_id": id, "output_index": index,
	})
	done, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.done", "sequence_number": p.nextResponsesSeq(),
		"output_index": index, "item": p.responsesItemJSON(item),
	})
	return append(chunks,
		responsesSSE("response.web_search_call.completed", completed),
		responsesSSE("response.output_item.done", done),
	)
}

func (p *claudeProjection) responsesFunctionCall(call ToolCall) [][]byte {
	index := p.nextResponsesOutput()
	arguments := call.Arguments
	if arguments == "" {
		arguments = "{}"
	}
	id := "fc_" + call.ToolCallID
	item := responsesOutputItem{
		Index: index, ID: id, Type: "function_call", Status: "completed",
		CallID: call.ToolCallID, Name: call.Name, Arguments: arguments,
	}
	p.storeResponsesItem(item)
	added, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.added", "sequence_number": p.nextResponsesSeq(),
		"output_index": index,
		"item": map[string]any{
			"id": id, "type": "function_call", "status": "in_progress",
			"call_id": call.ToolCallID, "name": call.Name, "arguments": "",
		},
	})
	delta, _ := jsonx.Marshal(map[string]any{
		"type": "response.function_call_arguments.delta", "sequence_number": p.nextResponsesSeq(),
		"item_id": id, "output_index": index, "delta": arguments,
	})
	argsDone, _ := jsonx.Marshal(map[string]any{
		"type": "response.function_call_arguments.done", "sequence_number": p.nextResponsesSeq(),
		"item_id": id, "output_index": index, "arguments": arguments,
	})
	done, _ := jsonx.Marshal(map[string]any{
		"type": "response.output_item.done", "sequence_number": p.nextResponsesSeq(),
		"output_index": index, "item": p.responsesItemJSON(item),
	})
	return [][]byte{
		responsesSSE("response.output_item.added", added),
		responsesSSE("response.function_call_arguments.delta", delta),
		responsesSSE("response.function_call_arguments.done", argsDone),
		responsesSSE("response.output_item.done", done),
	}
}

func (p *claudeProjection) responsesFinish(status, incompleteReason string) [][]byte {
	envelope := p.responsesEnvelope(status, p.responsesOutputJSON())
	if status == "incomplete" {
		reason := incompleteReason
		if reason == "" {
			reason = "max_output_tokens"
		}
		envelope["incomplete_details"] = map[string]any{"reason": reason}
	}
	payload, _ := jsonx.Marshal(map[string]any{
		"type": "response.completed", "sequence_number": p.nextResponsesSeq(),
		"response": envelope,
	})
	return [][]byte{responsesSSE("response.completed", payload)}
}

func (p *claudeProjection) responsesNonStream() ([]byte, error) {
	return jsonx.Marshal(p.responsesEnvelope("completed", p.responsesOutputJSON()))
}

func (p *claudeProjection) responsesEnvelope(status string, output []any) map[string]any {
	if output == nil {
		output = []any{}
	}
	envelope := map[string]any{
		"id": p.messageID, "object": "response", "created_at": p.respCreatedAt, "status": status,
		"model": p.model, "output": output, "error": nil,
	}
	if status == "in_progress" {
		// Official streaming created/in_progress examples set usage=null.
		// A partial usage object without input_tokens_details fails strict clients.
		envelope["usage"] = nil
		return envelope
	}
	envelope["usage"] = p.responsesUsage()
	return envelope
}

func (p *claudeProjection) responsesUsage() map[string]any {
	usage := map[string]any{
		"input_tokens": 0,
		"input_tokens_details": map[string]any{
			"cached_tokens":      0,
			"cache_write_tokens": 0,
		},
		"output_tokens": p.outputTokens(),
		"output_tokens_details": map[string]any{
			"reasoning_tokens": 0,
		},
		"total_tokens": p.outputTokens(),
	}
	if measured, ok := p.facts.measuredTurn(); ok {
		usage["input_tokens"] = measured.TotalInput
		usage["input_tokens_details"] = map[string]any{"cached_tokens": measured.CacheRead, "cache_write_tokens": measured.CacheWrite}
		usage["output_tokens_details"] = map[string]any{"reasoning_tokens": measured.Reasoning}
		usage["total_tokens"] = measured.TotalInput + measured.Output
	} else if p.facts.measuredEnabled {
		input := p.estimatedInputTokens()
		usage["input_tokens"] = input
		usage["total_tokens"] = input + p.outputTokens()
	}
	p.attachPrivateUsage(usage)
	return usage
}

func (p *claudeProjection) storeResponsesItem(item responsesOutputItem) {
	if p.respByIndex == nil {
		p.respByIndex = map[int]responsesOutputItem{}
	}
	p.respByIndex[item.Index] = item
}

func (p *claudeProjection) responsesOutputJSON() []any {
	out := make([]any, 0, len(p.respByIndex))
	for i := 0; i < p.responsesOut; i++ {
		item, ok := p.respByIndex[i]
		if !ok {
			continue
		}
		out = append(out, p.responsesItemJSON(item))
	}
	return out
}

func (p *claudeProjection) responsesItemJSON(item responsesOutputItem) map[string]any {
	switch item.Type {
	case "reasoning":
		return map[string]any{
			"id": item.ID, "type": "reasoning", "status": item.Status,
			"summary": []any{map[string]any{"type": "summary_text", "text": item.Text}},
		}
	case "web_search_call":
		return map[string]any{
			"id": item.ID, "type": "web_search_call", "status": item.Status, "action": item.Action,
		}
	case "function_call":
		return map[string]any{
			"id": item.ID, "type": "function_call", "status": item.Status,
			"call_id": item.CallID, "name": item.Name, "arguments": item.Arguments,
		}
	default:
		content := []any{map[string]any{
			"type": "output_text", "text": item.Text, "annotations": item.Annotations,
		}}
		if item.Annotations == nil {
			content[0].(map[string]any)["annotations"] = []any{}
		}
		return map[string]any{
			"id": item.ID, "type": "message", "status": item.Status, "role": "assistant", "content": content,
		}
	}
}

func (p *claudeProjection) responsesTextAnnotations() []any {
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
			"type": "url_citation", "url": url, "title": stringValue(block["title"]),
			"start_index": 0, "end_index": 0,
		})
	}
	return annotations
}

func (p *claudeProjection) responsesEvent(event string, payload map[string]any) [][]byte {
	data, _ := jsonx.Marshal(payload)
	return [][]byte{responsesSSE(event, data)}
}
