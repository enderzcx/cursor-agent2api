package cursoragentv1

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

const inferenceStreamPath = "/aiserver.v1.InferenceService/Stream"

type InferenceRunner struct {
	hosted inferenceHostedClient
}

type inferenceSegmentOutcome struct {
	Checkpoint []byte
	ToolSeen   bool
	Continue   bool
}

func NewInferenceRunner() *InferenceRunner {
	return &InferenceRunner{hosted: newCursorInferenceHostedClient()}
}

func (r *InferenceRunner) Run(ctx context.Context, request RunRequest, emit func(Event)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(request.AccessToken) == "" || strings.TrimSpace(request.ClientVersion) == "" {
		return fmt.Errorf("Cursor Sand access token and client version are required")
	}
	if emit == nil {
		emit = func(Event) {}
	}
	hostedUses := &inferenceHostedUseTracker{}
	hostedSteps := 0
	for {
		prepared, acceptedResult, err := prepareInferenceRun(ctx, request)
		if err != nil {
			return err
		}
		emitted := false
		observing := func(event Event) {
			if event.Type != EventToolResultAccepted {
				emitted = true
			}
			emit(event)
		}
		outcome, err := runInferenceSegment(ctx, prepared, observing, acceptedResult, r.hosted, hostedUses)
		if err != nil && !emitted && !prepared.dropReasoningSignatures && isForeignReasoningSignatureRejection(err) && historyCarriesForwardedSignature(prepared.HistoryMessages) {
			// Safety net for A/B case B: a signature we believed Cursor minted was
			// still refused. Remember the rejection and replay this turn text-only
			// (case C) exactly once; the accepted tool result was already announced.
			markHistorySignaturesRejected(prepared.HistoryMessages)
			prepared.dropReasoningSignatures = true
			sandSignatureRetries.Add(1)
			outcome, err = runInferenceSegment(ctx, prepared, observing, nil, r.hosted, hostedUses)
		}
		if err != nil {
			return err
		}
		if outcome.ToolSeen && prepared.HistoryRequestKey != "" {
			return nil
		}
		if !outcome.ToolSeen && !outcome.Continue {
			return nil
		}
		if outcome.Continue {
			hostedSteps++
			if hostedSteps >= maxInferenceHostedSteps {
				return fmt.Errorf("Cursor Sand hosted web loop exceeded %d inference steps", maxInferenceHostedSteps)
			}
		}
		request = prepared
		request.InitialCheckpoint = append([]byte(nil), outcome.Checkpoint...)
		request.HistoryMessages = nil
		request.SystemText = ""
		request.UserText = ""
		request.Resume = outcome.ToolSeen
	}
}

func runInferenceSegment(ctx context.Context, request RunRequest, emit func(Event), acceptedResult *ToolResult, hosted inferenceHostedClient, hostedUses *inferenceHostedUseTracker) (inferenceSegmentOutcome, error) {
	payload, err := encodeInferenceRequest(request)
	if err != nil {
		return inferenceSegmentOutcome{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, defaultInferenceBaseURL+inferenceStreamPath, bytes.NewReader(frameConnect(payload)))
	if err != nil {
		return inferenceSegmentOutcome{}, fmt.Errorf("build Cursor Sand request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+strings.TrimSpace(request.AccessToken))
	httpRequest.Header.Set("Content-Type", "application/connect+proto")
	httpRequest.Header.Set("Connect-Protocol-Version", "1")
	httpRequest.Header.Set("Connect-Accept-Encoding", "gzip")
	httpRequest.Header.Set("X-Cursor-Client-Type", runtimeSand)
	httpRequest.Header.Set("X-Cursor-Client-Version", strings.TrimSpace(request.ClientVersion))
	httpRequest.Header.Set("X-Cursor-Streaming", "true")
	httpRequest.Header.Set("X-Ghost-Mode", "true")
	httpRequest.Header.Set("User-Agent", "connect-es/1.6.1")
	httpRequest.Header.Set("Cookie", cursorRequestCookie(request.AccessToken))
	transport, err := cursorHTTP2Transport(request.ProxyURL, nil)
	if err != nil {
		return inferenceSegmentOutcome{}, err
	}
	response, err := (&http.Client{Transport: transport}).Do(httpRequest)
	if err != nil {
		transport.CloseIdleConnections()
		return inferenceSegmentOutcome{}, fmt.Errorf("Cursor Sand HTTP/2 request: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
		transport.CloseIdleConnections()
		return inferenceSegmentOutcome{}, &httpStatusError{code: response.StatusCode, text: fmt.Sprintf("Cursor Sand upstream HTTP %d", response.StatusCode)}
	}
	if acceptedResult != nil {
		copy := *acceptedResult
		emit(Event{Type: EventToolResultAccepted, ToolResult: &copy})
	}
	outcome := inferenceSegmentOutcome{}
	err = consumeInferenceStream(ctx, response.Body, request, emit, &outcome, hosted, hostedUses)
	_ = response.Body.Close()
	transport.CloseIdleConnections()
	return outcome, err
}

func consumeInferenceStream(ctx context.Context, reader io.Reader, request RunRequest, emit func(Event), outcome *inferenceSegmentOutcome, hosted inferenceHostedClient, hostedUses *inferenceHostedUseTracker) error {
	buffer := bytes.Buffer{}
	chunk := make([]byte, 32*1024)
	toolCalls := map[string]*inferenceToolCall{}
	callerToolSeen := false
	var usage *inferenceUsage
	var assistantText strings.Builder
	var reasoningText strings.Builder
	reasoningSignature := ""
	thinkingOpen := false
	// Cursor streams thinking_part deltas and then one part that carries only
	// the provider signature; is_final is not reliably set. Treat the signature
	// frame, the first non-thinking part, or stream end as block completion.
	closeThinking := func() {
		if !thinkingOpen {
			return
		}
		thinkingOpen = false
		emit(Event{Type: EventThinkingCompleted, Signature: reasoningSignature})
	}
	completedTools := []inferenceToolCall{}
	hostedCalls := []inferenceToolCall{}
	for {
		count, readErr := reader.Read(chunk)
		if count > 0 {
			_, _ = buffer.Write(chunk[:count])
			if buffer.Len() > maxConnectPayloadSize+connectHeaderSize {
				return errExceedsConnectLimit("Cursor Sand response frame")
			}
			for {
				flags, payload, consumed, complete, parseErr := parseConnectFrame(buffer.Bytes())
				if parseErr != nil {
					return parseErr
				}
				if !complete {
					break
				}
				buffer.Next(consumed)
				if flags&connectCompressionFlag != 0 {
					decompressed, decompressErr := decompressConnectPayload(payload)
					if decompressErr != nil {
						return decompressErr
					}
					payload = decompressed
				}
				if flags&connectEndStreamFlag != 0 {
					if err := parseConnectEndStream(payload); err != nil {
						return err
					}
					closeThinking()
					if len(toolCalls) > 0 {
						return fmt.Errorf("Cursor Sand ended with %d incomplete tool call(s)", len(toolCalls))
					}
					// tool_choice=any/tool without a tool call: return the text the
					// model produced (Kiro behavior) instead of failing the turn.
					hostedResults := make([]inferenceHostedToolResult, 0, len(hostedCalls))
					for _, call := range hostedCalls {
						result, search, executeErr := executeInferenceHostedCall(ctx, hosted, hostedUses, request, call)
						if executeErr != nil {
							return executeErr
						}
						emit(Event{Type: EventHostedSearchCall})
						emit(Event{Type: EventHostedSearchStarted, HostedSearch: cloneHostedSearch(search)})
						emit(Event{Type: EventHostedSearchCompleted, HostedSearch: cloneHostedSearch(search)})
						hostedResults = append(hostedResults, result)
					}
					if usage != nil {
						observation := &UsageObservation{Kind: UsageKindTurnEnded, Input: countValue(usage.Input), Output: countValue(usage.Output)}
						if usage.Extended {
							observation.CacheRead = countValue(usage.CacheRead)
							observation.CacheWrite = countValue(usage.CacheWrite)
						}
						emit(Event{Type: EventUsageObservation, Usage: observation})
					}
					if request.HistoryRequestKey == "" || len(hostedCalls) > 0 {
						checkpoint, err := encodeInferenceCheckpoint(request, assistantText.String(), inferenceReasoning{Text: reasoningText.String(), Signature: reasoningSignature}, completedTools, hostedResults...)
						if err != nil {
							return err
						}
						emit(Event{Type: EventCheckpoint, Checkpoint: checkpoint})
						if outcome != nil {
							outcome.Checkpoint = append([]byte(nil), checkpoint...)
						}
					}
					if callerToolSeen {
						emit(Event{Type: EventTurnEnded})
						if outcome != nil {
							outcome.ToolSeen = true
						}
						if request.HistoryRequestKey == "" {
							return nil
						}
					}
					if len(hostedCalls) > 0 {
						if outcome != nil {
							outcome.Continue = true
						}
						return nil
					}
					if err := hostedUses.validateRequired(request); err != nil {
						return err
					}
					emit(Event{Type: EventDone})
					return nil
				}
				part, err := decodeInferenceResponse(payload)
				if err != nil {
					return err
				}
				if part.ErrorCode != "" || part.ErrorText != "" {
					return &connectError{Code: valueOrDefault(part.ErrorCode, "unknown"), Message: valueOrDefault(part.ErrorText, "Cursor Sand inference failed")}
				}
				if part.Text != "" {
					closeThinking()
					assistantText.WriteString(part.Text)
					emit(Event{Type: EventText, Text: part.Text})
				}
				if part.Thinking != "" {
					thinkingOpen = true
					reasoningText.WriteString(part.Thinking)
					emit(Event{Type: EventThinking, Text: part.Thinking})
				}
				if part.ThinkSignature != "" {
					reasoningSignature = part.ThinkSignature
					rememberMintedSignature(reasoningSignature)
					thinkingOpen = true
					closeThinking()
				}
				if part.ThinkFinal {
					closeThinking()
				}
				if part.Tool != nil {
					closeThinking()
				}
				if part.Usage != nil && (usage == nil || part.Usage.Extended || !usage.Extended) {
					copy := *part.Usage
					usage = &copy
				}
				if part.Tool != nil {
					id := strings.TrimSpace(part.Tool.ID)
					if id == "" {
						return fmt.Errorf("Cursor Sand tool call is missing its id")
					}
					pending := toolCalls[id]
					if pending == nil {
						if len(toolCalls) >= maxPendingTools {
							return fmt.Errorf("Cursor Sand pending tools exceed %d", maxPendingTools)
						}
						if request.MaxParallelTools > 0 && len(toolCalls) >= request.MaxParallelTools {
							return fmt.Errorf("Cursor Sand returned parallel caller tools when the client disabled them")
						}
						pending = &inferenceToolCall{ID: id}
						toolCalls[id] = pending
					}
					if part.Tool.Name != "" {
						pending.Name = part.Tool.Name
					}
					pending.Arguments = mergeInferenceToolArguments(pending.Arguments, part.Tool.Arguments)
					if part.ToolFinal {
						if strings.TrimSpace(pending.Name) == "" {
							return fmt.Errorf("Cursor Sand tool call %q is missing its name", id)
						}
						if !inferenceToolDeclared(request.Tools, pending.Name) {
							return fmt.Errorf("Cursor Sand returned undeclared tool %q", pending.Name)
						}
						completedTools = append(completedTools, *pending)
						if inferenceHostedKind(pending.Name) != "" {
							hostedCalls = append(hostedCalls, *pending)
						} else {
							callerToolSeen = true
							emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: pending.ID, Name: pending.Name, Arguments: pending.Arguments}})
						}
						delete(toolCalls, id)
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return fmt.Errorf("Cursor Sand stream closed without Connect terminal trailer")
			}
			return fmt.Errorf("read Cursor Sand response: %w", readErr)
		}
	}
}

func mergeInferenceToolArguments(current, next string) string {
	if next == "" {
		return current
	}
	if current == "" || jsonx.Valid([]byte(next)) || strings.HasPrefix(next, current) {
		return next
	}
	return current + next
}

func inferenceToolDeclared(tools []ToolDefinition, name string) bool {
	name = strings.TrimSpace(name)
	for _, tool := range tools {
		if strings.TrimSpace(tool.Name) == name {
			return true
		}
	}
	return false
}

func countValue(value int64) ObservedCount { return ObservedCount{Present: true, Value: value} }

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}
