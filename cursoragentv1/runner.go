package cursoragentv1

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultFrameIdleTimeout  = 90 * time.Second
	maxBlobSize              = 16 << 20
	maxBlobStoreSize         = 32 << 20
)

type EventType int

const (
	EventText EventType = iota + 1
	EventThinking
	EventUsage
	EventCheckpoint
	EventToolCall
	EventToolResultAccepted
	EventTurnEnded
	EventHostedSearchCall
	EventDone
	EventHostedSearchStarted
	EventHostedSearchCompleted
	EventThinkingCompleted
	EventUsageObservation
)

type HostedSearchKind string

const (
	HostedSearchKindWebSearch HostedSearchKind = "web_search"
	HostedSearchKindWebFetch  HostedSearchKind = "web_fetch"
	HostedSearchKindExaSearch HostedSearchKind = "exa_search"
	HostedSearchKindExaFetch  HostedSearchKind = "exa_fetch"
)

type HostedSearchReference struct {
	Title string
	URL   string
	Chunk string
}

type HostedSearch struct {
	CallID     string
	Kind       HostedSearchKind
	Query      string
	URL        string
	ArgsJSON   string
	References []HostedSearchReference
	Content    string
	Error      string
}

type ToolDefinition struct {
	Name        string
	Description string
	InputSchema []byte
}

type ToolCall struct {
	MessageID  uint32
	ExecID     string
	ToolCallID string
	Name       string
	Arguments  string
}

type ToolResult struct {
	ToolCallID string
	Content    string
	IsError    bool
	// Media carries screenshots/documents returned by caller tools (for
	// example chrome-devtools) so they reach the model as native images.
	Media []MediaPart
}

type Event struct {
	Type EventType
	Text string
	// Signature is the provider thinking signature attached to a completed
	// reasoning block (Sand thinking_part.signature). Empty for Agent v1.
	Signature string
	// ProgressEvents preserves the original thinking-delta count when a
	// completed snapshot coalesces adjacent deltas. Zero means one live event.
	ProgressEvents int
	Tokens         int64
	Usage          *UsageObservation
	Checkpoint     []byte
	Blobs          map[string][]byte
	ToolCall       *ToolCall
	ToolResult     *ToolResult
	HostedSearch   *HostedSearch
}

type RunRequest struct {
	RuntimeProfile    string
	HistoryRequestKey string // finite full-history generation; never sent upstream
	BaseURL           string
	ProxyURL          string
	AccessToken       string
	ClientVersion     string
	RouteChannelID    int
	RouteUserID       int
	Model             string
	ConversationID    string
	UserText          string
	// UserMedia holds images/documents attached to the active user turn.
	UserMedia []MediaPart
	// ThinkingEnabled mirrors the caller's Anthropic thinking.type=enabled.
	ThinkingEnabled bool
	// dropReasoningSignatures is set by the Sand runner's single retry after a
	// foreign-signature rejection; history reasoning is then sent text-only.
	dropReasoningSignatures bool
	SystemText              string
	Tools                   []ToolDefinition
	HistoryMessages         [][]byte
	AllowHostedSearch       bool
	AllowHostedFetch        bool
	RequireHostedSearch     bool
	RequireHostedFetch      bool
	RequireHostedTool       bool
	HostedSearchMaxUses     *int
	HostedFetchMaxUses      *int
	HostedSearchDomains     hostedDomainPolicy
	HostedFetchDomains      hostedDomainPolicy
	RequireToolCall         bool
	MaxParallelTools        int
	DeleteCompletedState    bool
	FollowUpText            string
	InitialCheckpoint       []byte
	InitialBlobs            map[string][]byte
	Resume                  bool
	ToolResults             <-chan ToolResult
}

type Runner struct {
	opener            streamOpener
	heartbeatInterval time.Duration
	frameIdleTimeout  time.Duration
}

func NewRunner() *Runner {
	return &Runner{
		opener:            http2Opener{},
		heartbeatInterval: defaultHeartbeatInterval,
		frameIdleTimeout:  defaultFrameIdleTimeout,
	}
}

func newRunnerForTest(opener streamOpener, heartbeatInterval, frameIdleTimeout time.Duration) *Runner {
	return &Runner{opener: opener, heartbeatInterval: heartbeatInterval, frameIdleTimeout: frameIdleTimeout}
}

func (r *Runner) Run(ctx context.Context, request RunRequest, onEvent func(Event)) error {
	if r == nil || r.opener == nil {
		return fmt.Errorf("Cursor Agent v1 runner is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(request.UserText) == "" && !request.Resume {
		return fmt.Errorf("Cursor Agent v1 requires a non-empty user turn or resume action")
	}
	if onEvent == nil {
		onEvent = func(Event) {}
	}
	hostedSearchExecuted := false
	hostedFetchExecuted := false
	emit := func(event Event) {
		if event.Type == EventHostedSearchStarted || event.Type == EventHostedSearchCompleted {
			if hostedKindIsSearch(event.HostedSearch) {
				hostedSearchExecuted = true
			}
			if hostedKindIsFetch(event.HostedSearch) {
				hostedFetchExecuted = true
			}
		}
		onEvent(event)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	allowedTools := []string{"mcp_tool_call"}
	if request.AllowHostedSearch {
		allowedTools = append(allowedTools, "web_search_tool_call")
	}
	if request.AllowHostedFetch {
		allowedTools = append(allowedTools, "web_fetch_tool_call")
	}
	blobStore := cloneBlobs(request.InitialBlobs)
	if blobStore == nil {
		blobStore = make(map[string][]byte)
	}
	tools, err := normalizeToolDefinitions(request.Tools)
	if err != nil {
		return err
	}
	runPayload, err := encodeRunRequest(&runWireRequest{
		Model:           strings.TrimSpace(request.Model),
		ConversationID:  strings.TrimSpace(request.ConversationID),
		UserText:        request.UserText,
		UserMedia:       request.UserMedia,
		SystemText:      request.SystemText,
		Tools:           tools,
		HistoryMessages: cloneByteSlices(request.HistoryMessages),
		BlobStore:       blobStore,
		Checkpoint:      append([]byte(nil), request.InitialCheckpoint...),
		Resume:          request.Resume,
	})
	if err != nil {
		return err
	}
	if len(runPayload) > maxConnectPayloadSize {
		return errExceedsConnectLimit("Cursor Agent v1 run payload")
	}
	blobBytes, err := validateBlobStore(blobStore, maxBlobSize, maxBlobStoreSize)
	if err != nil {
		return err
	}
	upstream, err := r.opener.Open(runCtx, streamConfig{
		BaseURL:            request.BaseURL,
		ProxyURL:           request.ProxyURL,
		AccessToken:        request.AccessToken,
		ClientVersion:      request.ClientVersion,
		AllowedNativeTools: allowedTools,
	})
	if err != nil {
		return err
	}
	defer upstream.Close()
	if err := writeConnectPayload(upstream, runPayload); err != nil {
		return err
	}

	heartbeatInterval := r.heartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	idleTimeout := r.frameIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultFrameIdleTimeout
	}
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()
	go func() {
		for {
			select {
			case <-runCtx.Done():
				return
			case <-upstream.Done():
				return
			case <-heartbeat.C:
				if writeErr := writeConnectPayload(upstream, encodeHeartbeat()); writeErr != nil {
					return
				}
			}
		}
	}()

	var buffer bytes.Buffer
	turnEnded := false
	pendingTools := make(map[string]decodedWireMessage)
	streamMCP := make(map[string]decodedWireMessage)
	openHosted := make(map[string]*HostedSearch)
	toolCallSeen := false
	parkSignaled := false
	queuedResults := make(map[string]ToolResult)
	hostedUses := newHostedUseTracker()
	toolResults := request.ToolResults
	sendToolResult := func(pending decodedWireMessage, result ToolResult) error {
		if err := writeConnectPayload(upstream, encodeMCPResult(pending.ID, pending.ExecID, result)); err != nil {
			return err
		}
		delete(pendingTools, result.ToolCallID)
		if len(pendingTools) == 0 {
			parkSignaled = false
		}
		resetTimer(idle, idleTimeout)
		resultCopy := result
		emit(Event{Type: EventToolResultAccepted, ToolResult: &resultCopy})
		return nil
	}
	dataChannel := upstream.Data()
	for {
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-idle.C:
			if len(pendingTools) > 0 {
				idle.Reset(idleTimeout)
				continue
			}
			return fmt.Errorf("Cursor Agent v1 received no frames for %s", idleTimeout)
		case result, ok := <-toolResults:
			if !ok {
				toolResults = nil
				continue
			}
			result.ToolCallID = strings.TrimSpace(result.ToolCallID)
			if result.ToolCallID == "" {
				return fmt.Errorf("Cursor Agent v1 tool result id is required")
			}
			pending, exists := pendingTools[result.ToolCallID]
			if !exists {
				if len(queuedResults) >= maxPendingTools {
					return fmt.Errorf("Cursor Agent v1 queued tool results exceed %d", maxPendingTools)
				}
				queuedResults[result.ToolCallID] = result
				continue
			}
			if err := sendToolResult(pending, result); err != nil {
				return err
			}
		case chunk, ok := <-dataChannel:
			if !ok {
				if streamErr := upstream.Err(); streamErr != nil {
					return streamErr
				}
				return fmt.Errorf("Cursor Agent v1 HTTP/2 stream closed without Connect terminal trailer")
			}
			resetTimer(idle, idleTimeout)
			_, _ = buffer.Write(chunk)
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
					payload, err = decompressConnectPayload(payload)
					if err != nil {
						return err
					}
				}
				if flags&connectEndStreamFlag != 0 {
					if trailerErr := parseConnectEndStream(payload); trailerErr != nil {
						return trailerErr
					}
					if !turnEnded {
						return fmt.Errorf("Cursor Agent v1 stream ended before turnEnded")
					}
					if len(pendingTools) > 0 {
						return fmt.Errorf("Cursor Agent v1 stream ended with %d pending tool call(s)", len(pendingTools))
					}
					if len(streamMCP) > 0 {
						return fmt.Errorf("Cursor Agent v1 streamed caller tool %q without a matching exec", unmatchedStreamMCPName(streamMCP))
					}
					if len(openHosted) > 0 {
						return fmt.Errorf("Cursor Agent v1 stream ended with %d incomplete hosted search call(s)", len(openHosted))
					}
					// tool_choice=any/tool is a request the Cursor model may ignore;
					// Anthropic can guarantee it, Kiro cannot and returns the text.
					// Do the same rather than failing a turn that already streamed.
					if request.RequireHostedSearch && !request.Resume && !hostedSearchExecuted {
						return fmt.Errorf("Cursor Agent v1 completed without the required hosted search")
					}
					if request.RequireHostedFetch && !request.Resume && !hostedFetchExecuted {
						return fmt.Errorf("Cursor Agent v1 completed without the required hosted fetch")
					}
					if request.RequireHostedTool && !request.Resume && !hostedSearchExecuted && !hostedFetchExecuted {
						return fmt.Errorf("Cursor Agent v1 completed without the required hosted web tool")
					}
					emit(Event{Type: EventDone})
					return nil
				}
				message, decodeErr := decodeServerMessage(payload)
				if decodeErr != nil {
					return decodeErr
				}
				switch message.Type {
				case wireTextDelta:
					if message.Text != "" {
						emit(Event{Type: EventText, Text: message.Text})
					}
				case wireThinkingDelta:
					if message.Text != "" {
						emit(Event{Type: EventThinking, Text: message.Text})
					}
				case wireThinkingCompleted:
					emit(Event{Type: EventThinkingCompleted})
				case wireToolCallStarted, wireToolCallDelta, wireToolCallCompleted:
					if err := applyHostedSearchMessage(request, message, openHosted, emit); err != nil {
						return err
					}
				case wireIgnored:
				case wireStreamMCP:
					if err := recordStreamMCP(request.Tools, message, pendingTools, streamMCP); err != nil {
						return err
					}
				case wireAllowlistPrecheck:
					response, encodeErr := encodeAllowlistPrecheck(message.ID, message.ExecID, message.AllowlistKind, false)
					if encodeErr != nil {
						return encodeErr
					}
					if err := writeConnectPayload(upstream, response); err != nil {
						return err
					}
				case wireExecAbort:
					return fmt.Errorf("Cursor Agent v1 server aborted exec %d", message.ID)
				case wireTokenDelta:
					emit(Event{Type: EventUsage, Tokens: message.TokenDelta, Usage: cloneUsageObservation(message.Usage)})
				case wireCheckpoint:
					emit(Event{Type: EventCheckpoint, Checkpoint: append([]byte(nil), message.Checkpoint...), Blobs: cloneBlobs(blobStore)})
				case wireTurnEnded:
					turnEnded = true
					usage := cloneUsageObservation(message.Usage)
					if usage == nil {
						usage = &UsageObservation{Kind: UsageKindTurnEnded}
					}
					emit(Event{Type: EventUsageObservation, Usage: usage})
					if len(pendingTools) > 0 && !parkSignaled {
						parkSignaled = true
						emit(Event{Type: EventTurnEnded})
					}
				case wireKVGet:
					if err := writeConnectPayload(upstream, encodeKVGetResult(message.ID, blobStore[hexKey(message.BlobID)])); err != nil {
						return err
					}
				case wireKVSet:
					if err := storeBlob(blobStore, &blobBytes, hexKey(message.BlobID), message.BlobData, maxBlobSize, maxBlobStoreSize); err != nil {
						return err
					}
					if err := writeConnectPayload(upstream, encodeKVSetResult(message.ID)); err != nil {
						return err
					}
				case wireRequestContext:
					if err := writeConnectPayload(upstream, encodeRequestContextResult(message.ID, message.ExecID, request.SystemText, tools, requestContextPolicy{
						AllowHostedSearch:   request.AllowHostedSearch,
						AllowHostedFetch:    request.AllowHostedFetch,
						RequireToolCall:     request.RequireToolCall && !request.Resume && !toolCallSeen,
						RequireHostedSearch: request.RequireHostedSearch && !request.Resume && !hostedSearchExecuted,
						RequireHostedFetch:  request.RequireHostedFetch && !request.Resume && !hostedFetchExecuted,
						RequireHostedTool:   request.RequireHostedTool && !request.Resume && !hostedSearchExecuted && !hostedFetchExecuted,
					})); err != nil {
						return err
					}
				case wireInteractionQuery:
					var response []byte
					var encodeErr error
					hostedSearchApproved := false
					usesBefore := len(hostedUses.searchIDs) + len(hostedUses.fetchIDs)
					switch message.InteractionKind {
					case 2, 4, 5, 6, 9:
						approve, reason := hostedInteractionDecision(request, message, hostedUses)
						hostedSearchApproved = approve && (message.InteractionKind == 2 || message.InteractionKind == 5 || message.InteractionKind == 6 || message.InteractionKind == 9)
						response, encodeErr = encodeInteractionDecision(message.ID, message.InteractionKind, approve, reason)
					case 3:
						response = encodeAskQuestionRejected(message.ID, "interactive questions are unavailable through BeefAPI")
					case 7:
						response = encodeCreatePlanError(message.ID, "plan creation is unavailable through BeefAPI")
					default:
						encodeErr = fmt.Errorf("unsupported Cursor Agent v1 interaction query kind %d", message.InteractionKind)
					}
					if encodeErr != nil {
						return encodeErr
					}
					if err := writeConnectPayload(upstream, response); err != nil {
						return err
					}
					if hostedSearchApproved && len(hostedUses.searchIDs)+len(hostedUses.fetchIDs) > usesBefore {
						emit(Event{Type: EventHostedSearchCall})
					}
				case wireShell:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 2, 4, 3, "shell execution is disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireShellStream:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 14, 5, 3, "shell streaming is disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireBackgroundShell:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 16, 3, 3, "background shell execution is disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireRead:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 7, 3, 2, "workspace reads are disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireWrite:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 3, 6, 2, "workspace writes are disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireDelete:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 4, 6, 2, "workspace deletes are disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireList:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 8, 3, 2, "workspace listing is disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireGrep:
					if err := writeConnectPayload(upstream, encodeExecError(message.ID, message.ExecID, 5, "workspace search is disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireFetch:
					if err := writeConnectPayload(upstream, encodeFetchError(message.ID, message.ExecID, "arbitrary fetch is disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireDiagnostics:
					if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 9, 3, 2, "workspace diagnostics are disabled on the BeefAPI sidecar")); err != nil {
						return err
					}
				case wireMCP:
					if toolResults == nil {
						if err := writeConnectPayload(upstream, encodeExecRejected(message.ID, message.ExecID, 11, 3, 1, "caller-tool continuation is unavailable")); err != nil {
							return err
						}
						continue
					}
					message.ToolCallID = strings.TrimSpace(message.ToolCallID)
					if message.ToolCallID == "" {
						return fmt.Errorf("Cursor Agent v1 MCP call is missing tool_call_id")
					}
					delete(streamMCP, message.ToolCallID)
					if _, exists := pendingTools[message.ToolCallID]; !exists && len(pendingTools) >= maxPendingTools {
						return fmt.Errorf("Cursor Agent v1 pending tools exceed %d", maxPendingTools)
					}
					if _, exists := pendingTools[message.ToolCallID]; !exists && request.MaxParallelTools > 0 && len(pendingTools) >= request.MaxParallelTools {
						return fmt.Errorf("Cursor Agent v1 returned parallel caller tools when the client disabled them")
					}
					toolCallSeen = true
					pendingTools[message.ToolCallID] = *message
					if queued, exists := queuedResults[message.ToolCallID]; exists {
						delete(queuedResults, message.ToolCallID)
						if err := sendToolResult(*message, queued); err != nil {
							return err
						}
						continue
					}
					toolCall := &ToolCall{
						MessageID:  message.ID,
						ExecID:     message.ExecID,
						ToolCallID: message.ToolCallID,
						Name:       message.ToolName,
						Arguments:  message.ToolArguments,
					}
					emit(Event{Type: EventToolCall, ToolCall: toolCall})
					if request.HistoryRequestKey != "" {
						// Caller execution belongs to the next independent request.
						emit(Event{Type: EventDone})
						return nil
					}
					if request.MaxParallelTools == 1 && !parkSignaled {
						parkSignaled = true
						emit(Event{Type: EventTurnEnded})
					}
				case wireUnsupportedExec:
					detail := strings.TrimSpace(message.Text)
					if detail == "" {
						detail = "unknown exec variant"
					}
					return fmt.Errorf("Cursor Agent v1 server requested an unsupported exec operation: %s", detail)
				}
			}
		}
	}
}

// recordStreamMCP correlates transcript MCP announcements with the exec park
// path. Exec MCP is the only reply-required caller-tool signal. Catalog-matched
// stream frames wait for that exec; leftover catalog announcements fail closed
// so a caller tool cannot vanish. Uncatalogued Cursor-internal MCP is ignored.
// Native shell/fs stay rejected on the exec path, not here.
func recordStreamMCP(tools []ToolDefinition, message *decodedWireMessage, pending, streamed map[string]decodedWireMessage) error {
	if message == nil {
		return fmt.Errorf("Cursor Agent v1 streamed MCP frame is missing")
	}
	name := strings.TrimSpace(message.ToolName)
	id := strings.TrimSpace(message.ToolCallID)
	if id == "" {
		id = strings.TrimSpace(message.CallID)
		message.ToolCallID = id
	}
	if name == "" || id == "" {
		// Start/delta notifications can be partial. Wait for the complete
		// reply-required exec instead of rejecting or dispatching a fragment.
		return nil
	}
	if !catalogHasTool(tools, name) {
		return nil
	}
	if _, alreadyExec := pending[id]; alreadyExec {
		return nil
	}
	streamed[id] = *message
	return nil
}

func catalogHasTool(tools []ToolDefinition, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func hostedKindIsSearch(search *HostedSearch) bool {
	if search == nil {
		return false
	}
	switch search.Kind {
	case HostedSearchKindWebSearch, HostedSearchKindExaSearch:
		return true
	default:
		return false
	}
}

func hostedKindIsFetch(search *HostedSearch) bool {
	if search == nil {
		return false
	}
	switch search.Kind {
	case HostedSearchKindWebFetch, HostedSearchKindExaFetch:
		return true
	default:
		return false
	}
}

func hostedInteractionIsSearch(kind protowire.Number) bool {
	return kind == 2 || kind == 5
}

func hostedInteractionIsFetch(kind protowire.Number) bool {
	return kind == 6 || kind == 9
}

type hostedUseTracker struct {
	searchIDs map[string]struct{}
	fetchIDs  map[string]struct{}
}

func newHostedUseTracker() *hostedUseTracker {
	return &hostedUseTracker{searchIDs: map[string]struct{}{}, fetchIDs: map[string]struct{}{}}
}

func hostedUseID(message *decodedWireMessage) string {
	if message == nil {
		return ""
	}
	if id := strings.TrimSpace(message.ToolCallID); id != "" {
		return id
	}
	return fmt.Sprintf("anon:%d:%d", message.InteractionKind, message.ID)
}

func (t *hostedUseTracker) allow(kind protowire.Number, id string, max *int) bool {
	if t == nil {
		return false
	}
	set := t.searchIDs
	if hostedInteractionIsFetch(kind) {
		set = t.fetchIDs
	} else if !hostedInteractionIsSearch(kind) {
		return false
	}
	if _, seen := set[id]; seen {
		return true
	}
	if max != nil && len(set) >= *max {
		return false
	}
	set[id] = struct{}{}
	return true
}

func hostedInteractionDecision(request RunRequest, message *decodedWireMessage, uses *hostedUseTracker) (bool, string) {
	const disabled = "capability is not enabled by BeefAPI"
	if message == nil {
		return false, disabled
	}
	kind := message.InteractionKind
	if kind == 4 {
		return false, disabled
	}
	id := hostedUseID(message)
	switch {
	case hostedInteractionIsSearch(kind):
		if !request.AllowHostedSearch {
			return false, disabled
		}
		if !uses.allow(kind, id, request.HostedSearchMaxUses) {
			return false, "hosted search exceeded max_uses"
		}
		return true, ""
	case hostedInteractionIsFetch(kind):
		if !request.AllowHostedFetch {
			return false, disabled
		}
		if !uses.allow(kind, id, request.HostedFetchMaxUses) {
			return false, "hosted fetch exceeded max_uses"
		}
		return true, ""
	default:
		return false, disabled
	}
}

func unmatchedStreamMCPName(streamed map[string]decodedWireMessage) string {
	for _, message := range streamed {
		if name := strings.TrimSpace(message.ToolName); name != "" {
			return name
		}
	}
	return "unknown"
}

func applyHostedSearchMessage(request RunRequest, message *decodedWireMessage, open map[string]*HostedSearch, onEvent func(Event)) error {
	if message == nil || message.HostedSearch == nil {
		return fmt.Errorf("Cursor Agent v1 hosted search frame is missing its payload")
	}
	searchKind := hostedKindIsSearch(message.HostedSearch)
	fetchKind := hostedKindIsFetch(message.HostedSearch)
	if !searchKind && !fetchKind {
		return fmt.Errorf("Cursor Agent v1 hosted web tool kind is unsupported")
	}
	if searchKind && !request.AllowHostedSearch {
		return fmt.Errorf("Cursor Agent v1 hosted search is not enabled for this request")
	}
	if fetchKind && !request.AllowHostedFetch {
		return fmt.Errorf("Cursor Agent v1 hosted fetch is not enabled for this request")
	}
	search := cloneHostedSearch(message.HostedSearch)
	id := strings.TrimSpace(search.CallID)
	if id == "" {
		id = strings.TrimSpace(message.CallID)
		search.CallID = id
	}
	if id == "" {
		return fmt.Errorf("Cursor Agent v1 hosted search is missing call_id")
	}
	previous := open[id]
	previouslyReady := hostedSearchArgsReady(previous)
	if previous != nil {
		search = mergeHostedSearch(previous, search)
	}
	if message.Type == wireToolCallCompleted {
		delete(open, id)
		onEvent(Event{Type: EventHostedSearchCompleted, HostedSearch: cloneHostedSearch(search)})
		return nil
	}
	open[id] = search
	if !hostedSearchArgsReady(search) {
		return nil
	}
	if previouslyReady {
		return nil
	}
	onEvent(Event{Type: EventHostedSearchStarted, HostedSearch: cloneHostedSearch(search)})
	return nil
}

func hostedSearchArgsReady(search *HostedSearch) bool {
	if search == nil {
		return false
	}
	switch search.Kind {
	case HostedSearchKindWebFetch, HostedSearchKindExaFetch:
		return strings.TrimSpace(search.URL) != "" || strings.TrimSpace(search.Query) != ""
	default:
		return strings.TrimSpace(search.Query) != ""
	}
}

func cloneHostedSearch(search *HostedSearch) *HostedSearch {
	if search == nil {
		return nil
	}
	copy := *search
	if search.References != nil {
		copy.References = append([]HostedSearchReference(nil), search.References...)
	}
	return &copy
}

func mergeHostedSearch(current, next *HostedSearch) *HostedSearch {
	merged := cloneHostedSearch(current)
	if next == nil {
		return merged
	}
	if next.Kind != "" {
		merged.Kind = next.Kind
	}
	if next.Query != "" {
		merged.Query = next.Query
	}
	if next.URL != "" {
		merged.URL = next.URL
	}
	if next.ArgsJSON != "" {
		merged.ArgsJSON = next.ArgsJSON
	}
	if next.Content != "" {
		merged.Content = next.Content
	}
	if next.Error != "" {
		merged.Error = next.Error
	}
	if len(next.References) > 0 {
		merged.References = append([]HostedSearchReference(nil), next.References...)
	}
	return merged
}

func normalizeToolDefinitions(tools []ToolDefinition) ([]ToolDefinition, error) {
	normalized := make([]ToolDefinition, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			return nil, fmt.Errorf("Cursor Agent v1 tool name is required")
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("Cursor Agent v1 duplicate tool %q", name)
		}
		seen[name] = struct{}{}
		schema := tool.InputSchema
		if len(schema) == 0 {
			schema = []byte(`{"type":"object","properties":{}}`)
		}
		var schemaObject map[string]any
		if err := jsonx.Unmarshal(schema, &schemaObject); err != nil || schemaObject == nil {
			return nil, fmt.Errorf("Cursor Agent v1 tool %q input schema must be a JSON object", name)
		}
		canonical, err := jsonx.Marshal(schemaObject)
		if err != nil {
			return nil, fmt.Errorf("encode Cursor Agent v1 tool %q input schema: %w", name, err)
		}
		normalized = append(normalized, ToolDefinition{Name: name, Description: tool.Description, InputSchema: canonical})
	}
	return normalized, nil
}

func cloneByteSlices(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index := range values {
		cloned[index] = append([]byte(nil), values[index]...)
	}
	return cloned
}

func decompressConnectPayload(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("open Cursor Agent v1 compressed frame: %w", err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, int64(maxConnectPayloadSize)+1))
	if err != nil {
		return nil, fmt.Errorf("decompress Cursor Agent v1 frame: %w", err)
	}
	if len(decoded) > maxConnectPayloadSize {
		return nil, errExceedsConnectLimit("decompressed Cursor Agent v1 frame")
	}
	return decoded, nil
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func hexKey(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, current := range value {
		encoded[index*2] = digits[current>>4]
		encoded[index*2+1] = digits[current&0x0f]
	}
	return string(encoded)
}

func storeBlob(store map[string][]byte, total *int, key string, value []byte, maxSingle, maxTotal int) error {
	if key == "" {
		return fmt.Errorf("Cursor Agent v1 KV blob id is empty")
	}
	if len(value) > maxSingle {
		return fmt.Errorf("Cursor Agent v1 KV blob exceeds %d bytes", maxSingle)
	}
	previous := len(store[key])
	nextTotal := *total - previous + len(value)
	if nextTotal > maxTotal {
		return fmt.Errorf("Cursor Agent v1 KV blob store exceeds %d bytes", maxTotal)
	}
	store[key] = append([]byte(nil), value...)
	*total = nextTotal
	return nil
}

func validateBlobStore(store map[string][]byte, maxSingle, maxTotal int) (int, error) {
	total := 0
	for _, value := range store {
		if len(value) > maxSingle {
			return 0, fmt.Errorf("Cursor Agent v1 KV blob exceeds %d bytes", maxSingle)
		}
		total += len(value)
		if total > maxTotal {
			return 0, fmt.Errorf("Cursor Agent v1 KV blob store exceeds %d bytes", maxTotal)
		}
	}
	return total, nil
}
