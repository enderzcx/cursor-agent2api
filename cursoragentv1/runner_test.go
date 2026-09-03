package cursoragentv1

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeOpener struct {
	stream *fakeStream
	config streamConfig
}

func (o *fakeOpener) Open(_ context.Context, config streamConfig) (stream, error) {
	o.config = config
	return o.stream, nil
}

type fakeStream struct {
	data         chan []byte
	done         chan struct{}
	writes       chan []byte
	finishOne    sync.Once
	writeMu      sync.Mutex
	writeCount   int
	failWriteAt  int
	failWriteErr error
	errMu        sync.Mutex
	err          error
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		data:   make(chan []byte, 16),
		done:   make(chan struct{}),
		writes: make(chan []byte, 32),
	}
}

func (s *fakeStream) Write(payload []byte) error {
	s.writeMu.Lock()
	s.writeCount++
	writeCount := s.writeCount
	failAt := s.failWriteAt
	failErr := s.failWriteErr
	s.writeMu.Unlock()
	if failAt > 0 && writeCount >= failAt {
		if failErr == nil {
			failErr = io.ErrClosedPipe
		}
		return failErr
	}
	select {
	case <-s.done:
		if err := s.Err(); err != nil {
			return err
		}
		return io.ErrClosedPipe
	default:
	}
	s.writes <- append([]byte(nil), payload...)
	return nil
}

func (s *fakeStream) Data() <-chan []byte   { return s.data }
func (s *fakeStream) Done() <-chan struct{} { return s.done }
func (s *fakeStream) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}
func (s *fakeStream) Close() { s.finish(nil) }
func (s *fakeStream) finish(err error) {
	s.finishOne.Do(func() {
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		close(s.data)
		close(s.done)
	})
}

func baseRunRequest() RunRequest {
	return RunRequest{
		BaseURL:        "https://agent.example",
		AccessToken:    "access-token",
		ClientVersion:  "cli-test",
		Model:          "claude-sonnet-4-6",
		ConversationID: "conversation-1",
		UserText:       "hello",
		SystemText:     "be concise",
	}
}

func TestRunAndRequestContextCarryCallerToolCatalog(t *testing.T) {
	tool := ToolDefinition{
		Name:        "lookup_weather",
		Description: "Look up weather",
		InputSchema: []byte(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
	}
	run, err := encodeRunRequest(&runWireRequest{
		Model: "claude-sonnet-4-6", ConversationID: "conversation-tools", UserText: "weather", Tools: []ToolDefinition{tool},
	})
	require.NoError(t, err)
	agentRun := decodeBytes(run, agentClientRunRequest)
	toolSet := decodeBytes(agentRun, 4)
	definition := decodeBytes(toolSet, 1)
	require.Equal(t, tool.Name, decodeString(definition, 1))
	require.Equal(t, tool.Description, decodeString(definition, 2))
	require.Empty(t, decodeBytes(definition, 3), "raw JSON must not be sent in the opaque protobuf schema field")
	require.JSONEq(t, string(tool.InputSchema), decodeString(definition, 6))
	require.Equal(t, "beefapi", decodeString(definition, 4))
	require.Equal(t, tool.Name, decodeString(definition, 5))

	contextResult := encodeRequestContextResult(9, "exec-tools", "system rule", []ToolDefinition{tool}, requestContextPolicy{})
	execMessage := decodeBytes(contextResult, agentClientExecMessage)
	requestContextResult := decodeBytes(execMessage, 10)
	success := decodeBytes(requestContextResult, 1)
	requestContext := decodeBytes(success, 1)
	contextTool := decodeBytes(requestContext, 7)
	require.Equal(t, tool.Name, decodeString(contextTool, 1))
	require.Equal(t, "system rule", decodeString(requestContext, 16))
	require.Equal(t, "linux", decodeString(decodeBytes(requestContext, 4), 1))
	require.Zero(t, decodeVarint(requestContext, 17))
	require.Zero(t, decodeVarint(requestContext, 24))

	searchContextResult := encodeRequestContextResult(10, "exec-search", "", nil, requestContextPolicy{AllowHostedSearch: true})
	searchExecMessage := decodeBytes(searchContextResult, agentClientExecMessage)
	searchRequestContextResult := decodeBytes(searchExecMessage, 10)
	searchSuccess := decodeBytes(searchRequestContextResult, 1)
	searchRequestContext := decodeBytes(searchSuccess, 1)
	require.Equal(t, uint64(1), decodeVarint(searchRequestContext, 17))
	require.Zero(t, decodeVarint(searchRequestContext, 24))

	fetchContextResult := encodeRequestContextResult(11, "exec-fetch", "", nil, requestContextPolicy{AllowHostedFetch: true})
	fetchRequestContext := decodeBytes(decodeBytes(decodeBytes(fetchContextResult, agentClientExecMessage), 10), 1)
	fetchRequestContext = decodeBytes(fetchRequestContext, 1)
	require.Zero(t, decodeVarint(fetchRequestContext, 17))
	require.Equal(t, uint64(1), decodeVarint(fetchRequestContext, 24))
}

func TestColdStartCarriesRepeatedHistoryBlobsWithoutSystemBlob(t *testing.T) {
	first := []byte(`{"role":"user","content":"first"}`)
	second := []byte(`{"role":"assistant","content":"second"}`)
	blobs := map[string][]byte{}
	run, err := encodeRunRequest(&runWireRequest{
		Model: "claude-sonnet-4-6", ConversationID: "history", UserText: "third", SystemText: "system-only-in-context",
		HistoryMessages: [][]byte{first, second}, BlobStore: blobs,
	})
	require.NoError(t, err)
	conversationState := decodeBytes(decodeBytes(run, agentClientRunRequest), 1)
	rootIDs := repeatedBytes(conversationState, 1)
	require.Len(t, rootIDs, 2)
	require.Len(t, blobs, 2)
	for _, blob := range blobs {
		require.NotContains(t, string(blob), "system-only-in-context")
	}
}

func TestNormalizeToolDefinitionsRejectsInvalidOrDuplicateCatalog(t *testing.T) {
	_, err := normalizeToolDefinitions([]ToolDefinition{{Name: "bad", InputSchema: []byte(`[]`)}})
	require.ErrorContains(t, err, "must be a JSON object")
	_, err = normalizeToolDefinitions([]ToolDefinition{{Name: "same"}, {Name: "same"}})
	require.ErrorContains(t, err, "duplicate tool")
}

func repeatedBytes(data []byte, target protowire.Number) [][]byte {
	var values [][]byte
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return values
		}
		data = data[consumed:]
		if wireType == protowire.BytesType {
			value, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return values
			}
			if number == target {
				values = append(values, append([]byte(nil), value...))
			}
			data = data[n:]
			continue
		}
		n := protowire.ConsumeFieldValue(number, wireType, data)
		if n < 0 {
			return values
		}
		data = data[n:]
	}
	return values
}

func TestConnectFrameFragmentationAndTerminalError(t *testing.T) {
	framed := frameConnect([]byte("payload"))
	_, _, _, complete, err := parseConnectFrame(framed[:4])
	require.NoError(t, err)
	require.False(t, complete)

	flags, payload, consumed, complete, err := parseConnectFrame(framed)
	require.NoError(t, err)
	require.True(t, complete)
	require.Zero(t, flags)
	require.Equal(t, []byte("payload"), payload)
	require.Equal(t, len(framed), consumed)

	err = parseConnectEndStream([]byte(`{"error":{"code":"unavailable","message":"retry later"}}`))
	var terminal *connectError
	require.ErrorAs(t, err, &terminal)
	require.Equal(t, "unavailable", terminal.Code)
	require.Equal(t, http.StatusServiceUnavailable, terminal.StatusCode())
}

func TestConnectFrameRejectsOversizeBeforeBufferingPayload(t *testing.T) {
	header := make([]byte, connectHeaderSize)
	binary.BigEndian.PutUint32(header[1:], maxConnectPayloadSize+1)
	_, _, _, _, err := parseConnectFrame(header)
	require.EqualError(t, err, errExceedsConnectLimitSize("cursor Agent v1 Connect frame", maxConnectPayloadSize+1).Error())
}

func TestWriteConnectPayloadRejectsEveryOversizedResponseShape(t *testing.T) {
	tests := []struct {
		name    string
		payload func() []byte
	}{
		{
			name: "tool result",
			payload: func() []byte {
				return encodeMCPResult(1, "exec", ToolResult{ToolCallID: "tool", Content: strings.Repeat("x", maxConnectPayloadSize)})
			},
		},
		{
			name: "maximum blob plus envelope",
			payload: func() []byte {
				return encodeKVGetResult(1, make([]byte, maxBlobSize))
			},
		},
		{
			name: "request context",
			payload: func() []byte {
				return encodeRequestContextResult(1, "exec", strings.Repeat("x", maxConnectPayloadSize), nil, requestContextPolicy{})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := newFakeStream()
			payload := test.payload()
			require.Greater(t, len(payload), maxConnectPayloadSize)
			require.EqualError(t, writeConnectPayload(upstream, payload), errExceedsConnectLimit("Cursor Agent v1 outbound Connect payload").Error())
			require.Zero(t, upstream.writeCount)
		})
	}
}

func TestDecodeRejectsOverflowIDsRegardlessOfFieldOrder(t *testing.T) {
	overflowID := uint64(^uint32(0)) + 1
	tests := []struct {
		name       string
		outerField protowire.Number
		bodyField  protowire.Number
	}{
		{name: "KV", outerField: agentServerKVMessage, bodyField: 2},
		{name: "exec", outerField: agentServerExecMessage, bodyField: 2},
		{name: "interaction query", outerField: agentServerInteractionQuery, bodyField: 2},
	}
	for _, test := range tests {
		for _, idFirst := range []bool{true, false} {
			order := "id-last"
			if idFirst {
				order = "id-first"
			}
			t.Run(test.name+"/"+order, func(t *testing.T) {
				id := appendVarintField(nil, 1, overflowID)
				body := appendBytesField(nil, test.bodyField, nil)
				if idFirst {
					body = append(id, body...)
				} else {
					body = append(body, id...)
				}
				decoded, err := decodeServerMessage(appendBytesField(nil, test.outerField, body))
				require.Nil(t, decoded)
				require.ErrorContains(t, err, "id exceeds uint32")
			})
		}
	}
}

func TestRunnerDoesNotReplyToOverflowID(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)

	exec := appendBytesField(nil, 2, nil)
	exec = appendVarintField(exec, 1, uint64(^uint32(0))+1)
	upstream.data <- frameConnect(appendBytesField(nil, agentServerExecMessage, exec))
	require.ErrorContains(t, <-result, "exec id exceeds uint32")
	select {
	case reply := <-upstream.writes:
		t.Fatalf("unexpected reply to overflow id: %x", reply)
	default:
	}
}

func TestDecodePreservesInlineReplyRequestOverTrailingInteractionUpdate(t *testing.T) {
	exec := encodeServerRequestContext(7, "exec-7")
	_, execPayload, _, complete, err := parseConnectFrame(frameConnect(exec))
	require.NoError(t, err)
	require.True(t, complete)
	_, textPayload, _, complete, err := parseConnectFrame(frameConnect(encodeServerText("must not overwrite")))
	require.NoError(t, err)
	require.True(t, complete)
	combined := append(append([]byte(nil), execPayload...), textPayload...)

	decoded, err := decodeServerMessage(combined)
	require.NoError(t, err)
	require.Equal(t, wireRequestContext, decoded.Type)
	require.Equal(t, uint32(7), decoded.ID)
	require.Equal(t, "exec-7", decoded.ExecID)

	unknownExec := appendBytesField(nil, agentServerExecMessage, appendBytesField(nil, 99, nil))
	decoded, err = decodeServerMessage(append(unknownExec, textPayload...))
	require.Nil(t, decoded)
	require.ErrorContains(t, err, "protocol drift")
}

func TestDecodeExecKindsRemainDistinct(t *testing.T) {
	tests := []struct {
		field protowire.Number
		want  wireMessageType
	}{
		{field: 2, want: wireShell},
		{field: 14, want: wireShellStream},
		{field: 16, want: wireBackgroundShell},
		{field: 9, want: wireDiagnostics},
		{field: 11, want: wireMCP},
	}
	for _, test := range tests {
		message := appendVarintField(nil, 1, 1)
		message = appendBytesField(message, test.field, nil)
		decoded, err := decodeServerMessage(appendBytesField(nil, agentServerExecMessage, message))
		require.NoError(t, err)
		require.Equal(t, test.want, decoded.Type)
	}
}

func TestFetchRejectionUsesFetchErrorField(t *testing.T) {
	encoded := encodeFetchError(9, "exec-fetch", "disabled")
	_, execPayload, _, complete, err := parseConnectFrame(frameConnect(encoded))
	require.NoError(t, err)
	require.True(t, complete)
	top := decodeBytes(execPayload, agentClientExecMessage)
	result := decodeBytes(top, 20)
	fetchError := decodeBytes(result, 2)
	require.Equal(t, "disabled", decodeString(fetchError, 2))
	require.Empty(t, decodeString(fetchError, 1))
}

func TestDecodeProtoValueKeepsUnknownUTF8Readable(t *testing.T) {
	require.Equal(t, `{"raw":true}`, decodeProtoValue([]byte(`{"raw":true}`)))
	require.Equal(t, "_w", decodeProtoValue([]byte{0xff}))
}

func TestExecRejectionsUseCanonicalProtoOneofs(t *testing.T) {
	tests := []struct {
		name        string
		encoded     []byte
		outerField  protowire.Number
		variant     protowire.Number
		reasonField protowire.Number
	}{
		{name: "shell", encoded: encodeExecRejected(1, "shell", 2, 4, 3, "disabled"), outerField: 2, variant: 4, reasonField: 3},
		{name: "shell stream", encoded: encodeExecRejected(1, "stream", 14, 5, 3, "disabled"), outerField: 14, variant: 5, reasonField: 3},
		{name: "background shell", encoded: encodeExecRejected(1, "background", 16, 3, 3, "disabled"), outerField: 16, variant: 3, reasonField: 3},
		{name: "write", encoded: encodeExecRejected(1, "write", 3, 6, 2, "disabled"), outerField: 3, variant: 6, reasonField: 2},
		{name: "delete", encoded: encodeExecRejected(1, "delete", 4, 6, 2, "disabled"), outerField: 4, variant: 6, reasonField: 2},
		{name: "diagnostics", encoded: encodeExecRejected(1, "diagnostics", 9, 3, 2, "disabled"), outerField: 9, variant: 3, reasonField: 2},
		{name: "mcp", encoded: encodeExecRejected(1, "mcp", 11, 3, 1, "disabled"), outerField: 11, variant: 3, reasonField: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			execMessage := decodeBytes(test.encoded, agentClientExecMessage)
			result := decodeBytes(execMessage, test.outerField)
			rejected := decodeBytes(result, test.variant)
			require.Equal(t, "disabled", decodeString(rejected, test.reasonField))
		})
	}
}

func TestInteractionFailuresUseCanonicalNestedResults(t *testing.T) {
	ask := encodeAskQuestionRejected(4, "unavailable")
	askResponse := decodeBytes(ask, agentClientInteractionResponse)
	askResult := decodeBytes(decodeBytes(askResponse, 3), 1)
	require.Equal(t, "unavailable", decodeString(decodeBytes(askResult, 3), 1))

	plan := encodeCreatePlanError(7, "unavailable")
	planResponse := decodeBytes(plan, agentClientInteractionResponse)
	planResult := decodeBytes(decodeBytes(planResponse, 7), 1)
	require.Equal(t, "unavailable", decodeString(decodeBytes(planResult, 2), 1))
}

func TestEncodeResumeRequestUsesCheckpointAndResumeAction(t *testing.T) {
	blobs := map[string][]byte{"blob": []byte("value")}
	encoded, err := encodeRunRequest(&runWireRequest{
		Model:          "claude-sonnet-4-6",
		ConversationID: "conversation-1",
		BlobStore:      blobs,
		Checkpoint:     []byte("checkpoint"),
		Resume:         true,
	})
	require.NoError(t, err)
	run := decodeBytes(encoded, agentClientRunRequest)
	require.Equal(t, []byte("checkpoint"), decodeBytes(run, 1))
	action := decodeBytes(run, 2)
	require.True(t, hasField(action, 2), "resume_action must be selected")
	require.False(t, hasField(action, 1), "user_message_action must be absent")
	require.Equal(t, []byte("value"), blobs["blob"])
}

func TestRunnerTrailerErrorWinsAfterTurnEnded(t *testing.T) {
	upstream := newFakeStream()
	opener := &fakeOpener{stream: upstream}
	runner := newRunnerForTest(opener, time.Hour, time.Second)
	var events []Event
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), baseRunRequest(), func(event Event) { events = append(events, event) })
	}()

	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerText("hello"))
	upstream.data <- frameConnect(encodeServerTurnEnded())
	upstream.data <- frameConnectWithFlags([]byte(`{"error":{"code":"internal","message":"terminal failure"}}`), connectEndStreamFlag)

	err := <-result
	var terminal *connectError
	require.ErrorAs(t, err, &terminal)
	require.Equal(t, "internal", terminal.Code)
	require.Len(t, events, 2)
	require.Equal(t, EventText, events[0].Type)
	require.Equal(t, "hello", events[0].Text)
	require.Equal(t, EventUsageObservation, events[1].Type)
	require.Equal(t, UsageKindTurnEnded, events[1].Usage.Kind)
}

func TestRunnerIgnoresFeedbackRequestThenCompletesTextTurn(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	var events []Event
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), baseRunRequest(), func(event Event) { events = append(events, event) })
	}()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerFeedbackRequest())
	upstream.data <- frameConnect(encodeServerText("hello"))
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, []Event{
		{Type: EventText, Text: "hello"},
		{Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded}},
		{Type: EventDone},
	}, events)
	select {
	case reply := <-upstream.writes:
		t.Fatalf("feedback request must not produce a reply: %x", reply)
	default:
	}
}

func TestRunnerCleanTerminalRequiresTurnEndedAndEmitsDone(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	var events []Event
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), baseRunRequest(), func(event Event) { events = append(events, event) })
	}()
	assertRunWasWritten(t, upstream)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, []Event{
		{Type: EventUsageObservation, Usage: &UsageObservation{Kind: UsageKindTurnEnded}},
		{Type: EventDone},
	}, events)

	missing := newFakeStream()
	runner = newRunnerForTest(&fakeOpener{stream: missing}, time.Hour, time.Second)
	result = make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, missing)
	missing.data <- frameConnectWithFlags(nil, connectEndStreamFlag)
	require.EqualError(t, <-result, "Cursor Agent v1 stream ended before turnEnded")
}

func TestRunnerSendsHeartbeatUntilTerminal(t *testing.T) {
	upstream := newFakeStream()
	opener := &fakeOpener{stream: upstream}
	runner := newRunnerForTest(opener, 5*time.Millisecond, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)

	time.Sleep(24 * time.Millisecond)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, []string{"mcp_tool_call"}, opener.config.AllowedNativeTools)

	heartbeats := 0
	for {
		select {
		case write := <-upstream.writes:
			if topLevelField(t, write) == agentClientHeartbeat {
				heartbeats++
			}
		default:
			require.GreaterOrEqual(t, heartbeats, 2)
			return
		}
	}
}

func TestHeartbeatWriteFailureDoesNotPreemptBufferedTerminal(t *testing.T) {
	upstream := newFakeStream()
	upstream.failWriteAt = 2
	upstream.failWriteErr = errors.New("connection closing")
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, 5*time.Millisecond, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	time.Sleep(10 * time.Millisecond)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestRunnerAnswersKVContextInteractionAndRejectsShell(t *testing.T) {
	upstream := newFakeStream()
	opener := &fakeOpener{stream: upstream}
	runner := newRunnerForTest(opener, time.Hour, time.Second)
	request := baseRunRequest()
	request.AllowHostedSearch = true
	request.AllowHostedFetch = true
	events := make(chan Event, 4)
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), request, func(event Event) {
			events <- event
		})
	}()
	assertRunWasWritten(t, upstream)

	blobID := []byte{1, 2, 3}
	upstream.data <- frameConnect(encodeServerKVSet(21, blobID, []byte("blob")))
	upstream.data <- frameConnect(encodeServerKVGet(22, blobID))
	upstream.data <- frameConnect(encodeServerRequestContext(23, "exec-context"))
	upstream.data <- frameConnect(encodeServerInteractionQuery(24, 2))
	upstream.data <- frameConnect(encodeServerInteractionQuery(28, 9))
	upstream.data <- frameConnect(encodeServerShell(25, "exec-shell"))
	upstream.data <- frameConnect(encodeServerExec(26, "exec-diagnostics", 9))
	upstream.data <- frameConnect(encodeServerExec(27, "exec-mcp", 11))
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, []string{"mcp_tool_call", "web_search_tool_call", "web_fetch_tool_call"}, opener.config.AllowedNativeTools)

	counts := map[protowire.Number]int{}
	deadline := time.Now().Add(time.Second)
	for counts[agentClientKVMessage] < 2 || counts[agentClientExecMessage] < 4 || counts[agentClientInteractionResponse] < 2 {
		if time.Now().After(deadline) {
			break
		}
		select {
		case write := <-upstream.writes:
			counts[topLevelField(t, write)]++
		case <-time.After(time.Millisecond):
		}
	}
	require.Equal(t, 2, counts[agentClientKVMessage])
	require.Equal(t, 4, counts[agentClientExecMessage])
	require.Equal(t, 2, counts[agentClientInteractionResponse])
	require.Equal(t, EventHostedSearchCall, (<-events).Type)
	require.Equal(t, EventHostedSearchCall, (<-events).Type)
}

func TestDecodeRecognizesCurrentWebFetchInteractionQuery(t *testing.T) {
	decoded, err := decodeServerMessage(encodeServerInteractionQuery(29, 9))
	require.NoError(t, err)
	require.Equal(t, wireInteractionQuery, decoded.Type)
	require.Equal(t, uint32(29), decoded.ID)
	require.Equal(t, protowire.Number(9), decoded.InteractionKind)

	approved, err := encodeInteractionDecision(29, 9, true, "")
	require.NoError(t, err)
	response := decodeBytes(approved, agentClientInteractionResponse)
	require.Equal(t, uint64(29), decodeVarint(response, 1))
	require.NotEmpty(t, decodeBytes(response, 9))

	rejected, err := encodeInteractionDecision(30, 9, false, "search disabled")
	require.NoError(t, err)
	rejectedResponse := decodeBytes(rejected, agentClientInteractionResponse)
	rejectedResult := decodeBytes(rejectedResponse, 9)
	require.Equal(t, "search disabled", decodeString(decodeBytes(rejectedResult, 2), 1))
}

func TestRunnerParksForMCPAndContinuesWithMatchingToolResult(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	toolResults := make(chan ToolResult, 1)
	request := baseRunRequest()
	request.ToolResults = toolResults
	request.MaxParallelTools = 1
	events := make(chan Event, 8)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)

	upstream.data <- frameConnect(encodeServerMCP(t, 31, "exec-mcp", "tool-31", "lookup", map[string]any{"city": "Paris"}))
	toolCall := <-events
	require.Equal(t, EventToolCall, toolCall.Type)
	require.Equal(t, "tool-31", toolCall.ToolCall.ToolCallID)
	require.JSONEq(t, `{"city":"Paris"}`, toolCall.ToolCall.Arguments)
	park := <-events
	require.Equal(t, EventTurnEnded, park.Type, "serial Agent v1 tools must return control without waiting for upstream turnEnded")

	toolResults <- ToolResult{ToolCallID: "tool-31", Content: `{"temperature":19}`}
	accepted := <-events
	require.Equal(t, EventToolResultAccepted, accepted.Type)
	require.Equal(t, "tool-31", accepted.ToolResult.ToolCallID)

	var mcpReply []byte
	select {
	case mcpReply = <-upstream.writes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for MCP result")
	}
	execMessage := decodeBytes(mcpReply[connectHeaderSize:], agentClientExecMessage)
	mcpResult := decodeBytes(execMessage, 11)
	require.NotEmpty(t, decodeBytes(mcpResult, 1))

	upstream.data <- frameConnect(encodeServerText("done"))
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
	require.Equal(t, EventText, (<-events).Type)
	require.Equal(t, EventUsageObservation, (<-events).Type)
	require.Equal(t, EventDone, (<-events).Type)
}

func TestRunnerDisablesInboundIdleWhileCallerToolIsPending(t *testing.T) {
	upstream := newFakeStream()
	idleTimeout := 30 * time.Millisecond
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, idleTimeout)
	toolResults := make(chan ToolResult, 1)
	request := baseRunRequest()
	request.ToolResults = toolResults
	events := make(chan Event, 4)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, func(event Event) { events <- event }) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerMCP(t, 51, "exec-slow", "tool-slow", "slow", nil))
	require.Equal(t, EventToolCall, (<-events).Type)
	time.Sleep(55 * time.Millisecond)
	toolResults <- ToolResult{ToolCallID: "tool-slow", Content: "done"}
	require.Equal(t, EventToolResultAccepted, (<-events).Type)
	// The pending-loop timer was about to fire around 60ms. Acceptance must
	// start a fresh idle window instead of consuming that stale tick.
	time.Sleep(10 * time.Millisecond)
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestRunnerQueuesRestartToolResultUntilServerReissuesMCP(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	toolResults := make(chan ToolResult, 1)
	toolResults <- ToolResult{ToolCallID: "tool-restart", Content: "restored"}
	request := baseRunRequest()
	request.UserText = ""
	request.Resume = true
	request.InitialCheckpoint = []byte("checkpoint")
	request.ToolResults = toolResults
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), request, nil) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerMCP(t, 41, "exec-restart", "tool-restart", "lookup", nil))
	select {
	case reply := <-upstream.writes:
		require.Equal(t, agentClientExecMessage, topLevelField(t, reply))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for restored MCP result")
	}
	upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	require.NoError(t, <-result)
}

func TestRunnerRejectsStreamCloseWithoutConnectTerminal(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerTurnEnded())
	upstream.finish(nil)
	require.EqualError(t, <-result, "Cursor Agent v1 HTTP/2 stream closed without Connect terminal trailer")
}

func TestRunnerRejectsSilentUpstreamAtIdleDeadline(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, 10*time.Millisecond)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	require.EqualError(t, <-result, "Cursor Agent v1 received no frames for 10ms")
}

func TestRunnerRejectsMissingRequiredToolAndDisabledParallelViolation(t *testing.T) {
	t.Run("required tool not produced returns the text like Kiro", func(t *testing.T) {
		upstream := newFakeStream()
		runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
		request := baseRunRequest()
		request.RequireToolCall = true
		result := make(chan error, 1)
		go func() { result <- runner.Run(context.Background(), request, nil) }()
		assertRunWasWritten(t, upstream)
		upstream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
		require.NoError(t, <-result)
	})

	t.Run("parallel disabled", func(t *testing.T) {
		upstream := newFakeStream()
		runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
		request := baseRunRequest()
		request.Tools = []ToolDefinition{{Name: "lookup", InputSchema: []byte(`{"type":"object"}`)}}
		request.MaxParallelTools = 1
		request.ToolResults = make(chan ToolResult)
		result := make(chan error, 1)
		go func() { result <- runner.Run(context.Background(), request, func(Event) {}) }()
		assertRunWasWritten(t, upstream)
		upstream.data <- frameConnect(encodeServerMCP(t, 1, "exec-1", "call-1", "lookup", map[string]any{}))
		upstream.data <- frameConnect(encodeServerMCP(t, 2, "exec-2", "call-2", "lookup", map[string]any{}))
		require.EqualError(t, <-result, "Cursor Agent v1 returned parallel caller tools when the client disabled them")
	})
}

func TestHTTP2OpenerUsesFullDuplexConnectStream(t *testing.T) {
	received := make(chan []byte, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, 2, request.ProtoMajor)
		require.Equal(t, "application/connect+proto", request.Header.Get("Content-Type"))
		require.Equal(t, "Bearer access-token", request.Header.Get("Authorization"))
		require.Equal(t, "cli", request.Header.Get("X-Cursor-Client-Type"))
		require.Equal(t, "true", request.Header.Get("X-Cursor-Streaming"))
		require.Equal(t, "gzip,br", request.Header.Get("Connect-Accept-Encoding"))
		require.Equal(t, "CursorCookie=Cookie-access-token", request.Header.Get("Cookie"))
		header := make([]byte, connectHeaderSize)
		_, err := io.ReadFull(request.Body, header)
		require.NoError(t, err)
		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		_, err = io.ReadFull(request.Body, payload)
		require.NoError(t, err)
		received <- append(header, payload...)
		w.Header().Set("Content-Type", "application/connect+proto")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(frameConnectWithFlags(nil, connectEndStreamFlag))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	upstream, err := (http2Opener{}).Open(context.Background(), streamConfig{
		BaseURL:       server.URL,
		AccessToken:   "access-token",
		ClientVersion: "cli-test",
		TLSConfig:     &tls.Config{InsecureSkipVerify: true}, // #nosec G402 -- isolated httptest certificate.
	})
	require.NoError(t, err)
	defer upstream.Close()
	require.NoError(t, upstream.Write(frameConnect(encodeHeartbeat())))
	select {
	case write := <-received:
		require.Equal(t, agentClientHeartbeat, topLevelField(t, write))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for full-duplex request body")
	}
	select {
	case response := <-upstream.Data():
		flags, _, _, complete, parseErr := parseConnectFrame(response)
		require.NoError(t, parseErr)
		require.True(t, complete)
		require.Equal(t, connectEndStreamFlag, flags)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for full-duplex response body")
	}
}

func TestHTTP2OpenerUsesConfiguredHTTPConnectProxy(t *testing.T) {
	received := make(chan struct{}, 1)
	upstreamServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, 2, request.ProtoMajor)
		header := make([]byte, connectHeaderSize)
		_, err := io.ReadFull(request.Body, header)
		require.NoError(t, err)
		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		_, err = io.ReadFull(request.Body, payload)
		require.NoError(t, err)
		received <- struct{}{}
		w.Header().Set("Content-Type", "application/connect+proto")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(frameConnectWithFlags(nil, connectEndStreamFlag))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	upstreamServer.EnableHTTP2 = true
	upstreamServer.StartTLS()
	defer upstreamServer.Close()

	proxyHits := make(chan string, 1)
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodConnect, request.Method)
		proxyHits <- request.Host
		downstream, _, err := w.(http.Hijacker).Hijack()
		require.NoError(t, err)
		upstream, err := net.DialTimeout("tcp", request.Host, time.Second)
		require.NoError(t, err)
		_, err = downstream.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		require.NoError(t, err)
		go func() {
			_, _ = io.Copy(upstream, downstream)
			_ = upstream.Close()
		}()
		_, _ = io.Copy(downstream, upstream)
		_ = downstream.Close()
	}))
	defer proxyServer.Close()

	upstream, err := (http2Opener{}).Open(context.Background(), streamConfig{
		BaseURL: upstreamServer.URL, ProxyURL: proxyServer.URL, AccessToken: "access-token", ClientVersion: "cli-test",
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402 -- isolated httptest certificate.
	})
	require.NoError(t, err)
	defer upstream.Close()
	require.NoError(t, upstream.Write(frameConnect(encodeHeartbeat())))
	select {
	case host := <-proxyHits:
		require.Equal(t, strings.TrimPrefix(upstreamServer.URL, "https://"), host)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for HTTP CONNECT proxy")
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for proxied Agent v1 request")
	}
}

func TestHTTP2OpenerRejectsH2CInsteadOfAttemptingTLS(t *testing.T) {
	_, err := (http2Opener{}).Open(context.Background(), streamConfig{
		BaseURL:       "http://127.0.0.1:8080",
		AccessToken:   "access-token",
		ClientVersion: "cli-test",
	})
	require.EqualError(t, err, `unsupported Cursor Agent v1 scheme "http"`)
}

func assertRunWasWritten(t *testing.T, upstream *fakeStream) {
	t.Helper()
	select {
	case write := <-upstream.writes:
		require.Equal(t, agentClientRunRequest, topLevelField(t, write))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Agent v1 run request")
	}
}

func topLevelField(t *testing.T, framed []byte) protowire.Number {
	t.Helper()
	_, payload, _, complete, err := parseConnectFrame(framed)
	require.NoError(t, err)
	require.True(t, complete)
	number, _, consumed := protowire.ConsumeTag(payload)
	require.Greater(t, consumed, 0)
	return number
}

func hasField(data []byte, target protowire.Number) bool {
	for len(data) > 0 {
		number, wireType, consumed := protowire.ConsumeTag(data)
		if consumed < 0 {
			return false
		}
		data = data[consumed:]
		consumed = protowire.ConsumeFieldValue(number, wireType, data)
		if consumed < 0 {
			return false
		}
		if number == target {
			return true
		}
		data = data[consumed:]
	}
	return false
}

func encodeServerText(text string) []byte {
	delta := appendStringField(nil, 1, text)
	update := appendBytesField(nil, 1, delta)
	return appendBytesField(nil, agentServerInteractionUpdate, update)
}

func encodeServerTurnEnded() []byte {
	return appendBytesField(nil, agentServerInteractionUpdate, appendBytesField(nil, 14, nil))
}

func encodeServerKVSet(id uint32, blobID, blobData []byte) []byte {
	args := appendBytesField(nil, 1, blobID)
	args = appendBytesField(args, 2, blobData)
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 3, args)
	return appendBytesField(nil, agentServerKVMessage, message)
}

func encodeServerKVGet(id uint32, blobID []byte) []byte {
	args := appendBytesField(nil, 1, blobID)
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 2, args)
	return appendBytesField(nil, agentServerKVMessage, message)
}

func encodeServerRequestContext(id uint32, execID string) []byte {
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 10, nil)
	message = appendStringField(message, 15, execID)
	return appendBytesField(nil, agentServerExecMessage, message)
}

func encodeServerInteractionQuery(id uint32, kind protowire.Number) []byte {
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, kind, nil)
	return appendBytesField(nil, agentServerInteractionQuery, message)
}

func encodeServerShell(id uint32, execID string) []byte {
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 2, appendStringField(nil, 1, "pwd"))
	message = appendStringField(message, 15, execID)
	return appendBytesField(nil, agentServerExecMessage, message)
}

func encodeServerExec(id uint32, execID string, field protowire.Number) []byte {
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, field, nil)
	message = appendStringField(message, 15, execID)
	return appendBytesField(nil, agentServerExecMessage, message)
}

func encodeServerMCP(t *testing.T, id uint32, execID, toolCallID, name string, arguments map[string]any) []byte {
	t.Helper()
	mcp := appendStringField(nil, 1, name)
	for key, argument := range arguments {
		value, err := structpb.NewValue(argument)
		require.NoError(t, err)
		encodedValue, err := proto.Marshal(value)
		require.NoError(t, err)
		entry := appendStringField(nil, 1, key)
		entry = appendBytesField(entry, 2, encodedValue)
		mcp = appendBytesField(mcp, 2, entry)
	}
	mcp = appendStringField(mcp, 3, toolCallID)
	mcp = appendStringField(mcp, 5, name)
	message := appendVarintField(nil, 1, uint64(id))
	message = appendBytesField(message, 11, mcp)
	message = appendStringField(message, 15, execID)
	return appendBytesField(nil, agentServerExecMessage, message)
}

func TestStoreBlobEnforcesSingleAndTotalBudgets(t *testing.T) {
	store := map[string][]byte{}
	total := 0
	require.NoError(t, storeBlob(store, &total, "one", []byte("1234"), 4, 6))
	require.EqualError(t, storeBlob(store, &total, "large", []byte("12345"), 4, 6), "Cursor Agent v1 KV blob exceeds 4 bytes")
	require.EqualError(t, storeBlob(store, &total, "two", []byte("789"), 4, 6), "Cursor Agent v1 KV blob store exceeds 6 bytes")
	require.NoError(t, storeBlob(store, &total, "one", []byte("12"), 4, 6))
	require.Equal(t, 2, total)

	_, err := validateBlobStore(map[string][]byte{"system": []byte("12345")}, 4, 6)
	require.EqualError(t, err, "Cursor Agent v1 KV blob exceeds 4 bytes")
	_, err = validateBlobStore(map[string][]byte{"one": []byte("1234"), "two": []byte("789")}, 4, 6)
	require.EqualError(t, err, "Cursor Agent v1 KV blob store exceeds 6 bytes")
}

func TestRunnerCancellationIsNotReportedAsUpstreamFailure(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runner.Run(ctx, baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestRunnerForwardsModelTransportProxy(t *testing.T) {
	upstream := newFakeStream()
	opener := &fakeOpener{stream: upstream}
	runner := newRunnerForTest(opener, time.Hour, time.Second)
	request := baseRunRequest()
	request.ProxyURL = "socks5://proxy.example:1080"
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runner.Run(ctx, request, nil) }()
	assertRunWasWritten(t, upstream)
	require.Equal(t, request.ProxyURL, opener.config.ProxyURL)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestRunnerRejectsUnsupportedInteractionInsteadOfStalling(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	upstream.data <- frameConnect(encodeServerInteractionQuery(1, 8))
	err := <-result
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unsupported Cursor Agent v1 interaction query"), err.Error())
}

func TestRunnerRejectsUnknownExecInsteadOfStalling(t *testing.T) {
	upstream := newFakeStream()
	runner := newRunnerForTest(&fakeOpener{stream: upstream}, time.Hour, time.Second)
	result := make(chan error, 1)
	go func() { result <- runner.Run(context.Background(), baseRunRequest(), nil) }()
	assertRunWasWritten(t, upstream)
	message := appendVarintField(nil, 1, 1)
	message = appendBytesField(message, 99, nil)
	upstream.data <- frameConnect(appendBytesField(nil, agentServerExecMessage, message))
	require.ErrorContains(t, <-result, "protocol drift in exec")
}
