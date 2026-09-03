package cursoragentv1

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scriptedEngine struct {
	run func(context.Context, RunRequest, func(Event)) error
}

func (e *scriptedEngine) Run(ctx context.Context, request RunRequest, emit func(Event)) error {
	return e.run(ctx, request, emit)
}

func TestSessionManagerParksAndResumesLiveToolSession(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventCheckpoint, Checkpoint: []byte("checkpoint"), Blobs: map[string][]byte{"blob": []byte("value")}})
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{MessageID: 7, ExecID: "exec-7", ToolCallID: "tool-7", Name: "lookup", Arguments: `{"id":7}`}})
		select {
		case result := <-request.ToolResults:
			require.Equal(t, "tool-7", result.ToolCallID)
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventText, Text: "finished"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("session-1")

	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, meteredTurnID(request.BillingRunID), first.BillingRunID)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	_, err = manager.Start(request)
	require.ErrorIs(t, err, ErrSessionActive)

	second, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.NoError(t, err)
	require.Equal(t, first.BillingRunID, second.BillingRunID)
	secondEvents := collectManagedEvents(second.Events)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(secondEvents))
	require.NoError(t, <-second.Result)

	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	restored, err := store.Claim("session-1", "ignored", fingerprint, ClaimOptions{RequireExisting: true})
	require.NoError(t, err)
	require.Equal(t, []byte("checkpoint"), restored.Snapshot.Checkpoint)
	require.Equal(t, []byte("value"), restored.Snapshot.Blobs["blob"])
	require.Empty(t, restored.Snapshot.PendingTools)
	require.NoError(t, store.Release(restored.Owner))
}

func TestLiveResumeExplicitNoneRejectsNewCallerTool(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			require.Equal(t, "tool-a", result.ToolCallID)
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-b", Name: "lookup", Arguments: `{"next":true}`}})
			emit(Event{Type: EventTurnEnded})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("live-none-new-tool")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	none := request
	none.DisableCallerTools = true
	none.Run.Tools = nil
	none.ToolCatalogDigest = ""
	second, err := manager.ResumeTool(none, ToolResult{ToolCallID: "tool-a", Content: "ok"})
	require.NoError(t, err)
	events := collectManagedEvents(second.Events)
	require.Equal(t, []EventType{EventToolResultAccepted}, eventTypes(events))
	require.ErrorIs(t, <-second.Result, ErrCallerToolsDisabled)
}

func TestLiveResumeAutoStillAllowsNewCallerTool(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			require.Equal(t, "tool-a", result.ToolCallID)
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-b", Name: "lookup", Arguments: `{"next":true}`}})
			select {
			case second := <-request.ToolResults:
				require.Equal(t, "tool-b", second.ToolCallID)
				emit(Event{Type: EventToolResultAccepted, ToolResult: &second})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("live-auto-new-tool")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	second, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "ok"})
	require.NoError(t, err)
	require.Equal(t, EventToolResultAccepted, (<-second.Events).Type)
	require.Equal(t, EventToolCall, (<-second.Events).Type)
	require.NoError(t, manager.Park(request))
	for range second.Events {
	}
	require.NoError(t, <-second.Result)

	third, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-b", Content: "ok"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(third.Events)))
	require.NoError(t, <-third.Result)
}

func TestLiveResumeNoneDoesNotDisableLaterAutoSegment(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	var runs int
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs++
		if runs == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		require.NotEmpty(t, request.Tools)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-b", Name: "lookup", Arguments: `{}`}})
		emit(Event{Type: EventTurnEnded})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("live-none-then-auto")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	none := request
	none.DisableCallerTools = true
	none.Run.Tools = nil
	none.ToolCatalogDigest = ""
	second, err := manager.ResumeTool(none, ToolResult{ToolCallID: "tool-a", Content: "ok"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(second.Events)))
	require.NoError(t, <-second.Result)
	waitUntilInactive(t, manager, request.SessionID)

	follow := request
	follow.FollowUpText = "continue"
	follow.Run.UserText = ""
	follow.Run.Tools = nil
	follow.ToolCatalogDigest = ""
	follow.DisableCallerTools = false
	follow.RequireExisting = true
	follow.ReuseBillingRun = true
	third, err := manager.StartFollowUp(follow, []ToolResult{{ToolCallID: "tool-a", Content: "ok"}})
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-third.Events).Type)
	require.NoError(t, manager.Park(follow))
	for range third.Events {
	}
	require.NoError(t, <-third.Result)
}

func TestSessionManagerPreservesParallelToolBatch(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{MessageID: 1, ToolCallID: "tool-a", Name: "a", Arguments: `{}`}})
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{MessageID: 2, ToolCallID: "tool-b", Name: "b", Arguments: `{}`}})
		for index := 0; index < 2; index++ {
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("parallel-session")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	_, err = manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "a"})
	require.ErrorIs(t, err, ErrSessionNotPending, "a partial parallel-tool result set must not strand the remaining call")

	second, err := manager.ResumeTools(request, []ToolResult{
		{ToolCallID: "tool-a", Content: "a"},
		{ToolCallID: "tool-b", Content: "b"},
	})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(second.Events)))
	require.NoError(t, <-second.Result)
}

func TestSessionManagerRejectsUnseenToolArrivingAfterExplicitPark(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	afterPark := make(chan struct{})
	engine := &scriptedEngine{run: func(ctx context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-a", Name: "a", Arguments: `{}`}})
		select {
		case <-afterPark:
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-b", Name: "b", Arguments: `{}`}})
			<-ctx.Done()
			return ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("late-tool")
	output, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, "tool-a", (<-output.Events).ToolCall.ToolCallID)
	require.NoError(t, manager.Park(request))
	for range output.Events {
	}
	close(afterPark)
	waitUntilInactive(t, manager, request.SessionID)

	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	restored, err := store.Claim(request.SessionID, "ignored", fingerprint, ClaimOptions{AllowPending: true, RequireExisting: true})
	require.NoError(t, err)
	require.Len(t, restored.Snapshot.PendingTools, 1)
	require.Equal(t, "tool-a", restored.Snapshot.PendingTools[0].ToolCallID)
	require.NoError(t, store.Release(restored.Owner))
}

func TestSessionManagerResumesPersistedPendingToolAfterRestart(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	stopFirst := make(chan struct{})
	firstEngine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventCheckpoint, Checkpoint: []byte("checkpoint"), Blobs: map[string][]byte{"blob": []byte("value")}})
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{MessageID: 9, ExecID: "exec-9", ToolCallID: "tool-9", Name: "lookup", Arguments: `{}`}})
		path, _ := store.paths("restart-session")
		deadline := time.Now().Add(time.Second)
		for {
			state, readErr := readState(path)
			if readErr == nil && len(state.PendingTools) == 1 {
				break
			}
			if time.Now().After(deadline) {
				return errors.New("pending tool was not persisted before simulated process loss")
			}
			time.Sleep(time.Millisecond)
		}
		<-stopFirst
		return errors.New("simulated process loss")
	}}
	firstManager, err := newSessionManager(firstEngine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("restart-session")
	first, err := firstManager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, firstManager.Park(request))
	close(stopFirst)
	for range first.Events {
	}
	if firstErr := <-first.Result; firstErr != nil {
		require.EqualError(t, firstErr, "simulated process loss")
	}
	waitUntilInactive(t, firstManager, "restart-session")

	secondEngine := &scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		require.True(t, resumed.Resume)
		require.Equal(t, []byte("checkpoint"), resumed.InitialCheckpoint)
		require.Equal(t, []byte("value"), resumed.InitialBlobs["blob"])
		result := <-resumed.ToolResults
		require.Equal(t, "tool-9", result.ToolCallID)
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}
	secondManager, err := newSessionManager(secondEngine, store, time.Second)
	require.NoError(t, err)
	resumed, err := secondManager.ResumeTool(request, ToolResult{ToolCallID: "tool-9", Content: "restored"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(resumed.Events)))
	require.NoError(t, <-resumed.Result)
}

func TestSessionManagerAndRunnerResumeCheckpointAfterProcessRestart(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("runner-restart")

	firstStream := newFakeStream()
	firstRunner := newRunnerForTest(&fakeOpener{stream: firstStream}, time.Hour, time.Second)
	firstManager, err := newSessionManager(firstRunner, store, time.Second)
	require.NoError(t, err)
	first, err := firstManager.Start(request)
	require.NoError(t, err)
	assertRunWasWritten(t, firstStream)
	firstStream.data <- frameConnect(appendBytesField(nil, agentServerCheckpoint, []byte("checkpoint")))
	firstStream.data <- frameConnect(encodeServerMCP(t, 61, "exec-restart", "tool-restart", "lookup", map[string]any{"id": 1}))
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, firstManager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)
	firstStream.finish(errors.New("simulated process stop"))
	waitUntilInactive(t, firstManager, request.SessionID)

	secondStream := newFakeStream()
	secondRunner := newRunnerForTest(&fakeOpener{stream: secondStream}, time.Hour, time.Second)
	secondManager, err := newSessionManager(secondRunner, store, time.Second)
	require.NoError(t, err)
	resumeRequest := request
	resumeRequest.Run.Tools = nil
	resumeRequest.ToolCatalogDigest = ""
	second, err := secondManager.ResumeTools(resumeRequest, []ToolResult{{ToolCallID: "tool-restart", Content: "restored"}})
	require.NoError(t, err)
	select {
	case write := <-secondStream.writes:
		_, payload, _, complete, parseErr := parseConnectFrame(write)
		require.NoError(t, parseErr)
		require.True(t, complete)
		run := decodeBytes(payload, agentClientRunRequest)
		require.Equal(t, []byte("checkpoint"), decodeBytes(run, 1))
		require.True(t, hasField(decodeBytes(run, 2), 2), "restart must use ResumeAction")
		tool := decodeBytes(decodeBytes(run, 4), 1)
		require.Equal(t, "lookup", decodeString(tool, 1))
		require.Empty(t, decodeBytes(tool, 3))
		require.JSONEq(t, `{"type":"object","properties":{"id":{"type":"integer"}}}`, decodeString(tool, 6))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resumed Agent v1 request")
	}
	secondStream.data <- frameConnect(encodeServerRequestContext(62, "exec-context"))
	select {
	case reply := <-secondStream.writes:
		_, payload, _, complete, parseErr := parseConnectFrame(reply)
		require.NoError(t, parseErr)
		require.True(t, complete)
		execMessage := decodeBytes(payload, agentClientExecMessage)
		requestContextResult := decodeBytes(execMessage, 10)
		requestContext := decodeBytes(decodeBytes(requestContextResult, 1), 1)
		tool := decodeBytes(requestContext, 7)
		require.Equal(t, "lookup", decodeString(tool, 1))
		require.Empty(t, decodeBytes(tool, 3))
		require.JSONEq(t, `{"type":"object","properties":{"id":{"type":"integer"}}}`, decodeString(tool, 6))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for restored request-context catalog")
	}
	secondStream.data <- frameConnect(encodeServerMCP(t, 61, "exec-restart", "tool-restart", "lookup", map[string]any{"id": 1}))
	select {
	case reply := <-secondStream.writes:
		require.Equal(t, agentClientExecMessage, topLevelField(t, reply))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for restored tool result injection")
	}
	secondStream.data <- frameConnect(encodeServerText("resumed"))
	secondStream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := collectManagedEvents(second.Events)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventUsageObservation, EventDone}, eventTypes(events))
	require.NoError(t, <-second.Result)
}

func TestSessionManagerRestartExplicitNoneDoesNotRestoreCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("runner-restart-none")

	firstStream := newFakeStream()
	firstRunner := newRunnerForTest(&fakeOpener{stream: firstStream}, time.Hour, time.Second)
	firstManager, err := newSessionManager(firstRunner, store, time.Second)
	require.NoError(t, err)
	first, err := firstManager.Start(request)
	require.NoError(t, err)
	assertRunWasWritten(t, firstStream)
	firstStream.data <- frameConnect(appendBytesField(nil, agentServerCheckpoint, []byte("checkpoint")))
	firstStream.data <- frameConnect(encodeServerMCP(t, 61, "exec-restart-none", "tool-restart-none", "lookup", map[string]any{"id": 1}))
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, firstManager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)
	firstStream.finish(errors.New("simulated process stop"))
	waitUntilInactive(t, firstManager, request.SessionID)

	secondStream := newFakeStream()
	secondRunner := newRunnerForTest(&fakeOpener{stream: secondStream}, time.Hour, time.Second)
	secondManager, err := newSessionManager(secondRunner, store, time.Second)
	require.NoError(t, err)
	resumeRequest := request
	resumeRequest.Run.Tools = nil
	resumeRequest.ToolCatalogDigest = ""
	resumeRequest.DisableCallerTools = true
	second, err := secondManager.ResumeTools(resumeRequest, []ToolResult{{ToolCallID: "tool-restart-none", Content: "restored"}})
	require.NoError(t, err)
	select {
	case write := <-secondStream.writes:
		_, payload, _, complete, parseErr := parseConnectFrame(write)
		require.NoError(t, parseErr)
		require.True(t, complete)
		run := decodeBytes(payload, agentClientRunRequest)
		require.Equal(t, []byte("checkpoint"), decodeBytes(run, 1))
		require.True(t, hasField(decodeBytes(run, 2), 2), "restart must use ResumeAction")
		require.Empty(t, decodeBytes(run, 4), "explicit none must not restore the parked tool catalog")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resumed Agent v1 request")
	}
	secondStream.data <- frameConnect(encodeServerRequestContext(62, "exec-context"))
	select {
	case reply := <-secondStream.writes:
		_, payload, _, complete, parseErr := parseConnectFrame(reply)
		require.NoError(t, parseErr)
		require.True(t, complete)
		execMessage := decodeBytes(payload, agentClientExecMessage)
		requestContextResult := decodeBytes(execMessage, 10)
		requestContext := decodeBytes(decodeBytes(requestContextResult, 1), 1)
		require.Empty(t, decodeBytes(requestContext, 7), "explicit none must not restore request-context tools")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request-context reply")
	}
	secondStream.data <- frameConnect(encodeServerMCP(t, 61, "exec-restart-none", "tool-restart-none", "lookup", map[string]any{"id": 1}))
	select {
	case reply := <-secondStream.writes:
		require.Equal(t, agentClientExecMessage, topLevelField(t, reply))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for restored tool result injection")
	}
	secondStream.data <- frameConnect(encodeServerText("resumed"))
	secondStream.data <- append(frameConnect(encodeServerTurnEnded()), frameConnectWithFlags(nil, connectEndStreamFlag)...)
	events := collectManagedEvents(second.Events)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventUsageObservation, EventDone}, eventTypes(events))
	require.NoError(t, <-second.Result)
}

func TestSessionManagerRestartAcceptsCoveringParallelResults(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("partial-restart")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
		AllowPending: true, AllowCredentialChange: true, ToolCatalogDigest: request.ToolCatalogDigest,
	})
	require.NoError(t, err)
	require.NoError(t, store.SaveCheckpoint(claim.Owner, []byte("checkpoint"), nil))
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-a", Name: "a", Arguments: `{}`}, {ToolCallID: "tool-b", Name: "b", Arguments: `{}`}}))
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-b", Name: "b", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))

	engine := &scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		result := <-resumed.ToolResults
		require.Equal(t, "tool-b", result.ToolCallID, "already-applied tool-a must be filtered on restart")
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	output, err := manager.ResumeTools(request, []ToolResult{
		{ToolCallID: "tool-a", Content: "already applied"},
		{ToolCallID: "tool-b", Content: "remaining"},
	})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
}

func TestSessionManagerRestartRejectsChangedToolCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("catalog-mismatch")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
		AllowPending: true, AllowCredentialChange: true, ToolCatalogDigest: request.ToolCatalogDigest,
	})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-a", Name: "a", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))

	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		t.Fatal("changed tool catalog must fail before starting the provider run")
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request.ToolCatalogDigest = "sha256:changed-tools"
	_, err = manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "result"})
	require.ErrorIs(t, err, ErrSessionNotPending)
}

func TestSessionManagerRestartAcceptsOmittedToolCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("catalog-omitted")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
		AllowPending: true, AllowCredentialChange: true, ToolCatalogDigest: request.ToolCatalogDigest,
	})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-a", Name: "a", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))

	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		result := <-resumed.ToolResults
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request.ToolCatalogDigest = ""
	output, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "result"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
}

func TestResumeToolsOmittedEmptyCatalogRestoresSnapshot(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("catalog-omitted-restore")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
		AllowPending: true, AllowCredentialChange: true, ToolCatalogDigest: request.ToolCatalogDigest,
		ToolCatalog: request.Run.Tools,
	})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))

	var resumedTools []ToolDefinition
	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		resumedTools = cloneToolDefinitions(resumed.Tools)
		result := <-resumed.ToolResults
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request.Run.Tools = nil
	request.ToolCatalogDigest = ""
	output, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "result"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
	require.Len(t, resumedTools, 1)
	require.Equal(t, "lookup", resumedTools[0].Name)
}

func TestResumeToolsExplicitNoneCompletesPendingWithoutRestoringCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("catalog-none-resume")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
		AllowPending: true, AllowCredentialChange: true, ToolCatalogDigest: request.ToolCatalogDigest,
		ToolCatalog: request.Run.Tools,
	})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))

	var resumedTools []ToolDefinition
	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		resumedTools = cloneToolDefinitions(resumed.Tools)
		result := <-resumed.ToolResults
		require.Equal(t, "tool-a", result.ToolCallID)
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request.Run.Tools = nil
	request.ToolCatalogDigest = ""
	request.DisableCallerTools = true
	output, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "result"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
	require.Empty(t, resumedTools)
}

func TestStartFollowUpOmittedRestoresSnapshotCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	var followTools []ToolDefinition
	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
		if request.UserText == "continue" {
			followTools = cloneToolDefinitions(request.Tools)
		}
		emit(Event{Type: EventText, Text: "ok"})
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("follow-omitted")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(first.Events)))
	require.NoError(t, <-first.Result)
	waitUntilInactive(t, manager, request.SessionID)

	follow := request
	follow.FollowUpText = "continue"
	follow.Run.UserText = ""
	follow.Run.Tools = nil
	follow.ToolCatalogDigest = ""
	follow.RequireExisting = true
	follow.ReuseBillingRun = true
	output, err := manager.StartFollowUp(follow, nil)
	require.NoError(t, err)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
	require.Len(t, followTools, 1)
	require.Equal(t, "lookup", followTools[0].Name)
}

func TestStartFollowUpExplicitNoneDoesNotRestoreSnapshotCatalog(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	var followTools []ToolDefinition
	manager, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, request RunRequest, emit func(Event)) error {
		if request.UserText == "continue" {
			followTools = cloneToolDefinitions(request.Tools)
		}
		emit(Event{Type: EventText, Text: "ok"})
		emit(Event{Type: EventDone})
		return nil
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("follow-none")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(first.Events)))
	require.NoError(t, <-first.Result)
	waitUntilInactive(t, manager, request.SessionID)

	follow := request
	follow.FollowUpText = "continue"
	follow.Run.UserText = ""
	follow.Run.Tools = nil
	follow.ToolCatalogDigest = ""
	follow.DisableCallerTools = true
	follow.RequireExisting = true
	follow.ReuseBillingRun = true
	output, err := manager.StartFollowUp(follow, nil)
	require.NoError(t, err)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
	require.Empty(t, followTools)
}

func TestSessionManagerLifetimeLeasePreventsSecondRuntime(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	firstEngine := &scriptedEngine{run: func(ctx context.Context, _ RunRequest, _ func(Event)) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	firstManager, err := newSessionManager(firstEngine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("fenced-session")
	first, err := firstManager.Start(request)
	require.NoError(t, err)

	secondEngine := &scriptedEngine{run: func(ctx context.Context, _ RunRequest, _ func(Event)) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	secondManager, err := newSessionManager(secondEngine, store, time.Second)
	require.NoError(t, err)
	_, err = secondManager.Start(request)
	require.ErrorIs(t, err, ErrStateLeaseHeld)
	require.NoError(t, firstManager.Cancel(request))
	for range first.Events {
	}
	require.ErrorIs(t, <-first.Result, context.Canceled)
}

func TestSessionManagerConcurrentStartReservesOneOwner(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, _ RunRequest, _ func(Event)) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("single-owner")
	const attempts = 10
	var wait sync.WaitGroup
	results := make(chan error, attempts)
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := manager.Start(request)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	successes := 0
	activeErrors := 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrSessionActive) {
			activeErrors++
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, attempts-1, activeErrors)
	require.NoError(t, manager.Cancel(request))
}

func TestSessionManagerCancelStateAfterRestartRequiresSameCredentialScope(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("cancel-persisted")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.NoError(t, store.Release(claim.Owner))
	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error { return nil }}, store, time.Second)
	require.NoError(t, err)

	wrong := request
	wrong.CredentialScope = "channel:other"
	require.ErrorIs(t, manager.CancelState(wrong), ErrCredentialMismatch)
	require.NoError(t, manager.CancelState(request))
}

func TestSessionManagerLiveResumeAndCancelRequireCredentialIdentity(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool", Name: "lookup", Arguments: `{}`}})
		<-ctx.Done()
		return ctx.Err()
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("credential-live")
	output, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-output.Events).Type)
	require.NoError(t, manager.Park(request))
	for range output.Events {
	}

	wrong := request
	wrong.AccountKey = "other@example.com"
	_, err = manager.ResumeTool(wrong, ToolResult{ToolCallID: "tool", Content: "bad"})
	require.ErrorIs(t, err, ErrSessionNotPending)
	require.ErrorIs(t, manager.Cancel(wrong), ErrCredentialMismatch)
	require.ErrorIs(t, manager.CancelState(wrong), ErrCredentialMismatch)
	require.NoError(t, manager.Cancel(request))
}

func TestSessionManagerCancelResumeRaceNeverLeavesHungOutput(t *testing.T) {
	for iteration := 0; iteration < 12; iteration++ {
		store, err := NewFileStateStore(t.TempDir())
		require.NoError(t, err)
		engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool", Name: "lookup", Arguments: `{}`}})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				<-ctx.Done()
				return ctx.Err()
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		manager, err := newSessionManager(engine, store, time.Second)
		require.NoError(t, err)
		request := managedTestRequest("cancel-resume")
		first, err := manager.Start(request)
		require.NoError(t, err)
		require.Equal(t, EventToolCall, (<-first.Events).Type)
		require.NoError(t, manager.Park(request))
		for range first.Events {
		}

		start := make(chan struct{})
		cancelResult := make(chan error, 1)
		resumeResult := make(chan struct {
			output *ManagedOutput
			err    error
		}, 1)
		go func() {
			<-start
			cancelResult <- manager.Cancel(request)
		}()
		go func() {
			<-start
			output, resumeErr := manager.ResumeTool(request, ToolResult{ToolCallID: "tool", Content: "result"})
			resumeResult <- struct {
				output *ManagedOutput
				err    error
			}{output: output, err: resumeErr}
		}()
		close(start)
		require.NoError(t, <-cancelResult)
		resumed := <-resumeResult
		if resumed.err != nil {
			require.True(t,
				errors.Is(resumed.err, ErrStateLeaseHeld) || errors.Is(resumed.err, ErrSessionNotPending) || errors.Is(resumed.err, ErrSessionActive),
				resumed.err.Error(),
			)
			continue
		}
		require.NotNil(t, resumed.output)
		select {
		case <-resumed.output.Result:
		case <-time.After(time.Second):
			t.Fatal("cancel/resume race left a managed output open")
		}
	}
}

func TestSessionManagerFullEventBufferFailsInsteadOfDeadlockingResult(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		for index := 0; index < maxPendingTools*4+16; index++ {
			emit(Event{Type: EventText, Text: "chunk"})
		}
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	output, err := manager.Start(managedTestRequest("backpressure"))
	require.NoError(t, err)
	select {
	case resultErr := <-output.Result:
		require.EqualError(t, resultErr, "Cursor Agent v1 downstream event buffer is full")
	case <-time.After(time.Second):
		t.Fatal("full event buffer blocked managed output completion")
	}
}

func TestManagedOutputClosedByParkIsNotReportedAsBackpressure(t *testing.T) {
	output := newManagedOutput()
	output.close(nil)
	session := &managedSession{output: output, cancel: func() { t.Fatal("closed output must not cancel the live session") }}
	session.emit(Event{Type: EventText, Text: "late"})
	session.mu.Lock()
	require.NoError(t, session.stateErr)
	session.mu.Unlock()
}

func TestSessionManagerStartDoesNotRotateOwnerWhenToolResultIsPending(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("pending-start")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{AllowPending: true, AllowCredentialChange: true})
	require.NoError(t, err)
	require.Equal(t, uint64(1), claim.Owner.Generation)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool", Name: "lookup", Arguments: `{}`}}))
	require.NoError(t, store.Release(claim.Owner))

	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error { return nil }}, store, time.Second)
	require.NoError(t, err)
	_, err = manager.Start(request)
	require.ErrorIs(t, err, ErrSessionPending)

	next, err := store.Claim(request.SessionID, "ignored", fingerprint, ClaimOptions{AllowPending: true, RequireExisting: true})
	require.NoError(t, err)
	require.Equal(t, uint64(2), next.Owner.Generation, "failed Start must not rotate the durable owner")
	require.NoError(t, store.Release(next.Owner))
}

func managedTestRequest(sessionID string) ManagedRequest {
	return ManagedRequest{
		TurnMetering:      true,
		SessionID:         sessionID,
		BillingRunID:      "billing-" + sessionID,
		CredentialScope:   "channel:6401",
		AccountKey:        "account@example.com",
		ToolCatalogDigest: "sha256:test-tools",
		Run: RunRequest{
			BaseURL:       "https://agent.example",
			AccessToken:   "access-token",
			ClientVersion: "cli-test",
			Model:         "claude-sonnet-4-6",
			UserText:      "hello",
			Tools: []ToolDefinition{{
				Name: "lookup", Description: "Look up an id",
				InputSchema: []byte(`{"type":"object","properties":{"id":{"type":"integer"}}}`),
			}},
		},
	}
}

func collectManagedEvents(events <-chan Event) []Event {
	var result []Event
	for event := range events {
		result = append(result, event)
	}
	return result
}

func eventTypes(events []Event) []EventType {
	types := make([]EventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}

func waitUntilInactive(t *testing.T, manager *SessionManager, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		_, active := manager.active[sessionID]
		manager.mu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for managed session cleanup")
		}
		time.Sleep(time.Millisecond)
	}
}
