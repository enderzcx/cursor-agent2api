package cursoragentv1

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerationSegmentsHaveDistinctStableReceipts(t *testing.T) {
	var runs atomic.Int32
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			require.Equal(t, "tool-a", result.ToolCallID)
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-d", Name: "lookup", Arguments: `{}`}})
			select {
			case next := <-request.ToolResults:
				require.Equal(t, "tool-d", next.ToolCallID)
				emit(Event{Type: EventToolResultAccepted, ToolResult: &next})
				emit(Event{Type: EventText, Text: "done"})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("segment-abcd")

	startOut, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-startOut.Events).Type)
	require.NoError(t, manager.Park(request))
	for range startOut.Events {
	}
	require.NoError(t, <-startOut.Result)
	receiptA := startOut.BillingRunID
	require.NotEmpty(t, receiptA)

	resumeB := request
	resumeB.BillingRunID = "http-request-b"
	segmentB, err := manager.ResumeTool(resumeB, ToolResult{ToolCallID: "tool-a", Content: "result-a"})
	require.NoError(t, err)
	require.Equal(t, EventToolResultAccepted, (<-segmentB.Events).Type)
	require.Equal(t, EventToolCall, (<-segmentB.Events).Type)
	require.NoError(t, manager.Park(resumeB))
	for range segmentB.Events {
	}
	require.NoError(t, <-segmentB.Result)
	receiptB := segmentB.BillingRunID
	require.NotEmpty(t, receiptB)
	require.Equal(t, receiptA, receiptB, "tool continuations share one terminal usage receipt")

	resumeD := request
	resumeD.BillingRunID = "http-request-d"
	segmentD, err := manager.ResumeTool(resumeD, ToolResult{ToolCallID: "tool-d", Content: "result-d"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(segmentD.Events)))
	require.NoError(t, <-segmentD.Result)
	receiptD := segmentD.BillingRunID
	require.NotEmpty(t, receiptD)
	require.Equal(t, receiptA, receiptD)
	require.Equal(t, receiptB, receiptD, "the next tool result belongs to the same Agent turn")
	require.Equal(t, int32(1), runs.Load())
}

func TestIdenticalToolResultRetryReusesSegmentReceiptWithoutProviderRerun(t *testing.T) {
	var runs atomic.Int32
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-b", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventText, Text: "b-done"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("segment-replay")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	bRequest := request
	bRequest.BillingRunID = "http-b-1"
	segmentB, err := manager.ResumeTool(bRequest, ToolResult{ToolCallID: "tool-b", Content: "ok"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(segmentB.Events)))
	require.NoError(t, <-segmentB.Result)
	require.Equal(t, first.BillingRunID, segmentB.BillingRunID)
	waitUntilInactive(t, manager, request.SessionID)

	replayRequest := request
	replayRequest.SessionID = "ignored-client-session"
	replayRequest.BillingRunID = "http-b-retry"
	replay, err := manager.ResumeTool(replayRequest, ToolResult{ToolCallID: "tool-b", Content: "ok"})
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.Equal(t, segmentB.BillingRunID, replay.BillingRunID)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(replay.Events)))
	require.NoError(t, <-replay.Result)
	require.Equal(t, int32(1), runs.Load())

	restartedStore, err := NewFileStateStore(root)
	require.NoError(t, err)
	var restartedRuns atomic.Int32
	restarted, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		restartedRuns.Add(1)
		return errors.New("restart replay must not call the provider")
	}}, restartedStore, time.Second)
	require.NoError(t, err)
	restartReplay, err := restarted.ResumeTool(ManagedRequest{
		TurnMetering: true,
		SessionID:    "ignored", BillingRunID: "http-after-restart",
		CredentialScope: request.CredentialScope, AccountKey: request.AccountKey,
	}, ToolResult{ToolCallID: "tool-b", Content: "ok"})
	require.NoError(t, err)
	require.True(t, restartReplay.Replay)
	require.Equal(t, segmentB.BillingRunID, restartReplay.BillingRunID)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(restartReplay.Events)))
	require.NoError(t, <-restartReplay.Result)
	require.Equal(t, int32(0), restartedRuns.Load())
}

func TestConcurrentIdenticalResumeSharesOneOwnerAndReceipt(t *testing.T) {
	var runs atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-7", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			close(started)
			<-release
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventText, Text: "finished"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("segment-race")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	leader := make(chan *ManagedOutput, 1)
	go func() {
		output, resumeErr := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "result"})
		require.NoError(t, resumeErr)
		leader <- output
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for provider resume")
	}

	const retries = 12
	var wait sync.WaitGroup
	outputs := make(chan *ManagedOutput, retries)
	errorsCh := make(chan error, retries)
	wait.Add(retries)
	for index := 0; index < retries; index++ {
		go func() {
			defer wait.Done()
			retry := request
			retry.BillingRunID = "http-concurrent"
			retry.SessionID = "ignored"
			output, resumeErr := manager.ResumeTool(retry, ToolResult{ToolCallID: "tool-7", Content: "result"})
			if resumeErr != nil {
				errorsCh <- resumeErr
				return
			}
			outputs <- output
		}()
	}
	close(release)
	leaderOutput := <-leader
	for range leaderOutput.Events {
	}
	require.NoError(t, <-leaderOutput.Result)
	wait.Wait()
	close(outputs)
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	require.Equal(t, first.BillingRunID, leaderOutput.BillingRunID)
	receipts := map[string]struct{}{leaderOutput.BillingRunID: {}}
	for output := range outputs {
		require.True(t, output.Replay)
		require.Equal(t, leaderOutput.BillingRunID, output.BillingRunID)
		for range output.Events {
		}
		require.NoError(t, <-output.Result)
		receipts[output.BillingRunID] = struct{}{}
	}
	require.Equal(t, int32(1), runs.Load())
	require.Len(t, receipts, 1)
}

func TestCoveringHistoricalResultsDoNotMintANewReceipt(t *testing.T) {
	var runs atomic.Int32
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-a", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-d", Name: "lookup", Arguments: `{}`}})
			select {
			case next := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &next})
				emit(Event{Type: EventText, Text: "d-done"})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("segment-covering")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	segmentB, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-a", Content: "a"})
	require.NoError(t, err)
	require.Equal(t, EventToolResultAccepted, (<-segmentB.Events).Type)
	require.Equal(t, EventToolCall, (<-segmentB.Events).Type)
	require.NoError(t, manager.Park(request))
	for range segmentB.Events {
	}
	require.NoError(t, <-segmentB.Result)

	segmentD, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-d", Content: "d"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(segmentD.Events)))
	require.NoError(t, <-segmentD.Result)
	waitUntilInactive(t, manager, request.SessionID)

	covering, err := manager.ResumeTools(ManagedRequest{
		TurnMetering:    true,
		CredentialScope: request.CredentialScope, AccountKey: request.AccountKey, BillingRunID: "http-covering",
	}, []ToolResult{
		{ToolCallID: "tool-a", Content: "a"},
		{ToolCallID: "tool-d", Content: "d"},
	})
	require.NoError(t, err)
	require.True(t, covering.Replay)
	require.Equal(t, segmentD.BillingRunID, covering.BillingRunID)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(covering.Events)))
	require.NoError(t, <-covering.Result)
	require.Equal(t, int32(1), runs.Load(), "historical covering set must replay D instead of starting another generation")
}

func TestEmptyOrInvalidResumeDoesNotMintAReceipt(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		return errors.New("provider must not start")
	}}, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("segment-empty")
	_, err = manager.ResumeTools(request, nil)
	require.ErrorIs(t, err, ErrSessionNotPending)
	_, err = manager.ResumeTool(request, ToolResult{ToolCallID: "missing", Content: "nope"})
	require.ErrorIs(t, err, ErrSessionNotPending)

	entries, err := os.ReadDir(store.completedRoot)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		require.Failf(t, "invalid resume must not write a completed tombstone", "found %s", entry.Name())
	}
}

func TestMixedPendingFollowUpChainUsesOneSegmentReceipt(t *testing.T) {
	var runs atomic.Int32
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-mix", Name: "lookup", Arguments: `{}`}})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				emit(Event{Type: EventText, Text: "tool-applied"})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		require.Equal(t, "continue", request.UserText)
		emit(Event{Type: EventText, Text: "follow-up"})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("segment-mixed")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	mixed := request
	mixed.FollowUpText = "continue"
	mixed.BillingRunID = "http-mixed"
	segmentB, err := manager.ResumeTool(mixed, ToolResult{ToolCallID: "tool-mix", Content: "ok"})
	require.NoError(t, err)
	for range segmentB.Events {
	}
	require.NoError(t, <-segmentB.Result)
	require.Equal(t, first.BillingRunID, segmentB.BillingRunID)
	waitUntilInactive(t, manager, request.SessionID)

	follow := request
	follow.FollowUpText = "continue"
	follow.ReuseBillingRun = true
	follow.RequireExisting = true
	chained, err := manager.StartFollowUp(follow, []ToolResult{{ToolCallID: "tool-mix", Content: "ok"}})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(chained.Events)))
	require.NoError(t, <-chained.Result)
	require.Equal(t, segmentB.BillingRunID, chained.BillingRunID, "internal mixed follow-up must stay on B's segment")
	require.Equal(t, int32(2), runs.Load())

	replay, err := manager.ResumeTools(ManagedRequest{
		CredentialScope: request.CredentialScope, AccountKey: request.AccountKey, BillingRunID: "http-mixed-retry",
		TurnMetering: true,
		FollowUpText: "continue",
	}, []ToolResult{{ToolCallID: "tool-mix", Content: "ok"}})
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.Equal(t, chained.BillingRunID, replay.BillingRunID)
	require.NoError(t, <-replay.Result)
	require.Equal(t, int32(2), runs.Load())
}

func TestSegmentReceiptsAreCredentialIsolated(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-iso", Name: "lookup", Arguments: `{}`}})
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
	firstReq := managedTestRequest("iso-a")
	firstReq.CredentialScope = "tenant:1:user:7:channel:6401"
	firstOut, err := manager.Start(firstReq)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-firstOut.Events).Type)
	require.NoError(t, manager.Park(firstReq))
	for range firstOut.Events {
	}
	require.NoError(t, <-firstOut.Result)
	firstB, err := manager.ResumeTool(firstReq, ToolResult{ToolCallID: "tool-iso", Content: "ok"})
	require.NoError(t, err)
	for range firstB.Events {
	}
	require.NoError(t, <-firstB.Result)

	other := firstReq
	other.SessionID = "iso-b"
	other.CredentialScope = "tenant:1:user:8:channel:6401"
	_, err = manager.ResumeTool(other, ToolResult{ToolCallID: "tool-iso", Content: "ok"})
	require.ErrorIs(t, err, ErrSessionNotPending)
}

func TestCompletedTombstoneReplaySurvivesThreeMinuteWindow(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	store, err := newFileStateStore(t.TempDir(), defaultStateTTL, func() time.Time { return now })
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	claim, err := store.Claim("ttl-segment", "conversation", fingerprint, ClaimOptions{NewBillingRunID: "should-not-win"})
	require.NoError(t, err)
	segmentID := generationSegmentID(fingerprint, []string{"tool-7"}, mustCanonicalToolResults(t, []ToolResult{{ToolCallID: "tool-7", Content: "result"}}), "")
	require.NoError(t, store.SetBillingRunID(claim.Owner, segmentID))
	claim.Owner.BillingRunID = segmentID
	require.NoError(t, store.SaveCompleted(claim.Owner, []ToolResult{{ToolCallID: "tool-7", Content: "result"}}, []Event{{Type: EventDone}}, true, ""))
	require.NoError(t, store.Release(claim.Owner))

	now = now.Add(3*time.Minute + 5*time.Second)
	replay, err := store.LookupCompleted(fingerprint, []ToolResult{{ToolCallID: "tool-7", Content: "result"}}, "")
	require.NoError(t, err)
	require.Equal(t, segmentID, replay.BillingRunID)
}

func mustCanonicalToolResults(t *testing.T, results []ToolResult) []CompletedToolResult {
	t.Helper()
	canonical, err := canonicalToolResults(results)
	require.NoError(t, err)
	return canonical
}
