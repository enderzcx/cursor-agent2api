package cursoragentv1

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/stretchr/testify/require"
)

func TestCompletedTombstoneReplaysIdenticalToolResultWithoutProviderRerun(t *testing.T) {
	var runs atomic.Int32
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-7", Name: "lookup", Arguments: `{}`}})
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
	request := managedTestRequest("replay-session")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	second, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(second.Events)))
	require.NoError(t, <-second.Result)
	require.Equal(t, int32(1), runs.Load())

	replayRequest := request
	replayRequest.SessionID = "client-generated-new-session"
	replay, err := manager.ResumeTool(replayRequest, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.Equal(t, second.BillingRunID, replay.BillingRunID)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(replay.Events)))
	require.NoError(t, <-replay.Result)
	require.Equal(t, int32(1), runs.Load(), "identical tool_result replay must not start a second provider run")
}

func TestCompletedTombstoneConflictingReplayFailsClosed(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-7", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
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
	request := managedTestRequest("conflict-session")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)
	second, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.NoError(t, err)
	for range second.Events {
	}
	require.NoError(t, <-second.Result)

	_, err = manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "other"})
	require.ErrorIs(t, err, ErrToolResultConflict)
}

func TestCompletedTombstoneRetainedAtLeastThreeMinutes(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	store, err := newFileStateStore(t.TempDir(), defaultStateTTL, func() time.Time { return now })
	require.NoError(t, err)
	fingerprint, err := CredentialFingerprint("tenant:1:user:7:channel:6401", "account-1")
	require.NoError(t, err)
	claim, err := store.Claim("ttl-session", "conversation", fingerprint, ClaimOptions{AllowCredentialChange: true})
	require.NoError(t, err)
	require.NoError(t, store.SaveCompleted(claim.Owner, []ToolResult{{ToolCallID: "tool-7", Content: "result"}}, []Event{{Type: EventDone}}, true, ""))
	require.NoError(t, store.Release(claim.Owner))

	now = now.Add(3*time.Minute + 5*time.Second)
	replay, err := store.LookupCompleted(fingerprint, []ToolResult{{ToolCallID: "tool-7", Content: "result"}}, "")
	require.NoError(t, err)
	require.Equal(t, "ttl-session", replay.SessionID)

	now = now.Add(completedTombstoneTTL - (3*time.Minute + 5*time.Second) + time.Second)
	_, err = store.LookupCompleted(fingerprint, []ToolResult{{ToolCallID: "tool-7", Content: "result"}}, "")
	require.ErrorIs(t, err, ErrSessionNotPending)
}

func TestResumeIgnoresMutableSessionAnchorAndBindsCredentialFingerprint(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-7", Name: "lookup", Arguments: `{}`}})
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
	request := managedTestRequest("anchor-session")
	request.CredentialScope = "tenant:1:user:7:channel:6401"
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	wrongUser := request
	wrongUser.SessionID = "anchor-session"
	wrongUser.CredentialScope = "tenant:1:user:8:channel:6401"
	_, err = manager.ResumeTool(wrongUser, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.ErrorIs(t, err, ErrSessionNotPending)

	resume := request
	resume.SessionID = "ignored-client-session"
	second, err := manager.ResumeTool(resume, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.NoError(t, err)
	require.Equal(t, first.BillingRunID, second.BillingRunID)
	for range second.Events {
	}
	require.NoError(t, <-second.Result)
}

func TestCoveringSetResumesRemainingPendingWithoutSessionID(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	request := managedTestRequest("covering-session")
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	require.NoError(t, err)
	claim, err := store.Claim(request.SessionID, "conversation", fingerprint, ClaimOptions{
		AllowPending: true, AllowCredentialChange: true, ToolCatalogDigest: request.ToolCatalogDigest, ToolCatalog: request.Run.Tools,
	})
	require.NoError(t, err)
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-a", Name: "a"}, {ToolCallID: "tool-b", Name: "b"}}))
	require.NoError(t, store.SavePending(claim.Owner, []PendingTool{{ToolCallID: "tool-b", Name: "b"}}))
	require.NoError(t, store.Release(claim.Owner))

	engine := &scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		result := <-resumed.ToolResults
		require.Equal(t, "tool-b", result.ToolCallID)
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	resume := request
	resume.SessionID = ""
	output, err := manager.ResumeTools(resume, []ToolResult{
		{ToolCallID: "tool-a", Content: "already applied"},
		{ToolCallID: "tool-b", Content: "remaining"},
	})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
}

func TestMixedFollowUpAfterCompletedStartsNewTurn(t *testing.T) {
	var runs atomic.Int32
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-7", Name: "lookup", Arguments: `{}`}})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				emit(Event{Type: EventText, Text: "tool-done"})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		require.False(t, request.Resume)
		require.Equal(t, "continue", request.UserText)
		emit(Event{Type: EventText, Text: "follow-up"})
		emit(Event{Type: EventDone})
		return nil
	}}
	manager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("mixed-session")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)
	second, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "result"})
	require.NoError(t, err)
	for range second.Events {
	}
	require.NoError(t, <-second.Result)

	mixed := request
	mixed.SessionID = ""
	mixed.FollowUpText = "continue"
	mixed.Run.UserText = ""
	third, err := manager.ResumeTools(mixed, []ToolResult{{ToolCallID: "tool-7", Content: "result"}})
	require.NoError(t, err)
	require.True(t, third.Replay)
	require.Equal(t, second.BillingRunID, third.BillingRunID)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(third.Events)))
	require.NoError(t, <-third.Result)
	require.Equal(t, int32(1), runs.Load())
}

func TestResumeFlightFollowerCancelLeavesLeaderReplayable(t *testing.T) {
	var runs atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-follow", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventText, Text: "leader-finished"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("follower-cancel")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)

	leader := make(chan *ManagedOutput, 1)
	leaderErr := make(chan error, 1)
	go func() {
		output, resumeErr := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-follow", Content: "result"})
		if resumeErr != nil {
			leaderErr <- resumeErr
			return
		}
		leader <- output
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resume leader")
	}

	followerCtx, cancelFollower := context.WithCancel(context.Background())
	followerReq := request
	followerReq.Context = followerCtx
	followerErr := make(chan error, 1)
	go func() {
		_, resumeErr := manager.ResumeTool(followerReq, ToolResult{ToolCallID: "tool-follow", Content: "result"})
		followerErr <- resumeErr
	}()
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return len(manager.flights) == 1
	}, time.Second, 2*time.Millisecond)
	cancelFollower()
	select {
	case err := <-followerErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("disconnected resume follower did not return promptly")
	}

	close(release)
	var leaderOutput *ManagedOutput
	select {
	case err := <-leaderErr:
		require.NoError(t, err)
	case leaderOutput = <-leader:
	case <-time.After(2 * time.Second):
		t.Fatal("resume leader did not finish after follower cancel")
	}
	require.NotNil(t, leaderOutput)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(leaderOutput.Events)))
	require.NoError(t, <-leaderOutput.Result)
	require.Equal(t, int32(1), runs.Load())

	waitUntilInactive(t, manager, request.SessionID)
	replay, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-follow", Content: "result"})
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.Equal(t, leaderOutput.BillingRunID, replay.BillingRunID)
	require.Equal(t, []EventType{EventToolResultAccepted, EventText, EventDone}, eventTypes(collectManagedEvents(replay.Events)))
	require.NoError(t, <-replay.Result)
	require.Equal(t, int32(1), runs.Load(), "cancelled follower must not cancel the leader or prevent replay")
}

func TestFollowUpFollowerCancelDoesNotStartOrBill(t *testing.T) {
	var runs atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-followup", Name: "lookup", Arguments: `{}`}})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		close(started)
		select {
		case <-release:
			emit(Event{Type: EventText, Text: "leader-follow-up"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	manager, err := newSessionManager(engine, store, 2*time.Second)
	require.NoError(t, err)
	request := managedTestRequest("followup-follower")
	first, err := manager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, manager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)
	second, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-followup", Content: "ok"})
	require.NoError(t, err)
	for range second.Events {
	}
	require.NoError(t, <-second.Result)
	waitUntilInactive(t, manager, request.SessionID)

	covering := []ToolResult{{ToolCallID: "tool-followup", Content: "ok"}}
	follow := request
	follow.FollowUpText = "continue"
	follow.ReuseBillingRun = true
	follow.RequireExisting = true
	leader := make(chan *ManagedOutput, 1)
	leaderErr := make(chan error, 1)
	go func() {
		output, followErr := manager.StartFollowUp(follow, covering)
		if followErr != nil {
			leaderErr <- followErr
			return
		}
		leader <- output
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for follow-up leader")
	}

	followerCtx, cancelFollower := context.WithCancel(context.Background())
	follower := follow
	follower.Context = followerCtx
	followerErr := make(chan error, 1)
	startedWait := time.Now()
	go func() {
		_, waitErr := manager.StartFollowUp(follower, covering)
		followerErr <- waitErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancelFollower()
	select {
	case err := <-followerErr:
		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(startedWait), time.Second)
	case <-time.After(time.Second):
		t.Fatal("disconnected follow-up follower did not return promptly")
	}
	require.Equal(t, int32(2), runs.Load(), "cancelled follow-up follower must not start another provider run")

	close(release)
	var leaderOutput *ManagedOutput
	select {
	case err := <-leaderErr:
		require.NoError(t, err)
	case leaderOutput = <-leader:
	case <-time.After(2 * time.Second):
		t.Fatal("follow-up leader did not finish after follower cancel")
	}
	require.NotNil(t, leaderOutput)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(leaderOutput.Events)))
	require.NoError(t, <-leaderOutput.Result)
	require.Equal(t, int32(2), runs.Load())

	waitUntilInactive(t, manager, request.SessionID)
	baseReplay, err := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-followup", Content: "ok"})
	require.NoError(t, err)
	require.True(t, baseReplay.Replay)
	require.False(t, baseReplay.FollowUpApplied)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(baseReplay.Events)))
	require.NoError(t, <-baseReplay.Result)

	followReplay := request
	followReplay.FollowUpText = "continue"
	replay, err := manager.ResumeTools(followReplay, []ToolResult{{ToolCallID: "tool-followup", Content: "ok"}})
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.True(t, replay.FollowUpApplied)
	require.Equal(t, leaderOutput.BillingRunID, replay.BillingRunID)
	require.Equal(t, []EventType{EventText, EventDone}, eventTypes(collectManagedEvents(replay.Events)))
	require.NoError(t, <-replay.Result)
	require.Equal(t, int32(2), runs.Load())
}

func TestStartFollowUpCanceledClientDoesNotClaimOrBill(t *testing.T) {
	var runs atomic.Int32
	store, err := NewFileStateStore(t.TempDir())
	require.NoError(t, err)
	manager, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		runs.Add(1)
		return errors.New("provider must not start")
	}}, store, time.Second)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := managedTestRequest("canceled-followup")
	request.Context = ctx
	request.FollowUpText = "continue"
	_, err = manager.StartFollowUp(request, []ToolResult{{ToolCallID: "tool-x", Content: "ok"}})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(0), runs.Load())
}

func TestDuplicateRetryRaceOneProviderRun(t *testing.T) {
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
	request := managedTestRequest("race-session")
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
			output, resumeErr := manager.ResumeTool(request, ToolResult{ToolCallID: "tool-7", Content: "result"})
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

func TestRestartAndDisconnectReplayCompletedSnapshot(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	engine := &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-9", Name: "lookup", Arguments: `{}`}})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			search := &HostedSearch{CallID: "restart-search", Kind: HostedSearchKindWebSearch, Query: "q", References: []HostedSearchReference{{URL: "https://example.com", Chunk: "evidence"}}}
			emit(Event{Type: EventHostedSearchStarted, HostedSearch: search})
			emit(Event{Type: EventHostedSearchCompleted, HostedSearch: search})
			emit(Event{Type: EventText, Text: "restored"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	firstManager, err := newSessionManager(engine, store, time.Second)
	require.NoError(t, err)
	request := managedTestRequest("disconnect-session")
	first, err := firstManager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, firstManager.Park(request))
	for range first.Events {
	}
	require.NoError(t, <-first.Result)
	second, err := firstManager.ResumeTool(request, ToolResult{ToolCallID: "tool-9", Content: "ok"})
	require.NoError(t, err)
	originalEvents := collectManagedEvents(second.Events)
	require.NoError(t, <-second.Result)
	waitUntilInactive(t, firstManager, request.SessionID)

	restartedStore, err := NewFileStateStore(root)
	require.NoError(t, err)
	var restartedRuns atomic.Int32
	restarted, err := newSessionManager(&scriptedEngine{run: func(context.Context, RunRequest, func(Event)) error {
		restartedRuns.Add(1)
		t.Fatal("restart replay must not call the provider")
		return errors.New("provider rerun")
	}}, restartedStore, time.Second)
	require.NoError(t, err)
	replay, err := restarted.ResumeTool(ManagedRequest{
		SessionID: "ignored", CredentialScope: request.CredentialScope, AccountKey: request.AccountKey, TurnMetering: true,
	}, ToolResult{ToolCallID: "tool-9", Content: "ok"})
	require.NoError(t, err)
	require.True(t, replay.Replay)
	require.Equal(t, second.BillingRunID, replay.BillingRunID)
	require.Equal(t, originalEvents, collectManagedEvents(replay.Events))
	require.NoError(t, <-replay.Result)
	require.Equal(t, int32(0), restartedRuns.Load())
}

func TestAnchoredExecutorIdenticalToolResultReplayKeepsStableReceipt(t *testing.T) {
	var runs atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		runs.Add(1)
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-replay", Name: "lookup", Arguments: `{}`}})
		emit(Event{Type: EventTurnEnded})
		select {
		case result := <-request.ToolResults:
			emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
			emit(Event{Type: EventText, Text: "done"})
			emit(Event{Type: EventDone})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("session-replay", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	continuation := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + ids[0] + `","content":"ok"}]}]}`
	second, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored-session", continuation), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Equal(t, first.Headers.Get(HeaderUsageReceipt), second.Headers.Get(HeaderUsageReceipt))
	require.Empty(t, second.Headers.Get(HeaderUsageReplay))

	third, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("another-ignored-session", continuation), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Equal(t, second.Headers.Get(HeaderUsageReceipt), third.Headers.Get(HeaderUsageReceipt))
	require.Equal(t, "1", third.Headers.Get(HeaderUsageReplay))
	require.Contains(t, string(third.Payload), `"text":"done"`)
	require.Equal(t, int32(1), runs.Load())
}

func TestAnchoredExecutorMixedFollowUpAfterCompletedDoesNotRerunTools(t *testing.T) {
	var runs atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-mix", Name: "lookup", Arguments: `{}`}})
			emit(Event{Type: EventTurnEnded})
			select {
			case result := <-request.ToolResults:
				emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
				emit(Event{Type: EventText, Text: "tool-done"})
				emit(Event{Type: EventDone})
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		require.Equal(t, "continue", request.UserText)
		require.Len(t, request.Tools, 1)
		require.Equal(t, "lookup", request.Tools[0].Name)
		emit(Event{Type: EventText, Text: "next"})
		emit(Event{Type: EventDone})
		return nil
	}})
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("session-mix", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	_, err = executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"`+ids[0]+`","content":"ok"}]}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	mixedPayload := `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + ids[0] + `","content":"ok"},{"type":"text","text":"continue"}]}]
	}`
	turn, err := translateExecutionRequest(anchoredClaudeExecutorRequest("ignored-mix", mixedPayload), claudeExecutorOptions(false), "tenant:1:user:7:channel:6401", "account-1", "https://agent.example", "access", "cli-test")
	require.NoError(t, err)
	require.Equal(t, "continue", turn.managed.FollowUpText)
	mixed, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored-mix", mixedPayload), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Equal(t, int32(2), runs.Load())
	require.Empty(t, mixed.Headers.Get(HeaderUsageReplay))
	require.Contains(t, string(mixed.Payload), `"text":"next"`)
	require.NotEqual(t, first.Headers.Get(HeaderUsageReceipt), mixed.Headers.Get(HeaderUsageReceipt))
}

func TestFileStateStoreRejectsVersionZeroInsteadOfMigrating(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	path, _ := store.paths("v0-session")
	require.NoError(t, writeStateAtomic(path, StateSnapshot{
		Version: 0, SessionID: "v0-session", ConversationID: "conversation",
		CredentialFingerprint: "fingerprint", OwnerGeneration: 1, OwnerToken: "token",
		PendingTools: []PendingTool{{ToolCallID: "tool-1", Name: "lookup"}}, PendingBatchIDs: []string{"tool-1"},
		UpdatedAt: time.Now().UTC(),
	}))
	_, err = store.Claim("v0-session", "ignored", "fingerprint", ClaimOptions{AllowPending: true, RequireExisting: true})
	require.ErrorContains(t, err, "unsupported Cursor Agent v1 state version 0")
}

func TestFileStateStoreMigratesV1StateDeterministically(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	path, _ := store.paths("v1-session")
	require.NoError(t, writeStateAtomic(path, StateSnapshot{
		Version: 1, SessionID: "v1-session", ConversationID: "conversation",
		CredentialFingerprint: "fingerprint", OwnerGeneration: 1, OwnerToken: "token",
		PendingTools: []PendingTool{{ToolCallID: "tool-1", Name: "lookup"}}, PendingBatchIDs: []string{"tool-1"},
		UpdatedAt: time.Now().UTC(),
	}))
	next, err := store.Claim("v1-session", "ignored", "fingerprint", ClaimOptions{AllowPending: true, RequireExisting: true})
	require.NoError(t, err)
	require.Equal(t, stateVersion, next.Snapshot.Version)
	require.Equal(t, "conversation", next.Owner.ConversationID)
	require.Equal(t, "tool-1", next.Snapshot.PendingTools[0].ToolCallID)
	require.NoError(t, store.Release(next.Owner))
}

func TestAnchoredExecutorPendingMixedFollowUpChainsOneReceipt(t *testing.T) {
	var runs atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-pending", Name: "lookup", Arguments: `{}`}})
			emit(Event{Type: EventTurnEnded})
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
		require.False(t, request.Resume)
		require.Equal(t, "continue", request.UserText)
		require.Len(t, request.Tools, 1)
		require.Equal(t, "lookup", request.Tools[0].Name)
		emit(Event{Type: EventText, Text: "final-marker"})
		emit(Event{Type: EventDone})
		return nil
	}})
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("session-pending-mix", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	mixed, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"`+ids[0]+`","content":"ok"},{"type":"text","text":"continue"}]}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(mixed.Payload), `"text":"final-marker"`)
	require.NotContains(t, string(mixed.Payload), "tool-applied")
	require.Equal(t, first.Headers.Get(HeaderUsageReceipt), mixed.Headers.Get(HeaderUsageReceipt))
	require.Empty(t, mixed.Headers.Get(HeaderUsageReplay))
	require.Equal(t, int32(2), runs.Load())

	replay, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored-replay", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"`+ids[0]+`","content":"ok"},{"type":"text","text":"continue"}]}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Equal(t, mixed.Headers.Get(HeaderUsageReceipt), replay.Headers.Get(HeaderUsageReceipt))
	require.Equal(t, "1", replay.Headers.Get(HeaderUsageReplay))
	require.Equal(t, int32(2), runs.Load())
}

func TestAnchoredExecutorPendingMixedFollowUpNoneDoesNotRestoreCatalog(t *testing.T) {
	var runs atomic.Int32
	var followTools []ToolDefinition
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			require.Len(t, request.Tools, 1)
			require.Equal(t, "beefapi_conformance_canary", request.Tools[0].Name)
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-canary", Name: "beefapi_conformance_canary", Arguments: `{"marker":"cursor-v1-mixed"}`}})
			emit(Event{Type: EventTurnEnded})
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
		require.False(t, request.Resume)
		require.Equal(t, "continue after the tool result", request.UserText)
		followTools = cloneToolDefinitions(request.Tools)
		emit(Event{Type: EventText, Text: "BEEFAPI_CURSOR_V1_MIXED_OK"})
		emit(Event{Type: EventDone})
		return nil
	}})
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("session-pending-mix-none", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],
		"tools":[{"name":"beefapi_conformance_canary","input_schema":{"type":"object","properties":{"marker":{"type":"string"}}}}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	mixed, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored", `{
		"model":"claude-sonnet-4-6",
		"tool_choice":{"type":"none"},
		"tools":[{"name":"beefapi_conformance_canary","input_schema":{"type":"object","properties":{"marker":{"type":"string"}}}}],
		"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"`+ids[0]+`","content":"ok"},{"type":"text","text":"continue after the tool result"}]}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(mixed.Payload), `"text":"BEEFAPI_CURSOR_V1_MIXED_OK"`)
	require.NotContains(t, string(mixed.Payload), `"type":"tool_use"`)
	require.NotContains(t, string(mixed.Payload), "tool-applied")
	require.Empty(t, followTools)
	require.Equal(t, first.Headers.Get(HeaderUsageReceipt), mixed.Headers.Get(HeaderUsageReceipt))
	require.Empty(t, mixed.Headers.Get(HeaderUsageReplay))
	require.Equal(t, int32(2), runs.Load())
}

func TestDisconnectedParkedSessionStillResumesAfterNewManager(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStateStore(root)
	require.NoError(t, err)
	request := managedTestRequest("park-disconnect")
	stopFirst := make(chan struct{})
	firstEngine := &scriptedEngine{run: func(_ context.Context, _ RunRequest, emit func(Event)) error {
		emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "tool-d", Name: "lookup", Arguments: `{}`}})
		<-stopFirst
		return errors.New("simulated disconnect")
	}}
	firstManager, err := newSessionManager(firstEngine, store, time.Second)
	require.NoError(t, err)
	first, err := firstManager.Start(request)
	require.NoError(t, err)
	require.Equal(t, EventToolCall, (<-first.Events).Type)
	require.NoError(t, firstManager.Park(request))
	for range first.Events {
	}
	close(stopFirst)
	if firstErr := <-first.Result; firstErr != nil {
		require.EqualError(t, firstErr, "simulated disconnect")
	}
	waitUntilInactive(t, firstManager, request.SessionID)
	restartedStore, err := NewFileStateStore(root)
	require.NoError(t, err)
	second, err := newSessionManager(&scriptedEngine{run: func(_ context.Context, resumed RunRequest, emit func(Event)) error {
		require.True(t, resumed.Resume)
		result := <-resumed.ToolResults
		require.Equal(t, "tool-d", result.ToolCallID)
		emit(Event{Type: EventToolResultAccepted, ToolResult: &result})
		emit(Event{Type: EventDone})
		return nil
	}}, restartedStore, time.Second)
	require.NoError(t, err)
	output, err := second.ResumeTool(ManagedRequest{CredentialScope: request.CredentialScope, AccountKey: request.AccountKey, TurnMetering: true}, ToolResult{ToolCallID: "tool-d", Content: "ok"})
	require.NoError(t, err)
	require.Equal(t, []EventType{EventToolResultAccepted, EventDone}, eventTypes(collectManagedEvents(output.Events)))
	require.NoError(t, <-output.Result)
}

func TestExecutorChatHistoryMixedFollowUpBuildsOneNewRun(t *testing.T) {
	var runs atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-chat", Name: "lookup", Arguments: `{"id":1}`}})
			emit(Event{Type: EventDone})
			return nil
		}
		require.Equal(t, "continue", request.UserText)
		emit(Event{Type: EventText, Text: "chat-final"})
		emit(Event{Type: EventDone})
		return nil
	}})
	firstReq := cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatOpenAI,
		Payload:  []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}}]}`),
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "chat-mixed"},
	}
	first, err := executor.Execute(context.Background(), testExecutorAuth(), firstReq, cliproxyexecutor.Options{SourceFormat: firstReq.Format, ResponseFormat: firstReq.Format, OriginalRequest: firstReq.Payload, Headers: claudeExecutorOptions(false).Headers})
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 1)
	continuation := `{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"lookup"},{"role":"assistant","tool_calls":[{"id":"` + ids[0] + `","type":"function","function":{"name":"lookup","arguments":"{\"id\":1}"}}]},{"role":"tool","tool_call_id":"` + ids[0] + `","content":"ok"},{"role":"user","content":"continue"}]}`
	secondReq := cliproxyexecutor.Request{
		Model: "claude-sonnet-4-6", Format: sdktranslator.FormatOpenAI, Payload: []byte(continuation),
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "chat-mixed-ignored"},
	}
	second, err := executor.Execute(context.Background(), testExecutorAuth(), secondReq, cliproxyexecutor.Options{SourceFormat: secondReq.Format, ResponseFormat: secondReq.Format, OriginalRequest: secondReq.Payload, Headers: claudeExecutorOptions(false).Headers})
	require.NoError(t, err)
	require.Contains(t, string(second.Payload), "chat-final")
	require.NotEqual(t, first.Headers.Get(HeaderUsageReceipt), second.Headers.Get(HeaderUsageReceipt))
	require.Equal(t, int32(2), runs.Load())
}

func TestAnchoredExecutorCoveringSetMixedFollowUp(t *testing.T) {
	var runs atomic.Int32
	executor := testExecutor(t, &scriptedEngine{run: func(ctx context.Context, request RunRequest, emit func(Event)) error {
		n := runs.Add(1)
		if n == 1 {
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-a", Name: "alpha", Arguments: `{}`}})
			emit(Event{Type: EventToolCall, ToolCall: &ToolCall{ToolCallID: "call-b", Name: "beta", Arguments: `{}`}})
			emit(Event{Type: EventTurnEnded})
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
		}
		require.Equal(t, "next please", request.UserText)
		emit(Event{Type: EventText, Text: "covering-final"})
		emit(Event{Type: EventDone})
		return nil
	}})
	tools := `[{"name":"alpha","input_schema":{"type":"object"}},{"name":"beta","input_schema":{"type":"object"}}]`
	first, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("covering-mix", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"use tools"}],"tools":`+tools+`
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	ids := agentV1ToolCallIDs(first.Payload)
	require.Len(t, ids, 2)
	mixed, err := executor.Execute(context.Background(), testExecutorAuth(), anchoredClaudeExecutorRequest("ignored", `{
		"model":"claude-sonnet-4-6","messages":[{"role":"user","content":[
			{"type":"tool_result","tool_use_id":"`+ids[0]+`","content":"one"},
			{"type":"tool_result","tool_use_id":"`+ids[1]+`","content":"two"},
			{"type":"text","text":"next please"}
		]}]
	}`), claudeExecutorOptions(false))
	require.NoError(t, err)
	require.Contains(t, string(mixed.Payload), "covering-final")
	require.Equal(t, first.Headers.Get(HeaderUsageReceipt), mixed.Headers.Get(HeaderUsageReceipt))
	require.Equal(t, int32(2), runs.Load())
}
