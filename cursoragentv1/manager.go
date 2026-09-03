package cursoragentv1

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const defaultManagedRunTimeout = 30 * time.Minute

var (
	ErrSessionActive       = errors.New("Cursor Agent v1 session is already active")
	ErrSessionPending      = errors.New("Cursor Agent v1 session is waiting for tool results")
	ErrSessionNotPending   = errors.New("Cursor Agent v1 session has no matching pending tool")
	ErrMixedToolFollowUp   = errors.New("Cursor Agent v1 cannot mix a new user message with tool results")
	ErrCallerToolsDisabled = errors.New("Cursor Agent v1 caller tools are disabled")
)

type runEngine interface {
	Run(context.Context, RunRequest, func(Event)) error
}

type ManagedRequest struct {
	TurnMetering       bool
	RequestSegmentID   string
	SessionID          string
	BillingRunID       string
	CredentialScope    string
	AccountKey         string
	ToolCatalogDigest  string
	FollowUpText       string
	ActiveResults      []ToolResult
	CarryUsage         []Event
	RequireExisting    bool
	ReuseBillingRun    bool
	Context            context.Context
	Run                RunRequest
	DisableCallerTools bool
}

func requestContext(request ManagedRequest) context.Context {
	if request.Context != nil {
		return request.Context
	}
	return context.Background()
}

// applyCallerToolCatalog restores a persisted catalog only when this request
// omitted tools. Explicit tool_choice=none keeps tools disabled instead of
// treating an empty digest as "reuse the last catalog".
func applyCallerToolCatalog(request *ManagedRequest, snapshot []ToolDefinition, requireSnapshot bool) error {
	if request == nil {
		return ErrSessionNotPending
	}
	if request.DisableCallerTools {
		request.Run.Tools = nil
		return nil
	}
	if len(request.Run.Tools) > 0 || strings.TrimSpace(request.ToolCatalogDigest) != "" {
		return nil
	}
	if len(snapshot) == 0 && requireSnapshot {
		return ErrSessionNotPending
	}
	request.Run.Tools = cloneToolDefinitions(snapshot)
	return nil
}

type ManagedOutput struct {
	RequestSegmentID string
	Events           <-chan Event
	Result           <-chan error
	SessionID        string
	BillingRunID     string
	ConversationID   string
	OriginalBatch    []string
	Replay           bool
	HasPending       bool
	FollowUpApplied  bool
}

type SessionManager struct {
	runner     runEngine
	store      StateStore
	runTimeout time.Duration

	mu       sync.Mutex
	active   map[string]*managedSession
	starting map[string]struct{}
	flights  map[string]*resumeFlight
}

type resumeFlight struct {
	key    string
	digest string
	done   chan struct{}
	replay *CompletedReplay
	err    error
}

type managedSession struct {
	manager *SessionManager
	owner   OwnerLease
	request RunRequest
	cancel  context.CancelFunc

	toolResults chan ToolResult

	mu                 sync.Mutex
	output             *managedOutput
	pending            map[string]PendingTool
	stateErr           error
	closed             bool
	capturing          bool
	pendingBatch       []string
	segmentBatch       []string
	segmentResults     []ToolResult
	segmentSnapshot    eventSnapshot
	snapshotFollowUp   string
	terminalEvent      *Event
	flight             *resumeFlight
	flightFingerprint  string
	flightBatch        []string
	pendingFollowUp    string
	billingCarry       []Event
	disableCallerTools bool
}

type managedOutput struct {
	events chan Event
	result chan error
	mu     sync.Mutex
	closed bool
}

func NewSessionManager(runner *Runner, store StateStore) (*SessionManager, error) {
	if runner == nil {
		return nil, fmt.Errorf("Cursor Agent v1 runner is required")
	}
	return newSessionManager(runner, store, defaultManagedRunTimeout)
}

func newSessionManager(runner runEngine, store StateStore, runTimeout time.Duration) (*SessionManager, error) {
	if runner == nil || store == nil {
		return nil, fmt.Errorf("Cursor Agent v1 runner and state store are required")
	}
	if runTimeout <= 0 {
		runTimeout = defaultManagedRunTimeout
	}
	return &SessionManager{
		runner: runner, store: store, runTimeout: runTimeout,
		active: make(map[string]*managedSession), starting: make(map[string]struct{}),
		flights: make(map[string]*resumeFlight),
	}, nil
}

func (m *SessionManager) Start(request ManagedRequest) (*ManagedOutput, error) {
	if request.Run.HistoryRequestKey != "" {
		fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
		if err != nil {
			return nil, err
		}
		if replay, err := m.store.LookupHistory(fingerprint, request.Run.HistoryRequestKey); err == nil {
			return emitCompletedReplay(replay), nil
		} else if !errors.Is(err, ErrSessionNotPending) {
			return nil, err
		}
		request.SessionID = historySessionID(fingerprint, request.Run.HistoryRequestKey)
	}
	sessionID := strings.TrimSpace(request.SessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("Cursor Agent v1 session id is required")
	}
	if !m.reserve(sessionID) {
		return nil, ErrSessionActive
	}
	reserved := true
	defer func() {
		if reserved {
			m.releaseReservation(sessionID)
		}
	}()

	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return nil, err
	}
	conversationID := strings.TrimSpace(request.Run.ConversationID)
	if conversationID == "" {
		conversationID = uuid.NewString()
	}
	claim, err := m.store.Claim(sessionID, conversationID, fingerprint, ClaimOptions{
		RuntimeProfile:    request.Run.RuntimeProfile,
		HistoryRequestKey: request.Run.HistoryRequestKey, AllowPending: request.Run.HistoryRequestKey != "",
		AllowCredentialChange: !request.RequireExisting, RequireExisting: request.RequireExisting,
		ToolCatalogDigest: request.ToolCatalogDigest, ToolCatalog: request.Run.Tools,
		NewBillingRunID: billingIDForRequest(request), ReuseBillingRun: request.ReuseBillingRun,
	})
	if err != nil {
		if errors.Is(err, ErrStatePending) {
			return nil, ErrSessionPending
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSessionNotPending
		}
		return nil, err
	}
	request.Run.ConversationID = claim.Owner.ConversationID
	request.Run.RuntimeProfile = normalizeRuntimeProfile(claim.Snapshot.RuntimeProfile)
	if claim.Replay != nil {
		return emitCompletedReplay(*claim.Replay), nil
	}
	request.Run.InitialCheckpoint = append([]byte(nil), claim.Snapshot.Checkpoint...)
	claim.Owner.RequestSegmentID = request.BillingRunID
	request.Run.InitialBlobs = cloneBlobs(claim.Snapshot.Blobs)
	request.Run.Resume = false
	output, err := m.startClaimed(request.Run, claim.Owner, claim.Snapshot.PendingTools, nil, nil, nil, nil, request.DisableCallerTools)
	if err == nil {
		reserved = false
	}
	return output, err
}

func (m *SessionManager) ResumeTool(request ManagedRequest, result ToolResult) (*ManagedOutput, error) {
	return m.ResumeTools(request, []ToolResult{result})
}

func (m *SessionManager) ResumeTools(request ManagedRequest, results []ToolResult) (*ManagedOutput, error) {
	if len(results) == 0 {
		return nil, ErrSessionNotPending
	}
	for index := range results {
		results[index].ToolCallID = strings.TrimSpace(results[index].ToolCallID)
		if results[index].ToolCallID == "" {
			return nil, ErrSessionNotPending
		}
	}
	canonical, err := canonicalToolResults(results)
	if err != nil {
		return nil, err
	}
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return nil, err
	}
	submittedIDs := completedBatchIDs(canonical)
	originalBatch, err := m.store.OriginalPendingBatch(fingerprint, submittedIDs)
	if err != nil {
		return nil, err
	}
	accepted := canonical
	if filtered, ok := completedResultsForBatch(canonical, originalBatch); ok {
		accepted = filtered
	} else {
		originalBatch = submittedIDs
	}
	digest := toolResultDigest(accepted)
	followUp := strings.TrimSpace(request.FollowUpText)
	flight, leader, err := m.beginResumeFlight(fingerprint, originalBatch, digest, followUp)
	if err != nil {
		return nil, err
	}
	if !leader {
		output, waitErr := waitResumeFlight(requestContext(request), flight)
		if waitErr == nil && isMeteredTurn(output.BillingRunID) && !request.TurnMetering {
			return nil, meteringVersionError()
		}
		return output, waitErr
	}
	fail := func(err error) (*ManagedOutput, error) {
		m.endResumeFlight(fingerprint, originalBatch, flight, nil, err)
		return nil, err
	}

	sessionID, pendingErr := m.store.ResolvePending(fingerprint, submittedIDs)
	if pendingErr != nil && !errors.Is(pendingErr, ErrSessionNotPending) {
		return fail(pendingErr)
	}

	lookupResults := results
	if sessionID != "" {
		lookupResults = toolResultsFromCanonical(accepted)
	}
	replay, replayErr := m.lookupRequestReplay(fingerprint, lookupResults, followUp)
	if replayErr == nil {
		if isMeteredTurn(replay.BillingRunID) && !request.TurnMetering {
			return fail(meteringVersionError())
		}
		output := emitCompletedReplay(replay)
		m.endResumeFlight(fingerprint, originalBatch, flight, &replay, nil)
		return output, nil
	}
	if !errors.Is(replayErr, ErrSessionNotPending) {
		return fail(replayErr)
	}
	if sessionID == "" {
		return fail(ErrSessionNotPending)
	}

	request.SessionID = sessionID
	segmentID := generationSegmentID(fingerprint, originalBatch, accepted, followUp)
	segmentResults := toolResultsFromCanonical(accepted)

	m.mu.Lock()
	active := m.active[sessionID]
	m.mu.Unlock()
	if active != nil {
		active.mu.Lock()
		metered := isMeteredTurn(active.owner.BillingRunID)
		active.mu.Unlock()
		if metered && !request.TurnMetering {
			return fail(meteringVersionError())
		}
		if active.owner.CredentialHash != fingerprint {
			return fail(ErrCredentialMismatch)
		}
		if followUp != "" {
			active.mu.Lock()
			active.pendingFollowUp = followUp
			active.mu.Unlock()
		}
		m.bindResumeFlight(sessionID, fingerprint, originalBatch, flight)
		output, resumeErr := active.resume(results, originalBatch, segmentResults, segmentID, request.DisableCallerTools)
		if resumeErr != nil {
			return fail(resumeErr)
		}
		return output, nil
	}
	if !m.reserve(sessionID) {
		return fail(ErrSessionActive)
	}
	reserved := true
	defer func() {
		if reserved {
			m.releaseReservation(sessionID)
		}
	}()

	conversationID := strings.TrimSpace(request.Run.ConversationID)
	if conversationID == "" {
		conversationID = uuid.NewString()
	}
	claim, err := m.store.Claim(sessionID, conversationID, fingerprint, ClaimOptions{
		RuntimeProfile: request.Run.RuntimeProfile,
		// A resumed pending run keeps the catalog that issued its tool calls,
		// just like the live-owner path above. New caller declarations belong
		// to the next turn, not to this saved checkpoint's execution context.
		AllowPending: true, RequireExisting: true, RequireToolCatalog: true,
		ToolCatalogDigest: request.ToolCatalogDigest,
		NewBillingRunID:   segmentID,
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fail(ErrSessionNotPending)
		}
		return fail(err)
	}
	filteredResults, matches := filterPendingResults(claim.Snapshot.PendingTools, results)
	if isMeteredTurn(claim.Owner.BillingRunID) && !request.TurnMetering {
		_ = m.store.Release(claim.Owner)
		return fail(meteringVersionError())
	}
	claim.Owner.RequestSegmentID = segmentID
	if claim.ColdRebuild || !matches {
		_ = m.store.Release(claim.Owner)
		return fail(ErrSessionNotPending)
	}
	request.Run.ConversationID = claim.Owner.ConversationID
	request.Run.RuntimeProfile = normalizeRuntimeProfile(claim.Snapshot.RuntimeProfile)
	if len(claim.Snapshot.ToolCatalog) > 0 {
		request.Run.Tools = nil
		request.ToolCatalogDigest = ""
	}
	if err := applyCallerToolCatalog(&request, claim.Snapshot.ToolCatalog, true); err != nil {
		_ = m.store.Release(claim.Owner)
		return fail(err)
	}
	if followUp == "" {
		followUp = strings.TrimSpace(claim.Snapshot.FollowUpText)
		request.FollowUpText = followUp
	}
	request.Run.UserText = ""
	request.Run.Resume = true
	request.Run.InitialCheckpoint = append([]byte(nil), claim.Snapshot.Checkpoint...)
	request.Run.InitialBlobs = cloneBlobs(claim.Snapshot.Blobs)
	snapshotBatch := append([]string(nil), claim.Snapshot.PendingBatchIDs...)
	if len(snapshotBatch) == 0 {
		snapshotBatch = pendingToolIDs(claim.Snapshot.PendingTools)
	}
	output, err := m.startClaimed(request.Run, claim.Owner, claim.Snapshot.PendingTools, filteredResults, segmentResults, snapshotBatch, claim.Snapshot.UsageCarry, request.DisableCallerTools)
	if err != nil {
		return fail(err)
	}
	reserved = false
	if followUp != "" {
		m.mu.Lock()
		if session := m.active[sessionID]; session != nil {
			session.pendingFollowUp = followUp
		}
		m.mu.Unlock()
	}
	m.bindResumeFlight(sessionID, fingerprint, originalBatch, flight)
	return output, nil
}

func (m *SessionManager) Cancel(request ManagedRequest) error {
	sessionID := strings.TrimSpace(request.SessionID)
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return err
	}
	m.mu.Lock()
	session := m.active[sessionID]
	var output *managedOutput
	if session != nil && session.owner.CredentialHash != fingerprint {
		m.mu.Unlock()
		return ErrCredentialMismatch
	}
	if session != nil {
		session.mu.Lock()
		session.closed = true
		output = session.output
		session.output = nil
		session.mu.Unlock()
		delete(m.active, sessionID)
	}
	m.mu.Unlock()
	if session == nil {
		return nil
	}
	deleteErr := m.store.Delete(session.owner)
	session.cancel()
	if output != nil {
		output.close(context.Canceled)
	}
	releaseErr := m.store.Release(session.owner)
	if deleteErr != nil {
		return deleteErr
	}
	return releaseErr
}

// Park closes the current downstream turn after the protocol adapter has
// emitted the complete tool-call batch. It never decides batching by timer.
func (m *SessionManager) Park(request ManagedRequest) error {
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return err
	}
	m.mu.Lock()
	session := m.active[strings.TrimSpace(request.SessionID)]
	if session == nil {
		m.mu.Unlock()
		return ErrSessionNotPending
	}
	if session.owner.CredentialHash != fingerprint {
		m.mu.Unlock()
		return ErrCredentialMismatch
	}
	session.mu.Lock()
	if session.closed || len(session.pending) == 0 || session.output == nil {
		session.mu.Unlock()
		m.mu.Unlock()
		return ErrSessionNotPending
	}
	if followUp := strings.TrimSpace(session.pendingFollowUp); followUp != "" {
		if err := m.store.SaveFollowUp(session.owner, followUp); err != nil {
			session.mu.Unlock()
			m.mu.Unlock()
			return err
		}
	}
	if err := m.store.SavePending(session.owner, pendingValues(session.pending)); err != nil {
		session.mu.Unlock()
		m.mu.Unlock()
		return err
	}
	segmentResults := append([]ToolResult(nil), session.segmentResults...)
	segmentEvents := append([]Event(nil), session.segmentSnapshot.events...)
	snapshotFollowUp := session.snapshotFollowUp
	segmentBatch := append([]string(nil), session.segmentBatch...)
	hasPending := len(session.pending) > 0
	session.segmentResults = nil
	session.segmentSnapshot = eventSnapshot{}
	session.segmentBatch = nil
	session.capturing = false
	if hasPending {
		session.pendingBatch = pendingToolIDs(pendingValues(session.pending))
	} else {
		session.pendingBatch = nil
	}
	output := session.output
	session.output = nil
	session.mu.Unlock()
	m.mu.Unlock()
	if len(segmentResults) > 0 {
		if filtered, ok := toolResultsForBatch(segmentResults, segmentBatch); ok {
			segmentResults = filtered
		}
		if err := m.store.SaveCompleted(session.owner, segmentResults, segmentEvents, false, snapshotFollowUp); err != nil {
			output.close(err)
			session.finishFlight(err, nil, nil, hasPending, snapshotFollowUp)
			return err
		}
		session.finishFlight(nil, segmentResults, segmentEvents, hasPending, snapshotFollowUp)
	}
	output.close(nil)
	return nil
}

// CancelState removes a persisted session even when no live H2 owner remains,
// such as after a sidecar restart. The credential scope prevents one channel
// from deleting another channel's continuation state.
func (m *SessionManager) CancelState(request ManagedRequest) error {
	sessionID := strings.TrimSpace(request.SessionID)
	if sessionID == "" {
		return nil
	}
	m.mu.Lock()
	active := m.active[sessionID]
	m.mu.Unlock()
	if active != nil {
		if err := m.Cancel(request); err != nil {
			return err
		}
	}
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return err
	}
	return m.store.DeleteSession(sessionID, fingerprint)
}

func (m *SessionManager) startClaimed(request RunRequest, owner OwnerLease, restored []PendingTool, initialResults, covering []ToolResult, originalBatch []string, carry []Event, disableCallerTools bool) (*ManagedOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.runTimeout)
	if len(originalBatch) == 0 {
		originalBatch = pendingToolIDs(restored)
	}
	session := &managedSession{
		manager:            m,
		owner:              owner,
		request:            request,
		cancel:             cancel,
		toolResults:        make(chan ToolResult, maxPendingTools),
		pending:            make(map[string]PendingTool, len(restored)),
		capturing:          request.HistoryRequestKey != "" || len(covering) > 0 || len(carry) > 0,
		pendingBatch:       append([]string(nil), originalBatch...),
		segmentBatch:       append([]string(nil), originalBatch...),
		segmentResults:     append([]ToolResult(nil), covering...),
		pendingFollowUp:    strings.TrimSpace(request.FollowUpText),
		snapshotFollowUp:   completedFollowUp(request),
		billingCarry:       usageEvidenceEvents(carry),
		disableCallerTools: disableCallerTools,
	}
	for _, pending := range restored {
		session.pending[pending.ToolCallID] = pending
	}
	output := newManagedOutput()
	session.output = output
	request.ToolResults = session.toolResults
	session.request = request

	m.mu.Lock()
	if _, exists := m.active[owner.SessionID]; exists {
		m.mu.Unlock()
		cancel()
		_ = m.store.Release(owner)
		return nil, ErrSessionActive
	}
	delete(m.starting, owner.SessionID)
	m.active[owner.SessionID] = session
	m.mu.Unlock()

	for _, result := range initialResults {
		session.toolResults <- upstreamToolResult(session.pending, result)
	}
	go func() {
		for _, event := range carry {
			session.emit(event)
		}
		session.run(ctx)
	}()
	public := output.public(owner.SessionID, owner.BillingRunID)
	public.RequestSegmentID = owner.RequestSegmentID
	public.ConversationID = owner.ConversationID
	public.OriginalBatch = append([]string(nil), originalBatch...)
	return public, nil
}

func (m *SessionManager) reserve(sessionID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.active[sessionID]; exists {
		return false
	}
	if _, exists := m.starting[sessionID]; exists {
		return false
	}
	m.starting[sessionID] = struct{}{}
	return true
}

func (m *SessionManager) releaseReservation(sessionID string) {
	m.mu.Lock()
	delete(m.starting, sessionID)
	m.mu.Unlock()
}

func (s *managedSession) run(ctx context.Context) {
	err := s.manager.runner.Run(ctx, s.request, s.handleEvent)
	s.mu.Lock()
	if s.stateErr != nil {
		err = s.stateErr
	}
	s.mu.Unlock()
	s.manager.mu.Lock()
	s.mu.Lock()
	s.closed = true
	output := s.output
	s.output = nil
	pendingCount := len(s.pending)
	terminalEvent := s.terminalEvent
	segmentResults := append([]ToolResult(nil), s.segmentResults...)
	segmentEvents := append([]Event(nil), s.segmentSnapshot.events...)
	snapshotFollowUp := s.snapshotFollowUp
	segmentBatch := append([]string(nil), s.segmentBatch...)
	s.segmentResults = nil
	s.segmentSnapshot = eventSnapshot{}
	s.segmentBatch = nil
	s.capturing = false
	s.mu.Unlock()
	s.manager.mu.Unlock()
	if err == nil && s.request.HistoryRequestKey != "" {
		err = s.manager.store.SaveHistory(s.owner, s.request.HistoryRequestKey, segmentEvents)
	}
	if err == nil && s.request.HistoryRequestKey == "" && len(segmentResults) > 0 {
		if filtered, ok := toolResultsForBatch(segmentResults, segmentBatch); ok {
			segmentResults = filtered
		}
		if saveErr := s.manager.store.SaveCompleted(s.owner, segmentResults, segmentEvents, s.request.DeleteCompletedState && pendingCount == 0, snapshotFollowUp); saveErr != nil {
			err = saveErr
		}
	}
	s.finishFlight(err, segmentResults, segmentEvents, pendingCount > 0, snapshotFollowUp)
	if err == nil && s.request.HistoryRequestKey == "" && s.request.DeleteCompletedState && pendingCount == 0 && len(segmentResults) == 0 {
		_ = s.manager.store.Delete(s.owner)
	}
	_ = s.manager.store.Release(s.owner)
	// Publish idle only after persistence and lease release have finished.
	// Follow-up/recovery callers must not race a still-finalizing owner.
	s.manager.mu.Lock()
	if s.manager.active[s.owner.SessionID] == s {
		delete(s.manager.active, s.owner.SessionID)
	}
	s.manager.mu.Unlock()
	if output != nil {
		// Completion is visible only after its replay state has been saved.
		// In particular, a persistence failure must never follow EventDone.
		if err == nil && terminalEvent != nil && output.send(*terminalEvent) == outputSendFull {
			err = fmt.Errorf("Cursor Agent v1 downstream event buffer is full")
		}
		output.close(err)
	}
	s.cancel()
}

func (s *managedSession) handleEvent(event Event) {
	switch event.Type {
	case EventCheckpoint:
		if err := s.manager.store.SaveCheckpoint(s.owner, event.Checkpoint, event.Blobs); err != nil {
			s.failState(err)
		}
		return
	case EventToolCall:
		if event.ToolCall == nil {
			s.failState(fmt.Errorf("Cursor Agent v1 tool event is empty"))
			return
		}
		upstreamToolCallID := strings.TrimSpace(event.ToolCall.ToolCallID)
		s.mu.Lock()
		if _, exists := findPublicToolCallID(s.pending, upstreamToolCallID); exists {
			s.mu.Unlock()
			return
		}
		if s.disableCallerTools {
			s.mu.Unlock()
			s.failState(ErrCallerToolsDisabled)
			return
		}
		s.mu.Unlock()
		publicToolCallID := upstreamToolCallID
		if s.request.RouteUserID > 0 && s.request.RouteChannelID > 0 {
			publicToolCallID = routedAgentV1ToolCallID(s.request.RouteUserID, s.request.RouteChannelID)
		}
		pending := PendingTool{
			MessageID: event.ToolCall.MessageID, ExecID: event.ToolCall.ExecID,
			ToolCallID: publicToolCallID, UpstreamToolCallID: upstreamToolCallID,
			Name: event.ToolCall.Name, Arguments: event.ToolCall.Arguments,
		}
		s.mu.Lock()
		if _, exists := s.pending[pending.ToolCallID]; exists {
			s.mu.Unlock()
			return
		}
		if s.output == nil {
			s.mu.Unlock()
			s.failState(fmt.Errorf("Cursor Agent v1 received a new tool call after the downstream turn was parked"))
			return
		}
		if s.disableCallerTools {
			s.mu.Unlock()
			s.failState(ErrCallerToolsDisabled)
			return
		}
		s.pending[pending.ToolCallID] = pending
		s.mu.Unlock()
		publicEvent := event
		publicCall := *event.ToolCall
		publicCall.ToolCallID = publicToolCallID
		publicEvent.ToolCall = &publicCall
		s.emit(publicEvent)
		return
	case EventToolResultAccepted:
		if event.ToolResult == nil {
			return
		}
		s.mu.Lock()
		delete(s.pending, publicToolCallIDForUpstream(s.pending, event.ToolResult.ToolCallID))
		pendingList := pendingValues(s.pending)
		s.mu.Unlock()
		if err := s.manager.store.SavePending(s.owner, pendingList); err != nil {
			s.failState(err)
			return
		}
	}
	s.emit(event)
}

func (s *managedSession) resume(results []ToolResult, originalBatch []string, segmentResults []ToolResult, segmentID string, disableCallerTools bool) (*ManagedOutput, error) {
	s.mu.Lock()
	if s.closed || s.output != nil {
		s.mu.Unlock()
		return nil, ErrSessionActive
	}
	filteredResults, matches := filterPendingMapResults(s.pending, results)
	if !matches {
		s.mu.Unlock()
		return nil, ErrSessionNotPending
	}
	owner := s.owner
	s.mu.Unlock()
	requestSegmentID := segmentID
	if isMeteredTurn(owner.BillingRunID) {
		segmentID = owner.BillingRunID
	}
	if strings.TrimSpace(segmentID) != "" && segmentID != owner.BillingRunID {
		if err := s.manager.store.SetBillingRunID(owner, segmentID); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	if s.closed || s.output != nil {
		s.mu.Unlock()
		return nil, ErrSessionActive
	}
	s.owner.RequestSegmentID = requestSegmentID
	if strings.TrimSpace(segmentID) != "" {
		s.owner.BillingRunID = segmentID
	}
	s.disableCallerTools = disableCallerTools
	output := newManagedOutput()
	s.output = output
	s.capturing = true
	// A new tool-result request no longer includes the prior native action's
	// follow-up text, even while it continues the same upstream Run.
	s.snapshotFollowUp = ""
	s.segmentSnapshot = eventSnapshot{}
	if len(originalBatch) > 0 {
		s.segmentBatch = append([]string(nil), originalBatch...)
	} else if len(s.pendingBatch) > 0 {
		s.segmentBatch = append([]string(nil), s.pendingBatch...)
	} else {
		s.segmentBatch = pendingToolIDs(pendingValues(s.pending))
	}
	if len(segmentResults) > 0 {
		s.segmentResults = append([]ToolResult(nil), segmentResults...)
	} else {
		s.segmentResults = append([]ToolResult(nil), results...)
	}
	upstreamResults := make([]ToolResult, len(filteredResults))
	for index, result := range filteredResults {
		upstreamResults[index] = upstreamToolResult(s.pending, result)
	}
	sessionID := s.owner.SessionID
	billingRunID := s.owner.BillingRunID
	originalBatchCopy := append([]string(nil), s.segmentBatch...)
	carry := usageEvidenceEvents(s.billingCarry)
	s.mu.Unlock()

	for _, event := range carry {
		s.emit(event)
	}
	for _, result := range upstreamResults {
		select {
		case s.toolResults <- result:
		default:
			s.mu.Lock()
			if s.output == output {
				s.output = nil
			}
			s.mu.Unlock()
			output.close(fmt.Errorf("Cursor Agent v1 tool result queue is full"))
			return nil, fmt.Errorf("Cursor Agent v1 tool result queue is full")
		}
	}
	public := output.public(sessionID, billingRunID)
	public.RequestSegmentID = requestSegmentID
	public.OriginalBatch = originalBatchCopy
	return public, nil
}

func routedAgentV1ToolCallID(userID, channelID int) string {
	return fmt.Sprintf("toolu_bf_agentv1_u%d_c%d_%s", userID, channelID, strings.ReplaceAll(uuid.NewString(), "-", ""))
}

func upstreamToolResult(pending map[string]PendingTool, result ToolResult) ToolResult {
	if tool, ok := pending[result.ToolCallID]; ok && strings.TrimSpace(tool.UpstreamToolCallID) != "" {
		result.ToolCallID = tool.UpstreamToolCallID
	}
	return result
}

func findPublicToolCallID(pending map[string]PendingTool, upstreamToolCallID string) (string, bool) {
	upstreamToolCallID = strings.TrimSpace(upstreamToolCallID)
	for publicID, tool := range pending {
		candidate := strings.TrimSpace(tool.UpstreamToolCallID)
		if candidate == "" {
			candidate = publicID
		}
		if candidate == upstreamToolCallID {
			return publicID, true
		}
	}
	return "", false
}

func publicToolCallIDForUpstream(pending map[string]PendingTool, upstreamToolCallID string) string {
	if publicID, found := findPublicToolCallID(pending, upstreamToolCallID); found {
		return publicID
	}
	return strings.TrimSpace(upstreamToolCallID)
}

func (s *managedSession) emit(event Event) {
	s.mu.Lock()
	if s.stateErr != nil {
		s.mu.Unlock()
		return
	}
	if s.capturing {
		if err := s.segmentSnapshot.append(event); err != nil {
			s.mu.Unlock()
			s.failState(err)
			return
		}
	}
	if event.Type == EventDone {
		terminal := cloneEvent(event)
		s.terminalEvent = &terminal
		s.mu.Unlock()
		return
	}
	output := s.output
	s.mu.Unlock()
	if output == nil {
		return
	}
	switch output.send(event) {
	case outputSendFull:
		s.failState(fmt.Errorf("Cursor Agent v1 downstream event buffer is full"))
	case outputSendClosed:
		return
	}
}

func (s *managedSession) failState(err error) {
	s.mu.Lock()
	if s.stateErr == nil {
		s.stateErr = err
	}
	s.mu.Unlock()
	s.cancel()
}

func newManagedOutput() *managedOutput {
	// One legal parallel batch can contain maxPendingTools tool calls plus the
	// explicit turn boundary and nearby checkpoint/usage events. Keep enough
	// bounded headroom for that batch before downstream bootstrap starts.
	return &managedOutput{events: make(chan Event, maxPendingTools*4), result: make(chan error, 1)}
}

func (o *managedOutput) public(sessionID, billingRunID string) *ManagedOutput {
	return &ManagedOutput{Events: o.events, Result: o.result, SessionID: sessionID, BillingRunID: billingRunID}
}

func (m *SessionManager) StartFollowUp(request ManagedRequest, covering []ToolResult) (*ManagedOutput, error) {
	if isMeteredTurn(request.BillingRunID) && !request.TurnMetering {
		return nil, meteringVersionError()
	}
	if err := requestContext(request).Err(); err != nil {
		return nil, err
	}
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return nil, err
	}
	followUp := strings.TrimSpace(request.FollowUpText)
	if followUp == "" {
		return nil, fmt.Errorf("Cursor Agent v1 follow-up text is required")
	}
	sessionID := strings.TrimSpace(request.SessionID)
	if sessionID == "" {
		return nil, ErrSessionNotPending
	}
	if !m.reserve(sessionID) {
		output, waitErr := m.waitFollowUpSnapshot(requestContext(request), request, covering)
		if waitErr == nil {
			return output, nil
		}
		if requestContext(request).Err() != nil {
			return nil, waitErr
		}
		if !m.reserve(sessionID) {
			return m.waitFollowUpSnapshot(requestContext(request), request, covering)
		}
	}
	reserved := true
	defer func() {
		if reserved {
			m.releaseReservation(sessionID)
		}
	}()
	if covering == nil {
		covering = request.ActiveResults
	}
	if output, err := m.lookupFollowUpReplay(fingerprint, covering, followUp); err != nil || output != nil {
		return output, err
	}
	conversationID := strings.TrimSpace(request.Run.ConversationID)
	if conversationID == "" {
		conversationID = uuid.NewString()
	}
	// This is a new user turn after the covered tools completed, not an active
	// tool resume. Accept its explicit catalog just like Start; omitted tools
	// still inherit the saved catalog. Claim continues to reject pending tools,
	// other credentials and concurrent owners before updating the snapshot.
	claim, err := m.store.Claim(sessionID, conversationID, fingerprint, ClaimOptions{
		RuntimeProfile:  request.Run.RuntimeProfile,
		RequireExisting: true, ReuseBillingRun: request.ReuseBillingRun,
		RequireToolCatalog: request.ToolCatalogDigest == "",
		NewBillingRunID:    billingIDForRequest(request), ToolCatalogDigest: request.ToolCatalogDigest, ToolCatalog: request.Run.Tools,
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSessionNotPending
		}
		if errors.Is(err, ErrStatePending) {
			return nil, ErrSessionPending
		}
		if errors.Is(err, ErrStateLeaseHeld) {
			m.releaseReservation(sessionID)
			reserved = false
			return m.waitFollowUpSnapshot(requestContext(request), request, covering)
		}
		return nil, err
	}
	if len(claim.Snapshot.PendingTools) > 0 {
		_ = m.store.Release(claim.Owner)
		return nil, ErrSessionPending
	}
	request.Run.RuntimeProfile = normalizeRuntimeProfile(claim.Snapshot.RuntimeProfile)
	if output, err := m.lookupFollowUpReplay(fingerprint, covering, followUp); err != nil || output != nil {
		_ = m.store.Release(claim.Owner)
		return output, err
	}
	request.Run.ConversationID = claim.Owner.ConversationID
	if err := applyCallerToolCatalog(&request, claim.Snapshot.ToolCatalog, false); err != nil {
		_ = m.store.Release(claim.Owner)
		return nil, err
	}
	request.Run.UserText = followUp
	request.Run.FollowUpText = followUp
	request.Run.Resume = false
	request.Run.InitialCheckpoint = append([]byte(nil), claim.Snapshot.Checkpoint...)
	request.Run.InitialBlobs = cloneBlobs(claim.Snapshot.Blobs)
	if err := requestContext(request).Err(); err != nil {
		_ = m.store.Release(claim.Owner)
		return nil, err
	}
	followBatch := completedBatchIDsFromResults(covering)
	claim.Owner.RequestSegmentID = request.RequestSegmentID
	if err := m.store.SaveUsageCarry(claim.Owner, request.CarryUsage); err != nil {
		_ = m.store.Release(claim.Owner)
		return nil, err
	}
	output, err := m.startClaimed(request.Run, claim.Owner, nil, nil, covering, followBatch, append([]Event(nil), request.CarryUsage...), request.DisableCallerTools)
	if err == nil {
		reserved = false
	}
	return output, err
}

func (m *SessionManager) waitFollowUpSnapshot(ctx context.Context, request ManagedRequest, covering []ToolResult) (*ManagedOutput, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return nil, err
	}
	sessionID := strings.TrimSpace(request.SessionID)
	deadline := time.Now().Add(m.runTimeout)
	if m.runTimeout <= 0 {
		deadline = time.Now().Add(time.Second)
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		_, active := m.active[sessionID]
		m.mu.Unlock()
		if !active {
			if len(covering) == 0 {
				covering = request.ActiveResults
			}
			output, lookupErr := m.lookupFollowUpReplay(fingerprint, covering, request.FollowUpText)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if output != nil {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return output, nil
			}
			return nil, ErrSessionNotPending
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, ErrSessionActive
		case <-ticker.C:
		}
	}
}

func emitCompletedReplay(replay CompletedReplay) *ManagedOutput {
	output := newSnapshotOutput(replay.Events)
	public := output.public(replay.SessionID, replay.BillingRunID)
	public.RequestSegmentID = replay.RequestSegmentID
	public.Replay = true
	public.ConversationID = replay.ConversationID
	public.OriginalBatch = append([]string(nil), replay.OriginalBatch...)
	public.HasPending = replay.HasPending
	public.FollowUpApplied = strings.TrimSpace(replay.FollowUpText) != ""
	return public
}

func resumeFlightKey(fingerprint string, batchIDs []string, followUp string) string {
	return strings.TrimSpace(fingerprint) + "\x00" + strings.Join(normalizeToolIDs(batchIDs), "\x00") + "\x00" + normalizeFollowUp(followUp)
}

func (m *SessionManager) beginResumeFlight(fingerprint string, batchIDs []string, digest, followUp string) (*resumeFlight, bool, error) {
	key := resumeFlightKey(fingerprint, batchIDs, followUp)
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.flights[key]; existing != nil {
		if subtle.ConstantTimeCompare([]byte(existing.digest), []byte(digest)) != 1 {
			return nil, false, ErrToolResultConflict
		}
		return existing, false, nil
	}
	flight := &resumeFlight{key: key, digest: digest, done: make(chan struct{})}
	m.flights[key] = flight
	return flight, true, nil
}

func waitResumeFlight(ctx context.Context, flight *resumeFlight) (*ManagedOutput, error) {
	if flight == nil {
		return nil, ErrSessionNotPending
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
	}
	if flight.err != nil {
		return nil, flight.err
	}
	if flight.replay == nil {
		return nil, ErrSessionNotPending
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return emitCompletedReplay(*flight.replay), nil
}

func (m *SessionManager) endResumeFlight(fingerprint string, batchIDs []string, flight *resumeFlight, replay *CompletedReplay, err error) {
	if m == nil || flight == nil {
		return
	}
	key := flight.key
	if key == "" {
		key = resumeFlightKey(fingerprint, batchIDs, "")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.flights[key]; current == flight {
		delete(m.flights, key)
	}
	select {
	case <-flight.done:
		return
	default:
	}
	if replay != nil {
		copied := *replay
		flight.replay = &copied
	}
	flight.err = err
	close(flight.done)
}

func (m *SessionManager) bindResumeFlight(sessionID, fingerprint string, batchIDs []string, flight *resumeFlight) {
	m.mu.Lock()
	session := m.active[sessionID]
	m.mu.Unlock()
	if session == nil || flight == nil {
		return
	}
	session.mu.Lock()
	session.flight = flight
	session.flightFingerprint = fingerprint
	session.flightBatch = append([]string(nil), batchIDs...)
	session.mu.Unlock()
}

func (s *managedSession) finishFlight(err error, results []ToolResult, events []Event, hasPending bool, followUp string) {
	if s == nil || s.manager == nil {
		return
	}
	s.mu.Lock()
	flight := s.flight
	fingerprint := s.flightFingerprint
	batchIDs := append([]string(nil), s.flightBatch...)
	s.flight = nil
	s.flightFingerprint = ""
	s.flightBatch = nil
	s.mu.Unlock()
	if flight == nil {
		return
	}
	var replay *CompletedReplay
	if err == nil && len(results) > 0 {
		replay = &CompletedReplay{
			SessionID: s.owner.SessionID, ConversationID: s.owner.ConversationID,
			BillingRunID: s.owner.BillingRunID, RequestSegmentID: s.owner.RequestSegmentID, OriginalBatch: append([]string(nil), batchIDs...),
			Events: append([]Event(nil), events...), HasPending: hasPending,
			FollowUpText: followUp,
		}
	}
	s.manager.endResumeFlight(fingerprint, batchIDs, flight, replay, err)
}

func (o *managedOutput) close(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	close(o.events)
	o.result <- err
	close(o.result)
}

type outputSendResult int

const (
	outputSendOK outputSendResult = iota
	outputSendClosed
	outputSendFull
)

func (o *managedOutput) send(event Event) outputSendResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return outputSendClosed
	}
	select {
	case o.events <- event:
		return outputSendOK
	default:
		return outputSendFull
	}
}

func (m *SessionManager) lookupFollowUpReplay(fingerprint string, covering []ToolResult, followUp string) (*ManagedOutput, error) {
	if len(covering) == 0 {
		return nil, nil
	}
	replay, err := m.store.LookupCompleted(fingerprint, covering, followUp)
	if errors.Is(err, ErrSessionNotPending) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if normalizeFollowUp(replay.FollowUpText) != normalizeFollowUp(followUp) || replay.FollowUpText == "" {
		return nil, nil
	}
	return emitCompletedReplay(replay), nil
}

func (m *SessionManager) lookupRequestReplay(fingerprint string, results []ToolResult, followUp string) (CompletedReplay, error) {
	replay, err := m.store.LookupCompleted(fingerprint, results, followUp)
	if err == nil || !errors.Is(err, ErrSessionNotPending) {
		return replay, err
	}
	if normalizeFollowUp(followUp) == "" {
		return CompletedReplay{}, err
	}
	original, origErr := m.store.LookupCompleted(fingerprint, results, "")
	if origErr != nil {
		return CompletedReplay{}, err
	}
	original.FollowUpText = ""
	return original, nil
}

func completedFollowUp(request RunRequest) string {
	if request.Resume {
		return ""
	}
	return strings.TrimSpace(request.FollowUpText)
}

func toolResultsFromCanonical(results []CompletedToolResult) []ToolResult {
	out := make([]ToolResult, 0, len(results))
	for _, result := range results {
		out = append(out, ToolResult{ToolCallID: result.ToolCallID, Content: result.Content, IsError: result.IsError, Media: cloneMedia(result.Media)})
	}
	return out
}

func completedBatchIDsFromResults(results []ToolResult) []string {
	canonical, err := canonicalToolResults(results)
	if err != nil {
		return nil
	}
	return completedBatchIDs(canonical)
}

func pendingContains(pending []PendingTool, toolCallID string) bool {
	for _, current := range pending {
		if current.ToolCallID == toolCallID {
			return true
		}
	}
	return false
}

func filterPendingResults(pending []PendingTool, results []ToolResult) ([]ToolResult, bool) {
	pendingMap := make(map[string]PendingTool, len(pending))
	for _, current := range pending {
		pendingMap[current.ToolCallID] = current
	}
	return filterPendingMapResults(pendingMap, results)
}

func filterPendingMapResults(pending map[string]PendingTool, results []ToolResult) ([]ToolResult, bool) {
	seen := make(map[string]ToolResult, len(results))
	for _, result := range results {
		if _, exists := seen[result.ToolCallID]; exists {
			return nil, false
		}
		seen[result.ToolCallID] = result
	}
	filtered := make([]ToolResult, 0, len(pending))
	for toolCallID := range pending {
		result, exists := seen[toolCallID]
		if !exists {
			return nil, false
		}
		filtered = append(filtered, result)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ToolCallID < filtered[j].ToolCallID })
	return filtered, true
}

func pendingValues(pending map[string]PendingTool) []PendingTool {
	values := make([]PendingTool, 0, len(pending))
	for _, current := range pending {
		values = append(values, current)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ToolCallID < values[j].ToolCallID })
	return values
}
