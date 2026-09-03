package cursoragentv1

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/helps"
	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	_ "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator/builtin"
	"golang.org/x/sync/singleflight"
)

const (
	EnvStateDir        = "CURSOR_AGENT_V1_STATE_DIR"
	EnvStateTTL        = "CURSOR_AGENT_V1_STATE_TTL"
	HeaderUsageReceipt = "X-Cursor-Agent-V1-Usage-Receipt"
	HeaderUsageReplay  = "X-Cursor-Agent-V1-Usage-Replay"
	ProviderID         = "cursor-agent-v1"
	providerID         = ProviderID
	defaultRefreshURL  = "https://api2.cursor.sh/auth/exchange_user_api_key"
	runtimeAgentV1     = "agent_v1"
	runtimeSand        = "sand"
)

type Executor struct {
	manager        *SessionManager
	client         *http.Client
	refreshURL     string
	refreshGroup   singleflight.Group
	bootstrapCache executorBootstrapCache
}

type refreshedCredential struct {
	accessToken  string
	refreshToken string
	expired      string
	lastRefresh  string
}

func NewExecutor(stateDir string) (*Executor, error) {
	if !filepath.IsAbs(strings.TrimSpace(stateDir)) {
		return nil, fmt.Errorf("Cursor Agent v1 state directory must be absolute")
	}
	ttl := defaultStateTTL
	if rawTTL := strings.TrimSpace(os.Getenv(EnvStateTTL)); rawTTL != "" {
		parsedTTL, parseErr := time.ParseDuration(rawTTL)
		if parseErr != nil || parsedTTL <= 0 {
			return nil, fmt.Errorf("Cursor Agent v1 state TTL is invalid")
		}
		ttl = parsedTTL
	}
	store, err := newFileStateStore(stateDir, ttl, time.Now)
	if err != nil {
		return nil, err
	}
	manager, err := newSessionManager(newProfileRunner(), store, defaultManagedRunTimeout)
	if err != nil {
		return nil, err
	}
	return &Executor{manager: manager, refreshURL: defaultRefreshURL}, nil
}

func newExecutorForTest(manager *SessionManager, client *http.Client) *Executor {
	return &Executor{manager: manager, client: client, refreshURL: defaultRefreshURL}
}

func (*Executor) Identifier() string { return providerID }

func (e *Executor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	turn, managed, err := e.prepare(ctx, auth, req, opts)
	if err != nil {
		return nil, err
	}
	managed, err = e.maybeChainFollowUp(ctx, &turn, managed)
	if err != nil {
		return nil, err
	}
	projector := newClaudeProjection(ctx, turn)
	chunks := make(chan cliproxyexecutor.StreamChunk, 64)
	first, terminal, err := e.bootstrap(ctx, turn.managed, managed, projector)
	if err != nil {
		return nil, err
	}
	go e.emitStream(ctx, turn.managed, managed, projector, chunks, first, terminal)
	return &cliproxyexecutor.StreamResult{Headers: executionHeaders("text/event-stream", turn), Chunks: chunks}, nil
}

func (e *Executor) emitStream(ctx context.Context, request ManagedRequest, output *ManagedOutput, projector *claudeProjection, chunks chan<- cliproxyexecutor.StreamChunk, first [][]byte, terminal bool) {
	requestReplay := output != nil && output.Replay
	defer close(chunks)
	for _, payload := range first {
		select {
		case chunks <- cliproxyexecutor.StreamChunk{Payload: payload}:
		case <-ctx.Done():
			// A terminal bootstrap is either a completed turn or an explicitly
			// parked tool batch. Downstream cancellation may stop delivery, but
			// must not delete its durable checkpoint/pending continuation.
			if !terminal && !requestReplay {
				_ = e.manager.Cancel(request)
			}
			return
		}
	}
	if terminal {
		return
	}
	e.continueStream(ctx, request, output, projector, chunks)
}

func (e *Executor) bootstrap(ctx context.Context, request ManagedRequest, output *ManagedOutput, projector *claudeProjection) ([][]byte, bool, error) {
	replay := output != nil && output.Replay
	for {
		select {
		case <-ctx.Done():
			if !replay {
				_ = e.manager.Cancel(request)
			}
			return nil, false, ctx.Err()
		case event, ok := <-output.Events:
			if !ok {
				err := <-output.Result
				if err == nil {
					err = &requestError{status: http.StatusBadGateway, text: "Cursor Agent v1 ended before the first semantic payload"}
				}
				return nil, false, err
			}
			chunks, terminal, park, err := projector.handle(event)
			if err != nil {
				if !replay {
					_ = e.manager.Cancel(request)
				}
				return nil, false, err
			}
			if park && !replay {
				if err := e.manager.Park(request); err != nil && !errors.Is(err, ErrSessionNotPending) {
					_ = e.manager.Cancel(request)
					return nil, false, err
				}
			}
			if len(chunks) > 0 {
				return chunks, terminal, nil
			}
		}
	}
}

func (e *Executor) continueStream(ctx context.Context, request ManagedRequest, output *ManagedOutput, projector *claudeProjection, chunks chan<- cliproxyexecutor.StreamChunk) {
	replay := output != nil && output.Replay
	for {
		var event Event
		var ok bool
		select {
		case <-ctx.Done():
			if !replay {
				_ = e.manager.Cancel(request)
			}
			return
		case event, ok = <-output.Events:
			if !ok {
				if err := <-output.Result; err != nil && ctx.Err() == nil {
					sendStreamChunk(ctx, chunks, cliproxyexecutor.StreamChunk{Err: err})
				}
				return
			}
		}
		payloads, terminal, park, err := projector.handle(event)
		if err != nil {
			sendStreamChunk(ctx, chunks, cliproxyexecutor.StreamChunk{Err: err})
			if !replay {
				_ = e.manager.Cancel(request)
			}
			return
		}
		if park && !replay {
			if err := e.manager.Park(request); err != nil && !errors.Is(err, ErrSessionNotPending) {
				_ = e.manager.Cancel(request)
				sendStreamChunk(ctx, chunks, cliproxyexecutor.StreamChunk{Err: err})
				return
			}
		}
		for _, payload := range payloads {
			select {
			case chunks <- cliproxyexecutor.StreamChunk{Payload: payload}:
			case <-ctx.Done():
				// Park/completion transferred ownership before payload delivery.
				if !terminal && !replay {
					_ = e.manager.Cancel(request)
				}
				return
			}
		}
		if park {
			return
		}
		if terminal {
			return
		}
	}
}

func sendStreamChunk(ctx context.Context, chunks chan<- cliproxyexecutor.StreamChunk, chunk cliproxyexecutor.StreamChunk) bool {
	select {
	case chunks <- chunk:
		return true
	case <-ctx.Done():
		return false
	}
}

func (e *Executor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	turn, output, err := e.prepare(ctx, auth, req, opts)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	output, err = e.maybeChainFollowUp(ctx, &turn, output)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	projector := newClaudeProjection(ctx, turn)
	for {
		select {
		case <-ctx.Done():
			if output == nil || !output.Replay {
				_ = e.manager.Cancel(turn.managed)
			}
			return cliproxyexecutor.Response{}, ctx.Err()
		case event, ok := <-output.Events:
			if !ok {
				if resultErr := <-output.Result; resultErr != nil {
					if projector.semantic && errorStatusCode(resultErr) == http.StatusUnauthorized {
						resultErr = &requestError{status: http.StatusBadGateway, text: "Cursor Agent v1 authentication failed after semantic output"}
					}
					return cliproxyexecutor.Response{}, resultErr
				}
				return cliproxyexecutor.Response{}, &requestError{status: http.StatusBadGateway, text: "Cursor Agent v1 ended before completion"}
			}
			_, terminal, park, projectErr := projector.handle(event)
			if projectErr != nil {
				if output == nil || !output.Replay {
					_ = e.manager.Cancel(turn.managed)
				}
				return cliproxyexecutor.Response{}, projectErr
			}
			if park && (output == nil || !output.Replay) {
				if parkErr := e.manager.Park(turn.managed); parkErr != nil && !errors.Is(parkErr, ErrSessionNotPending) {
					_ = e.manager.Cancel(turn.managed)
					return cliproxyexecutor.Response{}, parkErr
				}
			}
			if terminal {
				payload, payloadErr := projector.nonStream()
				return cliproxyexecutor.Response{Payload: payload, Headers: executionHeaders("application/json", turn)}, payloadErr
			}
		}
	}
}

func executionHeaders(contentType string, turn translatedTurn) http.Header {
	header := http.Header{"Content-Type": {contentType}}
	if receipt := usageReceiptID(turn); receipt != "" {
		header.Set(HeaderUsageReceipt, receipt)
	}
	if turn.replay {
		header.Set(HeaderUsageReplay, "1")
	}
	return header
}

func (e *Executor) maybeChainFollowUp(ctx context.Context, turn *translatedTurn, output *ManagedOutput) (*ManagedOutput, error) {
	if turn == nil || output == nil {
		return output, nil
	}
	followUp := strings.TrimSpace(turn.managed.FollowUpText)
	if followUp == "" {
		return output, nil
	}
	if output.Replay && (output.FollowUpApplied || output.HasPending) {
		return output, nil
	}
	if output.Replay && !output.HasPending && !output.FollowUpApplied {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		events, err := drainManagedOutput(output)
		if err != nil {
			return nil, err
		}
		return e.startChainedFollowUp(ctx, turn, output, events)
	}
	parked := false
	buffered := make([]Event, 0, 16)
	for {
		select {
		case <-ctx.Done():
			// Park transferred ownership to durable continuation state. A
			// disconnected HTTP consumer must not delete that next tool batch.
			if !parked && !output.Replay {
				_ = e.manager.Cancel(turn.managed)
			}
			return nil, ctx.Err()
		case event, ok := <-output.Events:
			if !ok {
				if resultErr := <-output.Result; resultErr != nil {
					return nil, resultErr
				}
				if parked {
					return emitBufferedOutput(output, buffered), nil
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return e.startChainedFollowUp(ctx, turn, output, buffered)
			}
			if event.Type == EventTurnEnded {
				parked = true
				if parkErr := e.manager.Park(turn.managed); parkErr != nil && !errors.Is(parkErr, ErrSessionNotPending) {
					_ = e.manager.Cancel(turn.managed)
					return nil, parkErr
				}
			}
			buffered = append(buffered, event)
		}
	}
}

func (e *Executor) startChainedFollowUp(ctx context.Context, turn *translatedTurn, prior *ManagedOutput, priorEvents []Event) (*ManagedOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	request := turn.managed
	request.Context = ctx
	request.SessionID = prior.SessionID
	request.RequireExisting = true
	if request.Run.ConversationID == "" {
		request.Run.ConversationID = prior.ConversationID
	}
	covering := request.ActiveResults
	if len(prior.OriginalBatch) > 0 {
		if filtered, ok := toolResultsForBatch(request.ActiveResults, prior.OriginalBatch); ok {
			covering = filtered
		}
	}
	requestID, err := followUpRequestID(prior, request)
	if err != nil {
		return nil, err
	}
	sameMixedRequest := prior.RequestSegmentID == requestID || (prior.RequestSegmentID == "" && prior.BillingRunID != "" && prior.BillingRunID == requestID)
	request.RequestSegmentID = requestID
	if prior.Replay && !sameMixedRequest {
		request.ReuseBillingRun = false
		request.BillingRunID = requestID
		request.CarryUsage = nil
	} else {
		request.ReuseBillingRun = true
		request.BillingRunID = prior.BillingRunID
		request.CarryUsage = usageEvidenceEvents(priorEvents)
		if isMeteredTurn(request.BillingRunID) {
			request.CarryUsage = meteredUsageCarry(priorEvents)
		}
	}
	follow, err := e.manager.StartFollowUp(request, covering)
	if err != nil {
		return nil, mapManagerError(err)
	}
	turn.managed.SessionID = follow.SessionID
	turn.managed.BillingRunID = follow.BillingRunID
	turn.managed.RequestSegmentID = follow.RequestSegmentID
	turn.replay = follow.Replay
	return follow, nil
}

func emitBufferedOutput(prior *ManagedOutput, events []Event) *ManagedOutput {
	output := newSnapshotOutput(events)
	public := output.public(prior.SessionID, prior.BillingRunID)
	public.RequestSegmentID = prior.RequestSegmentID
	public.ConversationID = prior.ConversationID
	public.OriginalBatch = append([]string(nil), prior.OriginalBatch...)
	public.HasPending = true
	return public
}

func drainManagedOutput(output *ManagedOutput) ([]Event, error) {
	if output == nil {
		return nil, nil
	}
	events := make([]Event, 0, 16)
	for event := range output.Events {
		events = append(events, event)
	}
	return events, <-output.Result
}

func followUpRequestID(prior *ManagedOutput, request ManagedRequest) (string, error) {
	fingerprint, err := CredentialFingerprint(request.CredentialScope, request.AccountKey)
	if err != nil {
		return "", err
	}
	canonical, err := canonicalToolResults(request.ActiveResults)
	if err != nil {
		return "", err
	}
	batch := append([]string(nil), prior.OriginalBatch...)
	if len(batch) == 0 {
		batch = completedBatchIDs(canonical)
	}
	accepted, ok := completedResultsForBatch(canonical, batch)
	if !ok {
		accepted = canonical
		batch = completedBatchIDs(canonical)
	}
	return generationSegmentID(fingerprint, batch, accepted, request.FollowUpText), nil
}

func usageReceiptID(turn translatedTurn) string {
	billingRunID := strings.TrimSpace(turn.managed.BillingRunID)
	if billingRunID == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(turn.managed.CredentialScope + "\x00" + turn.managed.AccountKey + "\x00" + billingRunID))
	if isMeteredTurn(billingRunID) {
		return fmt.Sprintf("cursor-agent-v1:turn:%x", digest[:])
	}
	return fmt.Sprintf("cursor-agent-v1:%x", digest[:])
}

func (e *Executor) prepare(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (translatedTurn, *ManagedOutput, error) {
	if e == nil || e.manager == nil {
		return translatedTurn{}, nil, fmt.Errorf("Cursor Agent v1 executor is not configured")
	}
	credentialScope, accountKey, baseURL, proxyURL, accessToken, clientVersion, err := executorCredential(auth)
	if err != nil {
		return translatedTurn{}, nil, err
	}
	runtimeProfile, err := cursorRuntimeProfile(auth)
	if err != nil {
		return translatedTurn{}, nil, err
	}
	if runtimeProfile == runtimeSand {
		accessToken, err = e.validateCursorCredentialIdentity(ctx, auth, accessToken)
		baseURL = defaultInferenceBaseURL
	} else {
		baseURL, accessToken, err = e.bootstrapCursorRun(ctx, auth, baseURL, accessToken, clientVersion)
	}
	if err != nil {
		return translatedTurn{}, nil, err
	}
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata["beefapi_cursor_runtime_profile"] = runtimeProfile
	opts.Metadata = metadata
	turn, err := translateExecutionRequest(req, opts, credentialScope, accountKey, baseURL, accessToken, clientVersion)
	if err != nil {
		return translatedTurn{}, nil, err
	}
	turn.managed.Context = ctx
	turn.managed.Run.ProxyURL = proxyURL
	turn.managed.Run.RuntimeProfile = runtimeProfile
	if runtimeProfile == runtimeSand {
		if err := prepareInferenceTurn(&turn, req); err != nil {
			return translatedTurn{}, nil, err
		}
	}
	results := turn.managed.ActiveResults
	userText := turn.parsedUserText
	var output *ManagedOutput
	if turn.managed.Run.HistoryRequestKey != "" {
		output, err = e.manager.StartHistory(turn.managed)
	} else if len(results) > 0 {
		if strings.TrimSpace(turn.managed.FollowUpText) == "" && strings.TrimSpace(userText) != "" && !isClaudeCodeToolRecoveryText(userText) {
			turn.managed.FollowUpText = strings.TrimSpace(userText)
		}
		output, err = e.manager.ResumeTools(turn.managed, results)
		if err != nil && errors.Is(err, ErrSessionNotPending) {
			if strings.TrimSpace(userText) != "" {
				turn.managed.FollowUpText = ""
				turn.managed.Run.UserText = userText
			}
			if strings.TrimSpace(userText) != "" || restartFromResponsesTranscript(&turn, req) {
				if sessionID := strings.TrimSpace(metadataString(req.Metadata, cliproxyexecutor.ExecutionSessionMetadataKey)); sessionID != "" {
					turn.managed.SessionID = sessionID
				}
				output, err = e.manager.Start(turn.managed)
			}
		}
	} else {
		output, err = e.manager.Start(turn.managed)
	}
	if err != nil {
		return translatedTurn{}, nil, mapManagerError(err)
	}
	turn.managed.SessionID = output.SessionID
	turn.managed.BillingRunID = output.BillingRunID
	turn.managed.RequestSegmentID = output.RequestSegmentID
	turn.replay = output.Replay
	return turn, output, nil
}

// restartFromResponsesTranscript turns a Responses tool-result request whose
// pending session no longer exists (sidecar cutover, the conversation was
// served by another provider such as Kiro, or the state expired) into a fresh
// full-history run, the way Messages/Chat already work. It only applies when the
// client sent the whole transcript (no previous_response_id): a stateful
// continuation has nothing to rebuild from and keeps its explicit 409.
func restartFromResponsesTranscript(turn *translatedTurn, req cliproxyexecutor.Request) bool {
	if turn == nil || req.Format != sdktranslator.FormatOpenAIResponse || responsesRequiresExisting(req) {
		return false
	}
	var root map[string]any
	if jsonx.Unmarshal(turn.claudeRequest, &root) != nil {
		return false
	}
	messages, _ := root["messages"].([]any)
	if len(messages) == 0 {
		return false
	}
	history, err := structuredHistory(messages, true)
	if err != nil || len(history) == 0 {
		return false
	}
	turn.managed.Run.HistoryMessages = history
	turn.managed.Run.UserText = historyContinuationText
	turn.managed.Run.InitialCheckpoint = nil
	turn.managed.Run.InitialBlobs = nil
	turn.managed.Run.Resume = false
	turn.managed.FollowUpText = ""
	turn.managed.ActiveResults = nil
	return true
}

func cursorRuntimeProfile(auth *cliproxyauth.Auth) (string, error) {
	profile := strings.ToLower(metadataValue(auth, "runtime_profile"))
	if profile == "" || profile == runtimeAgentV1 {
		return runtimeAgentV1, nil
	}
	if profile == runtimeSand {
		return runtimeSand, nil
	}
	return "", &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 runtime profile is invalid"}
}

func (*Executor) CountTokens(_ context.Context, _ *cliproxyauth.Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	count, err := helps.CountClaudeInputTokens(req.Payload)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte(fmt.Sprintf(`{"input_tokens":%d}`, count))}, nil
}

func (e *Executor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil || strings.TrimSpace(metadataValue(auth, "api_key")) == "" {
		return nil, &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 API key is required to refresh access token"}
	}
	accountID := metadataValue(auth, "account_id")
	// A credential that only carries an API key has never been exchanged. Adopt
	// the identity of the first minted token instead of rejecting the refresh so
	// a plain {"type":"cursor-agent-v1","api_key":"key_..."} file is enough.
	activating := accountID == "" && metadataValue(auth, "access_token") == ""
	if accountID == "" && !activating {
		return nil, &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 account id is required for refresh"}
	}
	apiKey := metadataValue(auth, "api_key")
	generation := sha256.Sum256([]byte(accountID + "\x00" + apiKey))
	flight := e.refreshGroup.DoChan(fmt.Sprintf("%x", generation[:]), func() (any, error) {
		refreshCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return e.refreshOnce(refreshCtx, auth.Clone())
	})
	var result singleflight.Result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result = <-flight:
	}
	if result.Err != nil {
		return nil, result.Err
	}
	refreshed := result.Val.(*refreshedCredential)
	next := auth.Clone()
	if next.Metadata == nil {
		next.Metadata = make(map[string]any)
	}
	next.Metadata["access_token"] = refreshed.accessToken
	if activating {
		subject := cursorTokenSubject(refreshed.accessToken)
		if subject == "" {
			return nil, &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 access token has no identity subject"}
		}
		next.Metadata["account_id"] = subject
	}
	if refreshed.refreshToken != "" {
		next.Metadata["refresh_token"] = refreshed.refreshToken
	}
	if refreshed.expired != "" {
		next.Metadata["expired"] = refreshed.expired
	}
	next.Metadata["last_refresh"] = refreshed.lastRefresh
	return next, nil
}

func (e *Executor) refreshOnce(ctx context.Context, auth *cliproxyauth.Auth) (*refreshedCredential, error) {
	refreshURL := strings.TrimSpace(e.refreshURL)
	if refreshURL == "" {
		refreshURL = defaultRefreshURL
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshURL, strings.NewReader("{}"))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+metadataValue(auth, "api_key"))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	client := e.client
	if client == nil {
		client = helps.NewProxyAwareHTTPClient(ctx, nil, auth, 20*time.Second)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &httpStatusError{code: response.StatusCode, text: fmt.Sprintf("Cursor Agent v1 refresh HTTP %d", response.StatusCode)}
	}
	var token struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
	}
	if err := jsonx.Unmarshal(body, &token); err != nil || strings.TrimSpace(token.AccessToken) == "" {
		return nil, fmt.Errorf("Cursor Agent v1 refresh response is invalid")
	}
	currentSubject := cursorTokenSubject(metadataValue(auth, "access_token"))
	refreshedSubject := cursorTokenSubject(token.AccessToken)
	if currentSubject != "" && (refreshedSubject == "" || refreshedSubject != currentSubject) {
		return nil, &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 API key and access token belong to different identities"}
	}
	refreshed := &refreshedCredential{
		accessToken: strings.TrimSpace(token.AccessToken), refreshToken: strings.TrimSpace(token.RefreshToken),
		lastRefresh: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if expiry := jwtExpiry(token.AccessToken); !expiry.IsZero() {
		refreshed.expired = expiry.UTC().Format(time.RFC3339)
	}
	return refreshed, nil
}

func (*Executor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 raw HTTP requests are unsupported"}
}

func executorCredential(auth *cliproxyauth.Auth) (string, string, string, string, string, string, error) {
	if auth == nil {
		return "", "", "", "", "", "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 credential is missing"}
	}
	accessToken := metadataValue(auth, "access_token")
	if accessToken == "" {
		return "", "", "", "", "", "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 access token is missing"}
	}
	accountKey := metadataValue(auth, "account_id")
	if accountKey == "" {
		return "", "", "", "", "", "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 stable account identity is missing"}
	}
	baseURL := strings.TrimRight(metadataValue(auth, "base_url"), "/")
	if baseURL == "" {
		return "", "", "", "", "", "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 base URL is missing"}
	}
	clientVersion := metadataValue(auth, "client_version")
	if clientVersion == "" {
		return "", "", "", "", "", "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 client version is missing"}
	}
	credentialScope := ""
	if auth.Attributes != nil {
		credentialScope = strings.TrimSpace(auth.Attributes["credential_scope"])
	}
	if credentialScope == "" {
		return "", "", "", "", "", "", &requestError{status: http.StatusBadRequest, text: "Cursor Agent v1 credential scope is missing"}
	}
	return credentialScope, accountKey, baseURL, strings.TrimSpace(auth.ProxyURL), accessToken, clientVersion, nil
}

func metadataValue(auth *cliproxyauth.Auth, key string) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	value, ok := auth.Metadata[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func mapManagerError(err error) error {
	switch {
	case err == nil:
		return nil
	case err == ErrSessionPending, err == ErrSessionNotPending, err == ErrPendingAmbiguous, err == ErrCredentialMismatch, err == ErrToolResultConflict:
		return &requestError{status: http.StatusConflict, text: err.Error()}
	case err == ErrMixedToolFollowUp:
		return &requestError{status: http.StatusConflict, text: err.Error()}
	case err == ErrSessionActive, err == ErrStateLeaseHeld:
		return &requestError{status: http.StatusConflict, text: err.Error()}
	default:
		return err
	}
}

func errorStatusCode(err error) int {
	if status, ok := err.(interface{ StatusCode() int }); ok {
		return status.StatusCode()
	}
	return 0
}

func jwtExpiry(token string) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if jsonx.Unmarshal(raw, &claims) != nil || claims.Exp <= 0 {
		return time.Time{}
	}
	return time.Unix(claims.Exp, 0)
}

var _ cliproxyauth.ProviderExecutor = (*Executor)(nil)
