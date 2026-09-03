package plugin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/enderzcx/cursor-agent2api/cursoragentv1"
	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"gopkg.in/yaml.v3"
)

const Version = "0.2.0"

type Config struct {
	StateDir string `yaml:"state_dir" json:"state_dir"`
}

// hostRefreshInterval is advertised to the CPA host through auth metadata so
// plugin-owned credentials join the host refresh loop; plugin providers have no
// built-in refresh lead otherwise.
const hostRefreshInterval = 30 * time.Minute

type Plugin struct {
	executor *cursoragentv1.Executor
	stateDir string

	activatedMu sync.Mutex
	// activated caches credentials exchanged on first use until the host
	// persists the refreshed auth file and reloads it with an access token.
	activated map[string]*cliproxyauth.Auth
}

func Build(configYAML []byte) (*Plugin, error) {
	cfg := Config{}
	if len(bytes.TrimSpace(configYAML)) > 0 {
		if err := yaml.Unmarshal(configYAML, &cfg); err != nil {
			return nil, fmt.Errorf("cursor-agent-v1 config: %w", err)
		}
	}
	stateDir := strings.TrimSpace(cfg.StateDir)
	if stateDir == "" {
		stateDir = strings.TrimSpace(os.Getenv(cursoragentv1.EnvStateDir))
	}
	if stateDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("cursor-agent-v1 state directory is required")
		}
		stateDir = filepath.Join(home, ".cliproxy-cursor-agent-v1")
	}
	abs, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	exec, err := cursoragentv1.NewExecutor(abs)
	if err != nil {
		return nil, err
	}
	return &Plugin{executor: exec, stateDir: abs, activated: make(map[string]*cliproxyauth.Auth)}, nil
}

func (p *Plugin) Identifier() string { return cursoragentv1.ProviderID }

func (p *Plugin) Metadata() pluginapi.Metadata {
	return pluginapi.Metadata{
		Name:             "Cursor Agent v1",
		Version:          Version,
		Author:           "enderzcx",
		GitHubRepository: "https://github.com/enderzcx/cursor-agent2api",
		ConfigFields: []pluginapi.ConfigField{{
			Name:        "state_dir",
			Type:        pluginapi.ConfigFieldTypeString,
			Description: "Absolute directory for Agent v1 conversation state. Defaults to CURSOR_AGENT_V1_STATE_DIR or ~/.cliproxy-cursor-agent-v1.",
		}},
	}
}

func (p *Plugin) InputFormats() []string {
	return []string{"claude", "openai", "openai-response"}
}

func (p *Plugin) OutputFormats() []string {
	return []string{"claude", "openai", "openai-response"}
}

func (p *Plugin) RegisterModels(context.Context, pluginapi.ModelRegistrationRequest) (pluginapi.ModelRegistrationResponse, error) {
	return pluginapi.ModelRegistrationResponse{Provider: cursoragentv1.ProviderID, Models: p.models("")}, nil
}

func (p *Plugin) StaticModels(context.Context, pluginapi.StaticModelRequest) (pluginapi.ModelResponse, error) {
	return pluginapi.ModelResponse{Provider: cursoragentv1.ProviderID, Models: p.models("")}, nil
}

func (p *Plugin) ModelsForAuth(_ context.Context, req pluginapi.AuthModelRequest) (pluginapi.ModelResponse, error) {
	profile := ""
	if req.Metadata != nil {
		profile = strings.ToLower(strings.TrimSpace(fmt.Sprint(req.Metadata["runtime_profile"])))
	}
	if profile == "" || profile == "<nil>" {
		if data, ok := parseStorage(req.StorageJSON); ok {
			profile = strings.ToLower(strings.TrimSpace(data.RuntimeProfile))
		}
	}
	return pluginapi.ModelResponse{Provider: cursoragentv1.ProviderID, Models: p.models(profile)}, nil
}

func (p *Plugin) models(runtimeProfile string) []pluginapi.ModelInfo {
	ids := cursoragentv1.ModelsForRuntimeProfile(runtimeProfile)
	out := make([]pluginapi.ModelInfo, 0, len(ids))
	for _, id := range ids {
		out = append(out, pluginapi.ModelInfo{
			ID:                         id,
			Object:                     "model",
			OwnedBy:                    cursoragentv1.ProviderID,
			DisplayName:                id,
			Name:                       id,
			SupportedGenerationMethods: []string{"chat"},
			ContextLength:              200000,
			MaxCompletionTokens:        64000,
		})
	}
	return out
}

func (p *Plugin) ParseAuth(_ context.Context, req pluginapi.AuthParseRequest) (pluginapi.AuthParseResponse, error) {
	data, ok := parseStorage(req.RawJSON)
	if !ok {
		return pluginapi.AuthParseResponse{}, nil
	}
	auth := storageToAuthData(data, req.FileName, req.Path)
	return pluginapi.AuthParseResponse{Handled: true, Auth: auth}, nil
}

func (p *Plugin) StartLogin(context.Context, pluginapi.AuthLoginStartRequest) (pluginapi.AuthLoginStartResponse, error) {
	return pluginapi.AuthLoginStartResponse{Provider: cursoragentv1.ProviderID}, fmt.Errorf("cursor-agent-v1 login is file import only: save a JSON auth file with type %q and api_key", cursoragentv1.ProviderID)
}

func (p *Plugin) PollLogin(context.Context, pluginapi.AuthLoginPollRequest) (pluginapi.AuthLoginPollResponse, error) {
	return pluginapi.AuthLoginPollResponse{
		Status:  pluginapi.AuthLoginStatusError,
		Message: "cursor-agent-v1 has no interactive login",
	}, nil
}

func (p *Plugin) RefreshAuth(ctx context.Context, req pluginapi.AuthRefreshRequest) (pluginapi.AuthRefreshResponse, error) {
	auth := authFromPlugin(req.AuthID, req.AuthProvider, req.StorageJSON, req.Metadata, req.Attributes)
	next, err := p.executor.Refresh(ctx, auth)
	if err != nil {
		return pluginapi.AuthRefreshResponse{}, err
	}
	p.forgetActivated(auth.ID)
	data := authToAuthData(next)
	if len(data.StorageJSON) == 0 {
		data.StorageJSON = mergeStorage(req.StorageJSON, next)
	}
	return pluginapi.AuthRefreshResponse{Auth: data}, nil
}

// ensureActivated exchanges an API-key-only credential on first use so callers
// do not have to wait for the host refresh loop before the first request works.
func (p *Plugin) ensureActivated(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil || metaString(auth, "access_token") != "" || metaString(auth, "api_key") == "" {
		return auth, nil
	}
	p.activatedMu.Lock()
	cached := p.activated[auth.ID]
	p.activatedMu.Unlock()
	if cached != nil && !activationExpired(cached) {
		return cached, nil
	}
	next, err := p.executor.Refresh(ctx, auth)
	if err != nil {
		return nil, err
	}
	if auth.ID != "" {
		p.activatedMu.Lock()
		p.activated[auth.ID] = next
		p.activatedMu.Unlock()
	}
	return next, nil
}

func (p *Plugin) forgetActivated(id string) {
	if id == "" {
		return
	}
	p.activatedMu.Lock()
	delete(p.activated, id)
	p.activatedMu.Unlock()
}

func metaString(auth *cliproxyauth.Auth, key string) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	value, ok := auth.Metadata[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func activationExpired(auth *cliproxyauth.Auth) bool {
	raw := metaString(auth, "expired")
	if raw == "" {
		return false
	}
	expiry, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}
	return time.Until(expiry) < 2*time.Minute
}

func (p *Plugin) Execute(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	auth, nativeReq, opts := executionArgs(req)
	auth, err := p.ensureActivated(ctx, auth)
	if err != nil {
		return pluginapi.ExecutorResponse{}, err
	}
	resp, err := p.executor.Execute(ctx, auth, nativeReq, opts)
	if err != nil {
		return pluginapi.ExecutorResponse{}, err
	}
	return pluginapi.ExecutorResponse{Payload: resp.Payload, Headers: resp.Headers, Metadata: resp.Metadata}, nil
}

func (p *Plugin) ExecuteStream(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
	auth, nativeReq, opts := executionArgs(req)
	auth, err := p.ensureActivated(ctx, auth)
	if err != nil {
		return pluginapi.ExecutorStreamResponse{}, err
	}
	resp, err := p.executor.ExecuteStream(ctx, auth, nativeReq, opts)
	if err != nil {
		return pluginapi.ExecutorStreamResponse{}, err
	}
	out := make(chan pluginapi.ExecutorStreamChunk)
	go func() {
		defer close(out)
		if resp == nil {
			return
		}
		for chunk := range resp.Chunks {
			out <- pluginapi.ExecutorStreamChunk{Payload: chunk.Payload, Err: chunk.Err}
		}
	}()
	headers := http.Header{}
	if resp != nil {
		headers = resp.Headers
	}
	return pluginapi.ExecutorStreamResponse{Headers: headers, Chunks: out}, nil
}

func (p *Plugin) CountTokens(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	auth, nativeReq, opts := executionArgs(req)
	auth, err := p.ensureActivated(ctx, auth)
	if err != nil {
		return pluginapi.ExecutorResponse{}, err
	}
	resp, err := p.executor.CountTokens(ctx, auth, nativeReq, opts)
	if err != nil {
		return pluginapi.ExecutorResponse{}, err
	}
	return pluginapi.ExecutorResponse{Payload: resp.Payload, Headers: resp.Headers, Metadata: resp.Metadata}, nil
}

func (p *Plugin) HttpRequest(ctx context.Context, req pluginapi.ExecutorHTTPRequest) (pluginapi.ExecutorHTTPResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, firstNonEmpty(req.Method, http.MethodGet), req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return pluginapi.ExecutorHTTPResponse{}, err
	}
	httpReq.Header = cloneHeader(req.Headers)
	auth := authFromPlugin(req.AuthID, req.AuthProvider, req.StorageJSON, req.Metadata, req.Attributes)
	resp, err := p.executor.HttpRequest(ctx, auth, httpReq)
	if err != nil {
		return pluginapi.ExecutorHTTPResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return pluginapi.ExecutorHTTPResponse{}, err
	}
	return pluginapi.ExecutorHTTPResponse{StatusCode: resp.StatusCode, Headers: resp.Header.Clone(), Body: body}, nil
}

type storageFile struct {
	Type           string `json:"type"`
	APIKey         string `json:"api_key"`
	AccessToken    string `json:"access_token"`
	AccessToken2   string `json:"accessToken"`
	RefreshToken   string `json:"refresh_token"`
	RefreshToken2  string `json:"refreshToken"`
	AccountID      string `json:"account_id"`
	AccountID2     string `json:"accountId"`
	Email          string `json:"email"`
	Expired        string `json:"expired"`
	LastRefresh    string `json:"last_refresh"`
	BaseURL        string `json:"base_url"`
	ClientVersion  string `json:"client_version"`
	RuntimeProfile string `json:"runtime_profile"`
}

func parseStorage(raw []byte) (storageFile, bool) {
	var data storageFile
	if jsonx.Unmarshal(raw, &data) != nil {
		return storageFile{}, false
	}
	kind := strings.ToLower(strings.TrimSpace(data.Type))
	switch kind {
	case cursoragentv1.ProviderID:
		return data, true
	case "":
		if looksLikeCursorKey(data.APIKey) || looksLikeCursorKey(data.AccessToken) || looksLikeCursorKey(data.AccessToken2) {
			return data, true
		}
		return storageFile{}, false
	default:
		return storageFile{}, false
	}
}

func looksLikeCursorKey(apiKey string) bool {
	key := strings.TrimSpace(apiKey)
	return strings.HasPrefix(key, "key_") || strings.HasPrefix(key, "crsr_")
}

func storageToAuthData(data storageFile, fileName, path string) pluginapi.AuthData {
	access := firstNonEmpty(data.AccessToken, data.AccessToken2)
	refresh := firstNonEmpty(data.RefreshToken, data.RefreshToken2)
	account := firstNonEmpty(data.AccountID, data.AccountID2)
	metadata := map[string]any{
		"api_key":         strings.TrimSpace(data.APIKey),
		"access_token":    strings.TrimSpace(access),
		"refresh_token":   strings.TrimSpace(refresh),
		"account_id":      strings.TrimSpace(account),
		"email":           strings.TrimSpace(data.Email),
		"expired":         strings.TrimSpace(data.Expired),
		"last_refresh":    strings.TrimSpace(data.LastRefresh),
		"base_url":        strings.TrimSpace(data.BaseURL),
		"client_version":  strings.TrimSpace(data.ClientVersion),
		"runtime_profile": strings.ToLower(strings.TrimSpace(data.RuntimeProfile)),

		"refresh_interval_seconds": int(hostRefreshInterval / time.Second),
	}
	encoded, _ := jsonx.Marshal(data)
	label := firstNonEmpty(data.Email, account, fileName)
	return pluginapi.AuthData{
		Provider:    cursoragentv1.ProviderID,
		FileName:    firstNonEmpty(fileName, path),
		Label:       label,
		StorageJSON: encoded,
		Metadata:    metadata,
	}
}

func authFromPlugin(id, provider string, storage []byte, metadata map[string]any, attributes map[string]string) *cliproxyauth.Auth {
	auth := &cliproxyauth.Auth{
		ID:         strings.TrimSpace(id),
		Provider:   firstNonEmpty(provider, cursoragentv1.ProviderID),
		Metadata:   cloneMap(metadata),
		Attributes: cloneStrings(attributes),
	}
	if len(auth.Metadata) == 0 {
		if data, ok := parseStorage(storage); ok {
			parsed := storageToAuthData(data, "", "")
			auth.Metadata = parsed.Metadata
		}
	}
	return auth
}

func authToAuthData(auth *cliproxyauth.Auth) pluginapi.AuthData {
	if auth == nil {
		return pluginapi.AuthData{Provider: cursoragentv1.ProviderID}
	}
	return pluginapi.AuthData{
		Provider:   firstNonEmpty(auth.Provider, cursoragentv1.ProviderID),
		ID:         auth.ID,
		FileName:   auth.FileName,
		Label:      auth.Label,
		Prefix:     auth.Prefix,
		ProxyURL:   auth.ProxyURL,
		Disabled:   auth.Disabled,
		Metadata:   cloneMap(auth.Metadata),
		Attributes: cloneStrings(auth.Attributes),
	}
}

func mergeStorage(raw []byte, auth *cliproxyauth.Auth) []byte {
	data, _ := parseStorage(raw)
	if auth != nil && auth.Metadata != nil {
		if v := strings.TrimSpace(fmt.Sprint(auth.Metadata["access_token"])); v != "" && v != "<nil>" {
			data.AccessToken = v
		}
		if v := strings.TrimSpace(fmt.Sprint(auth.Metadata["refresh_token"])); v != "" && v != "<nil>" {
			data.RefreshToken = v
		}
		if v := strings.TrimSpace(fmt.Sprint(auth.Metadata["expired"])); v != "" && v != "<nil>" {
			data.Expired = v
		}
		if v := strings.TrimSpace(fmt.Sprint(auth.Metadata["last_refresh"])); v != "" && v != "<nil>" {
			data.LastRefresh = v
		}
		if v := strings.TrimSpace(fmt.Sprint(auth.Metadata["api_key"])); v != "" && v != "<nil>" {
			data.APIKey = v
		}
		if v := strings.TrimSpace(fmt.Sprint(auth.Metadata["account_id"])); v != "" && v != "<nil>" {
			data.AccountID = v
		}
	}
	data.Type = cursoragentv1.ProviderID
	encoded, _ := jsonx.Marshal(data)
	return encoded
}

func executionArgs(req pluginapi.ExecutorRequest) (*cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) {
	auth := authFromPlugin(req.AuthID, req.AuthProvider, req.StorageJSON, req.AuthMetadata, req.AuthAttributes)
	source := sdktranslator.FromString(strings.TrimSpace(req.SourceFormat))
	format := sdktranslator.FromString(strings.TrimSpace(req.Format))
	if source == "" {
		source = format
	}
	if format == "" {
		format = source
	}
	if source == "" {
		source = sdktranslator.FormatClaude
		format = sdktranslator.FormatClaude
	}
	return auth, cliproxyexecutor.Request{
			Model:    req.Model,
			Payload:  req.Payload,
			Format:   format,
			Metadata: req.Metadata,
		}, cliproxyexecutor.Options{
			Stream:          req.Stream,
			Alt:             req.Alt,
			Headers:         cloneHeader(req.Headers),
			Query:           req.Query,
			OriginalRequest: req.OriginalRequest,
			SourceFormat:    source,
			ResponseFormat:  format,
			Metadata:        req.Metadata,
		}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStrings(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneHeader(in http.Header) http.Header {
	if in == nil {
		return nil
	}
	return in.Clone()
}
