package cursoragentv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/enderzcx/cursor-agent2api/internal/helps"
	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"golang.org/x/sync/singleflight"
)

const (
	serverConfigPath        = "/aiserver.v1.ServerConfigService/GetServerConfig"
	serverConfigTimeout     = 10 * time.Second
	serverConfigCacheTTL    = 10 * time.Minute
	identityCheckCacheTTL   = 10 * time.Minute
	serverConfigBodyLimit   = 1 << 20
	defaultCursorClientType = "cli"
)

type timedString struct {
	value     string
	expiresAt time.Time
}

type executorBootstrapCache struct {
	mu       sync.RWMutex
	endpoint map[string]timedString
	identity map[string]timedString

	endpointGroup singleflight.Group
}

type cursorServerConfig struct {
	AgentURLConfig struct {
		AgentURL  string `json:"agentUrl"`
		AgentNURL string `json:"agentnUrl"`
	} `json:"agentUrlConfig"`
}

func (e *Executor) bootstrapCursorRun(ctx context.Context, auth *cliproxyauth.Auth, baseURL, accessToken, clientVersion string) (string, string, error) {
	effectiveAccessToken, err := e.validateCursorCredentialIdentity(ctx, auth, accessToken)
	if err != nil {
		return "", "", err
	}
	resolvedBaseURL, err := e.resolveCursorAgentBaseURL(ctx, auth, baseURL, effectiveAccessToken, clientVersion)
	if err != nil {
		return "", "", err
	}
	return resolvedBaseURL, effectiveAccessToken, nil
}

func (e *Executor) validateCursorCredentialIdentity(ctx context.Context, auth *cliproxyauth.Auth, accessToken string) (string, error) {
	configuredSubject := cursorTokenSubject(accessToken)
	if configuredSubject == "" {
		return accessToken, nil
	}
	apiKey := metadataValue(auth, "api_key")
	if apiKey == "" {
		return "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 API key is required to validate credential identity"}
	}
	key := digestBootstrapKey("identity", apiKey, configuredSubject)
	now := time.Now()
	e.bootstrapCache.mu.RLock()
	cached, ok := e.bootstrapCache.identity[key]
	e.bootstrapCache.mu.RUnlock()
	if ok && now.Before(cached.expiresAt) {
		return cached.value, nil
	}

	refreshedAuth, err := e.Refresh(ctx, auth.Clone())
	if err != nil {
		return "", err
	}
	refreshedAccessToken := metadataValue(refreshedAuth, "access_token")
	refreshedSubject := cursorTokenSubject(refreshedAccessToken)
	if refreshedSubject == "" || refreshedSubject != configuredSubject {
		return "", &requestError{status: http.StatusUnauthorized, text: "Cursor Agent v1 API key and access token belong to different identities"}
	}
	e.bootstrapCache.mu.Lock()
	if e.bootstrapCache.identity == nil {
		e.bootstrapCache.identity = make(map[string]timedString)
	}
	expiresAt := time.Now().Add(identityCheckCacheTTL)
	if tokenExpiry := jwtExpiry(refreshedAccessToken); !tokenExpiry.IsZero() {
		tokenExpiry = tokenExpiry.Add(-time.Minute)
		if tokenExpiry.Before(expiresAt) {
			expiresAt = tokenExpiry
		}
	}
	e.bootstrapCache.identity[key] = timedString{value: refreshedAccessToken, expiresAt: expiresAt}
	e.bootstrapCache.mu.Unlock()
	return refreshedAccessToken, nil
}

func (e *Executor) resolveCursorAgentBaseURL(ctx context.Context, auth *cliproxyauth.Auth, baseURL, accessToken, clientVersion string) (string, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !isCursorControlPlaneBaseURL(baseURL) {
		return baseURL, nil
	}
	key := digestBootstrapKey("endpoint", baseURL, cursorTokenSubject(accessToken))
	now := time.Now()
	e.bootstrapCache.mu.RLock()
	cached, ok := e.bootstrapCache.endpoint[key]
	e.bootstrapCache.mu.RUnlock()
	if ok && now.Before(cached.expiresAt) {
		return cached.value, nil
	}

	result := e.bootstrapCache.endpointGroup.DoChan(key, func() (any, error) {
		resolveCtx, cancel := context.WithTimeout(context.Background(), serverConfigTimeout)
		defer cancel()
		resolved, err := e.fetchCursorServerConfig(resolveCtx, auth, baseURL, accessToken, clientVersion)
		if err != nil {
			return "", err
		}
		e.bootstrapCache.mu.Lock()
		if e.bootstrapCache.endpoint == nil {
			e.bootstrapCache.endpoint = make(map[string]timedString)
		}
		e.bootstrapCache.endpoint[key] = timedString{value: resolved, expiresAt: time.Now().Add(serverConfigCacheTTL)}
		e.bootstrapCache.mu.Unlock()
		return resolved, nil
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case outcome := <-result:
		if outcome.Err != nil {
			return "", outcome.Err
		}
		return outcome.Val.(string), nil
	}
}

func (e *Executor) fetchCursorServerConfig(ctx context.Context, auth *cliproxyauth.Auth, baseURL, accessToken, clientVersion string) (string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, serverConfigTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, baseURL+serverConfigPath, bytes.NewReader([]byte("{}")))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(accessToken))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("X-Cursor-Client-Type", defaultCursorClientType)
	request.Header.Set("X-Cursor-Client-Version", strings.TrimSpace(clientVersion))
	client := e.client
	if client == nil {
		client = helps.NewProxyAwareHTTPClient(requestCtx, nil, auth, serverConfigTimeout)
	}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("Cursor Agent v1 server config request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, serverConfigBodyLimit+1))
	if err != nil {
		return "", fmt.Errorf("Cursor Agent v1 server config read: %w", err)
	}
	if len(body) > serverConfigBodyLimit {
		return "", fmt.Errorf("Cursor Agent v1 server config response is too large")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", &httpStatusError{code: response.StatusCode, text: fmt.Sprintf("Cursor Agent v1 server config HTTP %d", response.StatusCode)}
	}
	var config cursorServerConfig
	if err := jsonx.Unmarshal(body, &config); err != nil {
		return "", fmt.Errorf("Cursor Agent v1 server config is invalid: %w", err)
	}
	resolved := strings.TrimRight(strings.TrimSpace(config.AgentURLConfig.AgentNURL), "/")
	if resolved == "" {
		resolved = strings.TrimRight(strings.TrimSpace(config.AgentURLConfig.AgentURL), "/")
	}
	if err := validateDiscoveredCursorAgentURL(resolved); err != nil {
		return "", err
	}
	return resolved, nil
}

func isCursorControlPlaneBaseURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Scheme, "https") && strings.EqualFold(parsed.Hostname(), "api2.cursor.sh")
}

func validateDiscoveredCursorAgentURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("Cursor Agent v1 server config returned an invalid Agent URL")
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "cursor.sh" && !strings.HasSuffix(host, ".cursor.sh") {
		return fmt.Errorf("Cursor Agent v1 server config returned an untrusted Agent host")
	}
	return nil
}

func digestBootstrapKey(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return fmt.Sprintf("%x", digest[:])
}

func cursorTokenSubject(token string) string {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if jsonx.Unmarshal(payload, &claims) != nil {
		return ""
	}
	return strings.TrimSpace(claims.Subject)
}
