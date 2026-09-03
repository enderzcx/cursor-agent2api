package cursoragentv1

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/stretchr/testify/require"
)

func TestFetchCursorServerConfigSelectsAgentNURL(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		require.Equal(t, serverConfigPath, request.URL.Path)
		require.Equal(t, "Bearer access-token", request.Header.Get("Authorization"))
		require.Equal(t, defaultCursorClientType, request.Header.Get("X-Cursor-Client-Type"))
		require.Equal(t, "cli-test", request.Header.Get("X-Cursor-Client-Version"))
		_, _ = w.Write([]byte(`{"agentUrlConfig":{"agentUrl":"https://agent.api5.cursor.sh","agentnUrl":"https://agentn.global.api5.cursor.sh"}}`))
	}))
	defer server.Close()

	executor := newExecutorForTest(nil, server.Client())
	resolved, err := executor.fetchCursorServerConfig(context.Background(), nil, server.URL, "access-token", "cli-test")
	require.NoError(t, err)
	require.Equal(t, "https://agentn.global.api5.cursor.sh", resolved)
	require.EqualValues(t, 1, calls.Load())
}

func TestFetchCursorServerConfigRejectsUntrustedAgentHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"agentUrlConfig":{"agentnUrl":"https://attacker.example"}}`))
	}))
	defer server.Close()

	executor := newExecutorForTest(nil, server.Client())
	_, err := executor.fetchCursorServerConfig(context.Background(), nil, server.URL, "access-token", "cli-test")
	require.EqualError(t, err, "Cursor Agent v1 server config returned an untrusted Agent host")
}

func TestValidateCursorCredentialIdentityMatchesJWTSubjectAndCaches(t *testing.T) {
	var calls atomic.Int32
	configured := testCursorJWT("user-1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		require.Equal(t, "Bearer api-key", request.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"accessToken":"` + testCursorJWT("user-1") + `"}`))
	}))
	defer server.Close()

	executor := newExecutorForTest(nil, server.Client())
	executor.refreshURL = server.URL
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"account_id": "account-1", "api_key": "api-key", "access_token": configured}}
	first, err := executor.validateCursorCredentialIdentity(context.Background(), auth, configured)
	require.NoError(t, err)
	second, err := executor.validateCursorCredentialIdentity(context.Background(), auth, configured)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, "user-1", cursorTokenSubject(first))
	require.EqualValues(t, 1, calls.Load())
}

func TestValidateCursorCredentialIdentityRejectsDifferentJWTSubject(t *testing.T) {
	configured := testCursorJWT("user-1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"accessToken":"` + testCursorJWT("user-2") + `"}`))
	}))
	defer server.Close()

	executor := newExecutorForTest(nil, server.Client())
	executor.refreshURL = server.URL
	auth := &cliproxyauth.Auth{Metadata: map[string]any{"account_id": "account-1", "api_key": "api-key", "access_token": configured}}
	_, err := executor.validateCursorCredentialIdentity(context.Background(), auth, configured)
	require.EqualError(t, err, "Cursor Agent v1 API key and access token belong to different identities")
	require.Equal(t, http.StatusUnauthorized, errorStatusCode(err))
}

func TestCursorControlPlaneAndDiscoveredURLValidation(t *testing.T) {
	require.True(t, isCursorControlPlaneBaseURL("https://api2.cursor.sh"))
	require.False(t, isCursorControlPlaneBaseURL("https://agentn.global.api5.cursor.sh"))
	require.NoError(t, validateDiscoveredCursorAgentURL("https://agentn.global.api5.cursor.sh"))
	require.Error(t, validateDiscoveredCursorAgentURL("http://agentn.global.api5.cursor.sh"))
}

func testCursorJWT(subject string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"` + subject + `"}`))
	return header + "." + payload + ".signature"
}
