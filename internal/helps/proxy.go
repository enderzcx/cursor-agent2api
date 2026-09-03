package helps

import (
	"context"
	"net/http"
	"strings"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

// NewProxyAwareHTTPClient matches the CPA helper signature used by the Agent v1
// executor. cfg is ignored; proxy selection comes from auth.ProxyURL, then the
// context RoundTripper.
func NewProxyAwareHTTPClient(ctx context.Context, _ any, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	httpClient := &http.Client{}
	if timeout > 0 {
		httpClient.Timeout = timeout
	}
	proxyURL := ""
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL != "" {
		transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyURL)
		if errBuild != nil {
			log.Debugf("failed to setup proxy from URL: %s, falling back to context transport", proxyutil.Redact(proxyURL))
		} else if transport != nil {
			httpClient.Transport = transport
			return httpClient
		}
	}
	if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
		httpClient.Transport = rt
	}
	return httpClient
}
