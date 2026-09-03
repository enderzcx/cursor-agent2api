package cursoragentv1

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

const agentRunPath = "/agent.v1.AgentService/Run"

type stream interface {
	Write([]byte) error
	Data() <-chan []byte
	Done() <-chan struct{}
	Err() error
	Close()
}

type streamConfig struct {
	BaseURL            string
	ProxyURL           string
	AccessToken        string
	ClientVersion      string
	AllowedNativeTools []string
	TLSConfig          *tls.Config
}

type streamOpener interface {
	Open(context.Context, streamConfig) (stream, error)
}

type http2Opener struct{}

func (http2Opener) Open(ctx context.Context, cfg streamConfig) (stream, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid Cursor Agent v1 base URL")
	}
	if base.Scheme != "https" {
		return nil, fmt.Errorf("unsupported Cursor Agent v1 scheme %q", base.Scheme)
	}
	if strings.TrimSpace(cfg.AccessToken) == "" {
		return nil, fmt.Errorf("Cursor Agent v1 access token is required")
	}
	clientVersion := strings.TrimSpace(cfg.ClientVersion)
	if clientVersion == "" {
		return nil, fmt.Errorf("Cursor Agent v1 client version is required")
	}

	requestURL := *base
	requestURL.Path = strings.TrimRight(base.Path, "/") + agentRunPath
	requestURL.RawQuery = ""
	requestURL.Fragment = ""

	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	request, err := http.NewRequestWithContext(streamCtx, http.MethodPost, requestURL.String(), reader)
	if err != nil {
		cancel()
		_ = reader.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("build Cursor Agent v1 request: %w", err)
	}
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/connect+proto")
	request.Header.Set("Connect-Protocol-Version", "1")
	request.Header.Set("TE", "trailers")
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(cfg.AccessToken))
	request.Header.Set("X-Ghost-Mode", "true")
	request.Header.Set("X-Cursor-Client-Version", clientVersion)
	request.Header.Set("X-Cursor-Client-Type", defaultCursorClientType)
	request.Header.Set("X-Cursor-Streaming", "true")
	request.Header.Set("Connect-Accept-Encoding", "gzip,br")
	request.Header.Set("User-Agent", "connect-es/1.6.1")
	request.Header.Set("Cookie", cursorRequestCookie(cfg.AccessToken))
	if len(cfg.AllowedNativeTools) > 0 {
		request.Header.Set("X-Cursor-Agent-Allowed-Tools", strings.Join(cfg.AllowedNativeTools, ","))
	}

	transport, err := cursorHTTP2Transport(cfg.ProxyURL, cfg.TLSConfig)
	if err != nil {
		cancel()
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}
	result := &http2Stream{
		cancel: cancel,
		reader: reader,
		writer: writer,
		data:   make(chan []byte, 64),
		done:   make(chan struct{}),
	}
	go result.roundTrip(transport, request)
	return result, nil
}

func cursorHTTP2Transport(proxyURL string, tlsConfig *tls.Config) (*http2.Transport, error) {
	if tlsConfig != nil {
		tlsConfig = tlsConfig.Clone()
	}
	transport := &http2.Transport{TLSClientConfig: tlsConfig}
	if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
		dialer, _, dialErr := proxyutil.BuildDialer(proxyURL)
		if dialErr != nil {
			return nil, fmt.Errorf("configure Cursor Agent v1 proxy: %w", dialErr)
		}
		transport.DialTLSContext = func(dialCtx context.Context, network, addr string, clientTLS *tls.Config) (net.Conn, error) {
			conn, dialErr := proxyDialContext(dialCtx, dialer, network, addr)
			if dialErr != nil {
				return nil, fmt.Errorf("dial Cursor Agent v1 proxy: %w", dialErr)
			}
			tlsConfigForConn := clientTLS
			if tlsConfigForConn == nil {
				tlsConfigForConn = &tls.Config{}
			} else {
				tlsConfigForConn = tlsConfigForConn.Clone()
			}
			if tlsConfigForConn.ServerName == "" {
				host, _, splitErr := net.SplitHostPort(addr)
				if splitErr != nil {
					host = addr
				}
				tlsConfigForConn.ServerName = host
			}
			tlsConn := tls.Client(conn, tlsConfigForConn)
			if handshakeErr := tlsConn.HandshakeContext(dialCtx); handshakeErr != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("Cursor Agent v1 proxy TLS handshake: %w", handshakeErr)
			}
			return tlsConn, nil
		}
	}
	return transport, nil
}

func cursorRequestCookie(accessToken string) string {
	prefix := strings.TrimSpace(accessToken)
	if len(prefix) > 15 {
		prefix = prefix[:15]
	}
	return "CursorCookie=Cookie-" + prefix
}

func proxyDialContext(ctx context.Context, dialer proxy.Dialer, network, addr string) (net.Conn, error) {
	if contextual, ok := dialer.(proxy.ContextDialer); ok {
		return contextual.DialContext(ctx, network, addr)
	}
	result := make(chan struct {
		conn net.Conn
		err  error
	}, 1)
	go func() {
		conn, err := dialer.Dial(network, addr)
		dialed := struct {
			conn net.Conn
			err  error
		}{conn: conn, err: err}
		select {
		case result <- dialed:
		case <-ctx.Done():
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case dialed := <-result:
		return dialed.conn, dialed.err
	}
}

type httpStatusError struct {
	code int
	text string
}

func (e *httpStatusError) Error() string   { return e.text }
func (e *httpStatusError) StatusCode() int { return e.code }

type http2Stream struct {
	cancel context.CancelFunc
	reader *io.PipeReader
	writer *io.PipeWriter

	writeMu sync.Mutex
	errMu   sync.Mutex
	err     error
	data    chan []byte
	done    chan struct{}
	once    sync.Once
}

func (s *http2Stream) roundTrip(transport *http2.Transport, request *http.Request) {
	defer close(s.done)
	defer close(s.data)
	defer transport.CloseIdleConnections()

	response, err := transport.RoundTrip(request)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			s.setErr(fmt.Errorf("Cursor Agent v1 HTTP/2 request: %w", err))
		}
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		s.setErr(&httpStatusError{code: response.StatusCode, text: fmt.Sprintf("Cursor Agent v1 upstream HTTP %d", response.StatusCode)})
		return
	}

	buffer := make([]byte, 32*1024)
	for {
		count, readErr := response.Body.Read(buffer)
		if count > 0 {
			chunk := append([]byte(nil), buffer[:count]...)
			select {
			case s.data <- chunk:
			case <-request.Context().Done():
				return
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, context.Canceled) {
				s.setErr(fmt.Errorf("read Cursor Agent v1 HTTP/2 response: %w", readErr))
			}
			return
		}
	}
}

func (s *http2Stream) Write(payload []byte) error {
	if s == nil {
		return fmt.Errorf("Cursor Agent v1 stream is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.Err(); err != nil {
		return err
	}
	_, err := s.writer.Write(payload)
	if err != nil {
		return fmt.Errorf("write Cursor Agent v1 request: %w", err)
	}
	return nil
}

func (s *http2Stream) Data() <-chan []byte   { return s.data }
func (s *http2Stream) Done() <-chan struct{} { return s.done }

func (s *http2Stream) Err() error {
	if s == nil {
		return fmt.Errorf("Cursor Agent v1 stream is nil")
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *http2Stream) setErr(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

func (s *http2Stream) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.cancel()
		_ = s.writer.Close()
		_ = s.reader.Close()
	})
}
