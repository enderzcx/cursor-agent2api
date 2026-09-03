// Package cursoragentv1 implements the private Cursor Agent v1 transport used
// by BeefAPI's subscription-OAuth CPA provider. It is not the official type-62
// @cursor/sdk runtime.
package cursoragentv1

import (
	"encoding/binary"
	"fmt"
	"net/http"

	"github.com/enderzcx/cursor-agent2api/internal/jsonx"
)

const (
	connectCompressionFlag byte = 0x01
	connectEndStreamFlag   byte = 0x02
	connectHeaderSize           = 5
	maxConnectPayloadSize       = 16 << 20
)

type connectError struct {
	Code    string
	Message string
}

func (e *connectError) Error() string {
	if e == nil {
		return "cursor Agent v1 Connect error"
	}
	return fmt.Sprintf("cursor Agent v1 Connect error %s: %s", e.Code, e.Message)
}

func (e *connectError) StatusCode() int {
	if e == nil {
		return http.StatusBadGateway
	}
	switch e.Code {
	case "unauthenticated":
		return http.StatusUnauthorized
	case "permission_denied":
		return http.StatusForbidden
	case "invalid_argument", "failed_precondition", "out_of_range":
		return http.StatusBadRequest
	case "resource_exhausted":
		return http.StatusTooManyRequests
	case "unavailable":
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadGateway
	}
}

func frameConnect(payload []byte) []byte {
	return frameConnectWithFlags(payload, 0)
}

func connectLimitMiB() int {
	return maxConnectPayloadSize >> 20
}

func errExceedsConnectLimit(kind string) error {
	return fmt.Errorf("%s exceeds %d MiB", kind, connectLimitMiB())
}

func errExceedsConnectLimitSize(kind string, n uint32) error {
	return fmt.Errorf("%s exceeds %d MiB: %d", kind, connectLimitMiB(), n)
}

func frameConnectWithFlags(payload []byte, flags byte) []byte {
	frame := make([]byte, connectHeaderSize+len(payload))
	frame[0] = flags
	// All production writes pass through writeConnectPayload, which enforces
	// maxConnectPayloadSize before this framing-only helper is called.
	binary.BigEndian.PutUint32(frame[1:connectHeaderSize], uint32(len(payload))) // #nosec G115 -- checked by the sole production write helper
	copy(frame[connectHeaderSize:], payload)
	return frame
}

func writeConnectPayload(target stream, payload []byte) error {
	if len(payload) > maxConnectPayloadSize {
		return errExceedsConnectLimit("Cursor Agent v1 outbound Connect payload")
	}
	return target.Write(frameConnect(payload))
}

func parseConnectFrame(buffer []byte) (flags byte, payload []byte, consumed int, ok bool, err error) {
	if len(buffer) < connectHeaderSize {
		return 0, nil, 0, false, nil
	}
	length := binary.BigEndian.Uint32(buffer[1:connectHeaderSize])
	if length > maxConnectPayloadSize {
		return 0, nil, 0, false, errExceedsConnectLimitSize("cursor Agent v1 Connect frame", length)
	}
	if uint64(length) > uint64(^uint(0)>>1)-connectHeaderSize {
		return 0, nil, 0, false, fmt.Errorf("cursor Agent v1 Connect frame length overflows int: %d", length)
	}
	total := connectHeaderSize + int(length)
	if len(buffer) < total {
		return 0, nil, 0, false, nil
	}
	return buffer[0], buffer[connectHeaderSize:total], total, true, nil
}

func parseConnectEndStream(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	var trailer struct {
		Error *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := jsonx.Unmarshal(payload, &trailer); err != nil {
		return fmt.Errorf("parse cursor Agent v1 Connect end stream: %w", err)
	}
	if trailer.Error == nil {
		return nil
	}
	code := trailer.Error.Code
	if code == "" {
		code = "unknown"
	}
	message := trailer.Error.Message
	if message == "" {
		message = "unknown error"
	}
	return &connectError{Code: code, Message: message}
}
