package main

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int CursorAgentPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void CursorAgentPluginFree(void*, size_t);
extern void CursorAgentPluginShutdown(void);

static int cursoragent_call_host(cliproxy_host_api* api, const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	return api->call(api->host_ctx, method, request, request_len, response);
}

static void cursoragent_free_host_buffer(cliproxy_host_api* api, void* ptr, size_t len) {
	api->free_buffer(ptr, len);
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"unsafe"

	cursorplugin "github.com/enderzcx/cursor-agent2api/internal/plugin"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const pluginVersion = cursorplugin.Version

var abiState = struct {
	sync.RWMutex
	host   *C.cliproxy_host_api
	plugin *cursorplugin.Plugin
}{}

type abiLifecycleRequest struct {
	ConfigYAML []byte `json:"config_yaml"`
}

type abiRegistration struct {
	SchemaVersion uint32             `json:"schema_version"`
	Metadata      pluginapi.Metadata `json:"metadata"`
	Capabilities  abiCapabilities    `json:"capabilities"`
}

type abiCapabilities struct {
	AuthProvider          bool                         `json:"auth_provider"`
	ModelProvider         bool                         `json:"model_provider"`
	ModelRegistrar        bool                         `json:"model_registrar"`
	Executor              bool                         `json:"executor"`
	ExecutorModelScope    pluginapi.ExecutorModelScope `json:"executor_model_scope"`
	ExecutorInputFormats  []string                     `json:"executor_input_formats,omitempty"`
	ExecutorOutputFormats []string                     `json:"executor_output_formats,omitempty"`
	RequestTranslator     bool                         `json:"request_translator"`
	ResponseTranslator    bool                         `json:"response_translator"`
	ThinkingApplier       bool                         `json:"thinking_applier"`
	CommandLinePlugin     bool                         `json:"command_line_plugin"`
}

type abiIdentifierResponse struct {
	Identifier string `json:"identifier"`
}

type abiAuthLoginStartRequest struct {
	pluginapi.AuthLoginStartRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type abiAuthLoginPollRequest struct {
	pluginapi.AuthLoginPollRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type abiAuthRefreshRequest struct {
	pluginapi.AuthRefreshRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type abiAuthModelRequest struct {
	pluginapi.AuthModelRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type abiExecutorRequest struct {
	pluginapi.ExecutorRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
	StreamID       string `json:"stream_id,omitempty"`
}

type abiExecutorHTTPRequest struct {
	pluginapi.ExecutorHTTPRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type abiExecutorStreamResponse struct {
	Headers http.Header                     `json:"headers,omitempty"`
	Chunks  []pluginapi.ExecutorStreamChunk `json:"chunks,omitempty"`
}

type abiHostHTTPRequest struct {
	pluginapi.HTTPRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type abiHostHTTPStreamResponse struct {
	StatusCode int                         `json:"status_code"`
	Headers    http.Header                 `json:"headers,omitempty"`
	StreamID   string                      `json:"stream_id,omitempty"`
	Chunks     []pluginapi.HTTPStreamChunk `json:"chunks,omitempty"`
}

type abiHostHTTPStreamReadRequest struct {
	StreamID string `json:"stream_id"`
}

type abiHostHTTPStreamReadResponse struct {
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
	Done    bool   `json:"done,omitempty"`
}

type abiHostHTTPStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
}

type abiHostStreamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload,omitempty"`
}

type abiHostStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

type abiEmptyResponse struct{}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if host == nil || plugin == nil {
		return 1
	}
	abiState.Lock()
	abiState.host = host
	abiState.Unlock()
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.CursorAgentPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.CursorAgentPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.CursorAgentPluginShutdown)
	return 0
}

//export CursorAgentPluginCall
func CursorAgentPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeABIResponse(response, abiErrorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleABIMethod(context.Background(), C.GoString(method), requestBytes)
	if errHandle != nil {
		writeABIResponse(response, abiErrorEnvelopeFromError("plugin_error", errHandle))
		return 1
	}
	writeABIResponse(response, raw)
	return 0
}

//export CursorAgentPluginFree
func CursorAgentPluginFree(ptr unsafe.Pointer, len C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export CursorAgentPluginShutdown
func CursorAgentPluginShutdown() {
	abiState.Lock()
	abiState.plugin = nil
	abiState.host = nil
	abiState.Unlock()
}

func handleABIMethod(ctx context.Context, method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		return handleRegister(request)
	}
	p, errPlugin := currentPlugin()
	if errPlugin != nil {
		return nil, errPlugin
	}
	switch method {
	case pluginabi.MethodAuthIdentifier, pluginabi.MethodExecutorIdentifier:
		return abiOKEnvelope(abiIdentifierResponse{Identifier: p.Identifier()})
	case pluginabi.MethodAuthParse:
		var req pluginapi.AuthParseRequest
		if errDecode := json.Unmarshal(request, &req); errDecode != nil {
			return nil, errDecode
		}
		resp, errCall := p.ParseAuth(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodAuthLoginStart:
		var rpcReq abiAuthLoginStartRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.AuthLoginStartRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.StartLogin(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodAuthLoginPoll:
		var rpcReq abiAuthLoginPollRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.AuthLoginPollRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.PollLogin(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodAuthRefresh:
		var rpcReq abiAuthRefreshRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.AuthRefreshRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.RefreshAuth(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodModelRegister:
		var req pluginapi.ModelRegistrationRequest
		if errDecode := json.Unmarshal(request, &req); errDecode != nil {
			return nil, errDecode
		}
		resp, errCall := p.RegisterModels(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodModelStatic:
		var req pluginapi.StaticModelRequest
		if errDecode := json.Unmarshal(request, &req); errDecode != nil {
			return nil, errDecode
		}
		resp, errCall := p.StaticModels(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodModelForAuth:
		var rpcReq abiAuthModelRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.AuthModelRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.ModelsForAuth(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodExecutorExecute:
		var rpcReq abiExecutorRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.ExecutorRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.Execute(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodExecutorExecuteStream:
		var rpcReq abiExecutorRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.ExecutorRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.ExecuteStream(ctx, req)
		if errCall != nil {
			return nil, errCall
		}
		streamResp, errMarshal := marshalABIStreamResponse(ctx, rpcReq.StreamID, resp)
		if errMarshal != nil {
			return nil, errMarshal
		}
		return abiOKEnvelope(streamResp)
	case pluginabi.MethodExecutorCountTokens:
		var rpcReq abiExecutorRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.ExecutorRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.CountTokens(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	case pluginabi.MethodExecutorHTTPRequest:
		var rpcReq abiExecutorHTTPRequest
		if errDecode := json.Unmarshal(request, &rpcReq); errDecode != nil {
			return nil, errDecode
		}
		req := rpcReq.ExecutorHTTPRequest
		req.HTTPClient = abiHostHTTPClient{callbackID: rpcReq.HostCallbackID}
		resp, errCall := p.HttpRequest(ctx, req)
		return abiOKEnvelopeWithError(resp, errCall)
	default:
		return abiErrorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func handleRegister(request []byte) ([]byte, error) {
	var req abiLifecycleRequest
	if errDecode := json.Unmarshal(request, &req); errDecode != nil {
		return nil, errDecode
	}
	p, errBuild := cursorplugin.Build(req.ConfigYAML)
	if errBuild != nil {
		return nil, errBuild
	}
	metadata := p.Metadata()
	metadata.Version = pluginVersion
	abiState.Lock()
	abiState.plugin = p
	abiState.Unlock()
	return abiOKEnvelope(abiRegistration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata:      metadata,
		Capabilities: abiCapabilities{
			AuthProvider:          true,
			ModelProvider:         true,
			ModelRegistrar:        true,
			Executor:              true,
			ExecutorModelScope:    pluginapi.ExecutorModelScopeBoth,
			ExecutorInputFormats:  p.InputFormats(),
			ExecutorOutputFormats: p.OutputFormats(),
		},
	})
}

func currentPlugin() (*cursorplugin.Plugin, error) {
	abiState.RLock()
	defer abiState.RUnlock()
	if abiState.plugin == nil {
		return nil, fmt.Errorf("cursor-agent-v1 plugin is not registered")
	}
	return abiState.plugin, nil
}

type abiHostHTTPClient struct {
	callbackID string
}

func (c abiHostHTTPClient) Do(ctx context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	return callHost[pluginapi.HTTPResponse](pluginabi.MethodHostHTTPDo, abiHostHTTPRequest{
		HTTPRequest:    req,
		HostCallbackID: c.callbackID,
	})
}

func (c abiHostHTTPClient) DoStream(ctx context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	resp, errCall := callHost[abiHostHTTPStreamResponse](pluginabi.MethodHostHTTPDoStream, abiHostHTTPRequest{
		HTTPRequest:    req,
		HostCallbackID: c.callbackID,
	})
	if errCall != nil {
		return pluginapi.HTTPStreamResponse{}, errCall
	}
	if resp.StreamID != "" {
		chunks := make(chan pluginapi.HTTPStreamChunk)
		go readHostHTTPStream(ctx, resp.StreamID, chunks)
		return pluginapi.HTTPStreamResponse{StatusCode: resp.StatusCode, Headers: resp.Headers, Chunks: chunks}, nil
	}
	chunks := make(chan pluginapi.HTTPStreamChunk, len(resp.Chunks))
	for _, chunk := range resp.Chunks {
		chunks <- chunk
	}
	close(chunks)
	return pluginapi.HTTPStreamResponse{StatusCode: resp.StatusCode, Headers: resp.Headers, Chunks: chunks}, nil
}

func readHostHTTPStream(ctx context.Context, streamID string, out chan<- pluginapi.HTTPStreamChunk) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			closeHostHTTPStream(streamID)
			return
		default:
		}
		resp, errRead := callHost[abiHostHTTPStreamReadResponse](pluginabi.MethodHostHTTPStreamRead, abiHostHTTPStreamReadRequest{StreamID: streamID})
		if errRead != nil {
			closeHostHTTPStream(streamID)
			out <- pluginapi.HTTPStreamChunk{Err: errRead}
			return
		}
		if resp.Error != "" {
			out <- pluginapi.HTTPStreamChunk{Err: fmt.Errorf("%s", resp.Error)}
			return
		}
		if len(resp.Payload) > 0 {
			out <- pluginapi.HTTPStreamChunk{Payload: append([]byte(nil), resp.Payload...)}
		}
		if resp.Done {
			return
		}
	}
}

func closeHostHTTPStream(streamID string) {
	_, _ = callHost[abiEmptyResponse](pluginabi.MethodHostHTTPStreamClose, abiHostHTTPStreamCloseRequest{StreamID: streamID})
}

func marshalABIStreamResponse(ctx context.Context, streamID string, resp pluginapi.ExecutorStreamResponse) (abiExecutorStreamResponse, error) {
	if streamID == "" {
		chunks := make([]pluginapi.ExecutorStreamChunk, 0)
		for chunk := range resp.Chunks {
			chunks = append(chunks, chunk)
		}
		return abiExecutorStreamResponse{Headers: resp.Headers, Chunks: chunks}, nil
	}
	go pumpABIStream(ctx, streamID, resp.Chunks)
	return abiExecutorStreamResponse{Headers: resp.Headers}, nil
}

func pumpABIStream(ctx context.Context, streamID string, chunks <-chan pluginapi.ExecutorStreamChunk) {
	errorMessage := ""
	defer func() {
		_, _ = callHost[abiEmptyResponse](pluginabi.MethodHostStreamClose, abiHostStreamCloseRequest{StreamID: streamID, Error: errorMessage})
	}()
	for {
		select {
		case <-ctx.Done():
			if errCtx := ctx.Err(); errCtx != nil {
				errorMessage = errCtx.Error()
			}
			return
		case chunk, ok := <-chunks:
			if !ok {
				return
			}
			if chunk.Err != nil {
				errorMessage = chunk.Err.Error()
				return
			}
			if len(chunk.Payload) == 0 {
				continue
			}
			_, errCall := callHost[abiEmptyResponse](pluginabi.MethodHostStreamEmit, abiHostStreamEmitRequest{
				StreamID: streamID,
				Payload:  append([]byte(nil), chunk.Payload...),
			})
			if errCall != nil {
				errorMessage = errCall.Error()
				return
			}
		}
	}
}

func callHost[T any](method string, request any) (T, error) {
	var zero T
	abiState.RLock()
	host := abiState.host
	abiState.RUnlock()
	if host == nil || host.call == nil {
		return zero, fmt.Errorf("host callback is unavailable")
	}
	rawRequest, errMarshal := json.Marshal(request)
	if errMarshal != nil {
		return zero, errMarshal
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var requestPtr *C.uint8_t
	if len(rawRequest) > 0 {
		requestPtr = (*C.uint8_t)(unsafe.Pointer(&rawRequest[0]))
	}
	var resp C.cliproxy_buffer
	code := C.cursoragent_call_host(host, cMethod, requestPtr, C.size_t(len(rawRequest)), &resp)
	if resp.ptr != nil {
		defer C.cursoragent_free_host_buffer(host, resp.ptr, resp.len)
	}
	if code != 0 {
		return zero, fmt.Errorf("host callback %s failed with code %d", method, int(code))
	}
	rawResp := C.GoBytes(resp.ptr, C.int(resp.len))
	var envelope pluginabi.Envelope
	if errDecode := json.Unmarshal(rawResp, &envelope); errDecode != nil {
		return zero, errDecode
	}
	if !envelope.OK {
		if envelope.Error != nil {
			return zero, fmt.Errorf("%s", envelope.Error.Message)
		}
		return zero, fmt.Errorf("host callback %s failed", method)
	}
	var out T
	if len(envelope.Result) == 0 {
		return out, nil
	}
	if errDecode := json.Unmarshal(envelope.Result, &out); errDecode != nil {
		return zero, errDecode
	}
	return out, nil
}

func abiOKEnvelope(v any) ([]byte, error) {
	result, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(pluginabi.Envelope{OK: true, Result: result})
}

func abiOKEnvelopeWithError(v any, err error) ([]byte, error) {
	if err != nil {
		return abiErrorEnvelopeFromError("plugin_error", err), nil
	}
	return abiOKEnvelope(v)
}

func abiErrorEnvelopeFromError(code string, err error) []byte {
	if err == nil {
		return abiErrorEnvelope(code, "")
	}
	httpStatus := 0
	if statusProvider, ok := err.(interface{ StatusCode() int }); ok && statusProvider != nil {
		httpStatus = statusProvider.StatusCode()
	}
	return abiErrorEnvelopeWithStatus(code, err.Error(), httpStatus)
}

func abiErrorEnvelope(code string, message string) []byte {
	return abiErrorEnvelopeWithStatus(code, message, 0)
}

func abiErrorEnvelopeWithStatus(code string, message string, httpStatus int) []byte {
	raw, _ := json.Marshal(pluginabi.Envelope{
		OK: false,
		Error: &pluginabi.Error{
			Code:       code,
			Message:    message,
			HTTPStatus: httpStatus,
		},
	})
	return raw
}

func writeABIResponse(response *C.cliproxy_buffer, data []byte) {
	if response == nil {
		return
	}
	if len(data) == 0 {
		response.ptr = nil
		response.len = 0
		return
	}
	ptr := C.malloc(C.size_t(len(data)))
	if ptr == nil {
		response.ptr = nil
		response.len = 0
		return
	}
	C.memcpy(ptr, unsafe.Pointer(&data[0]), C.size_t(len(data)))
	response.ptr = ptr
	response.len = C.size_t(len(data))
}

func main() {}
