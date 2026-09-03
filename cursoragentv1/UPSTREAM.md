# Cursor Agent v1 transport provenance

This package is a minimal reimplementation of the Cursor Agent v1 transport,
maintained in enderzcx/cursor-agent2api and compiled into BeefAPI's CPA sidecar.
It intentionally does not copy or register a second gateway.

Behavioral references inspected for Packet 1:

- `kaitranntt/CLIProxyAPIPlus`, MIT, `internal/runtime/executor/cursor_executor.go`
  and `internal/auth/cursor/proto/`: Connect framing, five-second heartbeat,
  request/reply ordering, strict terminal errors, and fail-closed workspace exec.
- `openai/oh-my-pi`, MIT, Cursor provider: interaction-query replies and the
  rule that a live bidirectional run must not be half-closed before terminal.
- `enderzcx/beefapi-cursor2api`, AGPL-3.0: local protocol probes and the
  trailer-after-`turnEnded` regression. No AGPL source was copied into this
  package.

Packet 1 owns transport survival. Packet 2 owns the auth-scoped checkpoint and
parked caller-tool continuation state in this package, following the owner-token
and auth-bound checkpoint invariants from the MIT reference without copying its
gateway/conductor. Public protocol translation, channel registration, billing,
and account selection remain outside this package.

Packet 3 protocol fields were checked against the AGPL laboratory's generated
`agent.proto` field map: `McpToolDefinition`, `McpTools`, `AgentRunRequest`, and
`RequestContext`. Only the field layout was used; no generated or TypeScript
implementation was copied.

The Packet 3 executor reuses the vendored MIT CPA Chat/Messages/Responses
translator registry for public request and response projection. It does not
copy those translators or add Agent v1 to the vendored registry; session,
credential, Park/Resume, and Connect ownership stay in this package.

Typed protocol decode uses a MIT copy of `can1357/oh-my-pi`
`packages/ai/src/providers/cursor/proto/agent.proto` pinned at
`33cc6b9a043a74e00a157e72ca909272796d8461`, with that project's LICENSE
kept beside the proto. Generated Go lives in `gen/`. Encode helpers that
already passed Packet 1-3 tests stay handwritten; new decode of thinking,
toolCallStarted/delta/completed, hosted WebSearch/WebFetch/Exa, and
allowlist prechecks uses the generated types instead of expanding
protowire field maps.

The pristine pin keeps `TurnEndedUpdate` empty. The compile proto applies a
verified overlay for official CLI `2026.08.25-3e8eec8` generated fields 1-5
(`optional int64` input/output/cache_read/cache_write/reasoning tokens).
`proto/check.sh` enforces that overlay and regenerates `gen/agent.pb.go`.
No official implementation was copied.

Required caller-tool selection is conveyed through an always-apply global USER
`CursorRule` in `RequestContext.rules`, using the pinned schema and the OMP
rule-mapping reference. This supplements, not replaces, the existing caller
system `cloud_rule`: a live comparison did not prove that field ineffective.
The choice rule is omitted for auto/none, cold resume and after an observed
MCP invocation. Agent v1 has no hard `tool_choice` field; the actual MCP exec
and required-tool terminal checks remain authoritative. There is no text-to-tool
conversion, model alias, or retry. `TestAuthorizedRequiredToolRuleProbe` covers
a system-directed title request whose user text does not ask for a tool; it is
opt-in, one upstream call, and does not execute the caller tool.

The explicit type64 Sand profile is a separate finite server-streaming path at
`api2.cursor.sh/aiserver.v1.InferenceService/Stream`; it does not change the
Agent v1 endpoint or client type. The field layout for `InferenceStreamRequest`,
`InferenceRequestedModel`, messages, tools, tool results, stream parts and
extended usage was checked against the descriptors bundled with Cursor 3.18.9
in `cursor-always-local/dist/main.js`. No Cursor implementation code or token
material is copied into this package. `authorized_inference_stream_probe_test.go`
contains opt-in bounded live probes; normal tests use synthetic Connect frames.
