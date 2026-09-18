# cursor-agent2api

Use your Cursor subscription as an OpenAI / Anthropic compatible API.

`cursor-agent2api` speaks Cursor's private **Agent v1** Connect transport directly (no `cursor-agent` CLI subprocess, no browser automation) and ships as a [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) provider plugin. You get CLIProxyAPI's account pool, key management, usage stats and web control panel for free; this repository adds the Cursor provider.

> This is **not** the official `@cursor/sdk` gateway. That is a separate project (`cursor-sdk2api`). Do not mix credentials or state between the two.

## What you get

- `/v1/messages` (Anthropic), `/v1/chat/completions` (OpenAI Chat), `/v1/responses` (OpenAI Responses) served by the CLIProxyAPI host.
- Streaming, real `tool_use` / `function_call` round-trips, thinking/reasoning blocks, media in history, hosted web search projection.
- Multiple Cursor accounts in one pool with round-robin and cooldown, refreshed automatically.
- Web control panel at `http://<host>:8317/management.html` (accounts, usage, plugins, live API test).
- Models: `claude-fable-5-1`, `claude-fable-5`, `claude-opus-5`, `claude-opus-4-6`, `claude-opus-4-7`, `claude-opus-4-8`, `claude-sonnet-4-6`, `claude-sonnet-5`, `composer-2.5-fast`, `glm-5.2`, `kimi-k3`. Credentials with `"runtime_profile": "sand"` additionally expose `grok-4.6`.

## Quick start (Docker)

```bash
git clone https://github.com/enderzcx/cursor-agent2api.git
cd cursor-agent2api
CA2A_CURSOR_API_KEY=key_xxx docker compose up -d
docker compose logs -f   # prints the generated API key and management key on first start
```

Then:

```bash
curl http://127.0.0.1:8317/v1/messages \
  -H "x-api-key: <API key from the logs>" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"claude-sonnet-4-6","max_tokens":256,"messages":[{"role":"user","content":"hello"}]}'
```

Open `http://127.0.0.1:8317/management.html` with the management key to add more accounts, watch usage, or test requests from the browser.

Everything persistent lives in `./data`:

| Path | Purpose |
|---|---|
| `data/config.yaml` | CLIProxyAPI config, rendered once from `deploy/config.yaml`. Edit freely. |
| `data/auths/*.json` | One file per Cursor credential. |
| `data/state/` | Agent v1 conversation checkpoints and parked tool calls. |

Environment variables (all optional): `CA2A_API_KEY`, `CA2A_MANAGEMENT_KEY` (generated when unset), `CA2A_CURSOR_API_KEY` (seeds one credential file).

## Quick start (no Docker)

Download the bundle for your platform from [Releases](https://github.com/enderzcx/cursor-agent2api/releases) (`linux-amd64`, `linux-arm64`, `darwin-arm64`), then:

```bash
tar xzf cursor-agent2api-*.tar.gz && cd cursor-agent2api-*
CA2A_CURSOR_API_KEY=key_xxx ./run.sh
```

The bundle contains the matching `CLIProxyAPI` host binary, the plugin library, and a config template. `run.sh` creates `./data` next to it, the same layout as Docker.

## Adding Cursor accounts

Drop a JSON file into the auth directory (`data/auths/` by default). The host picks it up without a restart.

```json
{
  "type": "cursor-agent-v1",
  "api_key": "key_...",
  "email": "optional label",
  "runtime_profile": "agent_v1"
}
```

- `api_key` is the only required field. On first use the plugin exchanges it for an Agent v1 access token, records the account identity, and the host persists the refreshed file. Tokens are refreshed every 30 minutes.
- `runtime_profile`: `agent_v1` (default) or `sand`. Sand uses HTTP/2 directly at `https://api2.cursor.sh/aiserver.v1.InferenceService/Stream`, not a Box gateway or desktop patch. Account authorization and quota still apply; keep separate credential files if you want both catalogs.
- Optional per-account `proxy_url`, `disabled`, and other CLIProxyAPI auth-file fields work as documented upstream.

The control panel's Auth Files page shows each account's status (`active`, `unauthorized`, cooldown) and lets you upload/delete/disable files.

### Fable 5.1: explicit 1M context and Medium effort (Sand)

Select `runtime_profile: "sand"` in the account file. Existing accounts are not
automatically switched. Then send, for example, to `/v1/messages`:

```json
{
  "model": "claude-fable-5-1",
  "cursor_context": "1m",
  "thinking": {"type": "enabled", "budget_tokens": 1024},
  "output_config": {"effort": "medium"},
  "max_tokens": 2048,
  "stream": true,
  "messages": [{"role": "user", "content": "Reply exactly FABLE_OK."}]
}
```

`cursor_context` is this plugin's optional request extension for every Claude
model currently registered by this project. Omit it to retain the previous upstream selector.

| Models | Context values | Effort values |
| --- | --- | --- |
| Fable 5.1, Fable 5, Opus 5, Opus 4.8, Opus 4.7, Sonnet 5 | `300k`, `1m` | `low`, `medium`, `high`, `xhigh`, `max` |
| Opus 4.6, Sonnet 4.6 | `200k`, `1m` | `low`, `medium`, `high`, `max` |

The table follows the Cursor catalog observed on 2026-09-19, not a guarantee of
future upstream availability. Unsupported Claude combinations return an error.
`enabled` or `adaptive` enables Cursor thinking; this
maps the enablement/effort contract, not an exact Anthropic token-budget guarantee.
Fable thinking is always enabled, matching Claude Code; explicit `disabled` on
Fable returns 400 instead of silently downgrading. On the other models explicit
`thinking.type=disabled` remains false; omitted thinking retains prior behavior.
The gateway does not force Medium on all users: Claude Code's requested effort wins.
The 1M
selector sets Cursor Max mode; it does not confer account entitlement or prove
that a million-token payload will be accepted. No model substitution occurs.

Sand reads effort from the original caller protocol: Messages
`output_config.effort`, Chat `reasoning_effort`, or Responses `reasoning.effort`.
This update forwards explicit Claude/Grok effort. For the registered Grok 4.6
model, `max` is a compatibility alias for Cursor's highest tier, `xhigh`; it does
not select a different model. Other unsupported explicit values return an error.
Send the same parameters on
tool-result continuations. Ordinary `agent_v1` behavior is unchanged.

Only an API key is sufficient for normal account import; a copied desktop access
token alone is not a replacement for the API-key/account identity check. Do not
copy macOS Keychain contents into this project. See [verification](docs/sand-fable-refresh.md)
for the exact acceptance scope and remaining release gates. All eight registered
Claude models use this same parameter path; the Fable request above is an example.

## Build from source

Requirements: Go 1.26+, a C toolchain (the plugin is a `c-shared` library).

```bash
make dist          # dist/CLIProxyAPI + dist/cursor-agent2api.{so,dylib} + config template
make run           # start locally using ./data
make test          # unit tests
make proto         # proto drift gate (needs protoc + protoc-gen-go v1.34.1)
make docker        # local image
```

The plugin id is taken from the library file name; keep it `cursor-agent2api.<ext>`. Point an existing CLIProxyAPI at it with:

```yaml
plugins:
  enabled: true
  dir: "/path/to/plugins"
  configs:
    cursor-agent2api:
      enabled: true
      state_dir: /var/lib/cursor-agent2api
```

## Use as a Go library

```go
import "github.com/enderzcx/cursor-agent2api/cursoragentv1"

exec, err := cursoragentv1.NewExecutor(absoluteStateDir)
```

`cursoragentv1` implements CLIProxyAPI's `ProviderExecutor` (Execute, ExecuteStream, CountTokens, Refresh) and owns Connect framing, heartbeats, Park/Resume of caller tool calls, full-history rebuilds, usage reconciliation and state persistence. Embedders compile it in directly; they do not need the dynamic plugin.

## Repository layout

| Path | What |
|---|---|
| `cursoragentv1/` | The Go library: transport, protocol, translation, projection, state. `proto/` holds the pinned MIT `agent.proto`; `gen/` the generated bindings. |
| `internal/plugin/` | CLIProxyAPI plugin adapter (auth parsing, models, refresh, execute). |
| `cmd/cursor-agent2api/` | C ABI entry point for the dynamic library. |
| `internal/jsonx`, `internal/helps` | Small JSON and tokenizer helpers so the library only depends on CLIProxyAPI's public `sdk/` packages. |
| `deploy/` | Config template, Docker entrypoint, local runner. |

## Verify

```bash
go test ./cursoragentv1/... ./internal/...
bash cursoragentv1/proto/check.sh
```

Live probes (`cursoragentv1/*probe*_test.go`) compile but skip unless their environment flags provide real credentials.

## License

MIT. See `NOTICE` for third-party attributions (CLIProxyAPI helpers, the `can1357/oh-my-pi` proto pin).
