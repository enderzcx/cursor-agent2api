# Sand Claude compatibility refresh

Base: a78dbba (v0.2.1). Scope: independent cursor-agent2api plugin, not BeefAPI
deployment and not the Cursor desktop model-picker patch.

Existing direct HTTP/2 inference transport, OAuth identity validation and ordinary
Agent v1 default are retained. No Box provisioning or production channel changes.

Changes:
- Original-protocol reasoning effort survives translation into Sand parameters.
- All eight registered Claude models accept explicit catalog-backed context and effort.
  Opus/Sonnet 4.6 use 200k/1m without xhigh; the other six use 300k/1m and support xhigh.
- Explicit thinking disabled remains false, including Sonnet 5's legacy thinking-on selector.
- Sand full-history request identity includes thinking, effort and context, so
  changed generation parameters cannot reuse a completed request under the old key.
- Opt-in executor, streaming and tool round-trip probes can use fable-1m-medium.

Deterministic checks: full repository Go suite; targeted race suite; protobuf
selector tests across Messages/Chat/Responses; invalid parameter checks; absent
parameter legacy behavior; history identity separation.

2026-09-19 live executor acceptance (existing owner-authorized credential, memory-only):
- TestAuthorizedSandExecutorProbe PASS (5.78s), marker and usage receipt.
- TestAuthorizedSandExecutorToolProbe PASS (13.79s), tool + full-history result and receipts.
- TestAuthorizedSandStreamingParameters PASS (4.52s), streamed marker + message_stop.
All used claude-fable-5-1, cursor_context=1m, thinking enabled, effort=medium.
No production channel or credential records were modified.

Bounded lead structural review: parameter translation remains in the Sand adapter;
no Box lifecycle, account loader, deployment code, dependency or second transport
was introduced. Three generation knobs join the existing history identity.
This is local verification, not independent pre-release approval.

Limits: API-key credential import remains required. Desktop Heavy token-only probes
were rejected before inference by the existing identity check; no bypass was added.
The later owner-authorized channel 387 API-key run closes that Heavy credential gap:
all eight models passed text, streamed marker/terminal, and caller tool/result probes
at 1m + medium + thinking enabled (32 inference requests total).

| Model | Text | Tool/result | Streaming |
| --- | --- | --- | --- |
| claude-fable-5-1 | PASS 15.79s | PASS 9.26s | PASS 4.78s |
| claude-fable-5 | PASS 4.04s | PASS 7.58s | PASS 4.73s |
| claude-opus-5 | PASS 4.05s | PASS 9.95s | PASS 2.89s |
| claude-opus-4-8 | PASS 2.95s | PASS 9.42s | PASS 2.42s |
| claude-opus-4-7 | PASS 2.61s | PASS 4.72s | PASS 2.28s |
| claude-opus-4-6 | PASS 3.51s | PASS 6.32s | PASS 2.56s |
| claude-sonnet-5 | PASS 3.06s | PASS 7.08s | PASS 2.57s |
| claude-sonnet-4-6 | PASS 2.83s | PASS 6.07s | PASS 2.64s |

Channel 387 was read-only; credential values remained in child-process memory and
environment, with no auth-file or Keychain writes. These are local standalone
executor tests using production credentials, not traffic through production channel 387.
The later Fable thinking-default change does not change this explicit-thinking wire selector.

Native Claude Code behavior references (checked 2026-09-19):
- https://code.claude.com/docs/en/settings-reference#alwaysthinkingenabled:
  thinking defaults on in Claude Code. This is a client default, not a reason for
  the gateway to overwrite every caller's explicit controls.
- https://code.claude.com/docs/en/model-config#extended-thinking:
  Fable cannot disable thinking. The adapter sets Fable thinking true when omitted
  and rejects explicit disabled, rather than silently selecting a no-thinking variant.
  Other Claude models preserve explicit off; enabled/adaptive are mapped to Cursor on.
  Cursor's boolean/effort controls cannot guarantee exact Anthropic adaptive budgets.

This is not a million-token payload stress test. Live executor evidence does not
replace a packaged CLIProxyAPI host/API smoke or a published release receipt.

Rollback: revert this bounded change and restart the host. Existing auth files
need no migration. Omit new parameters to use the previous Sand selector defaults.
