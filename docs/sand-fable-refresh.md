# Sand Fable compatibility refresh

Base: a78dbba (v0.2.1). Scope: independent cursor-agent2api plugin, not BeefAPI
deployment and not the Cursor desktop model-picker patch.

Existing direct HTTP/2 inference transport, OAuth identity validation and ordinary
Agent v1 default are retained. No Box provisioning or production channel changes.

Changes:
- Original-protocol reasoning effort survives translation into Sand parameters.
- Explicit Fable 5.1 cursor_context=1m/300k selects the matching Cursor context/Max mode.
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
This is not a million-token payload stress test. Live executor evidence does not
replace a packaged CLIProxyAPI host/API smoke or a published release receipt.

Rollback: revert this bounded change and restart the host. Existing auth files
need no migration. Omit new parameters to use the previous Sand selector defaults.
