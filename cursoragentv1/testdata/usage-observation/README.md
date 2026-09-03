# Agent v1 usage evidence fixtures

Hex-encoded `AgentServerMessage` frames. Field numbers match Cursor Agent CLI
`2026.08.25-3e8eec8` generated `agent.v1.TurnEndedUpdate` (optional int64 1-5)
and `TokenDeltaUpdate.tokens` (int32 field 1). These are synthetic bytes, not
live captures and not official implementation.

`live-20260830.json` separately records real, owner-authorized numeric terminal
payloads and observed delta sums. These are payload bytes, not full server
envelopes. No credentials or original prompts are retained. The samples show
that tokenDelta is not equal to terminal output and must not be described as
exact measured output. A decoder test passing does not clear that release
blocker or establish per-HTTP attribution.
