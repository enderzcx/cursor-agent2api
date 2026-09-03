# Cursor Agent v1 protobuf contract

Typed `agent.v1` contract vendored from the MIT `can1357/oh-my-pi` Cursor
provider. This copy is used by the `cursoragentv1` library for Agent v1 decode
and hosted-search projection. It is not the official type-62 `sdk.v1` bridge.

| Field | Value |
|---|---|
| Source | `packages/ai/src/providers/cursor/proto/agent.proto` |
| Pin | `33cc6b9a043a74e00a157e72ca909272796d8461` |
| Pristine copy | `upstream/agent.proto` |
| License | MIT (`LICENSE` in this directory) |

The CI proto gate is non-skippable. It compares the adapted `agent.proto` to
the pristine pin after identifier-only flattening plus one verified overlay:
`TurnEndedUpdate` fields 1-5 as `optional int64` from Cursor Agent CLI
`2026.08.25-3e8eec8` generated `agent.v1.TurnEndedUpdate` (`T=3`, presence
optional). It then regenerates `../gen/agent.pb.go` with `protoc-gen-go v1.34.1`.
Missing tools fail the check. It does not use `/tmp` or the network. Do not
edit the pristine pin to look like the overlay.

Source verification for that overlay used the installed CLI's generated
descriptor only: `index.js` SHA256
`27219ec48f229eae15091d7facb511a80c2c995027bf6115e16873916286b83f`.
Its five fields are optional signed int64. This verifies wire shape, not
per-HTTP billing scope. Synthetic fixture bytes are explicitly identified in
`../testdata/usage-observation/README.md`.

```bash
export PATH="$(go env GOPATH)/bin:/opt/homebrew/bin:$PATH"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.1
bash cursoragentv1/proto/check.sh
```

Regenerate the Go bindings from the repository root:

```bash
export PATH="$(go env GOPATH)/bin:/opt/homebrew/bin:$PATH"
protoc --go_out=cursoragentv1/gen --go_opt=paths=source_relative -I cursoragentv1/proto cursoragentv1/proto/agent.proto
```

Go identifier adaptation: protobuf-go oneof wrappers use `Parent_Field`
names. The vendored file flattens colliding top-level `Parent_Field`
message identifiers by removing underscores so the generated package
compiles. The only additional semantic adaptation is the verified
`TurnEndedUpdate` overlay above. Do not copy proprietary CLI implementation.

Do not hand-edit `../gen/agent.pb.go`. Do not copy AGPL Cursor gateway source
into this tree.

Live Cursor Agent CLI `2026.08.25-3e8eec8` publishes
`InteractionUpdate.feedback_request = 21` as a UI survey
(`FeedbackRequestUpdate`). Decode treats a length-delimited field 21 with a
structurally valid payload as advisory and ignores it, matching the CLI
observer. Unknown exec/query variants retain their existing checks. A lone
unknown interaction field 42 is rejected, while the existing decoder can
ignore leftover fields beside a recognized update; its semantics have not
been established. Feedback handling does not modify the pristine pin; the
generated Go changes above are solely the TurnEnded overlay.
