#!/usr/bin/env bash
# Verify the vendored Cursor Agent v1 proto against its MIT pin and generated Go.
# This gate is non-skippable: missing protoc/protoc-gen-go v1.34.1 is a failure.
set -euo pipefail

PIN="33cc6b9a043a74e00a157e72ca909272796d8461"
PROTOC_GEN_GO_VERSION="v1.34.1"
HERE="$(cd "$(dirname "$0")" && pwd)"
VENDORED="$HERE/agent.proto"
UPSTREAM="$HERE/upstream/agent.proto"
UPSTREAM_PIN="$HERE/upstream/PIN"
GENERATED="$HERE/../gen/agent.pb.go"
LICENSE="$HERE/LICENSE"

die() { echo "cursor-agent-v1 proto check: $*" >&2; exit 1; }

test -f "$VENDORED" || die "missing $VENDORED"
test -f "$UPSTREAM" || die "missing pristine pin source $UPSTREAM"
test -f "$UPSTREAM_PIN" || die "missing $UPSTREAM_PIN"
test -f "$GENERATED" || die "missing $GENERATED"
test -f "$LICENSE" || die "missing MIT LICENSE"
grep -q "Pin: $PIN" "$VENDORED" || die "vendored proto does not record pin $PIN"
grep -q "MIT License" "$LICENSE" || die "LICENSE is not MIT"
got_pin="$(tr -d '[:space:]' < "$UPSTREAM_PIN")"
[[ "$got_pin" == "$PIN" ]] || die "upstream PIN $got_pin != $PIN"

# Official Cursor Agent CLI 2026.08.25-3e8eec8 generated descriptor:
# agent.v1.TurnEndedUpdate fields 1-5 are optional INT64 (ScalarType T=3).
# This overlay is the only allowed semantic drift from the MIT pin.

flatten_proto() {
  python3 - "$1" "$2" <<'PY'
import re, sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
names = re.findall(r'^message ([A-Za-z0-9]+_[A-Za-z0-9_]+) \{', text, re.M)
for name in sorted(set(names), key=len, reverse=True):
    text = re.sub(r'\b' + re.escape(name) + r'\b', name.replace('_', ''), text)
Path(sys.argv[2]).write_text(text)
PY
}

apply_verified_turn_ended_overlay() {
  python3 - "$1" "$2" <<'PY'
import sys
from pathlib import Path
src, dst = Path(sys.argv[1]), Path(sys.argv[2])
text = src.read_text()
empty = """message TurnEndedUpdate {
}"""
overlay = """message TurnEndedUpdate {
   optional int64 input_tokens = 1;
   optional int64 output_tokens = 2;
   optional int64 cache_read_tokens = 3;
   optional int64 cache_write_tokens = 4;
   optional int64 reasoning_tokens = 5;
}"""
count = text.count(empty)
if count != 1:
    raise SystemExit(f"expected exactly one empty TurnEndedUpdate in pin, found {count}")
if "optional int64 input_tokens = 1;" in text:
    raise SystemExit("pristine pin already contains TurnEndedUpdate overlay fields")
dst.write_text(text.replace(empty, overlay, 1))
PY
}

normalize_contract() {
  python3 - "$1" "$2" <<'PY'
import re, sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
text = re.sub(r'(?m)^//.*\n', '', text)
text = re.sub(r'option go_package = "[^"]*";\n', '', text)
text = re.sub(r'\n{3,}', '\n\n', text)
Path(sys.argv[2]).write_text(text.strip() + '\n')
PY
}

normalize_generated_go() {
  python3 - "$1" "$2" <<'PY'
import re, sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
text = re.sub(r'(?m)^// \tprotoc\s+v.*\n', '', text)
Path(sys.argv[2]).write_text(text)
PY
}

command -v protoc >/dev/null 2>&1 || die "protoc is required"
command -v protoc-gen-go >/dev/null 2>&1 || die "protoc-gen-go $PROTOC_GEN_GO_VERSION is required"
gen_ver="$(protoc-gen-go --version 2>/dev/null || true)"
[[ "$gen_ver" == "protoc-gen-go $PROTOC_GEN_GO_VERSION" ]] || die "protoc-gen-go version is '$gen_ver', want protoc-gen-go $PROTOC_GEN_GO_VERSION"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
flatten_proto "$UPSTREAM" "$tmpdir/pinned.raw"
apply_verified_turn_ended_overlay "$tmpdir/pinned.raw" "$tmpdir/pinned.overlaid"
normalize_contract "$tmpdir/pinned.overlaid" "$tmpdir/pinned.flatten"
normalize_contract "$VENDORED" "$tmpdir/vendored.norm"
if ! grep -q "optional int64 input_tokens = 1;" "$tmpdir/vendored.norm"; then
  die "adapted proto is missing verified TurnEndedUpdate overlay"
fi
if ! diff -u "$tmpdir/pinned.flatten" "$tmpdir/vendored.norm" >/dev/null; then
  diff -u "$tmpdir/pinned.flatten" "$tmpdir/vendored.norm" || true
  die "adapted proto drifted from pristine pin after identifier flattening and verified TurnEndedUpdate overlay"
fi

mkdir -p "$tmpdir/gen"
protoc --go_out="$tmpdir/gen" --go_opt=paths=source_relative -I "$HERE" "$VENDORED"
normalize_generated_go "$GENERATED" "$tmpdir/current.pb.go"
normalize_generated_go "$tmpdir/gen/agent.pb.go" "$tmpdir/regen.pb.go"
if ! diff -u "$tmpdir/current.pb.go" "$tmpdir/regen.pb.go" >/dev/null; then
  diff -u "$tmpdir/current.pb.go" "$tmpdir/regen.pb.go" | head -80
  die "generated Go bindings drifted from proto/agent.proto; regenerate with proto/README.md"
fi

echo "cursor-agent-v1 proto check: OK (pin $PIN, protoc-gen-go $PROTOC_GEN_GO_VERSION)"
