#!/usr/bin/env bash
# Run the host + plugin from a release archive or ./dist without Docker.
# Layout expected next to this script's parent (or overridden by env):
#   CLIProxyAPI (binary)  cursor-agent2api.<so|dylib>  config.template.yaml
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="${PLUGIN_DIR:-$HERE}"
DATA_DIR="${CA2A_DATA_DIR:-$HERE/data}"
CONFIG="$DATA_DIR/config.yaml"
BIN="$DIST_DIR/CLIProxyAPI"
TEMPLATE="$DIST_DIR/config.template.yaml"
[ -f "$TEMPLATE" ] || TEMPLATE="$HERE/config.yaml"

[ -x "$BIN" ] || { echo "CLIProxyAPI binary not found at $BIN" >&2; exit 1; }
mkdir -p "$DATA_DIR/auths" "$DATA_DIR/state"

random_key() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

if [ ! -f "$CONFIG" ]; then
  API_KEY="${CA2A_API_KEY:-$(random_key)}"
  MANAGEMENT_KEY="${CA2A_MANAGEMENT_KEY:-$(random_key)}"
  sed \
    -e "s|__API_KEY__|$API_KEY|" \
    -e "s|__MANAGEMENT_KEY__|$MANAGEMENT_KEY|" \
    -e "s|__DATA_DIR__|$DATA_DIR|g" \
    -e "s|/app/plugins|$DIST_DIR|" \
    "$TEMPLATE" > "$CONFIG"
  echo "cursor-agent2api: wrote $CONFIG"
  echo "cursor-agent2api: API key        = $API_KEY"
  echo "cursor-agent2api: management key = $MANAGEMENT_KEY"
fi

if [ -n "${CA2A_CURSOR_API_KEY:-}" ] && [ ! -f "$DATA_DIR/auths/cursor-default.json" ]; then
  printf '{"type":"cursor-agent-v1","api_key":"%s"}\n' "$CA2A_CURSOR_API_KEY" > "$DATA_DIR/auths/cursor-default.json"
  chmod 600 "$DATA_DIR/auths/cursor-default.json"
fi

exec "$BIN" -config "$CONFIG" "$@"
