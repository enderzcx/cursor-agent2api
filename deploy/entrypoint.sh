#!/bin/sh
# First start: render /data/config.yaml from the template and seed the auth dir.
# Later starts: leave user edits alone (the host rewrites the file itself when it
# hashes the management key).
set -eu

DATA_DIR="${CA2A_DATA_DIR:-/data}"
CONFIG="$DATA_DIR/config.yaml"
AUTH_DIR="$DATA_DIR/auths"
STATE_DIR="$DATA_DIR/state"

mkdir -p "$AUTH_DIR" "$STATE_DIR"

random_key() {
  if command -v od >/dev/null 2>&1; then
    head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'
  else
    date +%s%N
  fi
}

if [ ! -f "$CONFIG" ]; then
  API_KEY="${CA2A_API_KEY:-$(random_key)}"
  MANAGEMENT_KEY="${CA2A_MANAGEMENT_KEY:-$(random_key)}"
  sed \
    -e "s|__API_KEY__|$API_KEY|" \
    -e "s|__MANAGEMENT_KEY__|$MANAGEMENT_KEY|" \
    -e "s|__DATA_DIR__|$DATA_DIR|g" \
    /app/config.template.yaml > "$CONFIG"
  echo "cursor-agent2api: wrote $CONFIG"
  echo "cursor-agent2api: API key        = $API_KEY"
  echo "cursor-agent2api: management key = $MANAGEMENT_KEY"
  echo "cursor-agent2api: drop Cursor credentials into $AUTH_DIR (see README)"
fi

if [ -n "${CA2A_CURSOR_API_KEY:-}" ] && [ ! -f "$AUTH_DIR/cursor-default.json" ]; then
  printf '{"type":"cursor-agent-v1","api_key":"%s"}\n' "$CA2A_CURSOR_API_KEY" > "$AUTH_DIR/cursor-default.json"
  chmod 600 "$AUTH_DIR/cursor-default.json"
  echo "cursor-agent2api: seeded $AUTH_DIR/cursor-default.json from CA2A_CURSOR_API_KEY"
fi

exec /app/CLIProxyAPI -config "$CONFIG" "$@"
