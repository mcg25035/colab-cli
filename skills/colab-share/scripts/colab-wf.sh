#!/usr/bin/env bash
# colab-wf.sh — deploy / list / remove Cloudflare Workers that expose a single
# Colab port-forward to the public internet. See ../SKILL.md for the full flow.
#
# Usage:
#   colab-wf.sh publish <port> <endpoint>   deploy a Worker for <port> on <endpoint>
#   colab-wf.sh list                        list locally-recorded Worker forwards
#   colab-wf.sh rm <port> <endpoint>        delete the Worker (and local record)
#
# Endpoint is resolved by the caller (e.g. `colab runtime list --json`) and
# passed in explicitly, so this script stays decoupled from how colab-cli is
# invoked in a given environment.
set -euo pipefail

# Public installed-app credentials from colab-cli's config (overridable).
CLIENT_ID="${COLAB_CLIENT_ID:-1014160490159-cvot3bea7tgkp72a4m29h20d9ddo6bne.apps.googleusercontent.com}"
CLIENT_SECRET="${COLAB_CLIENT_SECRET:-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}"
AUTH_FILE="${COLAB_AUTH_FILE:-$HOME/.config/colab-cli/auth.json}"
WF_HOME="${COLAB_WF_HOME:-$HOME/.config/colab-cli/worker-forward}"
COMPAT_DATE="${COLAB_WF_COMPAT_DATE:-2025-06-01}"
TEMPLATE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/assets/worker.js"

die() { echo "error: $*" >&2; exit 1; }

worker_name() { # <port> <endpoint>  ->  validated worker name
  local name="${1}-${2}"
  name="$(echo "$name" | tr '[:upper:]' '[:lower:]')"
  [[ "$name" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "derived worker name '$name' is not a valid Worker name"
  [[ ${#name} -le 63 ]] || die "derived worker name '$name' exceeds 63 chars"
  echo "$name"
}

read_refresh_token() {
  [[ -f "$AUTH_FILE" ]] || die "auth file not found: $AUTH_FILE (run colab auth login)"
  AF="$AUTH_FILE" node -e "process.stdout.write(JSON.parse(require('fs').readFileSync(process.env.AF,'utf8')).refreshToken||'')" 2>/dev/null || true
}

cmd_publish() {
  local port="${1:?usage: publish <port> <endpoint>}" endpoint="${2:?usage: publish <port> <endpoint>}"
  [[ "$port" =~ ^[0-9]+$ ]] && (( port >= 1 && port <= 65535 )) || die "invalid port: $port"
  local name; name="$(worker_name "$port" "$endpoint")"
  local ts proj; ts="$(date +%Y%m%d-%H%M%S)"; proj="$WF_HOME/${name}-${ts}"
  mkdir -p "$proj"
  cp "$TEMPLATE" "$proj/worker.js"

  # Keep-alive deadline: a cron pings the runtime's keep-alive endpoint until
  # this absolute instant, then stops (no-op) so an idle runtime is reclaimed
  # instead of burning Colab quota forever. Default 8h; override with
  # COLAB_WF_KEEPALIVE_HOURS (0 disables keep-alive — deadline in the past).
  local hours="${COLAB_WF_KEEPALIVE_HOURS:-8}"
  [[ "$hours" =~ ^[0-9]+$ ]] || die "invalid COLAB_WF_KEEPALIVE_HOURS: $hours (integer hours)"
  local keepalive_until
  keepalive_until="$(node -e "process.stdout.write(new Date(Date.now()+${hours}*3600e3).toISOString())")"

  cat > "$proj/wrangler.toml" <<EOF
name = "${name}"
main = "worker.js"
compatibility_date = "${COMPAT_DATE}"

[triggers]
crons = ["*/5 * * * *"]

[vars]
ENDPOINT = "${endpoint}"
PORT = "${port}"
CLIENT_ID = "${CLIENT_ID}"
CLIENT_SECRET = "${CLIENT_SECRET}"
KEEPALIVE_UNTIL = "${keepalive_until}"
EOF
  echo ">> keep-alive: cron */5min until ${keepalive_until} (${hours}h)" >&2

  echo ">> deploying Worker '${name}' ..." >&2
  local out; out="$(cd "$proj" && npx --yes wrangler deploy 2>&1)" || { echo "$out" >&2; die "wrangler deploy failed"; }
  # Non-fatal: under `set -euo pipefail` a no-match grep would abort the script
  # here — i.e. AFTER the Worker is deployed but BEFORE the REFRESH_TOKEN secret
  # is uploaded, leaving a live-but-broken Worker that 503s forever. Tolerate an
  # empty URL (e.g. custom-domain-only deploys, or a changed wrangler output).
  local url; url="$(echo "$out" | grep -oE 'https://[a-z0-9.-]+workers\.dev' | head -1 || true)"

  echo ">> uploading REFRESH_TOKEN secret ..." >&2
  local rt; rt="$(read_refresh_token)"
  [[ -n "$rt" ]] || die "could not read refreshToken from $AUTH_FILE"
  printf '%s' "$rt" | (cd "$proj" && npx --yes wrangler secret put REFRESH_TOKEN >/dev/null 2>&1) \
    || die "wrangler secret put failed"

  cat > "$proj/meta.json" <<EOF
{"name":"${name}","port":${port},"endpoint":"${endpoint}","url":"${url}","created":"${ts}"}
EOF
  echo ">> done." >&2
  echo "$url"
}

cmd_list() {
  shopt -s nullglob
  local found=0
  for m in "$WF_HOME"/*/meta.json; do
    found=1
    node -e "const d=JSON.parse(require('fs').readFileSync(process.argv[1],'utf8'));console.log([d.name,d.port,d.endpoint,d.url,d.created].join('\t'))" "$m"
  done
  [[ $found -eq 1 ]] || echo "(no port-forward Workers recorded under $WF_HOME)"
}

cmd_rm() {
  local port="${1:?usage: rm <port> <endpoint>}" endpoint="${2:?usage: rm <port> <endpoint>}"
  local name; name="$(worker_name "$port" "$endpoint")"
  echo ">> deleting Worker '${name}' ..." >&2
  npx --yes wrangler delete --name "$name" 2>&1 | tail -3 >&2 || true
  shopt -s nullglob
  for d in "$WF_HOME/${name}-"*; do rm -rf "$d"; done
  echo ">> removed local records for '${name}'." >&2
}

main() {
  local sub="${1:-}"; shift || true
  case "$sub" in
    publish) cmd_publish "$@";;
    list)    cmd_list "$@";;
    rm)      cmd_rm "$@";;
    *) die "usage: colab-wf.sh {publish <port> <endpoint> | list | rm <port> <endpoint>}";;
  esac
}
main "$@"
