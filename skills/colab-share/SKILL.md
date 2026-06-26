---
name: colab-share
description: >-
  Publish a service running inside a private Google Colab runtime as a PUBLIC
  demo URL that anyone can open in a browser — turning something only reachable
  inside Colab into a shareable link. Use this whenever the user wants to share
  a Colab-hosted demo / web UI / API / TensorBoard / Gradio / Streamlit / dev
  server with others, get a public URL for something running on a Colab runtime,
  make a Colab port publicly reachable, or "publish" / "share" / "expose" a
  Colab service to the internet — especially when the public link must keep
  working after the local machine sleeps or shuts down. Implemented under the
  hood with a self-refreshing Cloudflare Worker; triggers even if the user says
  "public colab url", "cloudflare worker for colab", or just "let people outside
  access my colab server" without naming this skill.
---

# Colab Share

Publish a service running inside a private Google Colab runtime as a public demo
URL anyone can open — without the user's machine needing to stay online. Under
the hood it fronts the runtime with a self-refreshing Cloudflare Worker.

## Why this works (read once, it explains the whole design)

Colab's backend serves each `(runtime, port)` at a deterministic, auth-gated URL
`https://<port>-<endpoint>-b.<region>.prod.colab.dev`. Hitting it directly fails:
the edge requires a short-lived `X-Colab-Runtime-Proxy-Token` header (plus
`X-Colab-Client-Agent: vscode`) and rejects mismatched `Origin`.

The Worker is a stable public front that holds the credential and mints/refreshes
that token **at the Cloudflare edge**, then injects the headers and rewrites
`Origin`/`Referer` before proxying. Because the credential lives in the Worker
(not on the user's machine), forwarding keeps working when the laptop is asleep —
until the Colab instance is released, at which point the mint fails and the Worker
serves a friendly "runtime no longer available" page.

Credential chain the Worker runs autonomously:
`refresh_token` → `access_token` (oauth2.googleapis.com) → `runtime-proxy-token`
(colab.pa.googleapis.com, the response carries both the token AND the upstream
url, so the url need not be known ahead of time).

**Model: one Worker = one instance + one port.** When the instance is released,
that Worker is dead weight (it shows the unavailable page); delete it, and create
a fresh one for any new instance/port.

## Prerequisites — check these first

1. **A Cloudflare account (free plan works), with wrangler logged in.** Verify
   `npx wrangler whoami` shows `workers (write)`; otherwise run `npx wrangler
   login`. First deploy registers a free `*.workers.dev` subdomain (the
   `<account-subdomain>` in the URL).
2. **colab-cli is authenticated** and `~/.config/colab-cli/auth.json` exists
   (contains the `refreshToken` the Worker will reuse).
3. **A running Colab runtime** whose service listens on a **free** port.
   ⚠️ Port **8080 is already taken by Colab's internal node process** — the
   user's server must listen on a different free port (e.g. 7860, 8501, 9000).

## Operations

A bundled helper, `scripts/colab-wf.sh`, does the deterministic Cloudflare work.
It takes the `endpoint` explicitly so it stays independent of how colab-cli is
invoked in this environment. Resolve the endpoint yourself, then call it.

### Resolve the endpoint

Run colab-cli's runtime list and read the `endpoint` of the target runtime:

```bash
colab runtime list --json
```

Note on invocation: if a global `colab` fails (e.g. behind an HTTP proxy you'll
see `fetch failed`), run it from the colab-cli repo as
`node --use-env-proxy dist/index.js runtime list --json` (equivalently
`npm start -- runtime list --json`). Use whatever form works in this environment.

### Publish a port

```bash
scripts/colab-wf.sh publish <port> <endpoint>
```

This deterministically: creates a project under
`~/.config/colab-cli/worker-forward/<port>-<endpoint>-<timestamp>/`, writes
`wrangler.toml` (vars: `ENDPOINT`, `PORT`, `CLIENT_ID`, `CLIENT_SECRET`),
`wrangler deploy`s a Worker named `<port>-<endpoint>`, uploads the
`REFRESH_TOKEN` secret (read from `auth.json`), and prints the public URL:

```
https://<port>-<endpoint>.<account-subdomain>.workers.dev
```

Then verify (behind a proxy add `-x "$HTTPS_PROXY"`):

```bash
curl -sS -w '\n[%{http_code}]\n' "$URL/"
```

A `200` with the expected content means it works. A `503` with a "Runtime no
longer available" page means the mint failed (instance gone or auth issue); a
`502` "Service unreachable" page means nothing is listening on that port yet.

### List published forwards

```bash
scripts/colab-wf.sh list
```

Prints the locally-recorded forwards (name, port, endpoint, url, created). These
are local records under `worker-forward/`; a Worker may already be dead if its
instance was released.

### Unpublish (delete the Worker)

```bash
scripts/colab-wf.sh rm <port> <endpoint>
```

Deletes the Cloudflare Worker and removes the local project records.

## Important caveats — surface these to the user

- **Security / blast radius**: the Worker reuses the local colab-cli
  `refresh_token`, which carries the full `colaboratory` OAuth scope (it CANNOT
  touch Drive — that's a separate OAuth client — but it CAN operate any of the
  user's Colab runtimes). It sits as a Cloudflare secret (not public), but anyone
  who obtains that secret gains full Colab control. **Only use this for test
  instances with no sensitive data.** There is no built-in public access control;
  anyone with the URL can reach the service.
- **One Worker per instance+port**; releasing the instance does not auto-delete
  the Worker — it just starts serving the unavailable page. Clean up with `rm`.
- **WebSocket** is proxied best-effort (passthrough). Plain HTTP is fully tested.

## Customization knobs (env vars, rarely needed)

- `COLAB_WF_HOME` — where Worker projects are stored (default
  `~/.config/colab-cli/worker-forward`).
- `COLAB_AUTH_FILE` — path to colab-cli auth.json.
- `COLAB_CLIENT_ID` / `COLAB_CLIENT_SECRET` — override the OAuth client.
- `COLAB_WF_COMPAT_DATE` — wrangler `compatibility_date`.

The Worker source is `assets/worker.js` (copied into each project at publish
time). Edit it there to change proxy behavior or the unavailable page.
