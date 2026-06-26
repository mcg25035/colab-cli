// Self-refreshing public proxy for ONE Colab port-forward (one instance + one port).
//
// It mints a Google access_token from a long-lived refresh_token, then mints a
// short-lived Colab runtime-proxy-token, caches both in isolate memory, injects
// the auth headers + rewrites Origin/Referer, and proxies HTTP/WS to the
// deterministic *.prod.colab.dev upstream. Because everything runs at the CF
// edge and the credential lives in the Worker, forwarding keeps working even
// when the machine that deployed it is asleep or offline — until the Colab
// instance is released (after which the upstream mint fails and we 502).
//
// Bindings (set by the deploy script):
//   vars:    ENDPOINT, PORT, CLIENT_ID, CLIENT_SECRET
//   secret:  REFRESH_TOKEN

let cache = { access: null, accessExp: 0, token: null, url: null, tokenExp: 0 };

async function getAccess(env) {
  const now = Date.now();
  if (cache.access && now < cache.accessExp - 60_000) return cache.access;
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: env.CLIENT_ID,
      client_secret: env.CLIENT_SECRET,
      refresh_token: env.REFRESH_TOKEN,
    }),
  });
  const j = await r.json();
  if (!j.access_token) throw new Error('access refresh failed: ' + JSON.stringify(j).slice(0, 200));
  cache.access = j.access_token;
  cache.accessExp = now + (j.expires_in || 3600) * 1000;
  return cache.access;
}

async function getProxy(env) {
  const now = Date.now();
  if (cache.token && now < cache.tokenExp - 300_000) return { token: cache.token, url: cache.url };
  const access = await getAccess(env);
  const u = new URL('https://colab.pa.googleapis.com/v1/runtime-proxy-token');
  u.searchParams.set('endpoint', env.ENDPOINT);
  u.searchParams.set('port', String(env.PORT));
  const r = await fetch(u, {
    headers: { Authorization: 'Bearer ' + access, Accept: 'application/json', 'X-Colab-Client-Agent': 'vscode' },
  });
  let body = await r.text();
  const XSSI = ")]}'\n";
  if (body.startsWith(XSSI)) body = body.slice(XSSI.length);
  if (r.status !== 200) throw new Error('proxy mint failed: ' + r.status + ' ' + body.slice(0, 200));
  const j = JSON.parse(body);
  const ttl = parseInt(String(j.tokenTtl), 10) || 3600;
  cache.token = j.token;
  cache.url = j.url;
  cache.tokenExp = now + ttl * 1000;
  return { token: j.token, url: j.url };
}

// Unified page shown when the forward can't reach the Colab service — most
// commonly because the runtime has been released (the proxy-token mint then
// fails), but also when nothing is listening on the port yet.
function unavailablePage(env, kind, detail) {
  const released = kind === 'auth';
  const title = released ? 'Runtime no longer available' : 'Service unreachable';
  const msg = released
    ? 'The Colab runtime backing this forward has been released (or its credentials expired). This public URL is no longer active.'
    : `No service is responding on port ${env.PORT} inside the Colab runtime. Make sure your server is listening on that port.`;
  const html = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title} · colab-worker-forward</title>
<style>
  :root{color-scheme:light dark}
  body{margin:0;min-height:100vh;display:grid;place-items:center;
       font:16px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
       background:#0b0e14;color:#e6e6e6}
  .card{max-width:30rem;padding:2.5rem 2rem;text-align:center}
  .badge{font-size:3rem;line-height:1}
  h1{font-size:1.3rem;margin:.8rem 0 .4rem}
  p{color:#9aa4b2;margin:.4rem 0}
  code{background:#1b212d;padding:.15rem .4rem;border-radius:.3rem;font-size:.85em}
  .meta{margin-top:1.4rem;font-size:.8rem;color:#5b6675}
</style></head><body><div class="card">
  <div class="badge">${released ? '🌙' : '🔌'}</div>
  <h1>${title}</h1>
  <p>${msg}</p>
  <p class="meta">colab-worker-forward · port <code>${env.PORT}</code></p>
</div></body></html>`;
  return new Response(html, {
    status: released ? 503 : 502,
    headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' },
  });
}

export default {
  async fetch(request, env) {
    let proxy;
    try {
      proxy = await getProxy(env);
    } catch (e) {
      return unavailablePage(env, 'auth', e.message);
    }
    const upstream = new URL(proxy.url);
    const reqUrl = new URL(request.url);
    upstream.pathname = reqUrl.pathname;
    upstream.search = reqUrl.search;
    const origin = upstream.origin;

    const headers = new Headers(request.headers);
    headers.delete('host'); // let fetch derive Host from the upstream URL
    headers.set('X-Colab-Runtime-Proxy-Token', proxy.token);
    headers.set('X-Colab-Client-Agent', 'vscode');
    // Colab's edge rejects mismatched Origin with 404, so rewrite it to the upstream.
    if (headers.has('origin')) headers.set('origin', origin);
    if (headers.has('referer')) {
      try {
        const rf = new URL(headers.get('referer'));
        rf.protocol = upstream.protocol;
        rf.host = upstream.host;
        headers.set('referer', rf.toString());
      } catch { /* keep original */ }
    }

    // WebSocket upgrade: pass through (best-effort).
    if ((request.headers.get('Upgrade') || '').toLowerCase() === 'websocket') {
      return fetch(upstream.toString(), { method: request.method, headers, body: request.body });
    }

    const method = request.method;
    let resp;
    try {
      resp = await fetch(upstream.toString(), {
        method,
        headers,
        body: method === 'GET' || method === 'HEAD' ? undefined : request.body,
        redirect: 'manual',
      });
    } catch (e) {
      return unavailablePage(env, 'upstream', e.message);
    }
    // The upstream echoes the Colab origin in CORS; rewrite it back to the public URL.
    const respHeaders = new Headers(resp.headers);
    const acao = respHeaders.get('access-control-allow-origin');
    if (acao && acao.includes(origin)) respHeaders.set('access-control-allow-origin', reqUrl.origin);
    return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: respHeaders });
  },
};
