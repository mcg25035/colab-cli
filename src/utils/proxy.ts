import { HttpsProxyAgent } from 'https-proxy-agent';
import { getProxyForUrl } from 'proxy-from-env';

/**
 * Build an HTTPS proxy agent for the data plane (the `http-proxy` reverse proxy
 * and the WebSocket/transfer clients that talk to `*.prod.colab.dev`).
 *
 * With `targetUrl`, the proxy-vs-direct decision is delegated to
 * `proxy-from-env` so NO_PROXY is honored with standard semantics. `ws`/`wss`
 * is normalized to `http`/`https` first because `getProxyForUrl` picks the
 * proxy by scheme and WebSockets tunnel over HTTP(S). Without `targetUrl`,
 * falls back to the legacy env lookup (always proxy when one is configured).
 *
 * The control plane uses `fetch` via undici's `--use-env-proxy` and does not go
 * through here. See docs/DEVELOPMENT.md for the full data-/control-plane split.
 */
export function getProxyAgent(targetUrl?: string): HttpsProxyAgent<string> | undefined {
  let proxyUrl: string;
  if (targetUrl) {
    const normalized = targetUrl.replace(/^ws:/i, 'http:').replace(/^wss:/i, 'https:');
    proxyUrl = getProxyForUrl(normalized); // '' when the target should be reached directly
  } else {
    proxyUrl =
      process.env.HTTPS_PROXY ||
      process.env.https_proxy ||
      process.env.HTTP_PROXY ||
      process.env.http_proxy ||
      '';
  }
  if (!proxyUrl) return undefined;
  return new HttpsProxyAgent(proxyUrl);
}
