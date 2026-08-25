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
 * The control plane (Colab REST via fetch) has NO implicit proxy support:
 * `--use-env-proxy` is Node >= 22 only and the shebang no longer carries it,
 * so daemons/CLI run without it (node-guard/lifecycle warn when a proxy env
 * var is set on Node < 22). See docs/DEVELOPMENT.md for the full
 * data-/control-plane split.
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
