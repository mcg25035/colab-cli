// Node version floor. Imported FIRST from the entrypoint so it runs before
// any other module body — an old runtime gets a readable message instead of
// a deep stack trace.
//
// >= 20: fully supported.
// < 20 : refuse to start (googleapis alone requires 18 and the daemon/shell
//        stack assumes modern fetch/undici semantics; we do not validate
//        anything below 20).
const major = Number(process.versions.node.split('.')[0]);
if (!Number.isFinite(major) || major < 20) {
  console.error(
    `colab-cli requires Node.js >= 20 (running on ${process.version}).\n` +
      `Please upgrade: https://nodejs.org/  (or \`nvm install 20\`)`,
  );
  process.exit(1);
}

// 20/21 work, but the control-plane flag `--use-env-proxy` is Node >= 22.
// Degrade gracefully: tell the user once that env-proxy proxying for Colab
// REST calls is off; data-plane proxying (jupyter WS / transfers) still
// honors HTTP(S)_PROXY via https-proxy-agent regardless.
if (major < 22 && (process.env.HTTPS_PROXY || process.env.https_proxy || process.env.HTTP_PROXY || process.env.http_proxy)) {
  console.error(
    `[node-guard] ${process.version}: '--use-env-proxy' requires Node >= 22; ` +
      `control-plane (Colab REST) proxying is DISABLED. Data-plane proxying still works. ` +
      `Upgrade to Node >= 22 to restore full proxy support.`,
  );
}
