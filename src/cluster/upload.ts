import fs from 'fs';
import path from 'path';
import type { StoredServer } from '../runtime/storage.js';
import { getStoredServer } from '../runtime/storage.js';
import type { DaemonClient } from '../daemon/client.js';
import type { ConnectionProvider } from '../transfer/common.js';
import { uploadFile } from '../transfer/upload.js';

/**
 * Cluster-side wrapper around the existing chunked upload: turns the local
 * `src` file into the VM-side `dest`. Floored at the same 500 MiB chunked
 * cap as `colab fs upload`; bigger datasets land in Phase 3 (split or Drive).
 */

function makeConnectionProvider(server: StoredServer): ConnectionProvider {
  const accountId = server.accountId!;
  return {
    getProxyUrl() {
      return getStoredServer(accountId, server.id)?.proxyUrl ?? server.proxyUrl;
    },
    getToken() {
      return getStoredServer(accountId, server.id)?.token ?? server.token;
    },
  };
}

function makeDaemonExec(client: DaemonClient): (code: string) => Promise<string> {
  return async (code: string): Promise<string> => {
    const outputs = client.exec(code);
    const textParts: string[] = [];
    for await (const output of outputs) {
      if (output.type === 'stream' && output.text) textParts.push(output.text);
    }
    return textParts.join('');
  };
}

export async function clusterUpload(
  client: DaemonClient,
  server: StoredServer,
  src: string,
  dest: string,
): Promise<void> {
  const resolved = path.resolve(src);
  if (!fs.existsSync(resolved)) throw new Error(`upload src not found: ${resolved}`);
  // `normalizeRemotePath` doesn't treat a trailing-slash dest as a
  // directory (the verify step then 404s); resolve the filename ourselves.
  if (dest.endsWith('/')) {
    dest = dest + path.basename(resolved);
  }
  const result = await uploadFile(
    makeConnectionProvider(server),
    // Cluster uploads run unattended on whatever uplink the user has:
    // base64 inflates a 20MiB chunk to ~27MiB and 25-way parallel PUTs
    // saturate slow links until the contents API's 120s timeout fires (T6).
    // Smaller chunks + fewer lanes + one extra retry is slower on a fat pipe
    // but actually finishes on a thin one.
    {
      localPath: resolved,
      remotePath: dest,
      chunkSizeBytes: 5 * 1024 * 1024,
      maxConcurrency: 5,
      retries: 4,
    },
    makeDaemonExec(client),
  );
  if (!result.ok) {
    throw new Error(`upload ${resolved} -> ${dest} failed`);
  }
}
