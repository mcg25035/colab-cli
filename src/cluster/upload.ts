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
  const result = await uploadFile(
    makeConnectionProvider(server),
    { localPath: resolved, remotePath: dest },
    makeDaemonExec(client),
  );
  if (!result.ok) {
    throw new Error(`upload ${resolved} -> ${dest} failed`);
  }
}
