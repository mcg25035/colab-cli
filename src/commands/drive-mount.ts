import { handleEphemeralAuth } from '../auth/ephemeral.js';
import { DaemonClient } from '../daemon/client.js';
import { ColabClient } from '../colab/client.js';
import { RuntimeManager } from '../runtime/runtime-manager.js';
import { renderStream } from '../output/terminal-renderer.js';
import { createSpinner, isJsonMode, setJsonMode } from '../output/json-output.js';

const MOUNT_PATH = '/content/drive';
const MOUNT_CODE = `from google.colab import drive
drive.mount('${MOUNT_PATH}')`;

/**
 * `colab drive-mount -e <endpoint> [--account <email>]`
 *
 * Phase 1 FIX4 rewrote this command to use Colab backend's ephemeral
 * credentials-propagation flow, matching the official colab-cli's
 * `drivemount` semantics.
 *
 * Cross-account enforcement: resolveTarget only looks at the active
 * account's `servers.json` (RuntimeManager built per-account in
 * src/index.ts). An endpoint owned by account B does NOT appear in
 * account A's server table, so `colab drive-mount -e <B's endpoint>
 * --account A` throws here. Backend additionally pins login_hint
 * to the runtime owner's email, so even a hypothetical local lookup
 * miss cannot mount another account's Drive.
 *
 * `handleEphemeralAuth` (src/auth/ephemeral.ts) does the three-step
 * credentials-propagation dance against
 * `/tun/m/credentials-propagation/<endpoint>?authType=dfs_ephemeral`:
 *   1. dry-run probe;
 *   2. if backend says success → propagate;
 *   3. if backend returns unauthorized_redirect_uri → print + open the
 *      URL, wait for the human to consent in browser, then propagate.
 * MurphyLo's previous local-DriveFS-binary design (mount.ts +
 * mount-auth.ts, DRIVEFS_CLIENT_ID env var) was deleted in favor of this.
 *
 * `--json` is not compatible with the streaming kernel output this
 * command produces (we mirror `colab exec`'s stance): we warn and force
 * plain mode so the kernel's stdout/stderr render normally.
 */
export async function driveMountCommand(
  runtimeManager: RuntimeManager,
  colabClient: ColabClient,
  options: {
    endpoint?: string;
  },
): Promise<void> {
  if (isJsonMode()) {
    console.error('Warning: --json is not supported for `drive-mount` and will be ignored.');
    setJsonMode(false);
  }

  // Cross-account enforcement: lookup in active account's servers.json only.
  const server = await runtimeManager.resolveTarget(options.endpoint);

  const spinner = createSpinner('Connecting to daemon...').start();
  const client = new DaemonClient();
  try {
    await client.connect(server.accountId!, server.id);
    spinner.text = 'Mounting Google Drive via ephemeral auth flow...';
  } catch (err) {
    spinner.fail('Failed to connect to daemon');
    throw err;
  }

  // Override SIGINT: first Ctrl+C interrupts kernel, second force-exits.
  const origSigint = process.rawListeners('SIGINT').slice();
  process.removeAllListeners('SIGINT');
  let interrupted = false;
  const doInterrupt = () => {
    if (interrupted) process.exit(1);
    interrupted = true;
    client.interrupt();
  };
  process.on('SIGINT', doInterrupt);

  let hasError = false;
  try {
    const outputs = client.exec(MOUNT_CODE, {
      handleEphemeralAuth: async (authType) => {
        await handleEphemeralAuth(colabClient, server.endpoint, authType, server.label);
      },
    });
    hasError = await renderStream(outputs);
  } finally {
    process.removeAllListeners('SIGINT');
    for (const fn of origSigint) {
      process.on('SIGINT', fn as (...args: any[]) => void);
    }
    client.close();
  }

  if (hasError) {
    spinner.fail('Drive mount failed');
    process.exitCode = 1;
    return;
  }

  spinner.succeed(`Google Drive mounted at ${MOUNT_PATH}`);
}
