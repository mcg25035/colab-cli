import readline from 'readline';
import open from 'open';
import { AuthType } from '../colab/api.js';
import { ColabClient, ColabRequestError } from '../colab/client.js';
import { log } from '../logging/index.js';
import { isJsonMode, notifyAuthUrl } from '../output/json-output.js';

export class AuthConsentError extends Error {
  constructor(
    public readonly authType: AuthType,
    public readonly url: string,
  ) {
    super(`Interactive consent required for ${authType}`);
    this.name = 'AuthConsentError';
  }
}

export async function handleEphemeralAuth(
  apiClient: ColabClient,
  endpoint: string,
  authType: AuthType,
  serverLabel?: string,
): Promise<void> {
  // Colab backend's credentials-propagation flow:
  //   1. dry-run query. If backend says success → already authorized, propagate.
  //   2. If backend returns unauthorized_redirect_uri → user consents in
  //      browser (backend pins login_hint to runtime owner's email, so users
  //      cannot accidentally mount another account's Drive), then propagate.
  // Phase 1 FIX4 dropped MurphyLo's "local DriveFS + fake metadata server"
  // scheme in favor of this pure ephemeral flow. There are no local creds
  // that should bypass propagation.
  const dryRunResult = await apiClient.propagateCredentials(endpoint, {
    authType,
    dryRun: true,
  });
  log.trace(`[${authType}] Credentials propagation dry run:`, dryRunResult);

  if (dryRunResult.success) {
    await propagateCredentials(apiClient, endpoint, authType);
  } else if (dryRunResult.unauthorizedRedirectUri) {
    await promptUserConsent(
      apiClient,
      endpoint,
      authType,
      dryRunResult.unauthorizedRedirectUri,
      serverLabel,
    );
    // waitForConsent() returns only after a successful non-dry-run
    // propagation, so there is nothing left to do here.
  } else {
    throw new Error(
      `[${authType}] Credentials propagation dry run returned unexpected results: ${JSON.stringify(dryRunResult)}`,
    );
  }
}

async function promptUserConsent(
  apiClient: ColabClient,
  endpoint: string,
  authType: AuthType,
  unauthorizedRedirectUri: string,
  serverLabel?: string,
): Promise<void> {
  let message: string;
  let detail: string;
  const label = serverLabel ?? 'this runtime';
  switch (authType) {
    case AuthType.DFS_EPHEMERAL:
      message = `Permit "${label}" to access your Google Drive files?`;
      detail =
        'Granting access to Google Drive allows code executed in this runtime to modify files in your Google Drive.';
      break;
    case AuthType.AUTH_USER_EPHEMERAL:
      message = `Allow "${label}" to access your Google credentials?`;
      detail =
        'This allows code executed in this runtime to access your Google Drive and Google Cloud data.';
      break;
    default:
      throw new Error(`Unsupported auth type: ${String(authType)}`);
  }

  console.error(`\n${message}`);
  console.error(detail);

  // Phase 1 post-test (Bug #6): the y/n consent prompt is skipped entirely —
  // consent is treated as granted (auto-y). This keeps `drive-mount` (and any
  // other ephemeral-auth consumer) usable from scripts/agents that run with a
  // non-interactive stdin: the only required interaction is the browser
  // consent itself, which waitForConsent() detects by polling.
  notifyAuthUrl(`${authType} authorization for ${label}`, unauthorizedRedirectUri);
  try {
    await open(unauthorizedRedirectUri);
  } catch (err) {
    log.warn('Failed to open browser automatically:', err);
  }

  if (isJsonMode() && !process.stdin.isTTY) {
    throw new AuthConsentError(authType, unauthorizedRedirectUri);
  }

  await waitForConsent(apiClient, endpoint, authType);
}

const CONSENT_POLL_INTERVAL_MS = 3_000;
const CONSENT_TIMEOUT_MS = 10 * 60_000;

/**
 * Wait until the user finishes the browser consent.
 *
 * Resolves only after a successful non-dry-run propagation.
 *
 * Previously this relied solely on "Press Enter", which was invisible in
 * practice: `drive-mount`'s ora spinner redrew over the prompt line, so the
 * user consented in the browser and then watched a spinner forever while the
 * CLI waited for an Enter they never knew to press. Now we poll the dry-run
 * endpoint in the background and continue automatically once the backend
 * reports the credentials were propagated. Enter still works as a manual
 * "check now"; pressing it too early just keeps waiting instead of failing.
 */
async function waitForConsent(
  apiClient: ColabClient,
  endpoint: string,
  authType: AuthType,
): Promise<void> {
  const deadline = Date.now() + CONSENT_TIMEOUT_MS;
  const out = isJsonMode() ? process.stderr : process.stdout;
  const canPrompt = Boolean(process.stdin.isTTY);
  let rl: readline.Interface | undefined;
  let enterPressed: (() => void) | undefined;

  if (canPrompt) {
    rl = readline.createInterface({ input: process.stdin, output: out });
    rl.on('line', () => enterPressed?.());
    out.write('Waiting for browser authorization (auto-detected; press Enter to re-check now)...\n');
  } else {
    out.write('Waiting for browser authorization (auto-detected)...\n');
  }

  // Poll with dryRun=FALSE. Verified live (2026-11-23): after the user
  // consents, dryRun=true keeps returning success:false until a real
  // (dryRun=false) propagation has been made — so polling the dry run alone
  // never completes. This matches the daemon's background-exec polling in
  // src/daemon/server.ts (tryPropagateCredentialsForPolling).
  const isAuthorized = async (): Promise<boolean> => {
    try {
      const r = await apiClient.propagateCredentials(endpoint, { authType, dryRun: false });
      log.trace(`[${authType}] consent poll:`, r);
      return r.success;
    } catch (err) {
      if (err instanceof ColabRequestError && err.status >= 400 && err.status < 500) {
        log.debug(`[${authType}] consent poll: not ready (${err.status})`);
        return false;
      }
      throw err;
    }
  };

  try {
    while (true) {
      if (await isAuthorized()) {
        out.write('Authorization detected.\n');
        return;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new Error(
          `[${authType}] Timed out after ${CONSENT_TIMEOUT_MS / 60_000} minutes waiting for browser authorization`,
        );
      }
      const woke = await new Promise<'tick' | 'enter'>((resolve) => {
        const t = setTimeout(() => resolve('tick'), Math.min(CONSENT_POLL_INTERVAL_MS, remaining));
        enterPressed = () => {
          clearTimeout(t);
          resolve('enter');
        };
      });
      enterPressed = undefined;
      if (woke === 'enter' && !(await isAuthorized())) {
        out.write(
          'Backend does not report the authorization yet. Finish the consent in the browser ' +
            '(make sure the Google Drive permission checkbox is ticked); still waiting...\n',
        );
      }
    }
  } finally {
    rl?.close();
  }
}

async function propagateCredentials(
  apiClient: ColabClient,
  endpoint: string,
  authType: AuthType,
): Promise<void> {
  const result = await apiClient.propagateCredentials(endpoint, {
    authType,
    dryRun: false,
  });
  log.trace(`[${authType}] credentials propagation:`, result);
  if (!result.success) {
    throw new Error(`[${authType}] Credentials propagation unsuccessful`);
  }
}
