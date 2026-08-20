import readline from 'readline';
import open from 'open';
import { AuthType } from '../colab/api.js';
import { ColabClient } from '../colab/client.js';
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
    const consent = await promptUserConsent(
      authType,
      dryRunResult.unauthorizedRedirectUri,
      serverLabel,
    );
    if (!consent) {
      throw new Error(`User cancelled ${authType} authorization`);
    }
    await propagateCredentials(apiClient, endpoint, authType);
  } else {
    throw new Error(
      `[${authType}] Credentials propagation dry run returned unexpected results: ${JSON.stringify(dryRunResult)}`,
    );
  }
}

async function promptUserConsent(
  authType: AuthType,
  unauthorizedRedirectUri: string,
  serverLabel?: string,
): Promise<boolean> {
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
  // non-interactive stdin: the only required interaction is the URL + a final
  // Enter to confirm the authorization finished in the browser.
  notifyAuthUrl(`${authType} authorization for ${label}`, unauthorizedRedirectUri);
  try {
    await open(unauthorizedRedirectUri);
  } catch (err) {
    log.warn('Failed to open browser automatically:', err);
  }

  if (isJsonMode() && !process.stdin.isTTY) {
    throw new AuthConsentError(authType, unauthorizedRedirectUri);
  }

  await askQuestion('Press Enter after authorization is complete...');
  return true;
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

function askQuestion(prompt: string): Promise<string> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: isJsonMode() ? process.stderr : process.stdout,
  });
  return new Promise((resolve) => {
    rl.question(prompt, (answer) => {
      rl.close();
      resolve(answer);
    });
  });
}
