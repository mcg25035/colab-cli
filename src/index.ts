#!/usr/bin/env -S node --use-env-proxy --disable-warning=UNDICI-EHPA

import { Command, Option } from 'commander';
import fs from 'fs';
import path from 'path';
import { COLAB_API_DOMAIN, COLAB_GAPI_DOMAIN, OAUTH_CLIENT_ID, LEGACY_DRIVE_AUTH_FILE, accountDriveAuthFile } from './config.js';
import { AuthManager } from './auth/auth-manager.js';
import { ColabClient } from './colab/client.js';
import { RuntimeManager } from './runtime/runtime-manager.js';
import { log } from './logging/index.js';
import { setJsonMode, jsonError, jsonResult, isJsonMode } from './output/json-output.js';
import { AuthConsentError } from './auth/ephemeral.js';
import {
  migrateLegacyAccountIfNeeded,
  listRegistryAccounts,
  getActiveAccountEmail,
  setActiveAccountEmail,
  unregisterAccount,
  pickSuccessorAccountEmail,
  registerAccount,
  RegistryAccount,
} from './auth/accounts-registry.js';
import { removeStoredSession } from './auth/storage.js';
import { migrateLegacyServersIfNeeded } from './runtime/storage.js';
import { loginCommand, statusCommand, logoutCommand } from './commands/auth.js';
import {
  createRuntimeCommand,
  listAvailableRuntimesCommand,
  listRuntimesCommand,
  listRuntimeVersionsCommand,
  destroyRuntimeCommand,
  restartRuntimeCommand,
  resourcesCommand,
} from './commands/runtime.js';
import {
  execCommand,
  execBgCommand,
  execAttachCommand,
  execListCommand,
  execSendCommand,
  execClearCommand,
} from './commands/exec.js';
import { fsUploadCommand, fsDownloadCommand } from './commands/fs.js';
import {
  shellCommand,
  shellAttachCommand,
  shellListCommand,
  shellSendCommand,
  shellCloseCommand,
} from './commands/shell.js';
import {
  clusterStatusCommand,
  clusterSubmitCommand,
  clusterListCommand,
  clusterLogsCommand,
  clusterCancelCommand,
  clusterShutdownCommand,
  submitFromSpecCommand,
  clusterRehearseCommand,
} from './commands/cluster.js';
import {
  portForwardCreateCommand,
  portForwardListCommand,
  portForwardCloseCommand,
} from './commands/port-forward.js';
import { usageCommand } from './commands/usage.js';
import {
  driveLoginCommand,
  driveLogoutCommand,
  driveStatusCommand,
  driveListCommand,
  driveInfoCommand,
  driveUploadCommand,
  driveDownloadCommand,
  driveMkdirCommand,
  driveDeleteCommand,
  driveCopyCommand,
  driveRenameCommand,
  driveMoveCommand,
} from './commands/drive.js';
import { DriveAuthManager } from './drive/auth.js';
import { driveMountCommand } from './commands/drive-mount.js';

const program = new Command();

program
  .name('colab')
  .description('interact with Google Colab GPU runtimes from the terminal')
  .version('0.1.0')
  .allowExcessArguments(false)
  .option('--verbose', 'enable verbose logging')
  .option('--json', 'output results as JSON to stdout (for scripting)')
  .option('--account <email>', 'operate on this account instead of the registry active account')
  .configureHelp({
    subcommandTerm: (cmd) => {
      const args = (cmd as any).registeredArguments
        .map((arg: any) => arg.required ? `<${arg._name}>` : `[${arg._name}]`)
        .join(' ');
      return cmd.name() + (args ? ' ' + args : '');
    },
  })
  .hook('preAction', (thisCommand) => {
    const opts = thisCommand.opts();
    if (opts.verbose) {
      log.setVerbose(true);
    }
    if (opts.json) {
      setJsonMode(true);
    }
    if (opts.account) {
      cliAccountOverride = opts.account;
    }
  });

// Per-account shared state. CLI builds one (AuthManager, ColabClient,
// RuntimeManager) triple per active account and caches it for the process
// lifetime. The registry's `active` pointer picks the default account; the
// `--account` global flag can override on any command.
const accountCache = new Map<string, { auth: AuthManager; client: ColabClient; runtime: RuntimeManager }>();
let activeAccountId: string | undefined;
let cliAccountOverride: string | undefined; // from --account

function resolveActiveAccountId(): string | undefined {
  return cliAccountOverride ?? activeAccountId ?? getActiveAccountEmail();
}

async function ensureInitialized(): Promise<void> {
  if (activeAccountId && !cliAccountOverride) return;
  if (cliAccountOverride && accountCache.has(cliAccountOverride)) return;

  if (!OAUTH_CLIENT_ID) {
    console.error(
      'OAuth client ID not configured. Set COLAB_CLIENT_ID and COLAB_CLIENT_SECRET environment variables.',
    );
    process.exit(1);
  }

  // On first CLI invocation, adopt any legacy single-account creds.
  migrateLegacyAccountIfNeeded();

  const accountId = resolveActiveAccountId();
  if (!accountId) {
    // No accounts registered and no legacy creds to migrate — many commands
    // remain usable (e.g. `auth login`), but `ensureLoggedIn` will reject
    // them. Keep activeAccountId undefined so callers see "not logged in".
    return;
  }
  activeAccountId = accountId;

  // Migrate legacy servers.json into this account's tree if needed — must run
  // before any runtime command reads/writes servers.json.
  migrateLegacyServersIfNeeded(accountId);

  // Migrate legacy global drive-auth.json into the per-account path if the
  // email recorded in it is registered to colab. Drive-only users (email
  // not in registry) keep the legacy file (legacy behaviour preserved).
  migrateLegacyDriveAuth();

  await buildManagersForAccount(accountId);
}

/**
 * Phase 1 FIX4: lazily adopt MurphyLo's global `~/.config/colab-cli/drive-auth.json`
 * into `accounts/<email>/drive-auth.json` when possible.
 *
 * Migration rule:
 *   - Missing/corrupt legacy file: leave alone (nothering to do).
 *   - Legacy file email NOT registered to colab: leave alone. Those are
 *     drive-only users who haven't done `colab auth login`; creating a
 *     per-account dir for them would orphan their accountDir from the
 *     registry's active-pointer mechanism. Subsequent drive commands will
 *     still read the legacy file via DriveAuthManager's legacy fallback.
 *   - Legacy file email IS registered to colab AND per-account file
 *     already exists: per-account copy wins; just delete the stale legacy.
 *   - Legacy file email IS registered to colab AND per-account file
 *     doesn't exist: copy legacy → per-account, delete legacy, log success.
 *
 * Idempotent. Safe to call from every CLI invocation.
 */
function migrateLegacyDriveAuth(): void {
  if (!fs.existsSync(LEGACY_DRIVE_AUTH_FILE)) return;

  let data: unknown;
  try {
    data = JSON.parse(fs.readFileSync(LEGACY_DRIVE_AUTH_FILE, 'utf-8'));
  } catch (err) {
    log.warn(`Legacy drive-auth.json unreadable; leaving untouched: ${err}`);
    return;
  }

  const obj = data as { refreshToken?: unknown; email?: unknown };
  if (typeof obj.refreshToken !== 'string') {
    log.warn('Legacy drive-auth.json missing refreshToken; leaving untouched.');
    return;
  }
  const email = typeof obj.email === 'string' ? obj.email : undefined;
  if (!email) {
    // Can't migrate without an email anchor. Leave alone; the next
    // `colab drive login` will return the real email via userinfo and
    // store at the per-account path, then a future run of this fn (or
    // its legacy fallback in DriveAuthManager) will clean up the legacy.
    return;
  }

  // Only migrate when the email is already a registered colab account.
  // Otherwise this is a drive-only user — legacy behaviour preserved.
  const registeredEmails = listRegistryAccounts().map((a) => a.email);
  if (!registeredEmails.includes(email)) {
    return;
  }

  const dest = accountDriveAuthFile(email);
  if (fs.existsSync(dest)) {
    // Per-account creds win — just remove stale legacy.
    try {
      fs.unlinkSync(LEGACY_DRIVE_AUTH_FILE);
      log.info(`Removed stale legacy drive-auth.json (per-account copy at ${dest}).`);
    } catch (err) {
      log.warn(`Could not remove stale legacy drive-auth.json: ${err}`);
    }
    return;
  }

  try {
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.writeFileSync(dest, JSON.stringify(data, null, 2), { mode: 0o600 });
    fs.unlinkSync(LEGACY_DRIVE_AUTH_FILE);
    log.info(`Migrated global drive-auth.json → ${dest}.`);
  } catch (err) {
    log.warn(`Failed to migrate legacy drive-auth.json: ${err}`);
  }
}

async function buildManagersForAccount(accountId: string): Promise<void> {
  if (accountCache.has(accountId)) return;
  const auth = new AuthManager(accountId);
  await auth.initialize();
  const client = new ColabClient(
    new URL(COLAB_API_DOMAIN),
    new URL(COLAB_GAPI_DOMAIN),
    () => auth.getAccessToken(),
    () => auth.logout(),
  );
  const runtime = new RuntimeManager(client, accountId);
  accountCache.set(accountId, { auth, client, runtime });
}

function getActive(): { auth: AuthManager; client: ColabClient; runtime: RuntimeManager } {
  if (!activeAccountId) {
    throw new Error('No active account. Run `colab auth login` first.');
  }
  const entry = accountCache.get(activeAccountId);
  if (!entry) {
    throw new Error(`Account ${activeAccountId} not initialized.`);
  }
  return entry;
}

async function ensureLoggedIn(): Promise<void> {
  await ensureInitialized();
  if (!activeAccountId) {
    if (isJsonMode()) {
      jsonError('Not logged in. Run `colab auth login` first.');
    } else {
      console.error('Not logged in. Run `colab auth login` first.');
    }
    process.exit(1);
  }
  const { auth } = getActive();
  if (!auth.isLoggedIn()) {
    if (isJsonMode()) {
      jsonError(`Account ${activeAccountId} credentials missing or expired. Run \`colab auth login --account ${activeAccountId}\` to re-auth.`);
    } else {
      console.error(`Account ${activeAccountId} credentials missing or expired. Run \`colab auth login --account ${activeAccountId}\` to re-auth.`);
    }
    process.exit(1);
  }
}

// Auth commands
const auth = program.command('auth').description('manage authentication');

auth
  .command('login')
  .description('sign in with Google OAuth; adds the account to the registry and marks it active')
  .option('--account <email>', 're-auth a specific existing account by email')
  .action(async (opts) => {
    // Bootstrap path: login is the entry point for a fresh env (no accounts
    // yet), so it MUST NOT depend on an existing active account. The original
    // `getActive().auth` design throws in fresh env — this is the Phase 1 bug
    // caught by T4 testing. We instead build a throwaway AuthManager with a
    // placeholder accountId; AuthManager.login() fetches the real email from
    // userinfo and overwrites accountId from the response.
    await ensureInitialized();

    let auth: AuthManager;
    if (opts.account) {
      // Re-auth path: target account specified. Must already be registered.
      const exists = listRegistryAccounts().some((a) => a.email === opts.account);
      if (!exists) {
        console.error(
          `Account ${opts.account} is not registered.\n` +
          `If this is a brand-new Google account, omit --account and run \`colab auth login\` first.`,
        );
        process.exit(1);
      }
      // Build/cache a manager for this specific account.
      await buildManagersForAccount(opts.account);
      auth = accountCache.get(opts.account)!.auth;
    } else {
      // Fresh-login path: no active account required. Use placeholder
      // `__pending__` as the AuthManager's initial accountId; login() will
      // overwrite it with the real email after the OAuth userinfo roundtrip.
      auth = new AuthManager('__pending__');
    }

    await loginCommand(auth, { account: opts.account });

    // After login, AuthManager.login() has side effects:
    //   - stored session to accounts/<email>/auth.json
    //   - registerAccount() added it to registry
    //   - setActiveAccountEmail() pointed `active` at it
    //   - AuthManager.accountId updated from `__pending__` to real email
    // Make our process-local cache reflect this so the next CLI invocation in
    // the same process sees the new active account.
    const newActiveEmail = getActiveAccountEmail();
    if (newActiveEmail && newActiveEmail !== activeAccountId) {
      activeAccountId = newActiveEmail;
      // Build the proper cache entry (read back the now-stored session).
      await buildManagersForAccount(newActiveEmail);
    }
  });

auth
  .command('status')
  .description('show authentication status of the active account')
  .action(async () => {
    await ensureInitialized();
    if (!activeAccountId) {
      // No accounts at all → friendly message, mirroring `ensureLoggedIn`
      // behaviour rather than throwing a raw `getActive()` Error.
      if (isJsonMode()) {
        jsonResult({ command: 'auth.status', loggedIn: false });
      } else {
        console.log('Not logged in. Run `colab auth login` to sign in.');
      }
      return;
    }
    const { auth } = getActive();
    await statusCommand(auth);
  });

auth
  .command('logout')
  .description('sign out and revoke tokens for the active account (or a specific one)')
  .option('-a, --account <email>', 'logout a specific account by email')
  .action(async (opts) => {
    await ensureInitialized();
    const target = opts.account ?? activeAccountId;
    if (!target) return;
    // Use the cached manager for this account if present; else build a fresh throwaway one.
    let auth: AuthManager;
    if (accountCache.has(target)) {
      auth = accountCache.get(target)!.auth;
    } else {
      const m = new AuthManager(target);
      await m.initialize();
      auth = m;
    }
    await auth.logout();
    accountCache.delete(target);
    if (activeAccountId === target) {
      const succ = pickSuccessorAccountEmail(target);
      activeAccountId = succ;
      if (succ) await buildManagersForAccount(succ);
    }
  });

auth
  .command('list')
  .description('list all registered accounts; mark the active one')
  .action(async () => {
    // Adopt legacy single-account creds if present so the list reflects them.
    migrateLegacyAccountIfNeeded();
    // Adopt legacy servers.json into the active account too, so a freshly
    // migrated user doesn't see an empty `runtime list` until their next
    // runtime command triggers it.
    const active = getActiveAccountEmail();
    if (active) {
      migrateLegacyServersIfNeeded(active);
    }
    const accounts = listRegistryAccounts();
    const activeEmail = getActiveAccountEmail();
    if (isJsonMode()) {
      jsonResult({ command: 'auth.list', accounts, active });
      return;
    }
    if (accounts.length === 0) {
      console.log('No accounts. Run `colab auth login` to add one.');
      return;
    }
    console.log('\nAccounts:');
    for (const a of accounts) {
      const marker = a.email === activeEmail ? '*' : ' ';
      console.log(`  ${marker} ${a.email}  (${a.name})`);
    }
    console.log('');
  });

auth
  .command('switch <email>')
  .description('set the active account by email')
  .action(async (email: string) => {
    // Just update the registry pointer. Initialization (and any necessary
    // token refresh) happens lazily on the next command that actually needs
    // an access token. A previously-registered account whose refresh token
    // has expired should still appear switchable here — the user will get
    // told to re-auth when they actually invoke a runtime command.
    const accounts = listRegistryAccounts();
    if (!accounts.some((a) => a.email === email)) {
      console.error(`Account ${email} is not registered. Run \`colab auth list\` to see registered accounts.`);
      process.exit(1);
    }
    setActiveAccountEmail(email);
    activeAccountId = email;
    const reg = accounts.find((a) => a.email === email);
    if (isJsonMode()) {
      jsonResult({ command: 'auth.switch', active: email, name: reg?.name });
    } else {
      console.log(`Switched active account to: ${email} (${reg?.name ?? ''})`);
    }
  });

// Runtime commands
const runtime = program.command('runtime').description('manage runtimes');

runtime
  .command('create')
  .description('create a new runtime')
  .requiredOption(
    '-a, --accelerator <accelerator>',
    'accelerator in Colab UI semantics: CPU, H100, G4, A100, L4, T4, v6e-1, or v5e-1',
  )
  .addOption(
    new Option('-s, --shape <shape>', 'machine shape').choices([
      'standard',
      'high-ram',
    ]),
  )
  .option(
    '-v, --runtime-version <version>',
    'runtime version label (e.g. 2026.01). See `colab runtime versions`.',
  )
  .addOption(
    new Option('-k, --kernel <kernel>', 'kernel type')
      .choices(['python3', 'r', 'julia'])
      .default('python3'),
  )
  .action(async (opts) => {
    await ensureLoggedIn();
    await createRuntimeCommand(getActive().runtime, opts);
  });

runtime
  .command('available')
  .description('list available accelerators and machine shapes')
  .action(async () => {
    await ensureLoggedIn();
    await listAvailableRuntimesCommand(getActive().client);
  });

runtime
  .command('versions')
  .description('list available runtime versions and their environment details')
  .action(async () => {
    await ensureLoggedIn();
    await listRuntimeVersionsCommand(getActive().client);
  });

runtime
  .command('list')
  .description('list active runtimes')
  .action(async () => {
    await ensureLoggedIn();
    await listRuntimesCommand(getActive().runtime);
  });

runtime
  .command('destroy')
  .description('destroy a runtime')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (opts) => {
    await ensureLoggedIn();
    await destroyRuntimeCommand(getActive().runtime, opts.endpoint);
  });

runtime
  .command('restart')
  .description('restart the kernel without destroying the VM')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (opts) => {
    await ensureLoggedIn();
    await restartRuntimeCommand(getActive().runtime, opts.endpoint);
  });

runtime
  .command('resources')
  .description('show RAM, disk and GPU usage of a runtime')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (opts) => {
    await ensureLoggedIn();
    await resourcesCommand(getActive().runtime, getActive().client, opts.endpoint);
  });

// Usage command
program
  .command('usage')
  .description('show subscription tier and compute-unit usage')
  .action(async () => {
    await ensureLoggedIn();
    await usageCommand(getActive().client);
  });

// Exec command
const execCmd = program
  .command('exec [code]')
  .description('execute code on a runtime')
  .option('-f, --file <path>', 'execute code from a file. Omit both [code] and -f to read code from piped stdin/heredoc, useful for complex snippets with nested quotes or $() that are hard to escape')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('-o, --output-dir <path>', 'save image outputs (png, jpeg, gif, svg) to this directory')
  .option('-b, --background', 'run in background and return exec ID immediately')
  .action(async (code, opts) => {
    await ensureLoggedIn();
    if (opts.background) {
      await execBgCommand(getActive().runtime, {
        code,
        file: opts.file,
        endpoint: opts.endpoint,
        outputDir: opts.outputDir,
      });
    } else {
      await execCommand(getActive().runtime, getActive().client, {
        code,
        file: opts.file,
        endpoint: opts.endpoint,
        outputDir: opts.outputDir,
      });
    }
  });

execCmd
  .command('attach <id>')
  .description('attach to an execution (replay + stream output)')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('--no-wait', 'print buffered output and exit immediately')
  .option('--tail <n>', 'only last N outputs (implies --no-wait)', parseInt)
  .action(async (id: string, opts) => {
    await ensureLoggedIn();
    const endpoint = opts.endpoint ?? execCmd.opts().endpoint;
    await execAttachCommand(getActive().runtime, getActive().client, parseInt(id, 10), {
      endpoint,
      noWait: !opts.wait,
      tail: opts.tail,
    });
  });

execCmd
  .command('list')
  .description('list executions with status')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('--json', 'output results as JSON to stdout (for scripting)')
  .action(async (opts) => {
    await ensureLoggedIn();
    const endpoint = opts.endpoint ?? execCmd.opts().endpoint;
    await execListCommand(getActive().runtime, {
      endpoint,
    });
  });

execCmd
  .command('send <id>')
  .description('send stdin or interrupt to a running execution')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('--stdin <value>', 'send stdin input value')
  .option('--interrupt', 'interrupt the execution')
  .action(async (id: string, opts) => {
    await ensureLoggedIn();
    const endpoint = opts.endpoint ?? execCmd.opts().endpoint;
    await execSendCommand(getActive().runtime, parseInt(id, 10), {
      endpoint,
      stdin: opts.stdin,
      interrupt: opts.interrupt,
    });
  });

execCmd
  .command('clear [id]')
  .description('clear completed executions (all, or by ID)')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (id: string | undefined, opts) => {
    await ensureLoggedIn();
    const endpoint = opts.endpoint ?? execCmd.opts().endpoint;
    await execClearCommand(getActive().runtime, id ? parseInt(id, 10) : undefined, {
      endpoint,
    });
  });

// Shell commands (experimental)
const shellCmd = program
  .command('shell')
  .description('open an interactive terminal on a runtime (experimental)')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('-b, --background', 'create shell in background, print shell ID and exit')
  .option('-c, --cmd <cmd>', 'send a command to the shell after opening/attaching (no trailing newline added unless you include \\n)')
  .option('-s, --shell <id>', 'reuse an existing background shell by ID instead of opening a new one', parseInt)
  .action(async (opts) => {
    await ensureLoggedIn();
    await shellCommand(getActive().runtime, {
      endpoint: opts.endpoint,
      background: opts.background,
      cmd: opts.cmd,
      shellId: opts.shell,
    });
  });

shellCmd
  .command('attach <id>')
  .description('attach to a shell session for the active (or `--account`) account (replay buffer + stream live output)')
  .option('--no-wait', 'print buffered output and exit immediately')
  .option('--tail <n>', 'only last N lines of rendered output (implies --no-wait)', parseInt)
  .action(async (id: string, opts) => {
    await ensureLoggedIn();
    await shellAttachCommand(getActive().runtime.getAccountId(), parseInt(id, 10), {
      noWait: !opts.wait,
      tail: opts.tail,
    });
  });

shellCmd
  .command('list')
  .description('list active shell sessions for the active (or `--account`) account')
  .action(async () => {
    await ensureLoggedIn();
    await shellListCommand(getActive().runtime.getAccountId());
  });

shellCmd
  .command('send <id>')
  .description('send raw data or signal to a shell session')
  .option('-d, --data <data>', "raw data to send (supports \\n, \\x03 escapes; prefer single quotes for simple values). Omit --data to read raw bytes from piped stdin/heredoc, useful for complex commands with nested quotes or $() that are hard to escape")
  .option(
    '--signal <signal>',
    'send signal: INT (Ctrl+C), EOF (Ctrl+D), TSTP (Ctrl+Z), QUIT (Ctrl+\\)',
  )
  .action(async (id: string, opts) => {
    await ensureLoggedIn();
    await shellSendCommand(getActive().runtime.getAccountId(), parseInt(id, 10), {
      data: opts.data,
      signal: opts.signal,
    });
  });

shellCmd
  .command('close <id>')
  .description('close a shell session and kill its VM-side process tree (unlike EOF, works when a foreground process holds the PTY)')
  .action(async (id: string) => {
    await ensureLoggedIn();
    await shellCloseCommand(getActive().runtime.getAccountId(), parseInt(id, 10));
  });

// ── Cluster scheduler commands ──
const clusterCmd = program
  .command('cluster')
  .description('cluster job scheduler (submit commands to idle VMs across all accounts)');

clusterCmd
  .command('status')
  .description('show account/VM pool overview and queue summary')
  .action(async () => {
    await ensureLoggedIn();
    await clusterStatusCommand();
  });

clusterCmd
  .command('submit')
  .description('submit a cluster job (either -c inline command, or -f a job spec JSON with setup_file/uploads/command)')
  .option('-c, --cmd <cmd>', 'shell command to run on the assigned VM (quote it!)')
  .option('-f, --file <file>', 'job spec JSON: {name?, accelerator?, setup_file?, uploads?, command}')
  .option('-n, --name <name>', 'job name')
  .option('-a, --accelerator <accelerator>', 'request accelerator for a newly provisioned runtime (CPU, L4, T4, ...)')
  .action(async (opts) => {
    await ensureLoggedIn();
    if (opts.file) {
      await submitFromSpecCommand(opts.file);
      return;
    }
    if (!opts.cmd) throw new Error('Either --cmd or --file is required');
    await clusterSubmitCommand(opts.cmd, { name: opts.name, accelerator: opts.accelerator });
  });

clusterCmd
  .command('shutdown')
  .description('shut down the cluster daemon (jobs persist and resume after next connect)')
  .action(async () => {
    await ensureLoggedIn();
    await clusterShutdownCommand();
  });

clusterCmd
  .command('rehearse <setupFile>')
  .description('validate a setup script on ONE VM (agent bring-up loop): runs setup only, keeps VM idle after; repeat until green')
  .option('-n, --name <name>', 'job name')
  .action(async (setupFile: string, opts) => {
    await ensureLoggedIn();
    await clusterRehearseCommand(setupFile, opts.name);
  });

clusterCmd
  .command('list')
  .description('list cluster jobs')
  .action(async () => {
    await ensureLoggedIn();
    await clusterListCommand();
  });

clusterCmd
  .command('logs <jobId>')
  .description('show output snapshot/tail for a job')
  .option('--tail <n>', 'last N lines', parseInt)
  .action(async (jobId: string, opts) => {
    await ensureLoggedIn();
    await clusterLogsCommand(parseInt(jobId, 10), opts.tail);
  });

clusterCmd
  .command('cancel <jobId>')
  .description('cancel a queued job or kill a running job (kills its whole VM-side process tree)')
  .action(async (jobId: string) => {
    await ensureLoggedIn();
    await clusterCancelCommand(parseInt(jobId, 10));
  });

// Port forwarding commands
const portForward = program  .command('port-forward')
  .alias('pf')
  .description('forward a runtime port to a local bind address (HTTP/WebSocket)');

portForward
  .command('create <spec>')
  .description(
    'create a forward; spec is REMOTE, LOCAL:REMOTE, or HOST:LOCAL:REMOTE',
  )
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('--tls', 'serve locally over HTTPS with a self-signed certificate')
  .action(async (spec: string, opts) => {
    await ensureLoggedIn();
    await portForwardCreateCommand(getActive().runtime, spec, { endpoint: opts.endpoint, tls: opts.tls });
  });

portForward
  .command('list')
  .description('list active port forwards')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (opts) => {
    await ensureLoggedIn();
    await portForwardListCommand(getActive().runtime, { endpoint: opts.endpoint });
  });

portForward
  .command('close [id]')
  .description('close a port forward by ID, or all with --all')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .option('--all', 'close all port forwards')
  .action(async (id: string | undefined, opts) => {
    await ensureLoggedIn();
    await portForwardCloseCommand(getActive().runtime, id, {
      endpoint: opts.endpoint,
      all: opts.all,
    });
  });

// File system commands
const fsCmd = program.command('fs').description('transfer files to and from a runtime');

fsCmd
  .command('upload <local-path>')
  .description('upload a file to the runtime')
  .option('-r, --remote-path <path>', 'remote destination path (default: content/<filename>)')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (localPath, opts) => {
    await ensureLoggedIn();
    await fsUploadCommand(getActive().runtime, {
      localPath,
      remotePath: opts.remotePath,
      endpoint: opts.endpoint,
    });
  });

fsCmd
  .command('download <remote-path>')
  .description('download a file from the runtime')
  .option('-o, --output <path>', 'local destination path (default: ./<filename>)')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint')
  .action(async (remotePath, opts) => {
    await ensureLoggedIn();
    await fsDownloadCommand(getActive().runtime, {
      remotePath,
      localPath: opts.output,
      endpoint: opts.endpoint,
    });
  });

// Drive commands use a separate OAuth client (rclone's public credentials)
// but Phase 1 FIX4 stores their refresh token per-account under
// `accounts/<email>/drive-auth.json` so multiple accounts no longer clobber
// each other. Drive-only users (no colab login) keep a default
// `__pending__` placeholder; the DriveAuthManager.login flow overwrites
// the placeholder with the real email returned by Google userinfo and
// then registers the account so future commands resolve to it.
let driveAuth: DriveAuthManager | undefined;

/**
 * Phase 1 FIX4: resolve the accountId that drive commands should target.
 *
 * Precedence:
 *   1. `--account <email>` global flag (cliAccountOverride)
 *   2. Active colab account (activeAccountId set by ensureInitialized)
 *   3. Email recorded inside legacy global drive-auth.json — supports
 *      drive-only users upgrading from MurphyLo's single-account layout
 *   4. `__pending__` placeholder — bootstrap path: DriveAuthManager.login
 *      will replace it with the email from Google userinfo after OAuth.
 */
function resolveDriveAccountId(): string {
  if (cliAccountOverride) return cliAccountOverride;
  if (activeAccountId) return activeAccountId;
  if (fs.existsSync(LEGACY_DRIVE_AUTH_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(LEGACY_DRIVE_AUTH_FILE, 'utf-8')) as { email?: unknown };
      if (typeof data.email === 'string' && data.email) return data.email;
    } catch { /* ignore */ }
  }
  return '__pending__';
}

function ensureDriveInit(): DriveAuthManager {
  if (!driveAuth) driveAuth = new DriveAuthManager(resolveDriveAccountId());
  return driveAuth;
}

async function ensureDriveLoggedIn(): Promise<DriveAuthManager> {
  const da = ensureDriveInit();
  if (!da.isAuthorized()) {
    if (isJsonMode()) {
      jsonError('Drive not authorized. Run `colab drive login` first.');
    } else {
      console.error('Drive not authorized. Run `colab drive login` first.');
    }
    process.exit(1);
  }
  return da;
}

const drive = program.command('drive').description('manage files on Google Drive');

drive
  .command('login')
  .description('authorize Google Drive access')
  .action(async () => {
    // ensureInitialized() pre-warms the registry (legacy auth migration
    // might have registered an account since the previous CLI invocation
    // — we want our resolveDriveAccountId to pick that up). However,
    // drive login itself is bootstrap-friendly: with no colab account
    // and no legacy drive-auth.json (or an unexpected email there), the
    // DriveAuthManager gets `__pending__` and mutates it to the real
    // email after OAuth userinfo.
    await ensureInitialized();
    const da = ensureDriveInit();
    await driveLoginCommand(da);

    // After login, `da.accountId` is the real email (either pre-existing
    // account or freshly discovered via Google userinfo). Register it
    // into the colab registry if missing so future commands resolve to
    // it via activeAccountId. Then drop a stale legacy drive-auth.json
    // once its email maps to a registered colab account (FIX4 migration).
    const realEmail = da.getAccountId();
    if (realEmail && realEmail !== '__pending__') {
      const alreadyRegistered = listRegistryAccounts().some((a) => a.email === realEmail);
      if (!alreadyRegistered) {
        registerAccount(realEmail, da.getEmail() ?? realEmail);
      }
      // Both register and re-ensure so setActiveAccountEmail succeeds.
      setActiveAccountEmail(realEmail);
      // Re-run legacy cleanup now that the email is registered.
      migrateLegacyDriveAuth();
    }
  });

drive
  .command('logout')
  .description('remove stored Google Drive credentials')
  .action(async () => {
    await driveLogoutCommand(ensureDriveInit());
  });

drive
  .command('status')
  .description('show Google Drive authorization status')
  .action(async () => {
    await driveStatusCommand(ensureDriveInit());
  });

drive
  .command('list [folder-id]')
  .description(
    'list files in a Google Drive folder, including files shared with you',
  )
  .action(async (folderId) => {
    const da = await ensureDriveLoggedIn();
    await driveListCommand(da, folderId);
  });

drive
  .command('info <item-id>')
  .description('show metadata for a Google Drive file or folder')
  .action(async (itemId) => {
    const da = await ensureDriveLoggedIn();
    await driveInfoCommand(da, itemId);
  });

drive
  .command('upload <local-path>')
  .description('upload a file to Google Drive (best for large files)')
  .option('-p, --parent <folder-id>', 'parent folder ID (default: root)')
  .action(async (localPath, opts) => {
    const da = await ensureDriveLoggedIn();
    await driveUploadCommand(da, localPath, opts);
  });

drive
  .command('download <file-id>')
  .description('download a file from Google Drive')
  .option('-o, --output <path>', 'local output path')
  .action(async (fileId, opts) => {
    const da = await ensureDriveLoggedIn();
    await driveDownloadCommand(da, fileId, opts);
  });

drive
  .command('mkdir <name>')
  .description('create a folder on Google Drive')
  .option('-p, --parent <folder-id>', 'parent folder ID (default: root)')
  .action(async (name, opts) => {
    const da = await ensureDriveLoggedIn();
    await driveMkdirCommand(da, name, opts.parent);
  });

drive
  .command('delete <file-id>')
  .description('delete a file or folder on Google Drive')
  .option('--permanent', 'permanently delete instead of moving to trash')
  .action(async (fileId, opts) => {
    const da = await ensureDriveLoggedIn();
    await driveDeleteCommand(da, fileId, opts);
  });

drive
  .command('copy <file-id>')
  .description('copy a file on Google Drive (destination defaults to My Drive root)')
  .option('--to <folder-id>', 'destination folder ID (default: root)')
  .option('--name <name>', 'name for the copied file')
  .action(async (itemId, opts) => {
    const da = await ensureDriveLoggedIn();
    await driveCopyCommand(da, itemId, opts);
  });

drive
  .command('rename <item-id> <new-name>')
  .description('rename a file or folder on Google Drive')
  .action(async (itemId, newName) => {
    const da = await ensureDriveLoggedIn();
    await driveRenameCommand(da, itemId, newName);
  });

drive
  .command('move <item-id>')
  .description(
    'move a file or folder in My Drive (non-owned shared files are copied instead)',
  )
  .requiredOption('--to <folder-id>', 'destination folder ID')
  .action(async (itemId, opts) => {
    const da = await ensureDriveLoggedIn();
    await driveMoveCommand(da, itemId, opts.to);
  });

// drive-mount — Phase 1 FIX4: rewired to Colab backend's ephemeral
// credentials-propagation flow. Requires a registered colab account
// (active or via --account) so we can resolveTarget in THIS account's
// servers.json (natural cross-account enforcement via lookup failure).
// No separate "drive-mount login/logout/status" subcommands — there
// are no persistent local mount creds anymore (the daemon gets creds
// propagated by Colab backend each time the kernel calls
// google.colab.drive.mount()).
program
  .command('drive-mount')
  .description('mount the active account\'s Google Drive on a runtime via the ephemeral credentials-propagation flow')
  .option('-e, --endpoint <endpoint>', 'runtime endpoint (default: most recent runtime for this account)')
  .action(async (opts) => {
    await ensureLoggedIn();
    const { runtime, client } = getActive();
    await driveMountCommand(runtime, client, { endpoint: opts.endpoint });
  });

// Graceful shutdown (daemons are independent processes and keep running)
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));

// Run
program.parseAsync().catch((err) => {
  if (isJsonMode() && err instanceof AuthConsentError) {
    jsonResult({ error: 'consent_required', authType: err.authType, url: err.url });
  } else if (isJsonMode()) {
    jsonError(err instanceof Error ? err.message : String(err));
  } else {
    console.error(`Error: ${err instanceof Error ? err.message : String(err)}`);
  }
  log.debug(err instanceof Error ? err.stack : undefined);
  process.exit(1);
});
