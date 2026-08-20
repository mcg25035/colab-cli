import os from 'os';
import path from 'path';

export const COLAB_API_DOMAIN = 'https://colab.research.google.com';
export const COLAB_GAPI_DOMAIN = 'https://colab.pa.googleapis.com';

// OAuth2 credentials extracted from VS Code Colab extension
export const OAUTH_CLIENT_ID = process.env.COLAB_CLIENT_ID ?? '1014160490159-cvot3bea7tgkp72a4m29h20d9ddo6bne.apps.googleusercontent.com';
export const OAUTH_CLIENT_SECRET = process.env.COLAB_CLIENT_SECRET ?? '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';

export const REQUIRED_SCOPES = [
  'profile',
  'email',
  'https://www.googleapis.com/auth/colaboratory',
] as const;

// Drive uses a separate OAuth client (rclone's public credentials)
// because the Colab extension's OAuth client doesn't have Drive API access.
export const DRIVE_CLIENT_ID = process.env.COLAB_DRIVE_CLIENT_ID ?? '202264815644.apps.googleusercontent.com';
export const DRIVE_CLIENT_SECRET = process.env.COLAB_DRIVE_CLIENT_SECRET ?? 'X4Z3ca8xfWDb1Voo-F9a7ZxJ';
export const DRIVE_SCOPES = [
  'email',
  'https://www.googleapis.com/auth/drive',
] as const;

export const CONFIG_DIR = path.join(os.homedir(), '.config', 'colab-cli');

// Drive REST (colab drive *) — per-account credentials. MurphyLo originally
// stored these in a global file that got overwritten when multiple Google
// accounts each did `drive login`. Phase 1 FIX4 puts them under the
// accountDir so N accounts can keep N independent Drive refresh tokens.
export function accountDriveAuthFile(accountId: string): string {
  return path.join(accountDir(accountId), 'drive-auth.json');
}

// Legacy global Drive auth path (kept for migration detection only).
export const LEGACY_DRIVE_AUTH_FILE = path.join(CONFIG_DIR, 'drive-auth.json');

// --- Multi-account paths -----------------------------------------------------
//
// Layout (new in Phase 1):
//   ~/.config/colab-cli/
//     accounts/
//       registry.json                list of accounts + active pointer
//       <email>/
//         auth.json                  refresh token for this account
//         servers.json               runtimes owned by this account
//         drive-auth.json            refresh token for Google Drive REST API
//                                    (per-account since FIX4; legacy global
//                                    drive-auth.json auto-migrated to here)
//         daemon-<serverId>.sock     per-runtime daemon socket (may fall back
//                                    to $XDG_RUNTIME_DIR/colab-cli/ when the
//                                    accountDir overflows Linux AF_UNIX
//                                    sun_path[108] cap — see FIX3)
//         daemon-<serverId>.pid      ...
//         daemon-<serverId>.log      ...
//         daemon-<serverId>.lock     ...
//         exec-logs-<serverId>/      background exec NDJSON logs
//         outputs/                   saved image outputs for this account
//     drive-uploads/                 shared Drive upload scratch dir
//     next-shell-id                 shared shell id counter
//
// Legacy single-account layout (auto-migrated on first run):
//   ~/.config/colab-cli/auth.json
//   ~/.config/colab-cli/servers.json
//   ~/.config/colab-cli/drive-auth.json   (FIX4: migrated to accounts/<email>/)
//   ~/.config/colab-cli/daemon-<serverId>.sock/.pid/.log/.lock
//   ~/.config/colab-cli/exec-logs-<serverId>/
//   ~/.config/colab-cli/outputs/

export const ACCOUNTS_DIR = path.join(CONFIG_DIR, 'accounts');
export const ACCOUNTS_REGISTRY_FILE = path.join(ACCOUNTS_DIR, 'registry.json');

// Legacy paths (kept for migration detection; do NOT write to these post-migration).
export const LEGACY_AUTH_FILE = path.join(CONFIG_DIR, 'auth.json');
export const LEGACY_SERVERS_FILE = path.join(CONFIG_DIR, 'servers.json');

// Paths that remain GLOBAL across accounts (Drive-specific things were
// moved per-account in Phase 1 FIX4; these have no per-account need).
//   drive-uploads/      scratch dir for chunked Drive uploads (uses rclone
//                       OAuth client, but scratch files are user/process
//                       scoped, not account-scoped)
//   next-shell-id       shell id counter (shell sessions are scope by
//                       daemon per-runtime anyway; counter is just a seed)
export const DRIVE_UPLOADS_DIR = path.join(CONFIG_DIR, 'drive-uploads');
export const SHELL_COUNTER_FILE = path.join(CONFIG_DIR, 'next-shell-id');

// Filesystem-safe account id (= email). `@` and `.` are valid in POSIX paths;
// we keep the raw email so the directory name is human-inspectable.
function safeAccountDir(email: string): string {
  if (!email) throw new Error('accountId (email) must be provided');
  // Reject path-traversal attempts defensively; all callers should already
  // pull email straight from Google userinfo so this is belt-and-braces.
  if (email.includes('/') || email.includes('..') || email.includes('\0')) {
    throw new Error(`Invalid account email for path: ${JSON.stringify(email)}`);
  }
  return email;
}

export function accountDir(accountId: string): string {
  return path.join(ACCOUNTS_DIR, safeAccountDir(accountId));
}

export function accountAuthFile(accountId: string): string {
  return path.join(accountDir(accountId), 'auth.json');
}

export function accountServersFile(accountId: string): string {
  return path.join(accountDir(accountId), 'servers.json');
}

// Linux limits AF_UNIX sun_path to 108 bytes; bind() silently truncates a
// path that overflows it, leaving a stale socket inode whose truncated name
// doesn't match the string that `fs.chmodSync` / `fs.unlinkSync` operate on.
// Phase 1.10 caught exactly this regression on
//   /home/.../colab-cli/accounts/<email>/daemon-<uuid>.sock  (111 bytes)
// → kernel truncated to 108 bytes ending `.s`, then `chmod .sock` ENOENT,
// subsequent spawn saw stale `.s` and `listen EADDRINUSE`. Other daemon
// files (.pid/.log/.lock/exec-logs/) are not socket-bound and have no such
// cap; they always stay in accountDir for human-inspectable bookkeeping.
const UNIX_SOCK_PATH_MAX = 100; // safety margin under hard 108-byte cap

export function accountSocketPath(accountId: string, serverId: string): string {
  const candidate = path.join(accountDir(accountId), `daemon-${serverId}.sock`);
  if (candidate.length <= UNIX_SOCK_PATH_MAX) return candidate;
  // Fall back to a shorter per-user runtime dir ($XDG_RUNTIME_DIR is the
  // canonical Linux per-user runtime dir — typically /run/user/<uid>/;
  // os.tmpdir() as a last resort). We keep the full UUID in the filename
  // for debuggability (collision-free across accounts since UUIDs are
  // globally unique) — the account owner is recoverable via the per-account
  // servers.json (which maps serverId → endpoint via StoredServer.accountId).
  const fallbackDir = process.env.XDG_RUNTIME_DIR ?? os.tmpdir();
  return path.join(fallbackDir, 'colab-cli', `daemon-${serverId}.sock`);
}

export function accountPidPath(accountId: string, serverId: string): string {
  return path.join(accountDir(accountId), `daemon-${serverId}.pid`);
}

export function accountLogPath(accountId: string, serverId: string): string {
  return path.join(accountDir(accountId), `daemon-${serverId}.log`);
}

export function accountLockPath(accountId: string, serverId: string): string {
  return path.join(accountDir(accountId), `daemon-${serverId}.lock`);
}

export function accountExecLogsDir(accountId: string, serverId: string): string {
  return path.join(accountDir(accountId), `exec-logs-${serverId}`);
}

export function accountOutputsDir(accountId: string, serverId: string): string {
  return path.join(accountDir(accountId), 'outputs', serverId);
}
