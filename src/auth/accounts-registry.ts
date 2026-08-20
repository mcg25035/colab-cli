import fs from 'fs';
import { z } from 'zod';
import { ACCOUNTS_DIR, ACCOUNTS_REGISTRY_FILE, LEGACY_AUTH_FILE } from '../config.js';
import { getLegacyStoredSession, storeSession, RefreshableSession } from './storage.js';

// --- Registry schema --------------------------------------------------------

const RegistryAccountSchema = z.object({
  email: z.string(),
  name: z.string(),
  addedAt: z.string().optional(),
});
export type RegistryAccount = z.infer<typeof RegistryAccountSchema>;

const RegistrySchema = z.object({
  version: z.literal(1).default(1),
  accounts: z.array(RegistryAccountSchema).default([]),
  active: z.string().nullable().default(null),
});
export type Registry = z.infer<typeof RegistrySchema>;

// --- Public registry ops ----------------------------------------------------

function emptyRegistry(): Registry {
  return { version: 1, accounts: [], active: null };
}

function ensureAccountsDir(): void {
  fs.mkdirSync(ACCOUNTS_DIR, { recursive: true });
}

export function readRegistry(): Registry {
  try {
    if (!fs.existsSync(ACCOUNTS_REGISTRY_FILE)) {
      return emptyRegistry();
    }
    const data = fs.readFileSync(ACCOUNTS_REGISTRY_FILE, 'utf-8');
    return RegistrySchema.parse(JSON.parse(data));
  } catch (err) {
    // Corrupt registry — log and reset so the user can recover by re-logging in.
    console.warn('Warning: registry.json was unreadable, resetting:', err);
    return emptyRegistry();
  }
}

function writeRegistry(reg: Registry): void {
  ensureAccountsDir();
  fs.writeFileSync(ACCOUNTS_REGISTRY_FILE, JSON.stringify(reg, null, 2), { mode: 0o600 });
}

export function listRegistryAccounts(): RegistryAccount[] {
  return readRegistry().accounts;
}

export function getActiveAccountEmail(): string | undefined {
  const reg = readRegistry();
  return reg.active ?? undefined;
}

export function setActiveAccountEmail(email: string): void {
  const reg = readRegistry();
  if (!reg.accounts.some((a) => a.email === email)) {
    throw new Error(
      `Account ${email} is not registered. Run \`colab auth login\` first to add it.`,
    );
  }
  reg.active = email;
  writeRegistry(reg);
}

export function clearActiveAccountEmail(): void {
  const reg = readRegistry();
  reg.active = null;
  writeRegistry(reg);
}

/** Upsert account into the registry (idempotent by email). Returns updated list. */
export function registerAccount(email: string, name: string): RegistryAccount[] {
  const reg = readRegistry();
  const entry: RegistryAccount = { email, name, addedAt: new Date().toISOString() };
  const without = reg.accounts.filter((a) => a.email !== email);
  without.push(entry);
  reg.accounts = without;
  reg.active = email;
  writeRegistry(reg);
  return without;
}

/** Remove account from registry. Does NOT delete credentials on disk — caller
 * should call removeStoredSession() separately. */
export function unregisterAccount(email: string): void {
  const reg = readRegistry();
  reg.accounts = reg.accounts.filter((a) => a.email !== email);
  if (reg.active === email) {
    // Pick a successor: most recently added remaining, else null.
    reg.active = reg.accounts.length > 0
      ? reg.accounts[reg.accounts.length - 1].email
      : null;
  }
  writeRegistry(reg);
}

/** Get a successor account email after the given one was logged out, or undefined. */
export function pickSuccessorAccountEmail(excluding?: string): string | undefined {
  const reg = readRegistry();
  const candidates = reg.accounts
    .filter((a) => a.email !== excluding)
    .sort((a, b) => (a.addedAt ?? '').localeCompare(b.addedAt ?? ''));
  return candidates.length > 0 ? candidates[candidates.length - 1].email : undefined;
}

// --- One-time legacy migration ---------------------------------------------
//
// If the user has an old `~/.config/colab-cli/auth.json` from the single-account
// era, adopt it into the new layout: move it under `accounts/<email>/auth.json`,
// add the email to the registry, set it active. Idempotent — re-runs are a no-op
// as long as the legacy file has been removed by the first migration pass.
//
// The legacy `servers.json` is migrated lazily by `src/runtime/storage.ts`
// per-account because it needs the accountId column from each record's auth
// (which we synthesize from the migrated session).
//
// This function is safe to call on every CLI invocation.

export function migrateLegacyAccountIfNeeded(): void {
  if (!fs.existsSync(LEGACY_AUTH_FILE)) return;

  // Already migrated? If so, the file is stale leftover and we shouldn't
  // re-import. But it's impossible to tell "already migrated and file was
  // forgotten" vs "brand new single-account file before any login" without a
  // marker. Resolve by checking if there are already any registered accounts;
  // if yes and legacy file still exists, just delete the legacy file (log).
  const reg = readRegistry();
  if (reg.accounts.length > 0 || reg.active) {
    // Already have multi-account state. Legacy file is left over from a
    // failed migration or manual copy. Don't import again — but leave it
    // (don't delete) so the user can recover manually if needed.
    return;
  }

  // Fresh install with legacy single-account file. Import it.
  const session: RefreshableSession | undefined = getLegacyStoredSession();
  if (!session) return;

  // Defensive: avoid creating dir for an empty email.
  if (!session.account.id) return;

  // Re-store via storage layer so the per-account path is consistent. The
  // storeSession helper derives the destination from session.account.id.
  storeSession(session);

  // Register in the registry and mark active.
  registerAccount(session.account.id, session.account.label);

  // Delete the legacy file so subsequent runs are a no-op.
  try { fs.unlinkSync(LEGACY_AUTH_FILE); } catch {}

  // Note: we don't migrate servers.json here because storeServer already
  // writes per-account; runtime/storage.ts handles its own legacy migration
  // when first listing servers for the migrated account.
}
