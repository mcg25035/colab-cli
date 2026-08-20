import fs from 'fs';
import { z } from 'zod';
import { CONFIG_DIR, LEGACY_AUTH_FILE, accountAuthFile, accountDir } from '../config.js';
import { getActiveAccountEmail } from './accounts-registry.js';

const RefreshableSessionSchema = z.object({
  id: z.string(),
  refreshToken: z.string(),
  account: z.object({
    id: z.string(), // email
    label: z.string(), // display name
  }),
  scopes: z.array(z.string()),
});

export type RefreshableSession = z.infer<typeof RefreshableSessionSchema>;

/**
 * Read a specific account's stored session.
 *
 * @param accountId  email (the unique account key). Required — there is no
 *                   implicit "active" fallback here. CLI entry points must
 *                   resolve the active account via the registry and pass it
 *                   through explicitly.
 */
export function getStoredSession(accountId: string): RefreshableSession | undefined {
  try {
    const file = accountAuthFile(accountId);
    if (!fs.existsSync(file)) return undefined;
    const data = fs.readFileSync(file, 'utf-8');
    return RefreshableSessionSchema.parse(JSON.parse(data));
  } catch {
    return undefined;
  }
}

/** Convenience: read the session for the currently-active account, if any. */
export function getActiveStoredSession(): RefreshableSession | undefined {
  const active = getActiveAccountEmail();
  if (!active) return undefined;
  return getStoredSession(active);
}

/** Only for one-time legacy migration. Reads the pre-multi-account `auth.json`. */
export function getLegacyStoredSession(): RefreshableSession | undefined {
  try {
    if (!fs.existsSync(LEGACY_AUTH_FILE)) return undefined;
    const data = fs.readFileSync(LEGACY_AUTH_FILE, 'utf-8');
    return RefreshableSessionSchema.parse(JSON.parse(data));
  } catch {
    return undefined;
  }
}

/**
 * Persist a session. The destination path is derived from `session.account.id`
 * (the email), so callers do NOT need to pass accountId. This is intentional
 * so that background-auth processes that just received tokens from Google
 * can save them without needing an out-of-band account id.
 */
export function storeSession(session: RefreshableSession): void {
  const accountId = session.account.id;
  if (!accountId) {
    throw new Error('Cannot store session: account.id (email) is empty');
  }
  fs.mkdirSync(accountDir(accountId), { recursive: true });
  fs.writeFileSync(accountAuthFile(accountId), JSON.stringify(session, null, 2), {
    mode: 0o600,
  });
}

/** Remove a specific account's credentials on disk. */
export function removeStoredSession(accountId: string): void {
  try {
    fs.unlinkSync(accountAuthFile(accountId));
  } catch {
    // File may not exist
  }
}
