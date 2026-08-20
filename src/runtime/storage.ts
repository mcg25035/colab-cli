import fs from 'fs';
import path from 'path';
import { UUID } from 'crypto';
import { z } from 'zod';
import { Variant } from '../colab/api.js';

const VARIANTS = ['DEFAULT', 'GPU', 'TPU'] as const;
import {
  CONFIG_DIR,
  LEGACY_SERVERS_FILE,
  accountServersFile,
  accountDir,
} from '../config.js';
import { isUUID } from '../utils/uuid.js';

const StoredServerSchema = z.object({
  id: z
    .string()
    .refine(isUUID)
    .transform((s) => s as UUID),
  accountId: z.string().optional(), // added in Phase 1 — legacy records have it undefined
  label: z.string(),
  variant: z.enum(VARIANTS),
  accelerator: z.string().optional(),
  endpoint: z.string(),
  proxyUrl: z.string(),
  token: z.string(),
  tokenExpiry: z.coerce.date(),
  dateAssigned: z.coerce.date(),
  kernelName: z.string().optional().default('python3'),
});

export type StoredServer = z.infer<typeof StoredServerSchema>;

function ensureAccountDir(accountId: string): void {
  fs.mkdirSync(accountDir(accountId), { recursive: true });
}

/**
 * Write `servers.json` for an account with `fsync` durability. Phase 1.13
 * FIX7: `fs.writeFileSync` returns once the kernel has copied bytes into the
 * page cache, but before that cache has been flushed to the inode — which
 * opens a small race window when another CLI process is spawned *immediately*
 * after `runtime create` exits and runs `resolveTarget(endpoint)` before the
 * fs metadata is visible cross-process. Fsyncing after the write closes the
 * window. We atomically replace the file via write-temp + rename for
 * crash-safety (a partial write never becomes the canonical servers.json).
 */
function writeServersFileSync(file: string, servers: StoredServer[]): void {
  const tmp = `${file}.tmp.${process.pid}`;
  fs.writeFileSync(tmp, JSON.stringify(servers, null, 2), { mode: 0o600 });
  // fsync the temp file so its bytes (not just metadata) hit disk before
  // the upcoming atomic rename presents it as the canonical servers.json.
  const fd = fs.openSync(tmp, 'r');
  try {
    fs.fdatasyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(tmp, file);
}

/**
 * List runtimes owned by a specific account. Records with no `accountId`
 * field are treated as belonging to whichever account's servers.json file
 * they live in (post-migration that's correct; pre-migration legacy records
 * are routed via `migrateLegacyServersIfNeeded`).
 */
export function listStoredServers(accountId: string): StoredServer[] {
  const file = accountServersFile(accountId);
  try {
    if (!fs.existsSync(file)) return [];
    const data = fs.readFileSync(file, 'utf-8');
    const parsed = z.array(StoredServerSchema).parse(JSON.parse(data));
    // Backfill `accountId` defensively on read so callers can rely on it
    // for any record that was created before this backfill ran.
    return parsed.map((s) => (s.accountId ? s : { ...s, accountId }));
  } catch {
    return [];
  }
}

/** List runtimes across ALL accounts (cluster scheduler use). */
export function listAllStoredServers(): StoredServer[] {
  const accountsRoot = path.join(CONFIG_DIR, 'accounts');
  const out: StoredServer[] = [];
  if (!fs.existsSync(accountsRoot)) return out;
  for (const entry of fs.readdirSync(accountsRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const acctDir = path.join(accountsRoot, entry.name);
    const file = path.join(acctDir, 'servers.json');
    if (!fs.existsSync(file)) continue;
    try {
      const data = fs.readFileSync(file, 'utf-8');
      const parsed = z.array(StoredServerSchema).parse(JSON.parse(data));
      for (const s of parsed) {
        out.push(s.accountId ? s : { ...s, accountId: entry.name });
      }
    } catch {
      // Skip unreadable per-account servers file
    }
  }
  return out;
}

export function getStoredServer(accountId: string, id: UUID): StoredServer | undefined {
  return listStoredServers(accountId).find((s) => s.id === id);
}

/** Look up a runtime across all accounts by serverId (cluster scheduler use). */
export function findStoredServerById(id: UUID): StoredServer | undefined {
  return listAllStoredServers().find((s) => s.id === id);
}

/** Look up a runtime across all accounts by endpoint (CLI compatibility). */
export function findStoredServerByEndpoint(endpoint: string): StoredServer | undefined {
  return listAllStoredServers().find((s) => s.endpoint === endpoint);
}

export function storeServer(server: StoredServer): void {
  if (!server.accountId) {
    throw new Error('Cannot store server: accountId is required');
  }
  ensureAccountDir(server.accountId);
  const file = accountServersFile(server.accountId);
  const existing = listStoredServers(server.accountId).filter((s) => s.id !== server.id);
  existing.push({ ...server, accountId: server.accountId });
  writeServersFileSync(file, existing);
}

export function removeStoredServer(accountId: string, id: UUID): boolean {
  const servers = listStoredServers(accountId);
  const filtered = servers.filter((s) => s.id !== id);
  if (filtered.length === servers.length) return false;
  ensureAccountDir(accountId);
  writeServersFileSync(accountServersFile(accountId), filtered);
  return true;
}

export function updateServerToken(
  accountId: string,
  id: UUID,
  token: string,
  proxyUrl: string,
  tokenExpiry: Date,
): void {
  const servers = listStoredServers(accountId);
  const server = servers.find((s) => s.id === id);
  if (!server) return;
  server.token = token;
  server.proxyUrl = proxyUrl;
  server.tokenExpiry = tokenExpiry;
  ensureAccountDir(accountId);
  writeServersFileSync(accountServersFile(accountId), servers);
}

// --- Legacy migration -------------------------------------------------------
//
// `~/.config/colab-cli/servers.json` from the single-account era has no
// `accountId` field. We cannot infer which account owns those records from
// the records themselves — but we know that at legacy time there was exactly
// one account (the one whose auth.json we just migrated). So adopt ALL legacy
// records into that account's servers.json, then delete the legacy file.
//
// No-op if no legacy file exists or if no account is registered to receive
// the records (Phase 1 login flow registers the account before this runs at
// runtime create time; for the migration-on-first-CLI-invocation path,
// migrateLegacyAccountIfNeeded runs first and registers the session).

export function migrateLegacyServersIfNeeded(activeAccountId: string): void {
  if (!fs.existsSync(LEGACY_SERVERS_FILE)) return;
  if (!activeAccountId) return;

  try {
    const data = fs.readFileSync(LEGACY_SERVERS_FILE, 'utf-8');
    const parsed = z.array(StoredServerSchema).parse(JSON.parse(data));
    if (parsed.length === 0) {
      // Empty legacy file — clean it up so we don't keep checking it.
      try { fs.unlinkSync(LEGACY_SERVERS_FILE); } catch {}
      return;
    }

    // Merge legacy records into the active account's servers.json. Backfill
    // accountId on each record. If a runtime with the same id is already
    // present (e.g. user manually copied files), the existing record wins.
    const existing = listStoredServers(activeAccountId);
    const existingIds = new Set(existing.map((s) => s.id));
    for (const s of parsed) {
      if (!existingIds.has(s.id)) {
        existing.push({ ...s, accountId: activeAccountId });
      }
    }
    ensureAccountDir(activeAccountId);
    writeServersFileSync(accountServersFile(activeAccountId), existing);

    // Remove the legacy file so this is a one-shot migration.
    try { fs.unlinkSync(LEGACY_SERVERS_FILE); } catch {}
  } catch {
    // Corrupt or unreadable legacy file — leave it untouched so the user
    // can inspect manually. Don't crash the CLI.
  }
}
