import { listRegistryAccounts } from '../auth/accounts-registry.js';
import { listStoredServers, type StoredServer } from '../runtime/storage.js';
import { isDaemonRunning } from '../daemon/lifecycle.js';
import { DaemonClient } from '../daemon/client.js';
import type { Job } from './state.js';

/**
 * Pool state layer: a live, read-only view across all registered accounts —
 * their stored runtimes, whether each runtime's daemon is reachable, and
 * how many shells/jobs each VM currently carries. Nothing here is persisted;
 * Colab runtimes are ephemeral, so the pool is always recomputed.
 */

export interface PoolVm {
  accountId: string;
  server: StoredServer;
  daemonAlive: boolean;
  shellCount: number;
  /** Cluster jobs in 'running' state assigned to this VM. */
  jobIds: number[];
}

export interface PoolAccount {
  accountId: string;
  name?: string;
  vms: PoolVm[];
}

export interface Pool {
  accounts: PoolAccount[];
}

export async function snapshotPool(runningJobs: Job[]): Promise<Pool> {
  const accounts: PoolAccount[] = [];
  for (const acct of listRegistryAccounts()) {
    const vms: PoolVm[] = [];
    for (const server of listStoredServers(acct.email)) {
      const alive = await isDaemonRunning(acct.email, server.id);
      let shellCount = 0;
      if (alive) {
        try {
          const client = new DaemonClient();
          await client.connect(acct.email, server.id);
          shellCount = (await client.shellList()).filter((s) => s.status !== 'closed').length;
          client.close();
        } catch {
          // daemon flapped between probe and connect — report it alive with
          // an unknown shell count
          shellCount = -1;
        }
      }
      vms.push({
        accountId: acct.email,
        server,
        daemonAlive: alive,
        shellCount,
        jobIds: runningJobs
          .filter((j) => j.accountId === acct.email && j.endpoint === server.endpoint)
          .map((j) => j.id),
      });
    }
    accounts.push({ accountId: acct.email, name: acct.name, vms });
  }
  return { accounts };
}

/**
 * Pick an idle VM for a queued job: daemon alive, and no running cluster job
 * assigned to it (one job per VM — training workloads assume uncontended
 * machines; revisit if we ever want bin-packing).
 *
 * With `accelerator`, only consider VMs whose label carries that hardware
 * (a T4 job must not land on a CPU VM). Without one, prefer non-GPU VMs so
 * plain jobs don't park on expensive accelerators, but fall back to anything.
 */
export async function pickIdleVm(
  pool: Pool,
  accelerator?: string,
): Promise<{ accountId: string; server: StoredServer } | undefined> {
  const want = accelerator?.trim().toUpperCase().replace(/[-\s]/g, '');
  let fallback: { accountId: string; server: StoredServer } | undefined;
  for (const acct of pool.accounts) {
    for (const vm of acct.vms) {
      if (!vm.daemonAlive) continue;
      if (vm.jobIds.length > 0) continue;
      const hw = vm.server.label.toUpperCase();
      if (want && want !== 'CPU') {
        if (hw.includes(want)) return { accountId: acct.accountId, server: vm.server };
        continue;
      }
      // unaccelerated job: prefer CPU/default VMs, remember GPU ones as fallback
      if (!/GPU|TPU/.test(hw)) return { accountId: acct.accountId, server: vm.server };
      fallback ??= { accountId: acct.accountId, server: vm.server };
    }
  }
  return fallback;
}
