import { randomUUID, UUID } from 'crypto';
import {
  Assignment,
  ListedAssignment,
  Variant,
  Shape,
  variantToMachineType,
  shapeToMachineShape,
  isHighMemOnlyAccelerator,
} from '../colab/api.js';
import { ColabClient, ColabRequestError } from '../colab/client.js';
import { log } from '../logging/index.js';
import { startDaemon, stopDaemon } from '../daemon/lifecycle.js';
import {
  StoredServer,
  listStoredServers,
  storeServer,
  removeStoredServer,
} from './storage.js';

/**
 * RuntimeManager owns all runtimes belonging to a single account. Operations
 * are scoped to `accountId`: `list()` hits the global Colab API (single
 * account view from the bound ColabClient) but local storage reads/writes
 * only ever touch this account's servers.json.
 */
export class RuntimeManager {
  constructor(
    private readonly colabClient: ColabClient,
    private readonly accountId: string,
  ) {
    if (!accountId) throw new Error('RuntimeManager requires accountId');
  }

  getAccountId(): string {
    return this.accountId;
  }

  async resolveTarget(endpoint?: string): Promise<StoredServer> {
    // Phase 1.13 FIX7: brief retry loop. Even with fsync (writeServersFileSync),
    // there is still a small risk that the user (or a cluster scheduler script)
    // spawns a brand-new CLI process between the parent process exiting and the
    // filesystem metadata fully propagating to readers. We retry 3 times with
    // 500ms back-off (~1.5s total worst case); enough for any kernel page cache
    // to be visible cross-process without making a slow-runtime CLI appear to
    // hang. The previous "No local record for endpoint" wording at T11 keepalive
    // v3 race-condition user report was triggered by 5 parallel `runtime create`
    // → immediate `exec -e`, which is exactly this window.
    //
    // Path semantics (unchanged from MurphyLo baseline):
    //   - `endpoint` given: pure local lookup. User explicitly scoped; CLI
    //     must NOT ping backend or autonomously delete the entry on transient
    //     backend hiccups. Reverted from a prior FIX7 draft that added
    //     `assertServerLive` here — that was scope creep; this path stays
    //     pure-local.
    //   - no `endpoint`: fall back to "latest server" and ping backend to
    //     detect / clean up a local entry whose runtime Colab backend has
    //     reaped (idle timeout, Free tier quota churn). This stale-cleanup
    //     behavior is pre-existing MurphyLo baseline, not new in FIX7.
    if (endpoint) {
      for (let attempt = 0; attempt < 3; attempt++) {
        const server = this.getServerByEndpoint(endpoint);
        if (server) return server;
        if (attempt < 2) await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error(
        `No local record for endpoint ${endpoint}. ` +
        `If you just created this runtime, wait 1-2s and retry — the servers.json write may still be flushing. ` +
        `Otherwise run \`colab runtime list\` to see active runtimes.`,
      );
    }

    for (let attempt = 0; attempt < 3; attempt++) {
      const server = this.getLatestServer();
      if (server) {
        const assignments = await this.list();
        if (assignments.some((a) => a.endpoint === server.endpoint)) {
          return server;
        }

        await stopDaemon(this.accountId, server.id);
        removeStoredServer(this.accountId, server.id);

        const active = assignments.map((a) => a.endpoint);
        if (active.length === 0) {
          throw new Error(
            `Runtime ${server.endpoint} is no longer active (stale local record removed). No active runtimes — create one with \`colab runtime create\`.`,
          );
        }
        throw new Error(
          `Runtime ${server.endpoint} is no longer active (stale local record removed). Active runtimes: ${active.join(', ')}. Use --endpoint to specify.`,
        );
      }
      if (attempt < 2) await new Promise((r) => setTimeout(r, 500));
    }
    throw new Error(
      'No runtime found. Create one first with `colab runtime create`.',
    );
  }

  async create(options: {
    variant: Variant;
    accelerator?: string;
    shape?: Shape;
    version?: string;
    kernelName?: string;
  }): Promise<StoredServer> {
    const id = randomUUID();
    const accelerator = await this.resolveAccelerator(
      options.variant,
      options.accelerator,
    );
    const shape = this.resolveShape(options.variant, accelerator, options.shape);
    const { assignment } = await this.colabClient.assign(id, {
      variant: options.variant,
      accelerator,
      shape,
      version: options.version,
    });

    const tokenExpiry = new Date(
      Date.now() + assignment.runtimeProxyInfo.tokenExpiresInSeconds * 1000,
    );

    const assignedVariant = (assignment.variant ?? Variant.DEFAULT) as Variant;
    const server: StoredServer = {
      id,
      accountId: this.accountId,
      label: `Colab ${variantToMachineType(assignedVariant)}${
        assignment.accelerator !== 'NONE' ? ` ${assignment.accelerator}` : ''
      }`,
      variant: assignedVariant,
      accelerator: assignment.accelerator,
      endpoint: assignment.endpoint,
      proxyUrl: assignment.runtimeProxyInfo.url,
      token: assignment.runtimeProxyInfo.token,
      tokenExpiry,
      dateAssigned: new Date(),
      kernelName: options.kernelName ?? 'python3',
    };

    storeServer(server);
    await startDaemon(this.accountId, server.id);
    return server;
  }

  async destroy(endpoint: string): Promise<void> {
    const servers = listStoredServers(this.accountId);
    const server = servers.find((s) => s.endpoint === endpoint);

    try {
      await this.colabClient.unassign(endpoint);
    } catch (err) {
      if (!(err instanceof ColabRequestError && err.status === 404)) {
        throw err;
      }
      log.debug('Runtime already unassigned by backend:', endpoint);
    }

    if (server) {
      await stopDaemon(this.accountId, server.id);
      removeStoredServer(this.accountId, server.id);
    }
  }

  async list(): Promise<ListedAssignment[]> {
    return this.colabClient.listAssignments();
  }

  getLatestServer(): StoredServer | undefined {
    const servers = listStoredServers(this.accountId);
    if (servers.length === 0) return undefined;
    return servers.sort(
      (a, b) => b.dateAssigned.getTime() - a.dateAssigned.getTime(),
    )[0];
  }

  getServerByEndpoint(endpoint: string): StoredServer | undefined {
    return listStoredServers(this.accountId).find((s) => s.endpoint === endpoint);
  }

  private async resolveAccelerator(
    variant: Variant,
    accelerator?: string,
  ): Promise<string | undefined> {
    if (variant === Variant.DEFAULT) {
      return undefined;
    }

    if (accelerator) {
      return accelerator.toUpperCase();
    }

    const eligibleModels =
      this.colabClient
        .getUserInfo()
        .then(
          (userInfo) =>
            userInfo.eligibleAccelerators.find((acc) => acc.variant === variant)
              ?.models ?? [],
        );

    const model = (await eligibleModels)[0];
    if (!model) {
      throw new Error(
        `No eligible ${variantToMachineType(variant)} accelerators are available for the current account.`,
      );
    }

    log.debug(
      `Auto-selected ${variantToMachineType(variant)} accelerator: ${model}`,
    );
    return model;
  }

  private resolveShape(
    variant: Variant,
    accelerator: string | undefined,
    requestedShape: Shape | undefined,
  ): Shape | undefined {
    if (variant === Variant.DEFAULT || !accelerator) {
      return requestedShape ?? Shape.STANDARD;
    }

    if (!isHighMemOnlyAccelerator(accelerator)) {
      return requestedShape ?? Shape.STANDARD;
    }

    if (requestedShape === Shape.STANDARD) {
      throw new Error(
        `${variantToMachineType(variant)} ${accelerator} only supports ${shapeToMachineShape(Shape.HIGHMEM)} in CLI semantics. Use --shape high-ram or omit --shape.`,
      );
    }

    return Shape.HIGHMEM;
  }
}
