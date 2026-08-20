import http from 'http';
import https from 'https';
import { ColabClient } from '../colab/client.js';
import { createForwarder } from './forwarder.js';
import { PortTokenRefresher } from './token-refresher.js';
import type { TlsCredentials } from './tls.js';

export class ForwardSession {
  readonly startedAt = new Date();
  readonly tls: boolean;
  readonly localPort: number;

  private constructor(
    readonly id: number,
    readonly localHost: string,
    localPort: number,
    readonly remotePort: number,
    private readonly refresher: PortTokenRefresher,
    private readonly server: http.Server | https.Server,
  ) {
    // `localPort` may have been 0 (OS-assigned); record the real bound port
    // so callers can connect to it (e.g. shell bridges on 127.0.0.1).
    const address = server.address();
    this.localPort = typeof address === 'object' && address !== null ? address.port : localPort;
    this.tls = server instanceof https.Server;
  }

  get proxyUrl(): string {
    return this.refresher.proxyUrl;
  }

  static async open(
    id: number,
    localHost: string,
    localPort: number,
    remotePort: number,
    colabClient: ColabClient,
    endpoint: string,
    tls?: TlsCredentials,
  ): Promise<ForwardSession> {
    const refresher = new PortTokenRefresher(colabClient, endpoint, remotePort);
    await refresher.start();

    const server = createForwarder(refresher, tls);
    try {
      await new Promise<void>((resolve, reject) => {
        const onError = (err: Error) => {
          server.off('listening', onListening);
          reject(err);
        };
        const onListening = () => {
          server.off('error', onError);
          resolve();
        };
        server.once('error', onError);
        server.once('listening', onListening);
        server.listen(localPort, localHost);
      });
    } catch (err) {
      refresher.stop();
      throw err;
    }

    return new ForwardSession(id, localHost, localPort, remotePort, refresher, server);
  }

  async close(): Promise<void> {
    this.refresher.stop();
    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
      this.server.closeAllConnections?.();
    });
  }
}
