import fs from 'fs';
import { OAuth2Client } from 'google-auth-library';
import { GaxiosError } from 'gaxios';
import {
  accountDir,
  accountDriveAuthFile,
  DRIVE_CLIENT_ID,
  DRIVE_CLIENT_SECRET,
  DRIVE_SCOPES,
} from '../config.js';
import { runLoopbackFlow } from '../auth/loopback-flow.js';
import { log } from '../logging/index.js';
import { notifyAuthUrl } from '../output/json-output.js';

const REFRESH_MARGIN_MS = 5 * 60 * 1000;

interface DriveSession {
  refreshToken: string;
  clientId: string;
  email?: string;
}

export class DriveAuthManager {
  private oAuth2Client: OAuth2Client;
  private accessToken?: string;
  // Phase 1 FIX4: accountId is mutable. Constructor accepts a placeholder
  // ('__pending__') for the bootstrap path (drive login with no colab account
  // registered yet); `ensureAuthorized()` overwrites it with the real email
  // discovered from OAuth userinfo before any file I/O so the per-account
  // directory & file land on the true Drive owner's path.
  private accountId: string;

  constructor(accountId: string) {
    if (!accountId) throw new Error('DriveAuthManager requires accountId');
    this.accountId = accountId;
    this.oAuth2Client = new OAuth2Client(DRIVE_CLIENT_ID, DRIVE_CLIENT_SECRET);
  }

  getAccountId(): string {
    return this.accountId;
  }

  async ensureAuthorized(): Promise<void> {
    // Try loading stored credentials
    const stored = this.loadStored();
    if (stored) {
      // Detect client ID mismatch — re-authorize if user switched credentials
      if (stored.clientId !== DRIVE_CLIENT_ID) {
        log.warn(`Drive OAuth client changed (stored: ${stored.clientId.slice(0, 12)}…, current: ${DRIVE_CLIENT_ID.slice(0, 12)}…), re-authorizing...`);
        this.removeStored();
      } else {
        this.oAuth2Client.setCredentials({
          refresh_token: stored.refreshToken,
          token_type: 'Bearer',
        });
        try {
          await this.oAuth2Client.refreshAccessToken();
          this.accessToken = this.oAuth2Client.credentials.access_token!;
          return;
        } catch (err) {
          if (isInvalidGrantError(err)) {
            log.warn('Drive credentials expired, re-authorizing...');
            this.removeStored();
          } else {
            throw err;
          }
        }
      }
    }

    // No stored credentials or refresh failed — run OAuth flow
    console.error('Google Drive authorization required.');
    console.error('This uses a separate login from your Colab account.\n');

    await runLoopbackFlow(
      this.oAuth2Client,
      [...DRIVE_SCOPES],
      (url) => {
        notifyAuthUrl('Drive authorization', url);
      },
    );

    const accessToken = this.oAuth2Client.credentials.access_token;
    const refreshToken = this.oAuth2Client.credentials.refresh_token;
    if (!accessToken || !refreshToken) {
      throw new Error('Failed to obtain Drive credentials');
    }

    this.accessToken = accessToken;
    const email = await this.fetchEmail(accessToken);
    // Adopt the userinfo email as the authoritative accountId. This makes
    // `colab drive login` work even before any colab account is registered
    // (bootstrap path), and also catches accidental cross-account OAuth
    // (user runs `--account A@gmail.com` but signs in as B in browser): we
    // honor the real Drive owner rather than the (wrong) hint, storing at
    // the canonical `accounts/<real-email>/drive-auth.json` path.
    if (email && email !== this.accountId) {
      if (this.accountId !== '__pending__') {
        log.warn(
          `Drive login: hint account ${this.accountId} does not match OAuth userinfo email ${email}; ` +
          `using ${email} as the canonical account.`,
        );
      }
      this.accountId = email;
    }
    this.storeCredentials({ refreshToken, clientId: DRIVE_CLIENT_ID, email });
  }
  async getAccessToken(): Promise<string> {
    if (!this.accessToken) {
      await this.ensureAuthorized();
    }
    await this.refreshIfNeeded();
    return this.accessToken!;
  }

  private async refreshIfNeeded(): Promise<void> {
    const expiryDateMs = this.oAuth2Client.credentials.expiry_date;
    if (expiryDateMs && expiryDateMs > Date.now() + REFRESH_MARGIN_MS) {
      return;
    }
    await this.oAuth2Client.refreshAccessToken();
    this.accessToken = this.oAuth2Client.credentials.access_token!;
  }

  isAuthorized(): boolean {
    return !!this.loadStored();
  }

  getEmail(): string | undefined {
    return this.loadStored()?.email;
  }

  async logout(): Promise<void> {
    const stored = this.loadStored();
    if (stored) {
      try {
        await this.oAuth2Client.revokeToken(stored.refreshToken);
      } catch {
        // Token may already be expired/revoked
      }
    }
    this.removeStored();
    this.accessToken = undefined;
    this.oAuth2Client.setCredentials({});
  }

  private loadStored(): DriveSession | undefined {
    try {
      const file = accountDriveAuthFile(this.accountId);
      if (!fs.existsSync(file)) return undefined;
      const data = JSON.parse(fs.readFileSync(file, 'utf-8'));
      if (typeof data.refreshToken === 'string') {
        return { refreshToken: data.refreshToken, clientId: data.clientId ?? '', email: data.email };
      }
      return undefined;
    } catch {
      return undefined;
    }
  }

  private storeCredentials(session: DriveSession): void {
    fs.mkdirSync(accountDir(this.accountId), { recursive: true });
    fs.writeFileSync(accountDriveAuthFile(this.accountId), JSON.stringify(session, null, 2), { mode: 0o600 });
  }

  private async fetchEmail(token: string): Promise<string | undefined> {
    try {
      const res = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!res.ok) return undefined;
      const data = await res.json() as { email?: string };
      return data.email;
    } catch {
      return undefined;
    }
  }

  private removeStored(): void {
    try { fs.unlinkSync(accountDriveAuthFile(this.accountId)); } catch { /* ignore */ }
  }
}

function isInvalidGrantError(err: unknown): boolean {
  return (
    err instanceof GaxiosError &&
    err.status === 400 &&
    err.message.includes('invalid_grant')
  );
}
