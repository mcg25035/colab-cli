import { OAuth2Client } from 'google-auth-library';
import { GaxiosError } from 'gaxios';
import { v4 as uuid } from 'uuid';
import { z } from 'zod';
import { OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, REQUIRED_SCOPES } from '../config.js';
import { AUTHORIZATION_HEADER } from '../colab/headers.js';
import { log } from '../logging/index.js';
import {
  getStoredSession,
  storeSession,
  removeStoredSession,
  RefreshableSession,
} from './storage.js';
import { runLoopbackFlow } from './loopback-flow.js';
import {
  registerAccount,
  getActiveAccountEmail,
  setActiveAccountEmail,
  unregisterAccount,
  pickSuccessorAccountEmail,
} from './accounts-registry.js';

const REFRESH_MARGIN_MS = 5 * 60 * 1000; // 5 minutes

const GoogleUserInfoSchema = z.object({
  name: z.string(),
  email: z.string(),
});

/**
 * AuthManager is bound to a single account (by email). Multiple accounts in
 * the same process each get their own AuthManager instance. The CLI top level
 * maintains a per-account cache; cluster scheduler builds one per pool slot.
 *
 * `accountId` is required in the constructor so the manager knows which
 * per-account file to read. Use `getActiveAccountEmail()` at the CLI top
 * level to resolve it before constructing the manager.
 */
export class AuthManager {
  private oAuth2Client: OAuth2Client;
  private accountId: string;
  private session?: { id: string; accessToken: string; account: { id: string; label: string }; scopes: string[] };

  constructor(accountId: string) {
    if (!accountId) throw new Error('AuthManager requires accountId');
    this.oAuth2Client = new OAuth2Client(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET);
    this.accountId = accountId;
  }

  getAccountId(): string {
    return this.accountId;
  }

  async initialize(): Promise<void> {
    const stored = getStoredSession(this.accountId);
    if (!stored) {
      return;
    }

    this.oAuth2Client.setCredentials({
      refresh_token: stored.refreshToken,
      token_type: 'Bearer',
      scope: stored.scopes.join(' '),
    });

    try {
      await this.oAuth2Client.refreshAccessToken();
    } catch (err) {
      if (isInvalidGrantError(err) || isOAuthClientSwitchedError(err)) {
        log.warn(`Stored credentials for ${this.accountId} are invalid, clearing session.`);
        removeStoredSession(this.accountId);
        return;
      }
      throw err;
    }

    const accessToken = this.oAuth2Client.credentials.access_token;
    if (!accessToken) {
      throw new Error('Failed to refresh Google OAuth token.');
    }

    this.session = {
      id: stored.id,
      accessToken,
      account: stored.account,
      scopes: stored.scopes,
    };
  }

  isLoggedIn(): boolean {
    return this.session !== undefined;
  }

  getAccount(): { id: string; label: string } | undefined {
    return this.session?.account;
  }

  async getAccessToken(): Promise<string> {
    if (!this.session) {
      throw new Error(`Not logged in as ${this.accountId}. Run \`colab auth login\` first.`);
    }
    await this.refreshIfNeeded();
    return this.session.accessToken;
  }

  /**
   * Login flow. Adds this Google account to the registry. If the email is
   * already registered, the refresh token is updated and the account is
   * marked active.
   */
  async login(onAuthUrl?: (url: string) => void): Promise<{ name: string; email: string }> {
    const scopes = [...REQUIRED_SCOPES].sort();

    await runLoopbackFlow(this.oAuth2Client, scopes, onAuthUrl);

    const accessToken = this.oAuth2Client.credentials.access_token;
    const refreshToken = this.oAuth2Client.credentials.refresh_token;
    if (!accessToken || !refreshToken) {
      throw new Error('Failed to obtain credentials');
    }

    const user = await this.getUserInfo(accessToken);
    const existing = getStoredSession(user.email);

    const newSession: RefreshableSession = {
      id: existing?.id ?? uuid(),
      refreshToken,
      account: { id: user.email, label: user.name },
      scopes,
    };
    storeSession(newSession);
    registerAccount(user.email, user.name);
    setActiveAccountEmail(user.email);

    this.accountId = user.email;
    this.session = {
      id: newSession.id,
      accessToken,
      account: newSession.account,
      scopes,
    };

    return user;
  }

  /**
   * Logout and revoke this account's tokens. Removes from registry too. If
   * this was the active account, the registry auto-picks a successor.
   */
  async logout(): Promise<void> {
    if (!this.session) {
      // Even if not loaded in-memory, clean disk + registry for accountId.
      try {
        await this.oAuth2Client.revokeCredentials();
      } catch { /* token may not be loaded */ }
      removeStoredSession(this.accountId);
      unregisterAccount(this.accountId);
      return;
    }
    try {
      await this.oAuth2Client.revokeCredentials();
    } catch {
      // Token may already be expired/revoked
    }
    removeStoredSession(this.session.account.id);
    unregisterAccount(this.session.account.id);
    this.session = undefined;
  }

  private async refreshIfNeeded(): Promise<void> {
    if (!this.session) return;
    const expiryDateMs = this.oAuth2Client.credentials.expiry_date;
    if (expiryDateMs && expiryDateMs > Date.now() + REFRESH_MARGIN_MS) {
      return;
    }
    await this.oAuth2Client.refreshAccessToken();
    const accessToken = this.oAuth2Client.credentials.access_token;
    if (!accessToken) {
      throw new Error('Failed to refresh Google OAuth token.');
    }
    this.session = { ...this.session, accessToken };
  }

  private async getUserInfo(token: string): Promise<z.infer<typeof GoogleUserInfoSchema>> {
    const response = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
      headers: { [AUTHORIZATION_HEADER.key]: `Bearer ${token}` },
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch user info: ${response.statusText}`);
    }
    return GoogleUserInfoSchema.parse(await response.json());
  }
}

function isInvalidGrantError(err: unknown): boolean {
  return (
    err instanceof GaxiosError &&
    err.status === 400 &&
    err.message.includes('invalid_grant')
  );
}

function isOAuthClientSwitchedError(err: unknown): boolean {
  return err instanceof GaxiosError && err.status === 401;
}
