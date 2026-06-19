/**
 * Login fixture — per-worker authenticated context with state file caching.
 *
 * This is the KEY fixture that replaces the auth.setup.ts global project.
 * It handles authentication dynamically at test-worker level:
 *
 *   1. Reads the `user` option (injectable per suite via test.use).
 *   2. Checks if a state file (.auth/{username}.json) already exists on disk.
 *   3a. State EXISTS  → creates a browser context loaded with that state.
 *       The test starts already authenticated — no login UI interaction.
 *   3b. State MISSING → creates a fresh context, drives the LoginPage to log
 *       in, then saves the resulting storageState to disk.
 *       Subsequent tests (same or different workers) will find the file and
 *       skip the login step automatically.
 *
 * The built-in `context` fixture is overridden so that `page` (which Playwright
 * derives from context) inherits the authenticated session transparently.
 * Tests never need to call loginPage.login() themselves.
 *
 * Parallel execution note:
 *   Two workers using the same user may rarely hit a race condition when both
 *   enter the "state missing" branch simultaneously. In practice this is
 *   harmless (both write the same valid state), but a file lock can be added
 *   later if needed.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * User injection example (set at describe level, not inside tests):
 *
 *   import { test } from '../../fixtures/base-test';
 *   import { users } from '../../data/users';
 *
 *   test.describe('Reader access', () => {
 *     test.use({ user: users.reader });   // ← enforce via fixture
 *     test('can view datasets', async ({ page }) => { ... });
 *   });
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Login tests (tests that verify the login UI itself) must NOT use this
 * fixture — use login-test.ts instead, which starts with a clean context.
 */

import * as fs from 'fs';
import * as path from 'path';
import { test as base } from '@playwright/test';
import { users, type UserCredentials } from '../data/users';
import { LoginPage } from '../pages/login.page';
import { gmsTokenPath } from './login';
import type { DataHubLogger } from '../utils/logger';

// ── Fixture types ─────────────────────────────────────────────────────────────

type LoginFixtureOptions = {
  /**
   * The user whose auth state is loaded for this test.
   * Set at describe level: test.use({ user: resolvedUsers.reader })
   * Defaults to the admin user.
   */
  user: UserCredentials;
};

type LoginFixtures = {
  // logger comes from loggerFixture (merged in base-test.ts)
  logger: DataHubLogger;
};

// ── Auth file path helpers ────────────────────────────────────────────────────

const AUTH_DIR = path.join(__dirname, '../.auth');

function stateFilePath(username: string): string {
  return path.join(AUTH_DIR, `${username}.json`);
}

/**
 * Returns true if the saved state file contains at least one non-expired
 * session cookie. Playwright stores cookies with an `expires` field that is
 * a Unix timestamp (-1 means session-only). We treat any cookie whose
 * expiry is in the past as a stale session.
 *
 * This prevents the fixture from loading a saved state whose session cookies
 * have expired, which would cause tests to land on the login page instead of
 * the expected authenticated page.
 */
function isStateFileValid(stateFile: string): boolean {
  try {
    const raw = fs.readFileSync(stateFile, 'utf-8');
    const state = JSON.parse(raw) as { cookies?: Array<{ name: string; expires: number }> };
    const cookies = state.cookies ?? [];

    // A state without the DataHub auth cookie is from an incomplete or failed login.
    const actorCookie = cookies.find((c) => c.name === 'actor');
    if (!actorCookie) return false;

    const now = Date.now() / 1000;
    // Reject if the actor cookie has an explicit expiry that has passed.
    // Session cookies (expires === -1) are kept — server validates them.
    if (actorCookie.expires > 0 && actorCookie.expires < now) return false;

    return true;
  } catch {
    return false;
  }
}

// ── Fixture implementation ────────────────────────────────────────────────────

export const loginFixture = base.extend<LoginFixtures, LoginFixtureOptions>({
  // ── Option: injectable user (worker-scoped so all tests in a worker share) ──
  user: [users.admin, { option: true, scope: 'worker' }],

  // ── Override context: authenticated or fresh depending on state file ─────────
  // Because `page` is derived from `context`, every test automatically
  // receives an authenticated page without any explicit login step.
  context: async ({ user, browser, playwright, logger }, use) => {
    const stateFile = stateFilePath(user.username);

    if (fs.existsSync(stateFile) && isStateFileValid(stateFile)) {
      // Fast path: reuse saved session — no browser interaction needed.
      logger.info('reusing auth state', { user: user.username, stateFile });
      const ctx = await browser.newContext({
        storageState: stateFile,
        baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
      });
      await use(ctx);
      await ctx.close();
      return;
    }

    // Slow path: either the state file does not exist or the session cookies
    // have expired. Perform a fresh UI login and save the new state.
    if (fs.existsSync(stateFile)) {
      logger.warn('auth state has expired — re-authenticating', { user: user.username, stateFile });
      fs.unlinkSync(stateFile);
    } else {
      logger.info('auth state not found — logging in via UI', { user: user.username });
    }
    fs.mkdirSync(AUTH_DIR, { recursive: true });

    const ctx = await browser.newContext({
      baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
    });
    const page = await ctx.newPage();

    try {
      const loginPage = new LoginPage(page, logger);
      await loginPage.navigateToLogin();
      await loginPage.login(user.username, user.password);

      // Suppress onboarding UI for all tests that reuse this state.
      await page.evaluate(() => {
        localStorage.setItem('skipWelcomeModal', 'true');
        localStorage.setItem('skipOnboardingTour', 'true');
      });

      // Persist the authenticated session so subsequent tests skip login.
      await ctx.storageState({ path: stateFile });
      logger.info('auth state saved', { user: user.username, stateFile });

      // Mint a GMS personal access token using the live session cookies so
      // that API-level fixtures (seeding, cleanup) don't need a separate
      // auth-setup project to run first.
      const tokenFile = gmsTokenPath(user.username);
      const cookies = await ctx.cookies();
      const actorCookie = cookies.find((c) => c.name === 'actor');
      if (actorCookie) {
        const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
        const baseURL = process.env.BASE_URL ?? 'http://localhost:9002';
        const apiCtx = await playwright.request.newContext({
          baseURL,
          extraHTTPHeaders: { Cookie: cookieHeader },
        });
        try {
          const resp = await apiCtx.post('/api/v2/graphql', {
            data: {
              query: `mutation createAccessToken($input: CreateAccessTokenInput!) {
                createAccessToken(input: $input) { accessToken metadata { id } }
              }`,
              variables: {
                input: {
                  type: 'PERSONAL',
                  actorUrn: actorCookie.value,
                  duration: 'ONE_MONTH',
                  name: `Playwright Test Token — ${user.username}`,
                },
              },
            },
          });
          if (resp.ok()) {
            const body = (await resp.json()) as {
              data?: { createAccessToken?: { accessToken?: string; metadata?: { id?: string } } };
            };
            const token = body.data?.createAccessToken?.accessToken;
            const tokenId = body.data?.createAccessToken?.metadata?.id;
            if (token) {
              fs.writeFileSync(tokenFile, JSON.stringify({ token, tokenId, actorUrn: actorCookie.value }, null, 2));
              logger.info('GMS token saved', { user: user.username, tokenFile });
            }
          }
        } finally {
          await apiCtx.dispose();
        }
      }
    } catch (err) {
      logger.error('login failed', { user: user.username, error: String(err) });
      throw err;
    } finally {
      await page.close();
    }

    await use(ctx);
    await ctx.close();
  },
});

export type { UserCredentials };
