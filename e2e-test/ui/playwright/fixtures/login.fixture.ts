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
 *   import { resolvedUsers } from '../../fixtures/users';
 *
 *   test.describe('Reader access', () => {
 *     test.use({ user: resolvedUsers.reader });   // ← enforce via fixture
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
import { resolvedUsers, type UserCredentials } from './users';
import { LoginPage } from '../pages/login-page';
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

// ── Fixture implementation ────────────────────────────────────────────────────

export const loginFixture = base.extend<LoginFixtures, LoginFixtureOptions>({
  // ── Option: injectable user (worker-scoped so all tests in a worker share) ──
  user: [resolvedUsers.admin, { option: true, scope: 'worker' }],

  // ── Override context: authenticated or fresh depending on state file ─────────
  // Because `page` is derived from `context`, every test automatically
  // receives an authenticated page without any explicit login step.
  context: async ({ user, browser, logger }, use) => {
    const stateFile = stateFilePath(user.username);

    if (fs.existsSync(stateFile)) {
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

    // Slow path: perform UI login and save state for future workers/runs.
    logger.info('auth state not found — logging in via UI', { user: user.username });
    fs.mkdirSync(AUTH_DIR, { recursive: true });

    const ctx = await browser.newContext({
      baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
    });
    const page = await ctx.newPage();

    try {
      const loginPage = new LoginPage(page, logger);
      await loginPage.navigateToLogin();
      await loginPage.login(user.username, user.password);

      // Persist the authenticated session so subsequent tests skip login.
      await ctx.storageState({ path: stateFile });
      logger.info('auth state saved', { user: user.username, stateFile });
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
