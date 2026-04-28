/**
 * Login test fixture — base for tests that exercise the login UI itself.
 *
 * Composes loggerFixture and mockingFixture (no loginFixture) so that:
 *   - Every test starts with a fresh, unauthenticated browser context.
 *   - Structured logging is available (auto-use from loggerFixture).
 *   - API mocking is available for intercepting auth responses if needed.
 *   - loginPage is provided as a ready-to-use fixture — tests receive
 *     the LoginPage object directly and drive the login flow themselves.
 *
 * Why no loginFixture here:
 *   Tests that verify the login flow need to control authentication
 *   themselves — wrong credentials, SSO flows, error states, etc.
 *   The loginFixture would bypass all of that by auto-loading saved state.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Usage:
 *
 *   import { test, expect } from '../../fixtures/login-test';
 *
 *   test('shows error for bad credentials', async ({ loginPage, apiMock }) => {
 *     await loginPage.navigateToLogin();
 *     await loginPage.login('wrong', 'password');
 *     await expect(loginPage.errorMessage).toBeVisible();
 *   });
 *
 *   // Multi-user login test:
 *   const users = [resolvedUsers.admin, resolvedUsers.reader];
 *   users.forEach((user) => {
 *     test(`${user.username} can log in`, async ({ loginPage }) => {
 *       await loginPage.navigateToLogin();
 *       await loginPage.login(user.username, user.password);
 *     });
 *   });
 * ─────────────────────────────────────────────────────────────────────────────
 */

import { mergeTests } from '@playwright/test';
import { loggerFixture } from './logger.fixture';
import { mockingFixture } from './mocking.fixture';
import { LoginPage } from '../pages/login.page';

// ── Compose logger + mocking (no auth) ───────────────────────────────────────

const composedTest = mergeTests(loggerFixture, mockingFixture);

// ── Extend with a clean unauthenticated context and loginPage ─────────────────

type LoginTestFixtures = {
  loginPage: LoginPage;
};

export const test = composedTest.extend<LoginTestFixtures>({
  // Override context so every login test starts with no session cookies.
  // This ensures login tests cannot accidentally rely on a saved auth state.
  context: async ({ browser }, use) => {
    const ctx = await browser.newContext({
      storageState: undefined,
      baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
    });
    await use(ctx);
    await ctx.close();
  },

  // Provide loginPage directly so tests don't need to construct it manually.
  // logger is auto-injected by loggerFixture and passed through to LoginPage.
  loginPage: async ({ page, logger }, use) => {
    await use(new LoginPage(page, logger));
  },
});

export { expect } from '@playwright/test';
export type { ApiMocker } from './mocking.fixture';
