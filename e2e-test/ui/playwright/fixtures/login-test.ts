/**
 * Login test base fixture.
 *
 * Tests that verify the login flow itself must use this fixture instead of
 * base-test.ts. It deliberately excludes the authenticated-state capability:
 *
 *   - No storageState is loaded — every test starts with a fresh, anonymous
 *     browser context (equivalent to `storageState: { cookies: [], origins: [] }`)
 *   - No `user` option, no `gmsToken`, no per-user context override
 *   - No `cleanup` or `featureData` (login tests do not create entities)
 *
 * The structured logger and API mocker are included because login tests may
 * need to inspect or intercept network responses (e.g. mock an auth error).
 *
 * Usage:
 *
 *   import { test, expect } from '../../fixtures/login-test';
 *   import { LoginPage } from '../../pages/login-page';
 *
 *   test('shows error for bad credentials', async ({ page, apiMock }) => {
 *     const loginPage = new LoginPage(page);
 *     await loginPage.navigateToLogin();
 *     await loginPage.login('bad', 'creds');
 *     await expect(page.getByText(/Invalid credentials/i)).toBeVisible();
 *   });
 */

import * as path from 'path';
import { test as base } from '@playwright/test';
import { FileLogger, type StructuredLogger } from './logging';
import { PageApiMocker, type ApiMocker } from './api-mock';

type LoginTestFixtures = {
  logger: StructuredLogger;
  apiMock: ApiMocker;
};

export const test = base.extend<LoginTestFixtures>({
  // Ensure every login test starts with a pristine, unauthenticated context.
  context: async ({ browser }, use) => {
    const ctx = await browser.newContext({ storageState: undefined });
    await use(ctx);
    await ctx.close();
  },

  logger: async ({}, use, testInfo) => {
    const logsDir = path.join(process.cwd(), 'logs');
    const fileLogger = new FileLogger(testInfo, logsDir);
    await use(fileLogger);
    fileLogger.close();
  },

  apiMock: async ({ page }, use) => {
    await use(new PageApiMocker(page));
  },
});

export { expect } from '@playwright/test';
