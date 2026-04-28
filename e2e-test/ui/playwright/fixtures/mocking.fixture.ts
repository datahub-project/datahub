/**
 * Mocking fixture — opt-in API route interception for tests that need it.
 *
 * Provides the apiMock fixture backed by PageApiMocker from utils/api-mock.ts.
 * This is NOT auto-use: only tests that destructure apiMock from the fixture
 * will have route interception active, keeping clean tests unaffected.
 *
 * Compose this into base-test via mergeTests. Tests that only occasionally
 * need mocking can destructure apiMock without any additional setup.
 *
 * Usage:
 *   test('mocked flag test', async ({ apiMock, page }) => {
 *     await apiMock.setFeatureFlags({ themeV2Enabled: true });
 *     await page.goto('/');
 *   });
 */

import { test as base } from '@playwright/test';
import { PageApiMocker, type ApiMocker } from '../utils/api-mock';

type MockingFixtures = {
  /** DataHub-aware GraphQL/route interception helper. Opt-in (not auto-use). */
  apiMock: ApiMocker;
};

export const mockingFixture = base.extend<MockingFixtures>({
  // apiMock depends on page (provided by Playwright) and is initialised fresh
  // for each test. Routes registered here are automatically cleaned up when
  // the page closes at the end of the test.
  apiMock: async ({ page }, use) => {
    await use(new PageApiMocker(page));
    // Unregister all routes before the context closes to avoid "route.fetch:
    // Target page, context or browser has been closed" errors from in-flight
    // requests during test teardown.
    await page.unrouteAll({ behavior: 'ignoreErrors' });
  },
});

export type { ApiMocker };
