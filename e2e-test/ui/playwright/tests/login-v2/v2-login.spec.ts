/**
 * Login V2 tests — verify the login flow with Theme V2 enabled.
 *
 * Uses login-test (not base-test) so every test starts with a fresh,
 * unauthenticated context. The loginPage fixture is provided automatically.
 *
 * Theme V2 flags are intercepted via apiMock.setFeatureFlags so the flag
 * override lives in the fixture infrastructure — no ad-hoc page.route() calls
 * or GraphQLHelper needed.
 */

import { test, expect } from '../../fixtures/login-test';
import { users } from '../../data/users';

test.describe('Login with Theme V2', () => {
  test.beforeEach(async ({ apiMock, page }) => {
    // Force Theme V2 on via route interception — no GraphQLHelper required.
    await apiMock.setFeatureFlags({
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });

    await page.addInitScript(() => {
      localStorage.setItem('isThemeV2Enabled', 'false');
      localStorage.setItem('showHomePageRedesign', 'false');
      localStorage.setItem('isNavBarRedesignEnabled', 'false');
      localStorage.setItem('skipAcrylIntroducePage', 'true');
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  });

  test('logs in successfully with Theme V2 enabled', async ({ page, loginPage }) => {
    const { username, password } = users.admin;
    await page.goto('/');
    await loginPage.login(username, password);
    await expect(page.getByRole('button', { name: 'Discover', exact: true })).toBeVisible();
  });
});
