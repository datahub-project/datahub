import { test as base, expect } from '@playwright/test';
import { LoginPage } from '../../pages/LoginPage';
import { GraphQLHelper } from '../../helpers/GraphQLHelper';

/**
 * Login V2 tests - These tests verify login with Theme V2
 * Therefore, they MUST NOT use the shared authentication state
 */
const test = base.extend<{
  loginPage: LoginPage;
  graphqlHelper: GraphQLHelper;
}>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
  graphqlHelper: async ({ page }, use) => {
    await use(new GraphQLHelper(page));
  },
});

// Explicitly disable shared auth state for login tests
test.use({ storageState: { cookies: [], origins: [] } });

test.describe('Login with Theme V2', () => {
  test.beforeEach(async ({ page, graphqlHelper }) => {
    await graphqlHelper.setThemeV2Enabled(true);

    await page.addInitScript(() => {
      localStorage.setItem('isThemeV2Enabled', 'false');
      localStorage.setItem('showHomePageRedesign', 'false');
      localStorage.setItem('isNavBarRedesignEnabled', 'false');
      localStorage.setItem('skipAcrylIntroducePage', 'true');
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  });

  test('logs in successfully with Theme V2 enabled', async ({ page, loginPage }) => {
    await page.goto('/');

    await loginPage.login('datahub', 'datahub');

    await expect(page.getByRole('button', { name: 'Discover' })).toBeVisible();
  });
});
