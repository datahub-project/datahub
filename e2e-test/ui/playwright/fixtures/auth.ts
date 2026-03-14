import { test as base } from '@playwright/test';
import { LoginPage } from '../pages/LoginPage';

type AuthFixtures = {
  authenticatedPage: LoginPage;
};

export const test = base.extend<AuthFixtures>({
  authenticatedPage: async ({ page }, use) => {
    const loginPage = new LoginPage(page);
    await loginPage.navigateToLogin();
    await loginPage.login(
      process.env.TEST_USERNAME || 'datahub',
      process.env.TEST_PASSWORD || 'datahub'
    );
    await use(loginPage);
  },
});

export { expect } from '@playwright/test';
