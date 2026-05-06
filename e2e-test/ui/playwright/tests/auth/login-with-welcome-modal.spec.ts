/**
 * Login with Welcome Modal tests — verify the post-login onboarding modal.
 *
 * Uses login-test for a fresh, unauthenticated context each test.
 * loginPage is provided by the fixture. WelcomeModalPage is constructed
 * directly with logger so modal interactions are logged.
 */

import { test, expect } from '../../fixtures/login-test';
import { WelcomeModalPage } from '../../pages/welcome-modal.page';
import { users } from '../../data/users';

test.describe('Login with Welcome Modal', () => {
  let welcomeModalPage: WelcomeModalPage;

  test.beforeEach(async ({ page, loginPage, logger, logDir }) => {
    welcomeModalPage = new WelcomeModalPage(page, logger, logDir);
    const { username, password } = users.admin;

    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    // Navigate directly to /login — going to / while unauthenticated triggers a
    // JS redirect before load fires, which Playwright sees as ERR_ABORTED.
    await loginPage.navigateToLogin();
    await loginPage.login(username, password);
  });

  test('logs in and displays welcome modal for first-time users', async ({ page }) => {
    await welcomeModalPage.expectModalVisible();
    await expect(welcomeModalPage.modalTitle).toBeVisible();
    await welcomeModalPage.closeViaButton();

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });

  test('logs in and closes welcome modal via Get Started button', async ({ page }) => {
    await welcomeModalPage.expectModalVisible();
    await welcomeModalPage.clickLastCarouselDot();
    await welcomeModalPage.expectFinalSlideVisible();
    await welcomeModalPage.closeViaGetStarted();

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });

  test('logs in and closes welcome modal via Escape key', async ({ page }) => {
    await welcomeModalPage.expectModalVisible();
    await welcomeModalPage.closeViaEscape();

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });
});
