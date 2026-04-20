/**
 * Login with Welcome Modal tests — verify the post-login onboarding modal.
 *
 * Uses login-test for a fresh, unauthenticated context each test.
 * loginPage is provided by the fixture. WelcomeModalPage is constructed
 * directly with logger so modal interactions are logged.
 */

import { test, expect } from '../../fixtures/login-test';
import { WelcomeModalPage } from '../../pages/welcome-modal-page';
import { resolvedUsers } from '../../fixtures/users';

test.describe('Login with Welcome Modal', () => {
  test('logs in and displays welcome modal for first-time users', async ({
    page,
    loginPage,
    logger,
    logDir,
  }) => {
    const welcomeModalPage = new WelcomeModalPage(page, logger, logDir);
    const { username, password } = resolvedUsers.admin;

    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    await page.goto('/');
    await loginPage.login(username, password);

    await welcomeModalPage.expectModalVisible();
    await welcomeModalPage.expectModalTitle();
    await welcomeModalPage.closeViaButton();

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });

  test('logs in and closes welcome modal via Get Started button', async ({
    page,
    loginPage,
    logger,
    logDir,
  }) => {
    const welcomeModalPage = new WelcomeModalPage(page, logger, logDir);
    const { username, password } = resolvedUsers.admin;

    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    await page.goto('/');
    await loginPage.login(username, password);

    await welcomeModalPage.expectModalVisible();
    await welcomeModalPage.clickLastCarouselDot();
    await welcomeModalPage.expectFinalSlideVisible();
    await welcomeModalPage.closeViaGetStarted();

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });

  test('logs in and closes welcome modal via Escape key', async ({
    page,
    loginPage,
    logger,
    logDir,
  }) => {
    const welcomeModalPage = new WelcomeModalPage(page, logger, logDir);
    const { username, password } = resolvedUsers.admin;

    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    await page.goto('/');
    await loginPage.login(username, password);

    await welcomeModalPage.expectModalVisible();
    await welcomeModalPage.closeViaEscape();

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });
});
