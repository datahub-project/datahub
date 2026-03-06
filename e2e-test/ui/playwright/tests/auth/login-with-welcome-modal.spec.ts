import { test as base, expect } from '@playwright/test';
import { LoginPage } from '../../pages/LoginPage';
import { WelcomeModalPage } from '../../pages/WelcomeModalPage';

/**
 * Login with Welcome Modal tests - These tests verify login with modal functionality
 * Therefore, they MUST NOT use the shared authentication state
 */
const test = base.extend<{
  loginPage: LoginPage;
  welcomeModalPage: WelcomeModalPage;
}>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
  welcomeModalPage: async ({ page }, use) => {
    await use(new WelcomeModalPage(page));
  },
});

// Explicitly disable shared auth state for login tests
test.use({ storageState: { cookies: [], origins: [] } });

test.describe('Login with Welcome Modal', () => {
  test('logs in and displays welcome modal for first-time users', async ({
    page,
    loginPage,
    welcomeModalPage,
  }) => {
    // Ensure welcome modal is NOT skipped (simulate first-time user)
    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    await page.goto('/');

    await loginPage.login('datahub', 'datahub');

    // Wait for welcome modal to appear
    await welcomeModalPage.expectModalVisible();

    // Verify modal title
    await welcomeModalPage.expectModalTitle();

    // Close modal
    await welcomeModalPage.closeViaButton();

    // After closing modal, verify we're on home page with greeting
    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });

  test('logs in and closes welcome modal via Get Started button', async ({
    page,
    loginPage,
    welcomeModalPage,
  }) => {
    // Ensure welcome modal is NOT skipped
    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    await page.goto('/');
    await loginPage.login('datahub', 'datahub');

    // Wait for modal
    await welcomeModalPage.expectModalVisible();

    // Navigate to final slide
    await welcomeModalPage.clickLastCarouselDot();
    await welcomeModalPage.expectFinalSlideVisible();

    // Close via Get Started button
    await welcomeModalPage.closeViaGetStarted();

    // Verify we're on home page
    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });

  test('logs in and closes welcome modal via Escape key', async ({
    page,
    loginPage,
    welcomeModalPage,
  }) => {
    // Ensure welcome modal is NOT skipped
    await page.addInitScript(() => {
      localStorage.removeItem('skipWelcomeModal');
    });

    await page.goto('/');
    await loginPage.login('datahub', 'datahub');

    // Wait for modal
    await welcomeModalPage.expectModalVisible();

    // Close via Escape key
    await welcomeModalPage.closeViaEscape();

    // Verify we're on home page
    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible();
  });
});
