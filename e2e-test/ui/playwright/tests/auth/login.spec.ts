import { test, expect } from '../../fixtures/login-test';
import { LoginPage } from '../../pages/login-page';

/**
 * Login tests — verify the login flow itself.
 *
 * Imports from login-test.ts, which provides a fresh, unauthenticated context
 * for every test. These tests must NOT use base-test.ts.
 */

test.describe('Login', () => {
  test.beforeEach(async ({ page }) => {
    // Skip welcome modal for all tests to avoid interference.
    await page.addInitScript(() => {
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  });

  test('logs in successfully with valid credentials', async ({ page }) => {
    const loginPage = new LoginPage(page);
    await page.goto('/');

    await loginPage.login('datahub', 'datahub');

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible({
      timeout: 10000,
    });
    await expect(page).toHaveURL(/\/$|\/home/);
  });

  test('displays login form elements', async ({ page }) => {
    const loginPage = new LoginPage(page);
    await loginPage.navigateToLogin();

    await expect(loginPage.usernameInput).toBeVisible();
    await expect(loginPage.passwordInput).toBeVisible();
    await expect(loginPage.loginButton).toBeVisible();
  });

  test('shows error for invalid credentials', async ({ page }) => {
    const loginPage = new LoginPage(page);
    await loginPage.navigateToLogin();

    await loginPage.usernameInput.fill('invalid_user');
    await loginPage.passwordInput.fill('invalid_password');
    await loginPage.loginButton.click();

    await expect(page.getByText(/Invalid credentials|Login failed/i)).toBeVisible({
      timeout: 5000,
    });
  });

  test('disables login button when username is empty', async ({ page }) => {
    const loginPage = new LoginPage(page);
    await loginPage.navigateToLogin();

    await loginPage.passwordInput.fill('datahub');

    await expect(loginPage.loginButton).toBeDisabled();
    await expect(page).toHaveURL(/\/login/);
  });

  test('disables login button when password is empty', async ({ page }) => {
    const loginPage = new LoginPage(page);
    await loginPage.navigateToLogin();

    await loginPage.usernameInput.fill('datahub');

    await expect(loginPage.loginButton).toBeDisabled();
    await expect(page).toHaveURL(/\/login/);
  });

  test('redirects to home page after successful login', async ({ page }) => {
    const loginPage = new LoginPage(page);
    await page.goto('/');

    await loginPage.login('datahub', 'datahub');
    await page.waitForLoadState('networkidle');

    await expect(page).toHaveURL(/\/$|\/home/);
    await expect(page.getByRole('img', { name: /logo/i })).toBeVisible();
  });
});
