/**
 * Login tests — verify the login flow itself.
 *
 * Uses login-test which provides a fresh, unauthenticated context and the
 * loginPage fixture (with logger attached). No manual page object construction
 * needed — loginPage comes from the fixture.
 */

import { test, expect } from '../../fixtures/login-test';
import { users } from '../../data/users';
import { NETWORK_IDLE_TIMEOUT } from '../../utils/constants';

test.describe('Login', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  });

  test('logs in successfully with valid credentials', async ({ page, loginPage }) => {
    const { username, password } = users.admin;
    await loginPage.navigateToLogin();
    await loginPage.login(username, password);
    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible({ timeout: NETWORK_IDLE_TIMEOUT });
    await expect(page).toHaveURL(/\/$|\/home/);
  });

  test('displays login form elements', async ({ loginPage }) => {
    await loginPage.navigateToLogin();
    await expect(loginPage.usernameInput).toBeVisible();
    await expect(loginPage.passwordInput).toBeVisible();
    await expect(loginPage.loginButton).toBeVisible();
  });

  test('shows error for invalid credentials', async ({ page, loginPage }) => {
    await loginPage.navigateToLogin();
    await loginPage.usernameInput.fill('invalid_user');
    await loginPage.passwordInput.fill('invalid_password');
    await loginPage.loginButton.click();
    await expect(page.getByText(/Invalid credentials|Login failed/i)).toBeVisible({ timeout: 5000 });
  });

  test('disables login button when username is empty', async ({ page, loginPage }) => {
    await loginPage.navigateToLogin();
    await loginPage.passwordInput.fill('datahub');
    await expect(loginPage.loginButton).toBeDisabled();
    await expect(page).toHaveURL(/\/login/);
  });

  test('disables login button when password is empty', async ({ page, loginPage }) => {
    await loginPage.navigateToLogin();
    await loginPage.usernameInput.fill('datahub');
    await expect(loginPage.loginButton).toBeDisabled();
    await expect(page).toHaveURL(/\/login/);
  });

  test('redirects to home page after successful login', async ({ page, loginPage }) => {
    const { username, password } = users.admin;
    await loginPage.navigateToLogin();
    await loginPage.login(username, password);
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(/\/$|\/home/);
    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible({ timeout: NETWORK_IDLE_TIMEOUT });
  });
});
