import { test as base, expect } from '@playwright/test';
import { LoginPage } from '../../pages/LoginPage';

/**
 * Login tests - These tests verify the login functionality itself
 * Therefore, they MUST NOT use the shared authentication state
 */
const test = base.extend<{ loginPage: LoginPage }>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
});

// Explicitly disable shared auth state for login tests
test.use({ storageState: { cookies: [], origins: [] } });

test.describe('Login', () => {
  test.beforeEach(async ({ page }) => {
    // Skip welcome modal for all tests to avoid interference
    await page.addInitScript(() => {
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  });

  test('logs in successfully with valid credentials', async ({ page, loginPage }) => {
    await page.goto('/');

    await loginPage.login('datahub', 'datahub');

    // Wait for successful login - check for greeting message
    // The greeting changes based on time of day: "Good morning", "Good afternoon", "Good evening"
    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible({ timeout: 10000 });

    // Verify navigation to home page
    await expect(page).toHaveURL(/\/$|\/home/);
  });

  test('displays login form elements', async ({ loginPage }) => {
    await loginPage.navigateToLogin();

    // Verify all login form elements are present
    await expect(loginPage.usernameInput).toBeVisible();
    await expect(loginPage.passwordInput).toBeVisible();
    await expect(loginPage.loginButton).toBeVisible();
  });

  test('shows error for invalid credentials', async ({ page, loginPage }) => {
    await loginPage.navigateToLogin();

    await loginPage.usernameInput.fill('invalid_user');
    await loginPage.passwordInput.fill('invalid_password');
    await loginPage.loginButton.click();

    // Wait for error message
    // Note: Adjust the selector based on actual error message implementation
    await expect(page.getByText(/Invalid credentials|Login failed/i)).toBeVisible({
      timeout: 5000,
    });
  });

  test('disables login button when username is empty', async ({ page, loginPage }) => {
    await loginPage.navigateToLogin();

    // Fill only password
    await loginPage.passwordInput.fill('datahub');

    // Login button should be disabled
    await expect(loginPage.loginButton).toBeDisabled();

    // Should remain on login page
    await expect(page).toHaveURL(/\/login/);
  });

  test('disables login button when password is empty', async ({ page, loginPage }) => {
    await loginPage.navigateToLogin();

    // Fill only username
    await loginPage.usernameInput.fill('datahub');

    // Login button should be disabled
    await expect(loginPage.loginButton).toBeDisabled();

    // Should remain on login page
    await expect(page).toHaveURL(/\/login/);
  });

  test('redirects to home page after successful login', async ({ page, loginPage }) => {
    await page.goto('/');

    await loginPage.login('datahub', 'datahub');

    // Wait for redirect to complete
    await page.waitForLoadState('networkidle');

    // Verify URL is home page
    await expect(page).toHaveURL(/\/$|\/home/);

    // Verify home page elements are loaded
    await expect(page.getByRole('img', { name: /logo/i })).toBeVisible();
  });
});
