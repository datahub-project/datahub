/**
 * Manage Ingestion and Secret Privileges tests
 * Migrated from Cypress e2e/mutations/manage_ingestion_secret_privilege.js
 *
 * NOTE: The Cypress test is currently skipped (describe.skip) with
 *   // TODO: (v1_ui_removing) migrate this test
 * Tests rely on V1 UI components that are being removed. Preserved as skipped
 * tests to maintain test intent until V2 equivalents are built.
 *
 * Tests:
 *   1. Create Metadata Ingestion platform policy and assign to all users
 *   2. Create user and verify ingestion tab is accessible
 *   3. Verify new user can see ingestion and access Manage Ingestion tab
 */

import { test, expect } from '../../fixtures/base-test';
import { PoliciesPage } from '../../pages/settings/policies.page';

const testId = Math.floor(Math.random() * 100000);
const platformPolicyName = `Platform test policy ${testId}`;
const number = Math.floor(Math.random() * 100000);
const name = `Example Name ${number}`;
const email = `example${number}@example.com`;
const userPassword = 'Example password';

let _registeredEmail = '';

// ── Tests ───────────────────────────────────────────────────────────────────

test.describe.skip('Manage Ingestion and Secret Privileges', () => {
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ apiMock }) => {
    // Disable the ingestion page redesign so the classic ingestion UI is used
    await apiMock.setFeatureFlags({ showIngestionPageRedesign: false });
  });

  test('create Metadata Ingestion platform policy and assign privileges to all users', async ({
    page,
    logger,
    logDir,
  }) => {
    const policiesPage = new PoliciesPage(page, { logger, logDir });
    logger.step('navigate to policies');
    await policiesPage.navigate();

    // Filter to show all policies
    await page.getByTestId('policy-filter').click();
    await page.getByTestId('option-ALL').click();
    await page.waitForTimeout(500);

    // Deactivate any existing "All Users" policies that might conflict
    await policiesPage.deactivateExistingAllUserPolicies();
    await page.reload();

    // Create a new platform policy
    await page.getByText('Create new policy').click();
    await page.getByTestId('policy-name').clear();
    await page.getByTestId('policy-name').fill(platformPolicyName);
    await page.getByTestId('policy-type').getByTitle('Metadata').click();
    await page.getByTestId('platform').click({ force: true });

    // Policy description and privileges
    await page.getByTestId('policy-description').click();
    await page.getByTestId('policy-description').fill(`Platform policy description ${testId}`);
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated HTML id; no data-testid on this button
    await page.locator('#nextButton').click();
    await page.getByTestId('privileges').fill('Ingestion');
    // eslint-disable-next-line playwright/no-raw-locators -- rc-virtual-list internal class; no data-testid available
    await page.locator('.rc-virtual-list').getByText('Manage Metadata Ingestion').click({ force: true });
    await page.getByTestId('privileges').blur();
    await page.waitForTimeout(1000);
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated HTML id; no data-testid on this button
    await page.locator('#nextButton').click();

    // Assign to All Users
    await page.getByTestId('users').fill('All');
    // eslint-disable-next-line playwright/no-raw-locators -- rc-virtual-list internal class; no data-testid available
    await page.locator('.rc-virtual-list').getByText('All Users').click({ force: true });
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated HTML id; no data-testid on this button
    await page.locator('#saveButton').click();
    await expect(page.getByText('Successfully saved policy.')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('Successfully saved policy.')).not.toBeVisible({ timeout: 15000 });

    await page.reload();
    await policiesPage.searchForPolicy(platformPolicyName);
    // eslint-disable-next-line playwright/no-raw-locators -- tbody element has no ARIA role equivalent
    await expect(page.locator('tbody').getByText(platformPolicyName)).toBeVisible({ timeout: 15000 });

    // Log out admin
    await page.getByTestId('manage-account-menu').click();
    await page.getByTestId('log-out-menu-item').click({ force: true });
    await expect(page.getByText('Username')).toBeVisible();
  });

  test('Create user and verify ingestion tab not present', async ({ page, logger }) => {
    logger.step('invite new user');
    await page.goto('/settings/identities/users');
    await expect(page.getByText('Invite Users')).toBeVisible({ timeout: 15000 });
    await page.getByText('Invite Users').click();
    await expect(page.getByText(/signup\?invite_token=\w{32}/)).toBeVisible({ timeout: 15000 });
    const inviteLink = (await page.getByText(/signup\?invite_token=\w{32}/).textContent()) ?? '';

    await page.goto('/settings/identities/users');
    await page.getByTestId('manage-account-menu').click();
    await page.getByTestId('log-out-menu-item').click({ force: true });

    await page.goto(inviteLink);
    await page.getByTestId('email').fill(email);
    await page.getByTestId('name').fill(name);
    await page.getByTestId('password').fill(userPassword);
    await page.getByTestId('confirmPassword').fill(userPassword);
    await page.getByTestId('sign-up').click();
    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });

    // Suppress onboarding tour
    await page.keyboard.press('Control+ +Meta+ +h');
    await expect(page.getByText(name)).toBeVisible({ timeout: 15000 });
    _registeredEmail = email;
  });

  test('Verify new user can see ingestion and access Manage Ingestion tab', async ({ page, logger }) => {
    // Sign in as the new user
    logger.step('sign in as new user');
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
    await page.context().clearCookies();

    await page.goto('/login');
    await page.getByTestId('username').fill(email);
    await page.getByTestId('password').fill(userPassword);
    await page.getByTestId('sign-in').click();
    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });

    await page.keyboard.press('Control+ +Meta+ +h');
    await expect(page.getByText(name)).toBeVisible({ timeout: 15000 });

    // Navigate to the ingestion page
    logger.step('navigate to ingestion page');
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated HTML id on home page nav item; no data-testid available
    await page.locator('[id="home-page-ingestion"]').scrollIntoViewIfNeeded();
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated HTML id on home page nav item; no data-testid available
    await page.locator('[id="home-page-ingestion"]').click();
    await page.waitForTimeout(1000);
    // eslint-disable-next-line playwright/no-raw-locators -- clicking body to dismiss popovers; no semantic Playwright equivalent
    await page.locator('body').click();

    await expect(page.getByText('Manage Data Sources')).toBeVisible({ timeout: 30000 });
    await expect(page.getByText('Sources')).toBeVisible();
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design tabs nav list class; no data-testid available
    await expect(page.locator('.ant-tabs-nav-list').getByText('Source')).toBeVisible();
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design tab class; no data-testid on individual tab elements
    await expect(page.locator('.ant-tabs-tab')).toHaveCount(1);
  });
});
