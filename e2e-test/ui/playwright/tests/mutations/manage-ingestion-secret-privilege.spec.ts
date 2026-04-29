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

const testId = Math.floor(Math.random() * 100000);
const platformPolicyName = `Platform test policy ${testId}`;
const number = Math.floor(Math.random() * 100000);
const name = `Example Name ${number}`;
const email = `example${number}@example.com`;
const userPassword = 'Example password';

let _registeredEmail = '';

// ── Helpers ─────────────────────────────────────────────────────────────────

async function searchForPolicy(page: import('@playwright/test').Page, policyName: string): Promise<void> {
  await expect(page.locator('[data-testid="search-bar-input"]')).toBeVisible();
  await page.locator('[data-testid="search-bar-input"]').clear();
  await page.locator('[data-testid="search-bar-input"]').fill(policyName);
  await page.waitForTimeout(500);
}

async function _openRowMenu(page: import('@playwright/test').Page, policyName: string): Promise<void> {
  await page.locator('tr').filter({ hasText: policyName }).getByRole('button').last().click({ force: true });
  await page.waitForTimeout(300);
}

async function _clickMenuAction(page: import('@playwright/test').Page, actionText: string): Promise<void> {
  await page.locator('[data-testid^="menu-item-"]').getByText(actionText).click();
}

async function deactivateExistingAllUserPolicies(page: import('@playwright/test').Page): Promise<void> {
  await expect(page.locator('tbody tr td')).toBeVisible();
  const rows = page.locator('tbody tr');
  const count = await rows.count();
  for (let i = 0; i < count; i++) {
    const row = rows.nth(i);
    const roleText = await row.locator('td').nth(3).textContent();
    if (roleText?.includes('All Users')) {
      await row.getByRole('button').last().click({ force: true });
      await page.waitForTimeout(300);
      const deactivateItem = page.locator('[data-testid^="menu-item-"]').getByText('Deactivate');
      if ((await deactivateItem.count()) > 0) {
        await deactivateItem.click();
        await expect(page.getByText('Successfully deactivated policy.')).toBeVisible({ timeout: 15000 });
      }
    }
  }
}

// ── Tests ───────────────────────────────────────────────────────────────────

test.describe.skip('Manage Ingestion and Secret Privileges', () => {
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ apiMock }) => {
    // Disable the ingestion page redesign so the classic ingestion UI is used
    await apiMock.setFeatureFlags({ showIngestionPageRedesign: false });
  });

  test('create Metadata Ingestion platform policy and assign privileges to all users', async ({ page, logger }) => {
    logger.step('navigate to policies');
    await page.goto('/settings/permissions/policies');
    await expect(page.getByText('Manage Permissions')).toBeVisible({ timeout: 15000 });

    // Filter to show all policies
    await page.locator('[data-testid="policy-filter"]').click();
    await page.locator('[data-testid="option-ALL"]').click();
    await page.waitForTimeout(500);

    // Deactivate any existing "All Users" policies that might conflict
    await deactivateExistingAllUserPolicies(page);
    await page.reload();

    // Create a new platform policy
    await page.getByText('Create new policy').click();
    await page.locator('[data-testid="policy-name"]').clear();
    await page.locator('[data-testid="policy-name"]').fill(platformPolicyName);
    await page.locator('[data-testid="policy-type"]').locator('[title="Metadata"]').click();
    await page.locator('[data-testid="platform"]').click({ force: true });

    // Policy description and privileges
    await page.locator('[data-testid="policy-description"]').click();
    await page.locator('[data-testid="policy-description"]').fill(`Platform policy description ${testId}`);
    await page.locator('#nextButton').click();
    await page.locator('[data-testid="privileges"]').fill('Ingestion');
    await page.locator('.rc-virtual-list').getByText('Manage Metadata Ingestion').click({ force: true });
    await page.locator('[data-testid="privileges"]').blur();
    await page.waitForTimeout(1000);
    await page.locator('#nextButton').click();

    // Assign to All Users
    await page.locator('[data-testid="users"]').fill('All');
    await page.locator('.rc-virtual-list').getByText('All Users').click({ force: true });
    await page.locator('#saveButton').click();
    await expect(page.getByText('Successfully saved policy.')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('Successfully saved policy.')).not.toBeVisible({ timeout: 15000 });

    await page.reload();
    await searchForPolicy(page, platformPolicyName);
    await expect(page.locator('tbody').getByText(platformPolicyName)).toBeVisible({ timeout: 15000 });

    // Log out admin
    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });
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
    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });

    await page.goto(inviteLink);
    await page.locator('[data-testid="email"]').fill(email);
    await page.locator('[data-testid="name"]').fill(name);
    await page.locator('[data-testid="password"]').fill(userPassword);
    await page.locator('[data-testid="confirmPassword"]').fill(userPassword);
    await page.locator('[data-testid="sign-up"]').click();
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
    await page.locator('[data-testid="username"]').fill(email);
    await page.locator('[data-testid="password"]').fill(userPassword);
    await page.locator('[data-testid="sign-in"]').click();
    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });

    await page.keyboard.press('Control+ +Meta+ +h');
    await expect(page.getByText(name)).toBeVisible({ timeout: 15000 });

    // Navigate to the ingestion page
    logger.step('navigate to ingestion page');
    await page.locator('[id="home-page-ingestion"]').scrollIntoViewIfNeeded();
    await page.locator('[id="home-page-ingestion"]').click();
    await page.waitForTimeout(1000);
    await page.locator('body').click();

    await expect(page.getByText('Manage Data Sources')).toBeVisible({ timeout: 30000 });
    await expect(page.getByText('Sources')).toBeVisible();
    await expect(page.locator('.ant-tabs-nav-list').getByText('Source')).toBeVisible();
    await expect(page.locator('.ant-tabs-tab')).toHaveCount(1);
  });
});
