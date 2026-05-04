/**
 * Dataset ownership tests — migrated from Cypress e2e/mutations/dataset_ownership.js
 *
 * NOTE: The Cypress test is currently skipped (describe.skip) with
 *   // TODO: (v1_ui_removing) migrate this test
 * Tests rely on V1 UI components that are being removed. Preserved as skipped
 * tests to maintain test intent until V2 equivalents are built.
 *
 * Tests:
 *   1. Create test user and group, add user to group
 *   2-9. Add/remove user ownership (Business Owner, Data Steward, None, Technical Owner)
 *   6-9. Add/remove group ownership (Business Owner, Data Steward, None, Technical Owner)
 */

import { test, expect } from '../../fixtures/base-test';
import { DatasetPage } from '../../pages/dataset.page';

const testId = Math.floor(Math.random() * 100000);
const username = `Example Name ${testId}`;
const email = `example${testId}@example.com`;
const password = 'Example password';
const groupName = `Test group ${testId}`;

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)';
const DATASET_NAME = 'SampleCypressHiveDataset';

test.describe.skip('add, remove ownership for dataset', () => {
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ page }) => {
    await page.evaluate(() => {
      localStorage.setItem('skipAcrylIntroducePage', 'true');
    });
  });

  test('create test user and test group, add user to a group', async ({ page }) => {
    // Create user via invite link
    await page.goto('/settings/identities/users');
    await page.getByText('Invite Users').click();
    await expect(page.getByText(/signup\?invite_token=\w{32}/)).toBeVisible({ timeout: 15000 });
    const inviteLink = (await page.getByText(/signup\?invite_token=\w{32}/).textContent()) ?? '';
    await page.goto('/settings/identities/users');

    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });

    await page.goto(inviteLink);
    await page.locator('[data-testid="email"]').fill(email);
    await page.locator('[data-testid="name"]').fill(username);
    await page.locator('[data-testid="password"]').fill(password);
    await page.locator('[data-testid="confirmPassword"]').fill(password);
    await page.locator('[data-testid="sign-up"]').click();
    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });
    await page.keyboard.press('Control+ +Meta+ +h'); // hideOnboardingTour
    await expect(page.getByText(username)).toBeVisible({ timeout: 15000 });

    await page.locator('[data-testid="manage-account-menu"]').click();
    await page.locator('[data-testid="log-out-menu-item"]').click({ force: true });

    // Log back in as admin
    await page.locator('[data-testid="username"]').fill('datahub');
    await page.locator('[data-testid="password"]').fill('datahub');
    await page.locator('[data-testid="sign-in"]').click();

    // Create group
    await page.goto('/settings/identities/groups');
    await page.getByText('Create Group').click();
    await expect(page.getByText('Create new group')).toBeVisible();
    await page.locator('#name').fill(groupName);
    await page.locator('#description').fill('Test group description');
    await page.getByText('Advanced').click();
    await expect(page.getByText('Group Id')).toBeVisible();
    await page.locator('#groupId').fill(String(testId));
    await page.locator('#createGroupButton').click();
    await expect(page.getByText('Created group!')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(groupName)).toBeVisible();

    // Add user to group
    await page.goto(`/group/urn:li:corpGroup:${testId}/assets`);
    await page.getByText(groupName).click();
    await expect(page.getByText(groupName)).toBeVisible();
    await page.getByRole('tab', { name: 'Members' }).click();
    await page.getByText('Add Member').click();
    await expect(page.locator('[data-testid="add-members-select"]')).toBeVisible({ timeout: 10000 });
    await page.locator('[data-testid="add-members-select-base"]').click({ force: true });
    await page.locator('[data-testid="dropdown-search-input"]').fill(username);
    await page.locator('[data-testid="add-members-select-dropdown"]').getByText(username).click({ force: true });
    await page.getByRole('dialog').getByRole('button', { name: 'Add' }).click({ force: true });
    await expect(page.getByText('Group members added!')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(username)).toBeVisible({ timeout: 10000 });
  });

  for (const ownerType of ['Business Owner', 'Data Steward', 'None', 'Technical Owner']) {
    test(`open test dataset page, add and remove user ownership (${ownerType})`, async ({ page, logger, logDir }) => {
      // userHref is dynamic — contains the test-run-specific testId in the URN
      const userHref = `[href="/user/urn:li:corpuser:example${testId}@example.com/owner of"]`;
      const datasetPage = new DatasetPage(page, logger, logDir);
      await datasetPage.navigateToDataset(DATASET_URN);
      await expect(page.getByText(DATASET_NAME)).toBeVisible({ timeout: 30000 });
      await datasetPage.addOwner(username, ownerType);
      // Verify in owned assets
      await page.waitForTimeout(3000);
      await page.getByText(username).click();
      await expect(page.getByText(DATASET_NAME)).toBeVisible({ timeout: 30000 });
      await datasetPage.navigateToDataset(DATASET_URN);
      await datasetPage.removeOwner(username, userHref);
    });
  }

  for (const ownerType of ['Business Owner', 'Data Steward', 'None', 'Technical Owner']) {
    test(`open test dataset page, add and remove group ownership (${ownerType})`, async ({ page, logger, logDir }) => {
      // groupHref is dynamic — contains the test-run-specific testId in the URN
      const groupHref = `[href="/group/urn:li:corpGroup:${testId}/owner of"]`;
      const datasetPage = new DatasetPage(page, logger, logDir);
      await datasetPage.navigateToDataset(DATASET_URN);
      await expect(page.getByText(DATASET_NAME)).toBeVisible({ timeout: 30000 });
      await datasetPage.addOwner(groupName, ownerType);
      await page.waitForTimeout(3000);
      await page.getByText(groupName).click();
      await expect(page.getByText(DATASET_NAME)).toBeVisible({ timeout: 30000 });
      await datasetPage.navigateToDataset(DATASET_URN);
      await datasetPage.removeOwner(groupName, groupHref);
    });
  }
});
