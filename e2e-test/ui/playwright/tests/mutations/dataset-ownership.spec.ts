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
import { GroupsPage } from '../../pages/groups.page';
import { LoginPage } from '../../pages/login.page';

const testId = Math.floor(Math.random() * 100000);
const username = `Example Name ${testId}`;
const email = `example${testId}@example.com`;
const password = process.env.TEST_USER_PASSWORD ?? 'Example password';
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
    const groupsPage = new GroupsPage(page);
    const loginPage = new LoginPage(page);

    // Create user via invite link
    await page.goto('/settings/identities/users');
    await page.getByText('Invite Users').click();
    await expect(page.getByText(/signup\?invite_token=\w{32}/)).toBeVisible({ timeout: 15000 });
    const inviteLink = (await page.getByText(/signup\?invite_token=\w{32}/).textContent()) ?? '';
    await page.goto('/settings/identities/users');

    await page.getByTestId('manage-account-menu').click();
    await page.getByTestId('log-out-menu-item').click({ force: true });

    await page.goto(inviteLink);
    await page.getByTestId('email').fill(email);
    await page.getByTestId('name').fill(username);
    await page.getByTestId('password').fill(password);
    await page.getByTestId('confirmPassword').fill(password);
    await page.getByTestId('sign-up').click();
    await expect(page.getByText('Welcome back')).toBeVisible({ timeout: 30000 });
    await page.keyboard.press('Control+ +Meta+ +h'); // hideOnboardingTour
    await expect(page.getByText(username)).toBeVisible({ timeout: 15000 });

    await page.getByTestId('manage-account-menu').click();
    await page.getByTestId('log-out-menu-item').click({ force: true });

    // Log back in as admin to create the group
    const adminUsername = process.env.DATAHUB_ADMIN_USERNAME ?? 'datahub';
    const adminPassword = process.env.DATAHUB_ADMIN_PASSWORD ?? 'datahub';
    await loginPage.login(adminUsername, adminPassword);

    // Create group
    await groupsPage.navigateToGroups();
    await groupsPage.createGroup(groupName, 'Test group description', String(testId));

    // Add user to group
    await groupsPage.addMember(`urn:li:corpGroup:${testId}`, groupName, username);
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
