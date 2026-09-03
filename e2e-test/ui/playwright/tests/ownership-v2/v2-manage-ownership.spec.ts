/**
 * Ownership Types Management V2 tests — migrated from Cypress e2e/ownershipV2/v2_manage_ownership.js
 *
 * Tests the complete CRUD workflow for custom ownership types in the settings page:
 * - Create a new ownership type
 * - Edit its description
 * - Delete it
 *
 * Uses base-test fixture for authenticated context.
 *
 * Prerequisites: None (test creates and deletes its own data)
 */

import { test, expect } from '../../fixtures/base-test';
import { OwnershipManagementPage } from '../../pages/ownership-management.page';
import { withTimestamp } from '../../utils/random';

test.use({ featureName: 'ownership-v2' });

test.describe('manage ownership', () => {
  let ownershipPage: OwnershipManagementPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags({ showNavBarRedesign: true });
    ownershipPage = new OwnershipManagementPage(page, logger, logDir);
    await ownershipPage.navigate();
  });

  test('create, edit, and delete ownership type', async ({ page, logger }) => {
    const testOwnershipType = withTimestamp('OT');
    const initialDescription = 'Test description';
    const editedDescription = 'Edited description';

    // CREATE: Add a new ownership type
    logger.info(`Creating ownership type: ${testOwnershipType}`);
    await ownershipPage.openCreateModal();
    await ownershipPage.setOwnershipTypeName(testOwnershipType);
    await ownershipPage.setOwnershipTypeDescription(initialDescription);
    await ownershipPage.saveOwnershipType(testOwnershipType);

    // Verify creation by navigating to fresh page state
    await page.reload();
    await expect(page.getByText('Manage Ownership')).toBeVisible();
    await ownershipPage.expectItemVisible(testOwnershipType);
    logger.info(`Created: ${testOwnershipType}`);

    // EDIT: Update the description
    logger.info(`Editing ownership type: ${testOwnershipType}`);
    await ownershipPage.openRowDropdown(testOwnershipType);
    await ownershipPage.clickEditMenu();
    await ownershipPage.setOwnershipTypeDescription(editedDescription);
    await ownershipPage.saveOwnershipType(testOwnershipType);

    // Verify edit
    await page.reload();
    await expect(page.getByText('Manage Ownership')).toBeVisible();
    await ownershipPage.expectItemVisible(editedDescription);
    logger.info(`Edited: ${testOwnershipType}`);

    // DELETE: Remove the ownership type
    logger.info(`Deleting ownership type: ${testOwnershipType}`);
    await ownershipPage.openRowDropdown(testOwnershipType);
    await ownershipPage.clickDeleteMenu();

    // Verify deletion
    await page.reload();
    await expect(page.getByText('Manage Ownership')).toBeVisible();
    await ownershipPage.expectItemHidden(testOwnershipType);
    logger.info(`Deleted: ${testOwnershipType}`);

    logger.info('Test completed successfully');
  });
});
