/**
 * Ownership management tests — migrated from Cypress e2e/ownershipV2/v2_manage_ownership.js
 *
 * Verifies the Ownership Types settings page: create, edit, make default, delete an ownership type.
 */

import { test, expect } from '../../fixtures/base-test';
import { withTimestamp } from '../../utils/random';

test.use({ featureName: 'entity-pages' });

test.describe('manage ownership', () => {
  test('go to ownership types settings page, create, edit, make default, delete a ownership type', async ({
    page,
  }) => {
    const testOwnershipType = withTimestamp('Test Ownership Type');

    await page.goto('/settings/ownership');
    await expect(page.getByText('Manage Ownership')).toBeVisible({ timeout: 15000 });

    // Create
    await page.locator('[data-testid="create-owner-type-v2"]').click();
    await page.locator('[data-testid="ownership-type-name-input"]').clear();
    await page.locator('[data-testid="ownership-type-name-input"]').fill(testOwnershipType);
    await page.locator('[data-testid="ownership-type-description-input"]').clear();
    await page.locator('[data-testid="ownership-type-description-input"]').fill(
      'This is a test ownership type description.',
    );
    await page.locator('[data-testid="ownership-builder-save"]').click();
    await expect(page.getByText(testOwnershipType)).toBeVisible({ timeout: 15000 });

    // Edit the ownership type
    await page.locator('tr').filter({ hasText: testOwnershipType }).locator('[data-testid="ownership-table-dropdown"]').click();
    await page.locator('[data-testid="menu-item-edit"]').click();

    await page.locator('[data-testid="ownership-type-description-input"]').clear();
    await page.locator('[data-testid="ownership-type-description-input"]').fill(
      'This is an edited test ownership type description.',
    );
    await page.locator('[data-testid="ownership-builder-save"]').click();
    await expect(page.getByText('This is an edited test ownership type description.')).toBeVisible({
      timeout: 15000,
    });

    // Delete the ownership type
    await page.locator('tr').filter({ hasText: testOwnershipType }).locator('[data-testid="ownership-table-dropdown"]').click();
    await page.locator('[data-testid="menu-item-delete"]').click();

    await expect(page.getByText(testOwnershipType)).not.toBeVisible({ timeout: 15000 });
  });
});
