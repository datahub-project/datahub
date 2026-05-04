/**
 * Dataset deprecation tests — migrated from Cypress e2e/mutations/deprecations.js
 *
 * NOTE: The Cypress test is currently skipped (describe.skip) with
 *   // TODO: (v1_ui_removing) migrate this test
 * Tests rely on V1 UI components that are being removed. Preserved as skipped
 * tests to maintain test intent until V2 equivalents are built.
 *
 * Tests:
 *   - Mark a dataset as deprecated, then un-deprecated (via three-dot menu and tooltip)
 */

import { test, expect } from '../../fixtures/base-test';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,cypress_logging_events,PROD)';
const DATASET_NAME = 'cypress_logging_events';

test.describe.skip('dataset deprecation', () => {
  test('go to dataset and check deprecation works', async ({ page, logger }) => {
    logger.step('navigate to dataset', { urn: URN });
    await page.goto(`/dataset/${encodeURIComponent(URN)}/`);
    await expect(page.getByText(DATASET_NAME)).toBeVisible({ timeout: 30000 });

    // Open three-dot dropdown and mark as deprecated
    await page.locator('[data-testid="entity-header-dropdown"]').click();
    await page.getByText('Mark as deprecated').click();

    // Fill in the deprecation form modal
    await expect(page.getByText('Add Deprecation Details')).toBeVisible();
    await page.locator('.ProseMirror-focused').type('test deprecation');
    await page.locator('.ant-modal-footer > button:nth-child(2)').click();
    await expect(page.getByText('Deprecation Updated')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('DEPRECATED')).toBeVisible();

    // Un-deprecate via the three-dot menu
    await page.locator('[data-testid="entity-header-dropdown"]').click();
    await page.getByText('Mark as un-deprecated').click();
    await expect(page.getByText('Deprecation Updated')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('DEPRECATED')).not.toBeVisible({ timeout: 10000 });

    // Mark as deprecated again
    await page.locator('[data-testid="entity-header-dropdown"]').click();
    await page.getByText('Mark as deprecated').click();
    await expect(page.getByText('Add Deprecation Details')).toBeVisible();
    await page.locator('.ProseMirror-focused').type('test deprecation');
    await page.locator('.ant-modal-footer > button:nth-child(2)').click();
    await expect(page.getByText('Deprecation Updated')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('DEPRECATED')).toBeVisible();

    // Hover over the DEPRECATED badge and un-deprecate via the tooltip action
    await page.getByText('DEPRECATED').hover({ force: true });
    await expect(page.getByText('Deprecation note')).toBeVisible();
    await page.getByRole('tooltip').getByText('Mark as un-deprecated').click();
    await expect(page.getByText('Confirm Mark as un-deprecated')).toBeVisible();
    await page.getByRole('button', { name: 'Yes' }).click();
    await expect(page.getByText('Marked assets as un-deprecated!')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('DEPRECATED')).not.toBeVisible({ timeout: 10000 });
  });
});
