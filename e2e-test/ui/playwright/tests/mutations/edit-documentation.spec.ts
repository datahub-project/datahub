/**
 * Edit documentation (v1) tests — migrated from Cypress e2e/mutations/edit_documentation.js
 *
 * Verifies that schema field descriptions can be edited and saved for a
 * pre-seeded Hive dataset.
 *
 * Prerequisites: SamplePlaywrightHiveDataset must exist with field `field_foo`
 * whose description is "Foo field description has changed".
 */

import { test, expect } from '../../fixtures/base-test';
import { EntityDocumentationPage } from '../../pages/entity-documentation.page';

test.use({ featureName: 'mutations' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)';

test.describe('edit documentation and link to dataset', () => {
  let docPage: EntityDocumentationPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    docPage = new EntityDocumentationPage(page, logger, logDir);
  });

  test('edit field documentation', async ({ page }) => {
    const testId = Math.floor(Math.random() * 100000);
    const documentationEdited = `This is test${testId} documentation EDITED`;

    await docPage.navigateToDatasetSchemaTab(DATASET_URN);

    // Click field_foo row to open the field drawer / edit form.
    // Using the #column-<fieldPath> id set by SchemaTable's onRow handler is
    // more reliable than matching bare text, which can hit multiple elements.
    await page.locator('#column-field_foo').click();

    // Open field description edit modal
    await page.locator('[data-testid="edit-field-description"]').click();
    await expect(page.getByText('Update description')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('Foo field description has changed').first()).toBeVisible();

    // Clear the editor and type new description.
    // The description-editor data-testid is on the outer container div; target the
    // inner contenteditable so that fill() works correctly.
    const editor = page.locator('[data-testid="description-editor"]').locator('[contenteditable="true"]');
    await editor.waitFor({ state: 'visible', timeout: 10000 });
    await editor.fill('');
    await page.waitForTimeout(1000);
    await page.keyboard.type(documentationEdited);
    await page.locator('[data-testid="description-modal-update-button"]').click();
    await expect(page.getByText('Updated!')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(documentationEdited).first()).toBeVisible();
    await expect(page.getByText('(edited)')).toBeVisible();

    // Restore original description
    await page.locator('[data-testid="edit-field-description"]').click();
    const editor2 = page.locator('[data-testid="description-editor"]').locator('[contenteditable="true"]');
    await editor2.waitFor({ state: 'visible', timeout: 10000 });
    await editor2.fill('');
    await page.waitForTimeout(1000);
    await page.keyboard.type('Foo field description has changed');
    await page.locator('[data-testid="description-modal-update-button"]').click();
    await expect(page.getByText('Updated!')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('Foo field description has changed').first()).toBeVisible();
    await expect(page.getByText('(edited)')).toBeVisible();
  });
});
