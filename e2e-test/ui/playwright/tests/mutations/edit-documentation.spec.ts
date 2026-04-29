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
    await docPage.editFieldDescription('field_foo', documentationEdited);
    await expect(page.getByText(documentationEdited).first()).toBeVisible();
    await expect(page.getByText('(edited)')).toBeVisible();

    // Restore original description
    await docPage.editFieldDescription('field_foo', 'Foo field description has changed');
    await expect(page.getByText('Foo field description has changed').first()).toBeVisible();
    await expect(page.getByText('(edited)')).toBeVisible();
  });
});
