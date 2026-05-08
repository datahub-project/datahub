/**
 * Schema blame tests — migrated from Cypress e2e/schema_blameV2/v2_schema_blame.js
 *
 * The v1 schema_blame test (describe.skip) is omitted; only the active V2 tests
 * are migrated here.
 *
 * Verifies: blame view activation, field visibility across schema versions,
 * field descriptions, tags, and history tab.
 *
 * Prerequisites: SamplePlaywrightHiveDataset must exist with schema versions 0.0.0 and 1.0.0
 * including fields: field_foo (with Legacy tag), field_bar, field_baz.
 */

import { test, expect } from '../../fixtures/base-test';
import { DatasetPage } from '../../pages/entity/dataset.page';

test.use({ featureName: 'entity-pages' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)';

test.describe('schema blame', () => {
  test('can activate the blame view and verify for the latest version of a dataset', async ({ page, logger, logDir }) => {
    const datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await datasetPage.schema.expectFieldVisible('field_baz');
    await page.locator('body').click();

    // Verify fields present/absent at latest version
    await datasetPage.schema.expectFieldVisible('field_foo');
    await datasetPage.schema.expectFieldVisible('field_baz');
    await datasetPage.schema.expectFieldNotVisible('field_bar');
    await expect(page.getByText('Foo field description has changed')).toBeVisible();
    await expect(page.getByText('Baz field description')).toBeAttached();

    // Click field_foo and verify Legacy tag
    await datasetPage.schema.clickField('field_foo');
    await datasetPage.schema.drawer.expectTagVisible('Legacy');

    // Select version 1.0.0
    await datasetPage.schema.selectVersion(/1\.0\.0\s*-/);

    // Activate blame view
    await datasetPage.schema.clickBlameButton();

    // Verify fields for 1.0.0
    await datasetPage.schema.expectFieldVisible('field_bar');
    await expect(page.getByText('field_foo')).toBeVisible();
    await expect(page.getByText('Bar field description')).toBeVisible();
    await expect(page.getByText('Foo field description')).toBeVisible();

    // Verify History tab is visible
    await expect(page.getByText('History')).toBeVisible();
  });

  test('can activate the blame view and verify for an older version of a dataset', async ({ page, logger, logDir }) => {
    const datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await datasetPage.schema.expectFieldVisible('field_baz');
    await page.locator('body').click();

    // Select version 0.0.0
    await expect(page.locator('.ant-select-selection-item')).toBeVisible();
    await datasetPage.schema.selectVersion(/0\.0\.0\s*-/);

    // Verify fields for 0.0.0
    await datasetPage.schema.expectFieldVisible('field_foo');
    await datasetPage.schema.expectFieldVisible('field_bar');
    await datasetPage.schema.expectFieldNotVisible('field_baz');
    await expect(page.getByText('Foo field description')).toBeAttached();
    await expect(page.getByText('Bar field description')).toBeAttached();

    // Click field_foo and verify Legacy tag is absent at this version
    await datasetPage.schema.clickField('field_foo');
    await datasetPage.schema.expectFieldTagNotVisible('field_foo', 'Legacy');

    // Click history icon
    await datasetPage.schema.clickHistoryIcon();
  });
});
