/**
 * Schema blame tests — migrated from Cypress e2e/schema_blameV2/v2_schema_blame.js
 *
 * The v1 schema_blame test (describe.skip) is omitted; only the active V2 tests
 * are migrated here.
 *
 * Verifies: dataset page loads and displays available metadata tabs.
 *
 * Prerequisites: SamplePlaywrightHiveDataset must exist with schema metadata.
 */

import { test, expect } from '../../fixtures/base-test';
import { DatasetPage } from '../../pages/entity/dataset.page';
import { TIMEOUTS } from './constants';

test.use({ featureName: 'entity-pages' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)';
const DATASET_NAME = 'SamplePlaywrightHiveDataset';

test.describe('schema blame', () => {
  test('can navigate to dataset and verify page loads', async ({ page, logger, logDir }) => {
    const datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await page.waitForLoadState('networkidle');

    // Verify dataset page loaded with dataset name visible in header
    await expect(page.getByTestId('entity-header-test-id').getByText(DATASET_NAME)).toBeVisible({
      timeout: TIMEOUTS.NORMAL,
    });

    // Verify tab navigation is available
    await expect(page.locator('[role="tab"]').first()).toBeVisible({ timeout: TIMEOUTS.NORMAL });
  });

  test('can access dataset overview', async ({ page, logger, logDir }) => {
    const datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await page.waitForLoadState('networkidle');

    // Verify dataset page loaded
    await expect(page.getByTestId('entity-header-test-id').getByText(DATASET_NAME)).toBeVisible({
      timeout: TIMEOUTS.NORMAL,
    });

    // Verify tabs are present
    const tabButtons = page.locator('[role="tab"]');
    const tabCount = await tabButtons.count();
    expect(tabCount).toBeGreaterThan(0);
  });
});
