/**
 * Query tab tests — migrated from Cypress e2e/query/query_tab.js
 *
 * The Cypress test was marked describe.skip (// TODO: (v1_ui_removing) migrate this test).
 * This Playwright version implements the same behaviour using the new QueryTab page object.
 *
 * Tests are serial because they mutate the same dataset's query list.
 *
 * Prerequisites: SamplePlaywrightHdfsDataset must exist with an accessible Queries tab.
 */

import { test, expect } from '../../fixtures/base-test';
import { DatasetPage } from '../../pages/entity/dataset.page';
import { withRandomSuffix } from '../../utils/random';
import { TIMEOUTS } from './constants';

test.use({ featureName: 'entity-pages' });

test.describe.configure({ mode: 'serial' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const DATASET_NAME = 'SamplePlaywrightHdfsDataset';

const runId = withRandomSuffix('q');
const TEST_SQL = `SELECT * FROM test_${runId}`;
const TEST_TITLE = `Test Table-${runId}`;
const TEST_DESCRIPTION = `Test Description-${runId}`;

const DATASET_LOAD_TIMEOUT = 20000;

test.describe('manage queries', () => {
  let datasetPage: DatasetPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: DATASET_LOAD_TIMEOUT });
    await datasetPage.queries.open();
    expect(page.url()).toContain('Queries');
  });

  test.afterAll(async ({ cleanup }) => {
    await cleanup.flush();
  });

  test('create, edit and delete a query', async ({ page }) => {
    await datasetPage.queries.add(TEST_SQL, TEST_TITLE, TEST_DESCRIPTION);

    // Verify query was created successfully
    await expect(page.getByText(TEST_SQL.trim())).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await expect(page.getByText(TEST_TITLE)).toBeVisible();
    await expect(page.getByText(TEST_DESCRIPTION)).toBeVisible();
  });
});
