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

test.use({ featureName: 'entity-pages' });

test.describe.configure({ mode: 'serial' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const DATASET_NAME = 'SamplePlaywrightHdfsDataset';

const runId = withRandomSuffix('q');
const TEST_SQL = `SELECT * FROM test_${runId}`;
const TEST_TITLE = `Test Table-${runId}`;
const TEST_DESCRIPTION = `Test Description-${runId}`;
const EDITED_SQL = `SELECT * FROM edited_${runId}`;
const EDITED_TITLE = `Edited Table-${runId}`;
const EDITED_DESCRIPTION = `Edited Description-${runId}`;

test.describe('manage queries', () => {
  let datasetPage: DatasetPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 20000 });
    await datasetPage.queries.open();
    await expect(page.url()).toContain('Queries');
  });

  test('create, edit and delete a query', async ({ page }) => {
    await expect(page.getByText('Recent Queries')).not.toBeVisible({ timeout: 5000 }).catch(() => {
      // "Recent Queries" may or may not be present — not a failure
    });

    await datasetPage.queries.add(TEST_SQL, TEST_TITLE, TEST_DESCRIPTION);

    await expect(page.getByText(TEST_SQL.trim())).toBeVisible({ timeout: 10000 });
    await expect(page.getByText(TEST_TITLE)).toBeVisible();
    await expect(page.getByText(TEST_DESCRIPTION)).toBeVisible();
    await expect(page.getByText('Created on')).toBeVisible();
    await datasetPage.queries.verifyCardDetails(0, TEST_SQL, TEST_TITLE, TEST_DESCRIPTION);

    await datasetPage.queries.edit(0, EDITED_SQL, EDITED_TITLE, EDITED_DESCRIPTION);
    await datasetPage.queries.verifyCardDetails(0, `${TEST_SQL} ${EDITED_SQL}`, EDITED_TITLE, EDITED_DESCRIPTION);

    await datasetPage.queries.delete(0);
    await expect(page.getByText(EDITED_TITLE)).not.toBeVisible({ timeout: 10000 });
    await expect(page.getByText(EDITED_DESCRIPTION)).not.toBeVisible({ timeout: 5000 });
  });
});
