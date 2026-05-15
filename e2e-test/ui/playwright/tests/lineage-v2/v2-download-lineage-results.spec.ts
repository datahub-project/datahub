/**
 * Download lineage results V2 tests
 * Migrated from Cypress e2e/lineageV2/v2_download_lineage_results.js
 *
 * Tests downloading lineage results as CSV at 1st, 2nd, and 3+ degree of dependencies.
 * Verifies that the downloaded CSV contains the expected URNs for each degree.
 *
 * NOTE: This test downloads files to the filesystem. In CI it relies on
 * Playwright's file download interception (not cy.readFile). The downloaded
 * CSV content is read directly from the download stream so no fixed download
 * directory is required.
 *
 * Uses apiMock.setFeatureFlags to disable lineageGraphV3 so the V2 graph renders.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

// ── Constants ───────────────────────────────────────────────────────────────

const TEST_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

const FIRST_DEGREE_URNS = [
  'urn:li:chart:(looker,playwright_baz1)',
  'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)',
  'urn:li:mlFeature:(playwright-test-2,some-playwright-feature-1)',
];

const SECOND_DEGREE_URNS = [
  'urn:li:chart:(looker,playwright_baz2)',
  'urn:li:dashboard:(looker,playwright_baz)',
  'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)',
  'urn:li:mlPrimaryKey:(playwright-test-2,some-playwright-feature-2)',
  'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,playwright-model,PROD)',
];

const THIRD_DEGREE_PLUS_URNS = [
  'urn:li:dataJob:(urn:li:dataFlow:(airflow,playwright_dag_abc,PROD),playwright_task_123)',
  'urn:li:dataJob:(urn:li:dataFlow:(airflow,playwright_dag_abc,PROD),playwright_task_456)',
  'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_logging_events,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:hive,fct_playwright_users_created,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:hive,fct_playwright_users_created_no_tag,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:hive,fct_playwright_users_deleted,PROD)',
  'urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,playwright-model-package-group,PROD)',
];

// ── Tests ────────────────────────────────────────────────────────────────────

test.describe('download lineage results to .csv file', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  test('download and verify lineage results for 1st, 2nd and 3+ degree of dependencies', async ({
    page,
    logger,
    logDir,
  }) => {
    // Download and verify can take significant time
    test.setTimeout(120000);

    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToDataset(TEST_DATASET_URN, 'SamplePlaywrightKafkaDataset');
    await lp.clickLineageTab();

    // ── 1st degree ───────────────────────────────────────────────────────────

    await lp.clickImpactAnalysis();
    const firstDegreeCsv = await lp.downloadCsvAndRead('first_degree_results.csv');

    for (const urn of FIRST_DEGREE_URNS) {
      expect(firstDegreeCsv).toContain(urn);
    }
    for (const urn of SECOND_DEGREE_URNS) {
      expect(firstDegreeCsv).not.toContain(urn);
    }
    for (const urn of THIRD_DEGREE_PLUS_URNS) {
      expect(firstDegreeCsv).not.toContain(urn);
    }

    // ── 1st + 2nd degree ─────────────────────────────────────────────────────

    await lp.clickDegree2Filter();
    await page.waitForTimeout(5000);
    // Verify result count is between 8-9 (flexible range as displayed in the UI)
    await expect(page.getByText(/1 - [8-9] of [8-9]/).first()).toBeVisible({ timeout: 15000 });

    const secondDegreeCsv = await lp.downloadCsvAndRead('second_degree_results.csv');

    for (const urn of FIRST_DEGREE_URNS) {
      expect(secondDegreeCsv).toContain(urn);
    }
    for (const urn of SECOND_DEGREE_URNS) {
      expect(secondDegreeCsv).toContain(urn);
    }
    for (const urn of THIRD_DEGREE_PLUS_URNS) {
      expect(secondDegreeCsv).not.toContain(urn);
    }

    // ── 3+ degree (multi-page download) ─────────────────────────────────────

    await lp.clickDegree3PlusFilter();
    await page.waitForTimeout(5000);
    await expect(page.getByText(/1 - 10/).first()).toBeVisible({ timeout: 15000 });

    const thirdDegreeCsv = await lp.downloadCsvAndRead('third_plus_degree_results.csv');

    for (const urn of FIRST_DEGREE_URNS) {
      expect(thirdDegreeCsv).toContain(urn);
    }
    for (const urn of SECOND_DEGREE_URNS) {
      expect(thirdDegreeCsv).toContain(urn);
    }
    for (const urn of THIRD_DEGREE_PLUS_URNS) {
      expect(thirdDegreeCsv).toContain(urn);
    }
  });
});
