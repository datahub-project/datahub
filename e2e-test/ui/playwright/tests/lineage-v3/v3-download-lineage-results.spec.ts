/**
 * Download Lineage Results tests — migrated from Cypress v3_download_lineage_results.js
 *
 * DATA SEEDING: Static datasets from fixtures/data.json (auto-seeded via test.use below)
 */

import { test } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';
import { verifyCSVContent } from '../../utils/file-utils';

test.use({ featureName: 'lineage-v3' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

// CSV Export Files
const CSV_FILES = {
  FIRST_DEGREE: 'first_degree_results.csv',
  SECOND_DEGREE: 'second_degree_results.csv',
  THIRD_PLUS_DEGREE: 'third_plus_degree_results.csv',
} as const;

// UI Strings
const UI_STRINGS = {
  IMPACT_ANALYSIS: 'Impact Analysis',
  CREATING_CSV: 'Creating CSV',
  ENTITY_COUNT: /\d+ - \d+ of \d+/,
} as const;

// Result count patterns
const RESULT_PATTERNS = {
  RESULTS_8_TO_9: /1 - [8-9] of [8-9]/,
  RESULTS_10: /1 - 10/,
} as const;

// Expected URNs at each degree level (matches Cypress coverage)
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

test.describe('CSV Export - Lineage Results', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      lineageGraphV3: true,
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  test('download and verify lineage results for 1st, 2nd and 3+ degree of dependencies', async ({ page, context }) => {
    await lineagePage.goToLineageGraph('dataset', DATASET_URN);
    await lineagePage.clickText(UI_STRINGS.IMPACT_ANALYSIS);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // ─── 1st Degree Download ────────────────────────────────────────────────
    let downloadPath = await lineagePage.downloadCSV(context, CSV_FILES.FIRST_DEGREE);
    await verifyCSVContent(downloadPath, FIRST_DEGREE_URNS, [...SECOND_DEGREE_URNS, ...THIRD_DEGREE_PLUS_URNS]);

    // ─── 2nd Degree Download ────────────────────────────────────────────────
    await lineagePage.degree2Filter.click();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Verify result count is between 8-9 (flexible range as displayed in the UI)
    await lineagePage.expectResultTextVisible(RESULT_PATTERNS.RESULTS_8_TO_9, TIMEOUTS.MEDIUM);

    downloadPath = await lineagePage.downloadCSV(context, CSV_FILES.SECOND_DEGREE);
    await verifyCSVContent(downloadPath, [...FIRST_DEGREE_URNS, ...SECOND_DEGREE_URNS], THIRD_DEGREE_PLUS_URNS);

    // ─── 3+ Degree Download ─────────────────────────────────────────────────
    await lineagePage.degree3PlusFilter.click();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Verify result count shows all 10 items
    await lineagePage.expectResultTextVisible(RESULT_PATTERNS.RESULTS_10, TIMEOUTS.MEDIUM);

    downloadPath = await lineagePage.downloadCSV(context, CSV_FILES.THIRD_PLUS_DEGREE);
    await verifyCSVContent(downloadPath, [...FIRST_DEGREE_URNS, ...SECOND_DEGREE_URNS, ...THIRD_DEGREE_PLUS_URNS], []);
  });
});
