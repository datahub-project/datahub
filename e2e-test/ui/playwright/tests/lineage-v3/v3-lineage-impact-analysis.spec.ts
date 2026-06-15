/**
 * Impact Analysis V3 tests — migrated from Cypress v3_impact_analysis.js
 *
 * Tests the impact analysis view accessible via the Lineage tab:
 * - 1-hop vs multi-hop lineage visibility
 * - Advanced filtering by description text
 * - Column-level impact analysis and toggling
 * - Time-range filtering of lineage edges
 * - Data job input changes over time
 * - Editing upstream/downstream lineage from impact analysis view
 */

import { request as playwrightRequest } from '@playwright/test';
import { test } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS, gmsUrl, LOAD_STATES } from '../../utils/constants';
import { seedTimeRangeLineage } from '../../utils/lineage-time-seeder';
import { readGmsToken } from '../../fixtures/login';
import { users } from '../../data/users';

test.use({ featureName: 'lineage-v3' });

// ── Constants ───────────────────────────────────────────────────────────────

function getTimestampMillisNumDaysAgo(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const DATASET_NAME = 'SamplePlaywrightKafkaDataset';
const TRANSACTION_ETL_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)';
const MONTHLY_TEMPERATURE_DATASET_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)';

const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);

// UI text constants
const UI_TEXT = {
  IMPACT_ANALYSIS: 'Impact Analysis',
  USER_CREATIONS: 'User Creations',
  USER_DELETIONS: 'User Deletions',
  THREE_PLUS: '3+',
  ADVANCED: 'Advanced',
  ADD_FILTER: 'Add Filter',
  FILTER_TEXT: 'fct_users_deleted',
  HDFS_DATASET: 'SamplePlaywrightHdfsDataset',
  SHIPMENT_INFO: 'shipment_info',
  FEATURE_1: 'some-playwright-feature-1',
  BAZ_CHART: 'Baz Chart 1',
  DOWNSTREAM_COLUMN: 'Downstream column: shipment_info',
  AGGREGATED: 'aggregated',
  TRANSACTIONS: 'transactions',
  USER_PROFILE: 'user_profile',
  TEMPERATURE_ETL_1: 'temperature_etl_1',
  TEMPERATURE_ETL_2: 'temperature_etl_2',
} as const;

// ── Test Suite ──────────────────────────────────────────────────────────────

test.describe('impact analysis', () => {
  let lineagePage: LineageV3Page;

  // Seed time-range lineage data once before all tests in this suite
  test.beforeAll(async () => {
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken(users.admin.username);
      await seedTimeRangeLineage(apiContext, gmsToken);
    } finally {
      await apiContext.dispose();
    }
  });

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags({
      lineageGraphV3: true,
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  test('can see 1 hop of lineage by default', async () => {
    await lineagePage.goToDatasetLineage(DATASET_URN, DATASET_NAME);

    // Multi-hop datasets should not be visible at 1-hop depth
    await lineagePage.expectResultTextHidden(UI_TEXT.USER_CREATIONS);
    await lineagePage.expectResultTextHidden(UI_TEXT.USER_DELETIONS);
  });

  test('can see lineage multiple hops away', async () => {
    await lineagePage.goToDatasetLineage(DATASET_URN, DATASET_NAME);

    await lineagePage.clickImpactAnalysis();
    await lineagePage.clickText(UI_TEXT.THREE_PLUS);

    await lineagePage.expectResultTextVisible(UI_TEXT.USER_CREATIONS, TIMEOUTS.LONG);
    await lineagePage.expectResultTextVisible(UI_TEXT.USER_DELETIONS, TIMEOUTS.LONG);
  });

  test('can filter the lineage results', async () => {
    await lineagePage.goToDatasetLineage(DATASET_URN, DATASET_NAME);

    await lineagePage.clickImpactAnalysis();
    await lineagePage.clickText(UI_TEXT.THREE_PLUS);

    await lineagePage.addDescriptionFilter(UI_TEXT.FILTER_TEXT);

    await lineagePage.expectResultTextHidden(UI_TEXT.USER_CREATIONS);
    await lineagePage.expectResultTextVisible(UI_TEXT.USER_DELETIONS, TIMEOUTS.LONG);
  });

  test('can view column level impact analysis and turn it off', async ({ page }) => {
    // Navigate directly to the column lineage URL
    const columnParam = encodeURIComponent('[version=2.0].[type=boolean].field_bar');
    await page.goto(`/dataset/${DATASET_URN}/Lineage?column=${columnParam}&is_lineage_mode=false`);
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await page.waitForTimeout(TIMEOUTS.MEDIUM);

    await lineagePage.clickImpactAnalysis();

    await lineagePage.expectResultTextVisible(UI_TEXT.HDFS_DATASET, TIMEOUTS.LONG);
    await lineagePage.expectResultTextVisible(UI_TEXT.SHIPMENT_INFO, TIMEOUTS.MEDIUM);
    await lineagePage.expectResultTextHidden(UI_TEXT.FEATURE_1);
    await lineagePage.expectResultTextHidden(UI_TEXT.BAZ_CHART);

    // Toggle off column-level impact analysis
    await lineagePage.clickColumnLineageToggle();
    await page.waitForTimeout(TIMEOUTS.SHORT);

    await lineagePage.expectResultTextVisible(UI_TEXT.HDFS_DATASET, TIMEOUTS.MEDIUM);
    await lineagePage.expectResultTextHidden(UI_TEXT.SHIPMENT_INFO, TIMEOUTS.SHORT);
    await lineagePage.expectResultTextVisible(UI_TEXT.FEATURE_1, TIMEOUTS.MEDIUM);
    await lineagePage.expectResultTextVisible(UI_TEXT.BAZ_CHART, TIMEOUTS.MEDIUM);
  });

  test('can filter lineage edges by time', async ({ page }) => {
    await page.goto(
      `/dataset/${DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${JAN_1_2021_TIMESTAMP}&end_time_millis=${JAN_1_2022_TIMESTAMP}`,
    );
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await page.waitForTimeout(TIMEOUTS.MEDIUM);

    await lineagePage.clickImpactAnalysis();

    // No lineage edges should exist for the 2021 time window
    await lineagePage.expectResultTextHidden(UI_TEXT.HDFS_DATASET);
    await lineagePage.expectResultTextHidden(UI_TEXT.DOWNSTREAM_COLUMN);
    await lineagePage.expectResultTextHidden(UI_TEXT.FEATURE_1);
    await lineagePage.expectResultTextHidden(UI_TEXT.BAZ_CHART);
  });

  test('can see when the inputs to a data job change', async ({ page, apiMock }) => {
    test.setTimeout(90000);

    // DataJob root entities must use V3 graph
    await apiMock.setFeatureFlags({ lineageGraphV3: true });

    // Between 14 days ago and 7 days ago, only transactions was an input
    await page.goto(
      `/tasks/${TRANSACTION_ETL_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_14_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}`,
    );
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await page.waitForTimeout(TIMEOUTS.MEDIUM);

    await lineagePage.clickSidebarLineageTab();
    // Downstream
    await lineagePage.expectResultTextVisible(UI_TEXT.AGGREGATED, TIMEOUTS.EXTRA_LONG);
    // Upstream
    await lineagePage.clickUpstreamDirection();
    await lineagePage.expectResultTextVisible(UI_TEXT.TRANSACTIONS, TIMEOUTS.EXTRA_LONG);
    await lineagePage.expectResultTextNotVisible(UI_TEXT.USER_PROFILE, TIMEOUTS.SHORT);

    // From 7 days ago to now, user_profile was also added as an input
    await page.goto(
      `/tasks/${TRANSACTION_ETL_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_NOW}`,
    );
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await page.waitForTimeout(TIMEOUTS.MEDIUM);

    await lineagePage.clickSidebarLineageTab();
    // Downstream
    await lineagePage.expectResultTextVisible(UI_TEXT.AGGREGATED, TIMEOUTS.EXTRA_LONG);
    // Upstream
    await lineagePage.clickUpstreamDirection();
    await lineagePage.expectResultTextVisible(UI_TEXT.TRANSACTIONS, TIMEOUTS.EXTRA_LONG);
    await lineagePage.expectResultTextVisible(UI_TEXT.USER_PROFILE, TIMEOUTS.EXTRA_LONG);
  });

  test('can see when a data job is replaced', async ({ page }) => {
    // Between 14 days ago and 7 days ago — temperature_etl_1 is the input
    await page.goto(
      `/dataset/${MONTHLY_TEMPERATURE_DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_14_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}`,
    );
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await page.waitForTimeout(TIMEOUTS.SHORT);

    await lineagePage.clickSidebarLineageTab();
    await lineagePage.clickUpstreamDirection();

    await lineagePage.expectResultTextVisible(UI_TEXT.TEMPERATURE_ETL_1, TIMEOUTS.MEDIUM);

    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    await page.goto(
      `/dataset/${MONTHLY_TEMPERATURE_DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_NOW}`,
    );
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
    await page.waitForTimeout(TIMEOUTS.SHORT);

    await lineagePage.clickSidebarLineageTab();
    await lineagePage.clickUpstreamDirection();

    await lineagePage.expectResultTextVisible(UI_TEXT.TEMPERATURE_ETL_2, TIMEOUTS.MEDIUM);
  });

  test('editing upstream lineage will redirect to visual view with edit modal open', async ({ page }) => {
    await page.goto(`/dataset/${DATASET_URN}/Lineage?is_lineage_mode=false&lineageView=impact`);
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);

    await lineagePage.expectResultTextVisible(DATASET_NAME, TIMEOUTS.LONG);

    await lineagePage.clickLineageEditMenuButton();
    await lineagePage.clickEditUpstreamLineage();

    await lineagePage.expectResultTextVisible(`Select the Upstreams to add to ${DATASET_NAME}`, TIMEOUTS.MEDIUM);
  });

  test('editing downstream lineage will redirect to visual view with edit modal open', async ({ page }) => {
    await page.goto(`/dataset/${DATASET_URN}/Lineage?is_lineage_mode=false&lineageView=impact`);
    await page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);

    await lineagePage.expectResultTextVisible(DATASET_NAME, TIMEOUTS.LONG);

    await lineagePage.clickLineageEditMenuButton();
    await lineagePage.clickEditDownstreamLineage();

    await lineagePage.expectResultTextVisible(`Select the Downstreams to add to ${DATASET_NAME}`, TIMEOUTS.MEDIUM);
  });
});
