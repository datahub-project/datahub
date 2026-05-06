/**
 * Impact Analysis V2 tests
 * Migrated from Cypress e2e/lineageV2/v2_impact_analysis.js
 *
 * Tests the impact analysis view accessible via the Lineage tab:
 * - 1-hop vs multi-hop lineage visibility
 * - Advanced filtering by description text
 * - Column-level impact analysis and toggling
 * - Time-range filtering of lineage edges
 * - Data job input changes over time
 * - Editing upstream/downstream lineage from impact analysis view
 *
 * Uses apiMock.setFeatureFlags to disable lineageGraphV3 so the V2 graph renders.
 */

import { request as playwrightRequest } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';
import { seedTimeRangeLineage } from '../../utils/lineage-time-seeder';
import { gmsUrl } from '../../utils/constants';
import { readGmsToken } from '../../fixtures/login';

// ── Constants ───────────────────────────────────────────────────────────────

function getTimestampMillisNumDaysAgo(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const TRANSACTION_ETL_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)';
const MONTHLY_TEMPERATURE_DATASET_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)';

const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);

// ── Suite ────────────────────────────────────────────────────────────────────

test.describe('impact analysis', () => {
  // Seed time-range lineage data once before all tests in this suite.
  test.beforeAll(async () => {
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken('datahub');
      await seedTimeRangeLineage(apiContext, gmsToken);
    } finally {
      await apiContext.dispose();
    }
  });

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  test('can see 1 hop of lineage by default', async ({ page, logger, logDir }) => {
    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToDatasetLineage(DATASET_URN, 'SamplePlaywrightKafkaDataset');

    // Multi-hop datasets should not be visible at 1-hop depth
    await expect(page.getByText('User Creations')).toBeHidden();
    await expect(page.getByText('User Deletions')).toBeHidden();
  });

  test('can see lineage multiple hops away', async ({ page, logger, logDir }) => {
    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToDatasetLineage(DATASET_URN, 'SamplePlaywrightKafkaDataset');

    await lp.clickImpactAnalysis();
    await page.getByText('3+').click();

    await expect(page.getByText('User Creations').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('User Deletions').first()).toBeVisible({ timeout: 15000 });
  });

  test('can filter the lineage results as well', async ({ page, logger, logDir }) => {
    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToDatasetLineage(DATASET_URN, 'SamplePlaywrightKafkaDataset');

    await lp.clickImpactAnalysis();
    await page.getByText('3+').click();

    await lp.clickAdvancedFilter();
    await lp.clickAddFilter();
    await lp.clickFilterByDescription();
    await lp.typeFilterText('fct_users_deleted');
    await lp.confirmFilterText();

    await expect(page.getByText('User Creations')).toBeHidden();
    await expect(page.getByText('User Deletions').first()).toBeVisible({ timeout: 15000 });
  });

  test('can view column level impact analysis and turn it off', async ({ page, logger, logDir }) => {
    const lp = new LineageV2Page(page, logger, logDir);

    // Navigate directly to the column lineage URL
    const columnParam = encodeURIComponent('[version=2.0].[type=boolean].field_bar');
    await page.goto(`/dataset/${DATASET_URN}/Lineage?column=${columnParam}&is_lineage_mode=false`);
    await page.waitForLoadState('domcontentloaded');

    // Impact analysis can take a moment
    await page.waitForTimeout(5000);
    await lp.clickImpactAnalysis();

    await expect(page.getByText('SamplePlaywrightHdfsDataset').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('shipment_info').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('some-playwright-feature-1')).toBeHidden();
    await expect(page.getByText('Baz Chart 1')).toBeHidden();

    // Toggle off column-level impact analysis
    await lp.clickColumnLineageToggle();
    await page.waitForTimeout(2000);

    await expect(page.getByText('SamplePlaywrightHdfsDataset').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info')).toBeHidden();
    await expect(page.getByText('some-playwright-feature-1').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('Baz Chart 1').first()).toBeVisible({ timeout: 10000 });
  });

  test('can filter lineage edges by time', async ({ page, logger, logDir }) => {
    const lp = new LineageV2Page(page, logger, logDir);

    await page.goto(
      `/dataset/${DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${JAN_1_2021_TIMESTAMP}&end_time_millis=${JAN_1_2022_TIMESTAMP}`,
    );
    await page.waitForLoadState('domcontentloaded');
    await page.waitForTimeout(5000);

    await lp.clickImpactAnalysis();

    // No lineage edges should exist for the 2021 time window
    await expect(page.getByText('SamplePlaywrightHdfsDataset')).toBeHidden();
    await expect(page.getByText('Downstream column: shipment_info')).toBeHidden();
    await expect(page.getByText('some-playwright-feature-1')).toBeHidden();
    await expect(page.getByText('Baz Chart 1')).toBeHidden();
  });

  test('can see when the inputs to a data job change', async ({ page, apiMock, logger, logDir }) => {
    // The sidebar compact widget and impact analysis list view for data-job pages do not
    // respect URL time-range params in the current DataHub UI. The visual lineage graph
    // correctly applies time-range filtering, so use that approach here.
    test.setTimeout(90000);

    // DataJob root entities must use V3 — LineageGraph.tsx routes DataJob to V2 when
    // lineageGraphV3=false (because only DataFlow is hard-coded to always use V3). V2 renders
    // an empty canvas for DataJob roots because DEFAULT_IGNORE_AS_HOPS includes EntityType.DataJob,
    // causing the backend to treat the root as a transparent hop.
    await apiMock.setFeatureFlags({ lineageGraphV3: true });

    const lp = new LineageV2Page(page, logger, logDir);

    // Between 14 days ago and 7 days ago, only transactions was an input
    await lp.goToLineageGraphWithTimeRange(
      'tasks',
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await page.waitForTimeout(5000);
    await expect(page.getByText('aggregated').first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText('transactions').first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText('user_profile').first()).not.toBeVisible({ timeout: 5000 });

    // From 7 days ago to now, user_profile was also added as an input
    await lp.goToLineageGraphWithTimeRange(
      'tasks',
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForTimeout(5000);
    await expect(page.getByText('aggregated').first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText('transactions').first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText('user_profile').first()).toBeVisible({ timeout: 20000 });
  });

  test('can see when a data job is replaced', async ({ page, logger, logDir }) => {
    const lp = new LineageV2Page(page, logger, logDir);

    // Between 14 days ago and 7 days ago — temperature_etl_1 is the input
    await page.goto(
      `/dataset/${MONTHLY_TEMPERATURE_DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_14_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}`,
    );
    await page.waitForLoadState('domcontentloaded');
    await page.waitForTimeout(2000);
    await lp.clickSidebarLineageTab();
    await lp.clickUpstreamDirection();
    await expect(page.getByText('temperature_etl_1').first()).toBeVisible({ timeout: 10000 });

    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    await page.goto(
      `/dataset/${MONTHLY_TEMPERATURE_DATASET_URN}/Lineage?filter_degree___false___EQUAL___0=1&is_lineage_mode=false&page=1&unionType=0&start_time_millis=${TIMESTAMP_MILLIS_7_DAYS_AGO}&end_time_millis=${TIMESTAMP_MILLIS_NOW}`,
    );
    await page.waitForLoadState('domcontentloaded');
    await page.waitForTimeout(2000);
    await lp.clickSidebarLineageTab();
    await lp.clickUpstreamDirection();
    await expect(page.getByText('temperature_etl_2').first()).toBeVisible({ timeout: 10000 });
  });

  test('editing upstream lineage will redirect to visual view with edit modal open', async ({
    page,
    logger,
    logDir,
  }) => {
    const lp = new LineageV2Page(page, logger, logDir);

    await page.goto(`/dataset/${DATASET_URN}/Lineage?is_lineage_mode=false&lineageView=impact`);
    await page.waitForLoadState('domcontentloaded');
    await expect(page.getByText('SamplePlaywrightKafkaDataset').first()).toBeVisible({ timeout: 15000 });

    await lp.clickLineageEditMenuButton();
    await lp.clickEditUpstreamLineage();

    await expect(page.getByText('Select the Upstreams to add to SamplePlaywrightKafkaDataset')).toBeVisible({
      timeout: 10000,
    });
  });

  test('editing downstream lineage will redirect to visual view with edit modal open', async ({
    page,
    logger,
    logDir,
  }) => {
    const lp = new LineageV2Page(page, logger, logDir);

    await page.goto(`/dataset/${DATASET_URN}/Lineage?is_lineage_mode=false&lineageView=impact`);
    await page.waitForLoadState('domcontentloaded');
    await expect(page.getByText('SamplePlaywrightKafkaDataset').first()).toBeVisible({ timeout: 15000 });

    await lp.clickLineageEditMenuButton();
    await lp.clickEditDownstreamLineage();

    await expect(page.getByText('Select the Downstreams to add to SamplePlaywrightKafkaDataset')).toBeVisible({
      timeout: 10000,
    });
  });
});
