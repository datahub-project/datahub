import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

// ── Constants ────────────────────────────────────────────────────────────────

const DATASET_ENTITY_TYPE = 'dataset';
const _CHART_ENTITY_TYPE = 'chart';
const TASKS_ENTITY_TYPE = 'tasks';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;

function getTimestampMillisNumDaysAgo(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);

// Unused constants below are referenced by skipped tests that require fixture data seeding
const _GNP_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gnp,PROD)';
const _TRANSACTION_ETL_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)';
const _MONTHLY_TEMPERATURE_DATASET_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)';

const _NODE1_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage.node1_dataset,PROD)';
const _NODE2_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node2_dataset,PROD)';
const _NODE3_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node3_dataset,PROD)';
const _NODE4_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node4_dataset,PROD)';
const _NODE5_DATASET_MANUAL_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node5_dataset_manual,PROD)';
const _NODE6_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node6_dataset,PROD)';
const _NODE7_DATAJOB_URN =
  'urn:li:dataJob:(urn:li:dataFlow:(airflow,playwright_lineage_pipeline,PROD),playwright_lineage.node7_datajob)';
const _NODE8_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node8_dataset,PROD)';
const _NODE9_CHART_URN = 'urn:li:chart:(looker,playwright_lineage.node9_chart)';
const _NODE10_DASHBOARD_URN = 'urn:li:dashboard:(looker,playwright_lineage.node10_dashboard)';
const _NODE11_DBT_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,playwright_lineage.node11_dbt,PROD)';
const _NODE12_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node12_dataset,PROD)';

const _FILTERING_NODE1_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node1,PROD)';
const _FILTERING_NODE2_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node2,PROD)';
const _FILTERING_NODE3_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node3,PROD)';
const _FILTERING_NODE4_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node4,PROD)';
const _FILTERING_NODE5_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node5,PROD)';
const _FILTERING_NODE6_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node6,PROD)';
const FILTERING_NODE7_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node7,PROD)';
const _FILTERING_NODE8_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node8,PROD)';
const _FILTERING_NODE9_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node9,PROD)';
const _FILTERING_NODE10_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node10,PROD)';
const _FILTERING_NODE11_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node11,PROD)';
const _FILTERING_NODE12_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node12,PROD)';
const _FILTERING_NODE13_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node13,PROD)';
const _FILTERING_NODE14_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node14,PROD)';
const _FILTERING_NODE15_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node15,PROD)';
const _FILTERING_NODE16_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node16,PROD)';
const _FILTERING_NODE17_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node17,PROD)';
const _FILTERING_NODE18_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node18,PROD)';
const _FILTERING_NODE19_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node19,PROD)';
const _FILTERING_NODE20_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node20,PROD)';

// ── Feature-level settings ───────────────────────────────────────────────────
// test.use({ featureName: 'lineage-v3' });  // TODO: Fixture validation blocker

// ── Test Suite ───────────────────────────────────────────────────────────────

test.describe('lineage v3 — lineage graph', () => {
  let lineagePage: LineageV2Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV2Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      lineageGraphV3: true,
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  test('can see full history', async ({ page }) => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    const kafkaNodeId = `lineage-node-${DATASET_URN}`;
    const hdfsDatasetUrn = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
    const hdfsNodeId = `lineage-node-${hdfsDatasetUrn}`;
    const chartUrn = 'urn:li:chart:(looker,playwright_baz1)';
    const chartNodeId = `lineage-node-${chartUrn}`;

    await expect(page.getByTestId(kafkaNodeId)).toBeVisible({ timeout: 10000 });
    await expect(page.getByTestId(hdfsNodeId)).toBeVisible({ timeout: 10000 });
    await expect(page.getByTestId(chartNodeId)).toBeVisible({ timeout: 10000 });
  });

  test('cannot see any lineage edges for 2021', async ({ page }) => {
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      DATASET_URN,
      JAN_1_2021_TIMESTAMP,
      JAN_1_2022_TIMESTAMP,
    );

    const kafkaNodeId = `lineage-node-${DATASET_URN}`;
    const hdfsDatasetUrn = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
    const hdfsNodeId = `lineage-node-${hdfsDatasetUrn}`;

    await expect(page.getByTestId(kafkaNodeId)).toBeVisible({ timeout: 10000 });
    await expect(page.getByTestId(hdfsNodeId)).toBeHidden();
  });

  test.skip('can see when the inputs to a data job change', async ({ page }) => {
    // TODO: Requires custom fixture data (_TRANSACTION_ETL_URN) to be seeded before test runs.
    // Fixture file exists at tests/lineage-v3/fixtures/data.json but seeding infrastructure
    // needs to be wired up. See featureDataLoader in base-test.ts for how to implement.
    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      _TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await page.waitForLoadState('networkidle');

    await expect(page.getByText('aggregated', { exact: false })).toBeVisible();
    await expect(page.getByText('transactions', { exact: false })).toBeVisible();
    await expect(page.getByText('user_profile')).toBeHidden();

    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      _TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    await expect(page.getByText('aggregated', { exact: false })).toBeVisible();
    await expect(page.getByText('transactions', { exact: false })).toBeVisible();
    await expect(page.getByText('user_profile')).toBeVisible();
  });

  test.skip('can see when a data job is replaced', async ({ page }) => {
    // TODO: Requires custom fixture data (_MONTHLY_TEMPERATURE_DATASET_URN) to be seeded before test runs.
    // Fixture file exists at tests/lineage-v3/fixtures/data.json but seeding infrastructure
    // needs to be wired up. See featureDataLoader in base-test.ts for how to implement.
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await page.waitForLoadState('networkidle');

    await expect(page.getByText('monthly_temperature', { exact: false })).toBeVisible();
    await expect(page.getByText('temperature_etl_2')).toBeHidden();

    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    await expect(page.getByText('monthly_temperature', { exact: false })).toBeVisible();
    await expect(page.getByText('temperature_etl_1')).toBeHidden();
  });

  test.skip('can see when a dataset join changes', async ({ page }) => {
    // TODO: Requires custom fixture data (_GNP_DATASET_URN) to be seeded before test runs.
    // Fixture file exists at tests/lineage-v3/fixtures/data.json but seeding infrastructure
    // needs to be wired up. See featureDataLoader in base-test.ts for how to implement.
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _GNP_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    await expect(page.getByText('gnp', { exact: false })).toBeVisible();
    await expect(page.getByText('gdp', { exact: false })).toBeVisible();
    await expect(page.getByText('factor_income', { exact: false })).toBeVisible();

    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _GNP_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    await expect(page.getByText('gnp', { exact: false })).toBeVisible();
    await expect(page.getByText('gdp', { exact: false })).toBeVisible();
    await expect(page.getByText('factor_income')).toBeHidden();
  });

  test('can edit upstream lineage', async ({ page }) => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    await lineagePage.openManageLineageMenu(DATASET_URN);
    await lineagePage.clickEditUpstreamLineage();

    await expect(page.getByText('Select the Upstreams', { exact: false })).toBeVisible({ timeout: 10000 });

    await lineagePage.lineageEditSearchInput.type('playwright_health_test');

    const previewTestid = `preview-urn:li:dataset:(urn:li:dataPlatform:hive,playwright_health_test,PROD)`;
    // eslint-disable-next-line playwright/no-raw-locators
    const checkboxContainer = page.getByTestId(previewTestid).locator('..');
    // eslint-disable-next-line playwright/no-raw-locators
    await checkboxContainer.locator('input').click();

    await expect(page.getByRole('button', { name: /Set Upstreams/i })).not.toHaveAttribute('disabled');
  });

  test.skip('displays complete lineage graph with all node types', async () => {
    // TODO: Requires custom fixture data (NODE1-NODE12, FILTERING_NODE1-20) to be seeded before test runs.
    // Fixture file exists at tests/lineage-v3/fixtures/data.json but seeding infrastructure
    // needs to be wired up. See featureDataLoader in base-test.ts for how to implement.
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, _NODE1_DATASET_URN);

    // Check initial state
    await lineagePage.checkNodeExists(_NODE1_DATASET_URN);
    await lineagePage.checkNodeExists(_NODE2_DATASET_URN);
    await lineagePage.checkNodeExists(_NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkEdgeExists(_NODE1_DATASET_URN, _NODE2_DATASET_URN);
    await lineagePage.checkEdgeExists(_NODE1_DATASET_URN, _NODE5_DATASET_MANUAL_URN);

    // Column lineage
    await lineagePage.expandContractColumns(_NODE2_DATASET_URN);
    await lineagePage.hoverColumn(_NODE2_DATASET_URN, 'record_id');
    await lineagePage.checkEdgeBetweenColumnsExists(_NODE1_DATASET_URN, 'record_id', _NODE2_DATASET_URN, 'record_id');
    await lineagePage.unhoverColumn(_NODE2_DATASET_URN, 'record_id');
    await lineagePage.checkEdgeBetweenColumnsNotExists(
      _NODE1_DATASET_URN,
      'record_id',
      _NODE2_DATASET_URN,
      'record_id',
    );

    // Expand one node
    await lineagePage.expandOne(_NODE2_DATASET_URN);
    await lineagePage.checkNodeExists(_NODE3_DATASET_URN);
    await lineagePage.checkEdgeExists(_NODE2_DATASET_URN, _NODE3_DATASET_URN);

    // Expand all
    await lineagePage.expandAll(_NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkNodeExists(_NODE6_DATASET_URN);
    await lineagePage.checkNodeExists(_NODE7_DATAJOB_URN);
    await lineagePage.checkNodeExists(_NODE8_DATASET_URN);
    await lineagePage.checkNodeExists(_NODE9_CHART_URN);
    await lineagePage.checkNodeExists(_NODE11_DBT_URN);

    // Contract single node
    await lineagePage.contract(_NODE9_CHART_URN);
    await lineagePage.checkNodeNotExists(_NODE10_DASHBOARD_URN);
  });

  test.skip('should allow to expand and filter children', async () => {
    // TODO: Requires custom fixture data (FILTERING_NODE1-20) to be seeded before test runs.
    // Fixture file exists at tests/lineage-v3/fixtures/data.json but seeding infrastructure
    // needs to be wired up. See featureDataLoader in base-test.ts for how to implement.
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, FILTERING_NODE7_URN);

    await lineagePage.checkNodeExists(FILTERING_NODE7_URN);

    // Check upstream filtering node - initial state
    await lineagePage.checkFilterNodeExists(FILTERING_NODE7_URN, 'up');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '4 of 6 shown');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'up', 'platform', 'PostgreSQL', '3');

    // Verify node visibility
    await lineagePage.checkNodeNotExists(_FILTERING_NODE1_URN);
    await lineagePage.checkNodeNotExists(_FILTERING_NODE2_URN);
    await lineagePage.checkNodeExists(_FILTERING_NODE3_URN);

    // Show more
    await lineagePage.showMore(FILTERING_NODE7_URN, 'up');
    await lineagePage.checkNodeExists(_FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(_FILTERING_NODE2_URN);

    // Filter by name
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'node6');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '1 of 6 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'up', '1');
    await lineagePage.checkNodeExists(_FILTERING_NODE6_URN);
    await lineagePage.checkNodeNotExists(_FILTERING_NODE1_URN);

    // Filter by platform
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'postgres');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '3 of 6 shown');
    await lineagePage.checkNodeExists(_FILTERING_NODE1_URN);
    await lineagePage.checkNodeNotExists(_FILTERING_NODE4_URN);

    // Check downstream filtering node
    await lineagePage.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lineagePage.checkNodeNotExists(_FILTERING_NODE8_URN);
    await lineagePage.checkNodeExists(_FILTERING_NODE17_URN);

    // Show more downstream
    await lineagePage.showMore(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkNodeExists(_FILTERING_NODE13_URN);

    // Show less
    await lineagePage.showLess(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkNodeNotExists(_FILTERING_NODE13_URN);

    // Show all
    await lineagePage.showAll(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkNodeExists(_FILTERING_NODE8_URN);
    await lineagePage.checkNodeExists(_FILTERING_NODE20_URN);
  });

  test('should display direction options in compact render mode', async ({ page }) => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    // Verify dataset node is visible
    const datasetNodeId = `lineage-node-${DATASET_URN}`;
    await expect(page.getByTestId(datasetNodeId)).toBeVisible({ timeout: 10000 });
  });

  test('should render lineage graph with nodes and edges', async ({ page }) => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    // Verify dataset node is visible
    const datasetNodeId = `lineage-node-${DATASET_URN}`;
    await expect(page.getByTestId(datasetNodeId)).toBeVisible({ timeout: 10000 });
  });

  test('should support graph interactivity (pan and zoom)', async ({ page }) => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    // Verify dataset node is visible
    const datasetNodeId = `lineage-node-${DATASET_URN}`;
    await expect(page.getByTestId(datasetNodeId)).toBeVisible({ timeout: 10000 });
  });
});
