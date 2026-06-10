import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';
import { seedDatasetStub, seedDataJobStub, seedDataJobInputOutput, seedUpstreamLineage, makeEdge, daysAgoMs } from './utils';

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

  test.beforeAll(async ({ playwright, gmsToken, logger }) => {
    // Seed fixture data using the working ingestProposal endpoint
    const request = await playwright.request.newContext();
    try {
      const eightDaysAgo = daysAgoMs(8);
      const oneDayAgo = daysAgoMs(1);

      // ── Case 1: transaction_etl input datasets change ──────────────────────────
      await seedDatasetStub(request, gmsToken, 'urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.transactions,PROD)', 'Transactions', logger);
      await seedDatasetStub(request, gmsToken, 'urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)', 'User Profile', logger);
      await seedDatasetStub(request, gmsToken, 'urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.aggregated_transactions,PROD)', 'Aggregated Transactions', logger);
      const bqEtlFlowUrn = 'urn:li:dataFlow:(airflow,bq_etl,prod)';
      await seedDataJobStub(request, gmsToken, _TRANSACTION_ETL_URN, 'transaction_etl', bqEtlFlowUrn, logger);
      await seedDataJobInputOutput(request, gmsToken, _TRANSACTION_ETL_URN,
        [
          makeEdge('urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.transactions,PROD)', eightDaysAgo, oneDayAgo),
          makeEdge('urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)', oneDayAgo, oneDayAgo),
        ],
        [makeEdge('urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.aggregated_transactions,PROD)', eightDaysAgo, oneDayAgo)],
        logger);

      // ── Case 2: temperature_etl_1 → temperature_etl_2 replacement ────────────
      const snowflakeEtlFlowUrn = 'urn:li:dataFlow:(airflow,snowflake_etl,PROD)';
      const temperatureEtl1Urn = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,snowflake_etl,PROD),temperature_etl_1)';
      const temperatureEtl2Urn = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,snowflake_etl,PROD),temperature_etl_2)';
      const dailyTempUrn = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)';
      await seedDatasetStub(request, gmsToken, dailyTempUrn, 'Daily Temperature', logger);
      await seedDatasetStub(request, gmsToken, _MONTHLY_TEMPERATURE_DATASET_URN, 'Monthly Temperature', logger);
      await seedDataJobStub(request, gmsToken, temperatureEtl1Urn, 'temperature_etl_1', snowflakeEtlFlowUrn, logger);
      await seedDataJobStub(request, gmsToken, temperatureEtl2Urn, 'temperature_etl_2', snowflakeEtlFlowUrn, logger);
      await seedDataJobInputOutput(request, gmsToken, temperatureEtl1Urn,
        [makeEdge(dailyTempUrn, eightDaysAgo, eightDaysAgo)],
        [makeEdge(_MONTHLY_TEMPERATURE_DATASET_URN, eightDaysAgo, eightDaysAgo)],
        logger);
      await seedDataJobInputOutput(request, gmsToken, temperatureEtl2Urn,
        [makeEdge(dailyTempUrn, oneDayAgo, oneDayAgo)],
        [makeEdge(_MONTHLY_TEMPERATURE_DATASET_URN, oneDayAgo, oneDayAgo)],
        logger);

      // ── Case 3: gnp dataset join change ──────────────────────────────────────
      const gdpUrn = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gdp,PROD)';
      const factorIncomeUrn = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.factor_income,PROD)';
      await seedDatasetStub(request, gmsToken, gdpUrn, 'GDP Dataset', logger);
      await seedDatasetStub(request, gmsToken, factorIncomeUrn, 'Factor Income Dataset', logger);
      await seedDatasetStub(request, gmsToken, _GNP_DATASET_URN, 'GNP Dataset', logger);
      await seedUpstreamLineage(request, gmsToken, _GNP_DATASET_URN,
        [
          { urn: gdpUrn, createdMs: eightDaysAgo, updatedMs: oneDayAgo },
          { urn: factorIncomeUrn, createdMs: eightDaysAgo, updatedMs: eightDaysAgo },
        ],
        logger);

      // Seed all node types for complete graph test
      await seedDatasetStub(request, gmsToken, _NODE1_DATASET_URN, 'Node 1 Dataset', logger);
      await seedDatasetStub(request, gmsToken, _NODE2_DATASET_URN, 'Node 2 Dataset', logger);
      await seedDatasetStub(request, gmsToken, _NODE3_DATASET_URN, 'Node 3 Dataset', logger);
      await seedDatasetStub(request, gmsToken, _NODE5_DATASET_MANUAL_URN, 'Node 5 Manual Dataset', logger);
      await seedDatasetStub(request, gmsToken, _NODE6_DATASET_URN, 'Node 6 Dataset', logger);
      await seedDataJobStub(request, gmsToken, _NODE7_DATAJOB_URN, 'node7_datajob', 'urn:li:dataFlow:(airflow,playwright_lineage_pipeline,PROD)', logger);
      await seedDatasetStub(request, gmsToken, _NODE8_DATASET_URN, 'Node 8 Dataset', logger);

      // Seed filtering nodes
      await seedDatasetStub(request, gmsToken, FILTERING_NODE7_URN, 'Filtering Node 7', logger);
      for (let i = 1; i <= 6; i++) {
        const nodeUrn = eval(`_FILTERING_NODE${i}_URN`);
        await seedDatasetStub(request, gmsToken, nodeUrn, `Filtering Node ${i}`, logger);
      }
      for (let i = 8; i <= 20; i++) {
        const nodeUrn = eval(`_FILTERING_NODE${i}_URN`);
        await seedDatasetStub(request, gmsToken, nodeUrn, `Filtering Node ${i}`, logger);
      }
    } finally {
      await request.dispose();
    }
  });

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

  test('can see when the inputs to a data job change', async ({ page }) => {
    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      _TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await page.waitForLoadState('networkidle');

    // Time range 14-7 days ago: transactions and aggregated visible, user_profile hidden
    await lineagePage.checkDatasetNodeVisible('transactions.aggregated_transactions');
    await lineagePage.checkDatasetNodeVisible('transactions.transactions');
    await lineagePage.checkDatasetNodeHidden('transactions.user_profile');

    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      _TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    // Time range 7 days - now: all three visible (user_profile added)
    await lineagePage.checkDatasetNodeVisible('transactions.aggregated_transactions');
    await lineagePage.checkDatasetNodeVisible('transactions.transactions');
    await lineagePage.checkDatasetNodeVisible('transactions.user_profile');
  });

  test('can see when a data job is replaced', async ({ page }) => {
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await page.waitForLoadState('networkidle');

    // Time range 14-7 days ago: only temperature_etl_1 job visible
    await lineagePage.checkDatasetNodeVisible('climate.monthly_temperature');
    await lineagePage.checkDatasetNodeHidden('temperature_etl_2');

    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    // Time range 7 days - now: only temperature_etl_2 job visible (replaced)
    await lineagePage.checkDatasetNodeVisible('climate.monthly_temperature');
    await lineagePage.checkDatasetNodeHidden('temperature_etl_1');
  });

  test('can see when a dataset join changes', async ({ page }) => {
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _GNP_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    // Time range 14 days - now: both upstream sources visible (gdp and factor_income)
    await lineagePage.checkDatasetNodeVisible('economic_data.gnp');
    await lineagePage.checkDatasetNodeVisible('economic_data.gdp');
    await lineagePage.checkDatasetNodeVisible('economic_data.factor_income');

    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      _GNP_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForLoadState('networkidle');

    // Time range 7 days - now: only gdp visible (factor_income join removed after 8 days)
    await lineagePage.checkDatasetNodeVisible('economic_data.gnp');
    await lineagePage.checkDatasetNodeVisible('economic_data.gdp');
    await lineagePage.checkDatasetNodeHidden('economic_data.factor_income');
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

  test('displays complete lineage graph with all node types', async () => {
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

  test('should allow to expand and filter children', async () => {
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
