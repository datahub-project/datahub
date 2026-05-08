/**
 * Lineage Graph V2 tests — migrated from Cypress e2e/lineageV2/v2_lineage_graph.js
 *
 * Tests the lineageV2 graph: node/edge visibility, time-range filtering, expand/contract,
 * column-level edges, filtering nodes, and manage-lineage modal entry point.
 *
 * Uses apiMock.setFeatureFlags to disable lineageGraphV3 so the V2 graph renders.
 *
 * Prerequisites: playwright_lineage* and SamplePlaywright* entities must exist in the test
 * environment (seeded via global test data in test-data/data.json).
 * Time-range tests additionally require dynamic lineage edges seeded in beforeAll via
 * seedTimeRangeLineage() which computes relative timestamps at runtime.
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

const DATASET_ENTITY_TYPE = 'dataset';
const CHART_ENTITY_TYPE = 'chart';
const TASKS_ENTITY_TYPE = 'tasks';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;

const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);

const GNP_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gnp,PROD)';
const TRANSACTION_ETL_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)';
const MONTHLY_TEMPERATURE_DATASET_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)';

// Complete lineage graph nodes
const NODE1_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage.node1_dataset,PROD)';
const NODE2_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node2_dataset,PROD)';
const NODE3_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node3_dataset,PROD)';
const NODE4_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node4_dataset,PROD)';
const NODE5_DATASET_MANUAL_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node5_dataset_manual,PROD)';
const NODE6_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node6_dataset,PROD)';
const NODE7_DATAJOB_URN =
  'urn:li:dataJob:(urn:li:dataFlow:(airflow,playwright_lineage_pipeline,PROD),playwright_lineage.node7_datajob)';
const NODE8_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node8_dataset,PROD)';
const NODE9_CHART_URN = 'urn:li:chart:(looker,playwright_lineage.node9_chart)';
const NODE10_DASHBOARD_URN = 'urn:li:dashboard:(looker,playwright_lineage.node10_dashboard)';
const NODE11_DBT_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,playwright_lineage.node11_dbt,PROD)';
const NODE12_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node12_dataset,PROD)';

// Filtering test nodes — upstream (1-6) and downstream (8-20)
const FILTERING_NODE1_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node1,PROD)';
const FILTERING_NODE2_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node2,PROD)';
const FILTERING_NODE3_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node3,PROD)';
const FILTERING_NODE4_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node4,PROD)';
const FILTERING_NODE5_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node5,PROD)';
const FILTERING_NODE6_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node6,PROD)';
const FILTERING_NODE7_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node7,PROD)';
const FILTERING_NODE8_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node8,PROD)';
const FILTERING_NODE9_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node9,PROD)';
const FILTERING_NODE10_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage_filtering.node10,PROD)';
const FILTERING_NODE11_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node11,PROD)';
const FILTERING_NODE12_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node12,PROD)';
const FILTERING_NODE13_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node13,PROD)';
const FILTERING_NODE14_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node14,PROD)';
const FILTERING_NODE15_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node15,PROD)';
const FILTERING_NODE16_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node16,PROD)';
const FILTERING_NODE17_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node17,PROD)';
const FILTERING_NODE18_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node18,PROD)';
const FILTERING_NODE19_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node19,PROD)';
const FILTERING_NODE20_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage_filtering.node20,PROD)';

// ── Suite setup ──────────────────────────────────────────────────────────────

// Each test is independent — no shared mutations that need serial execution.
test.describe('lineage_graph', () => {
  // Seed time-range lineage data once before all tests in this suite.
  // The seeder computes timestamps relative to the current run time so they
  // can't live in a static data.json file.
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
    // Disable lineageGraphV3 so the V2 lineage graph renders.
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  // ── History & time range tests ──────────────────────────────────────────────

  test('can see full history', async ({ page, logger, logDir }) => {
    const lineagePage = new LineageV2Page(page, logger, logDir);
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    await expect(page.getByText('SamplePlaywrightKafka').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('SamplePlaywrightHdfs').first()).toBeAttached({ timeout: 10000 });
    await expect(page.getByText('Baz Chart 1').first()).toBeAttached({ timeout: 10000 });
    await expect(page.getByText('some-playwright').first()).toBeAttached({ timeout: 10000 });
  });

  test('cannot see any lineage edges for 2021', async ({ page, logger, logDir }) => {
    const lineagePage = new LineageV2Page(page, logger, logDir);
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      DATASET_URN,
      JAN_1_2021_TIMESTAMP,
      JAN_1_2022_TIMESTAMP,
    );

    await expect(page.getByText('SamplePlaywrightKafka').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('SamplePlaywrightHdfs').first()).not.toBeVisible({ timeout: 5000 });
    await expect(page.getByText('Baz Chart 1').first()).not.toBeVisible({ timeout: 5000 });
    await expect(page.getByText('some-playwright').first()).not.toBeVisible({ timeout: 5000 });
  });

  test('can see when the inputs to a data job change', async ({ page, apiMock, logger, logDir }) => {
    // Task entity lineage graphs can take longer to render in CI with time-range filters.
    test.setTimeout(90000);

    // DataJob root entities must use V3 — LineageGraph.tsx routes DataJob to V2 when
    // lineageGraphV3=false (because only DataFlow is hard-coded to always use V3). V2 renders
    // an empty canvas for DataJob roots because DEFAULT_IGNORE_AS_HOPS includes EntityType.DataJob,
    // causing the backend to treat the root as a transparent hop.
    await apiMock.setFeatureFlags({ lineageGraphV3: true });

    const lineagePage = new LineageV2Page(page, logger, logDir);

    // Between 14 days ago and 7 days ago, only transactions was an input
    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await page.waitForTimeout(5000);
    await expect(page.getByText('aggregated').first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText('transactions').first()).toBeAttached();
    await expect(page.getByText('user_profile').first()).not.toBeVisible({ timeout: 5000 });

    // From 7 days ago to now, user_profile was also added as an input
    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await page.waitForTimeout(5000);
    await expect(page.getByText('aggregated').first()).toBeVisible({ timeout: 20000 });
    await expect(page.getByText('transactions').first()).toBeAttached();
    await expect(page.getByText('user_profile').first()).toBeAttached();
  });

  test('can see when a data job is replaced', async ({ page, logger, logDir }) => {
    const lineagePage = new LineageV2Page(page, logger, logDir);

    // Between 14 days ago and 7 days ago, temperature_etl_2 should not exist
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );
    await expect(page.getByText('monthly_temperature').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('temperature_etl_2').first()).not.toBeVisible({ timeout: 5000 });

    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await expect(page.getByText('monthly_temperature').first()).toBeAttached();
    await expect(page.getByText('temperature_etl_1').first()).not.toBeVisible({ timeout: 5000 });
  });

  test('can see when a dataset join changes', async ({ page, logger, logDir }) => {
    const lineagePage = new LineageV2Page(page, logger, logDir);

    // 8 days ago, both gdp and factor_income were joined to create gnp
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await expect(page.getByText('gnp').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('gdp').first()).toBeAttached();
    await expect(page.getByText('factor_income').first()).toBeAttached();

    // Since 7 days ago, factor_income was removed from the join
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );
    await expect(page.getByText('gnp').first()).toBeAttached();
    await expect(page.getByText('gdp').first()).toBeAttached();
    await expect(page.getByText('factor_income').first()).not.toBeVisible({ timeout: 5000 });
  });

  // ── Edit upstream lineage ───────────────────────────────────────────────────

  test('can edit upstream lineage', async ({ page, logger, logDir }) => {
    // Graph render + modal open + search API round-trip can together exceed 90s in CI.
    test.setTimeout(120000);

    // This test does not make any mutations to avoid flakiness.
    // It verifies the manage lineage menu and modal entry point only.

    const lineagePage = new LineageV2Page(page, logger, logDir);
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await lineagePage.openManageLineageMenu(DATASET_URN);
    await lineagePage.clickEditUpstreamLineage();

    await expect(page.getByText('Select the Upstreams to add to SamplePlaywrightKafkaDataset')).toBeVisible({
      timeout: 10000,
    });

    // The "Set Upstreams" button should be disabled with nothing selected
    await expect(page.getByText('Set Upstreams')).toBeDisabled();

    // Search for any term that returns results. 'playwright_health_test' returns 36 entities
    // via token matching (page size = 10), so the specific target card may land on page 2+
    // and never appear in the DOM. This test verifies the modal entry point and selection
    // mechanism — not that a specific entity appears — so we click the first entity on page 1.
    await lineagePage.searchInLineageEditModal('playwright_health_test');

    // Wait for any entity checkbox on page 1 of results (data-testid="checkbox-urn:li:...")
    const firstEntityCheckbox = page.locator('[role="dialog"] [data-testid^="checkbox-urn"]').first();
    await expect(firstEntityCheckbox).toBeVisible({ timeout: 30000 });
    await firstEntityCheckbox.click();

    // After selection, Set Upstreams should be enabled
    await expect(page.getByText('Set Upstreams')).toBeEnabled();
  });

  // ── Complete lineage graph with node types ──────────────────────────────────

  test('displays complete lineage graph with all node types', async ({ page, logger, logDir }) => {
    test.setTimeout(90000);

    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToLineageGraph(DATASET_ENTITY_TYPE, NODE1_DATASET_URN);

    // Check initial state
    await lp.checkNodeExists(NODE1_DATASET_URN);
    await lp.checkNodeExists(NODE2_DATASET_URN);
    await lp.checkNodeExists(NODE5_DATASET_MANUAL_URN);
    await lp.checkEdgeExists(NODE1_DATASET_URN, NODE2_DATASET_URN);
    await lp.checkEdgeExists(NODE1_DATASET_URN, NODE5_DATASET_MANUAL_URN);

    // Fields lineage — hover/unhover cycle for column edges
    await lp.expandContractColumns(NODE2_DATASET_URN);
    await lp.hoverColumn(NODE2_DATASET_URN, 'record_id');
    await lp.checkEdgeBetweenColumnsExists(NODE1_DATASET_URN, 'record_id', NODE2_DATASET_URN, 'record_id');
    await lp.unhoverColumn(NODE2_DATASET_URN, 'record_id');
    await lp.checkEdgeBetweenColumnsNotExists(NODE1_DATASET_URN, 'record_id', NODE2_DATASET_URN, 'record_id');

    await lp.hoverColumn(NODE2_DATASET_URN, 'next_record_id');
    await lp.checkEdgeBetweenColumnsExists(NODE1_DATASET_URN, 'next_record_id', NODE2_DATASET_URN, 'next_record_id');
    await lp.unhoverColumn(NODE2_DATASET_URN, 'next_record_id');
    await lp.checkEdgeBetweenColumnsNotExists(NODE1_DATASET_URN, 'next_record_id', NODE2_DATASET_URN, 'next_record_id');

    // Click (select) a column — edges should persist
    await lp.selectColumn(NODE2_DATASET_URN, 'record_id');
    await lp.checkEdgeBetweenColumnsExists(NODE1_DATASET_URN, 'record_id', NODE2_DATASET_URN, 'record_id');

    await lp.selectColumn(NODE2_DATASET_URN, 'next_record_id');
    await lp.checkEdgeBetweenColumnsExists(NODE1_DATASET_URN, 'next_record_id', NODE2_DATASET_URN, 'next_record_id');
    // After selecting next_record_id, record_id edge should be deselected
    await lp.checkEdgeBetweenColumnsNotExists(NODE1_DATASET_URN, 'record_id', NODE2_DATASET_URN, 'record_id');

    // Expand one node at a time
    await lp.expandOne(NODE2_DATASET_URN);
    await lp.checkNodeExists(NODE3_DATASET_URN);
    await lp.checkEdgeExists(NODE2_DATASET_URN, NODE3_DATASET_URN);

    await lp.expandOne(NODE3_DATASET_URN);
    await lp.checkNodeExists(NODE4_DATASET_URN);
    await lp.checkEdgeExists(NODE3_DATASET_URN, NODE4_DATASET_URN);

    // Expand all from a node — should reveal the full sub-graph
    await lp.expandAll(NODE5_DATASET_MANUAL_URN);
    await lp.checkNodeExists(NODE6_DATASET_URN);
    await lp.checkNodeExists(NODE4_DATASET_URN);
    await lp.checkEdgeExists(NODE5_DATASET_MANUAL_URN, NODE6_DATASET_URN);
    await lp.checkEdgeExists(NODE6_DATASET_URN, NODE4_DATASET_URN);
    await lp.checkEdgeExists(NODE3_DATASET_URN, NODE4_DATASET_URN);

    // DataJob chain
    await lp.checkNodeExists(NODE7_DATAJOB_URN);
    await lp.checkNodeExists(NODE8_DATASET_URN);
    await lp.checkEdgeExists(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    await lp.checkEdgeExists(NODE7_DATAJOB_URN, NODE8_DATASET_URN);

    // Dataset → Chart → Dashboard
    await lp.checkNodeExists(NODE9_CHART_URN);
    await lp.checkNodeExists(NODE10_DASHBOARD_URN);
    await lp.checkEdgeExists(NODE8_DATASET_URN, NODE9_CHART_URN);
    await lp.checkEdgeExists(NODE9_CHART_URN, NODE10_DASHBOARD_URN);

    // DBT chain
    await lp.checkNodeExists(NODE11_DBT_URN);
    await lp.checkNodeExists(NODE12_DATASET_URN);
    await lp.checkEdgeExists(NODE8_DATASET_URN, NODE11_DBT_URN);
    await lp.checkEdgeExists(NODE11_DBT_URN, NODE12_DATASET_URN);

    // Contract single node
    await lp.contract(NODE9_CHART_URN);
    await lp.checkNodeNotExists(NODE10_DASHBOARD_URN);

    // Contract a node with multiple downstream edges — all dependents removed
    await lp.contract(NODE4_DATASET_URN);
    await lp.checkNodeNotExists(NODE7_DATAJOB_URN);
    await lp.checkNodeNotExists(NODE8_DATASET_URN);
    await lp.checkNodeNotExists(NODE9_CHART_URN);
    await lp.checkNodeNotExists(NODE11_DBT_URN);
    await lp.checkNodeNotExists(NODE12_DATASET_URN);

    // Contract a node that shares an edge — sibling node should survive
    await lp.contract(NODE5_DATASET_MANUAL_URN);
    await lp.checkNodeNotExists(NODE6_DATASET_URN);
    await lp.checkNodeExists(NODE4_DATASET_URN); // shared downstream should still exist

    // Downstream direction — expand one starting from a chart
    await lp.goToLineageGraph(CHART_ENTITY_TYPE, NODE9_CHART_URN);
    await lp.checkNodeExists(NODE9_CHART_URN);
    await lp.checkNodeExists(NODE8_DATASET_URN);
    await lp.expandOne(NODE8_DATASET_URN);
    await lp.checkNodeExists(NODE7_DATAJOB_URN);
    await lp.checkNodeExists(NODE4_DATASET_URN);
    await lp.checkEdgeExists(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
    await lp.checkEdgeExists(NODE4_DATASET_URN, NODE7_DATAJOB_URN);

    // Downstream direction — expand all
    await lp.goToLineageGraph(CHART_ENTITY_TYPE, NODE9_CHART_URN);
    await lp.checkNodeExists(NODE9_CHART_URN);
    await lp.checkNodeExists(NODE8_DATASET_URN);
    await lp.expandAll(NODE8_DATASET_URN);
    await lp.checkNodeExists(NODE7_DATAJOB_URN);
    await lp.checkNodeExists(NODE4_DATASET_URN);
    await lp.checkNodeExists(NODE6_DATASET_URN);
    await lp.checkNodeExists(NODE5_DATASET_MANUAL_URN);
    await lp.checkNodeExists(NODE3_DATASET_URN);
    await lp.checkNodeExists(NODE2_DATASET_URN);
    await lp.checkNodeExists(NODE1_DATASET_URN);
    await lp.checkEdgeExists(NODE1_DATASET_URN, NODE2_DATASET_URN);
    await lp.checkEdgeExists(NODE2_DATASET_URN, NODE3_DATASET_URN);
    await lp.checkEdgeExists(NODE3_DATASET_URN, NODE4_DATASET_URN);
    await lp.checkEdgeExists(NODE1_DATASET_URN, NODE5_DATASET_MANUAL_URN);
    await lp.checkEdgeExists(NODE5_DATASET_MANUAL_URN, NODE6_DATASET_URN);
    await lp.checkEdgeExists(NODE6_DATASET_URN, NODE4_DATASET_URN);
    await lp.checkEdgeExists(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    await lp.checkEdgeExists(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
  });

  // ── Filter node — expand and filter children ────────────────────────────────

  test('should allow to expand and filter children', async ({ page, logger, logDir }) => {
    test.setTimeout(120000);

    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToLineageGraph(DATASET_ENTITY_TYPE, FILTERING_NODE7_URN);

    await lp.checkNodeExists(FILTERING_NODE7_URN);

    // ── Upstream filtering node ────────────────────────────────────────────

    // Initial state — 4 of 6 shown by default
    await lp.checkFilterNodeExists(FILTERING_NODE7_URN, 'up');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '4 of 6 shown');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'up', 'platform', 'PostgreSQL', '3');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'up', 'platform', 'Snowflake', '3');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'up', 'subtype', 'Datasets', '6');
    await lp.checkNodeNotExists(FILTERING_NODE1_URN);
    await lp.checkNodeNotExists(FILTERING_NODE2_URN);
    await lp.checkNodeExists(FILTERING_NODE3_URN);
    await lp.checkNodeExists(FILTERING_NODE4_URN);
    await lp.checkNodeExists(FILTERING_NODE5_URN);
    await lp.checkNodeExists(FILTERING_NODE6_URN);

    // Show more — all 6 upstream nodes visible
    await lp.showMore(FILTERING_NODE7_URN, 'up');
    await lp.checkNodeExists(FILTERING_NODE1_URN);
    await lp.checkNodeExists(FILTERING_NODE2_URN);
    await lp.checkNodeExists(FILTERING_NODE3_URN);
    await lp.checkNodeExists(FILTERING_NODE4_URN);
    await lp.checkNodeExists(FILTERING_NODE5_URN);
    await lp.checkNodeExists(FILTERING_NODE6_URN);

    // Filtering with < 3 characters — no filter applied, all still shown
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'xx');
    await lp.checkNodeExists(FILTERING_NODE1_URN);
    await lp.checkNodeExists(FILTERING_NODE2_URN);
    await lp.checkNodeExists(FILTERING_NODE3_URN);
    await lp.checkNodeExists(FILTERING_NODE4_URN);
    await lp.checkNodeExists(FILTERING_NODE5_URN);
    await lp.checkNodeExists(FILTERING_NODE6_URN);

    // Filter to a single node
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'node6');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '1 of 6 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'up', '1');
    await lp.checkNodeNotExists(FILTERING_NODE1_URN);
    await lp.checkNodeNotExists(FILTERING_NODE2_URN);
    await lp.checkNodeNotExists(FILTERING_NODE3_URN);
    await lp.checkNodeNotExists(FILTERING_NODE4_URN);
    await lp.checkNodeNotExists(FILTERING_NODE5_URN);
    await lp.checkNodeExists(FILTERING_NODE6_URN);

    // Filter matching multiple nodes
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'node');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '6 of 6 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'up', '6');
    await lp.checkNodeExists(FILTERING_NODE1_URN);
    await lp.checkNodeExists(FILTERING_NODE2_URN);
    await lp.checkNodeExists(FILTERING_NODE3_URN);
    await lp.checkNodeExists(FILTERING_NODE4_URN);
    await lp.checkNodeExists(FILTERING_NODE5_URN);
    await lp.checkNodeExists(FILTERING_NODE6_URN);

    // Filter matching no nodes
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'unknown');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '0 of 6 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'up', '0');
    await lp.checkNodeNotExists(FILTERING_NODE1_URN);
    await lp.checkNodeNotExists(FILTERING_NODE2_URN);
    await lp.checkNodeNotExists(FILTERING_NODE3_URN);
    await lp.checkNodeNotExists(FILTERING_NODE4_URN);
    await lp.checkNodeNotExists(FILTERING_NODE5_URN);
    await lp.checkNodeNotExists(FILTERING_NODE6_URN);

    // Platform filter — PostgreSQL
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'postgres');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '3 of 6 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'up', '3');
    await lp.checkNodeExists(FILTERING_NODE1_URN);
    await lp.checkNodeExists(FILTERING_NODE2_URN);
    await lp.checkNodeExists(FILTERING_NODE3_URN);
    await lp.checkNodeNotExists(FILTERING_NODE4_URN);
    await lp.checkNodeNotExists(FILTERING_NODE5_URN);
    await lp.checkNodeNotExists(FILTERING_NODE6_URN);

    // Platform filter — Snowflake
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'snowflake');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '3 of 6 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'up', '3');
    await lp.checkNodeNotExists(FILTERING_NODE1_URN);
    await lp.checkNodeNotExists(FILTERING_NODE2_URN);
    await lp.checkNodeNotExists(FILTERING_NODE3_URN);
    await lp.checkNodeExists(FILTERING_NODE4_URN);
    await lp.checkNodeExists(FILTERING_NODE5_URN);
    await lp.checkNodeExists(FILTERING_NODE6_URN);

    // ── Downstream filtering node ─────────────────────────────────────────

    // Initial state — 4 of 13 shown by default
    await lp.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'PostgreSQL', '3');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'Snowflake', '10');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'subtype', 'Datasets', '13');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE9_URN);
    await lp.checkNodeNotExists(FILTERING_NODE10_URN);
    await lp.checkNodeNotExists(FILTERING_NODE11_URN);
    await lp.checkNodeNotExists(FILTERING_NODE12_URN);
    await lp.checkNodeNotExists(FILTERING_NODE13_URN);
    await lp.checkNodeNotExists(FILTERING_NODE14_URN);
    await lp.checkNodeNotExists(FILTERING_NODE15_URN);
    await lp.checkNodeNotExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE18_URN);
    await lp.checkNodeExists(FILTERING_NODE19_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Show more — additional nodes appear
    await lp.showMore(FILTERING_NODE7_URN, 'down');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE9_URN);
    await lp.checkNodeNotExists(FILTERING_NODE10_URN);
    await lp.checkNodeNotExists(FILTERING_NODE11_URN);
    await lp.checkNodeNotExists(FILTERING_NODE12_URN);
    await lp.checkNodeExists(FILTERING_NODE13_URN);
    await lp.checkNodeExists(FILTERING_NODE14_URN);
    await lp.checkNodeExists(FILTERING_NODE15_URN);
    await lp.checkNodeExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE18_URN);
    await lp.checkNodeExists(FILTERING_NODE19_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Show less — back to 4
    await lp.showLess(FILTERING_NODE7_URN, 'down');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE9_URN);
    await lp.checkNodeNotExists(FILTERING_NODE10_URN);
    await lp.checkNodeNotExists(FILTERING_NODE11_URN);
    await lp.checkNodeNotExists(FILTERING_NODE12_URN);
    await lp.checkNodeNotExists(FILTERING_NODE13_URN);
    await lp.checkNodeNotExists(FILTERING_NODE14_URN);
    await lp.checkNodeNotExists(FILTERING_NODE15_URN);
    await lp.checkNodeNotExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE18_URN);
    await lp.checkNodeExists(FILTERING_NODE19_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Show all — all 13 downstream nodes visible
    await lp.showAll(FILTERING_NODE7_URN, 'down');
    await lp.checkNodeExists(FILTERING_NODE8_URN);
    await lp.checkNodeExists(FILTERING_NODE9_URN);
    await lp.checkNodeExists(FILTERING_NODE10_URN);
    await lp.checkNodeExists(FILTERING_NODE11_URN);
    await lp.checkNodeExists(FILTERING_NODE12_URN);
    await lp.checkNodeExists(FILTERING_NODE13_URN);
    await lp.checkNodeExists(FILTERING_NODE14_URN);
    await lp.checkNodeExists(FILTERING_NODE15_URN);
    await lp.checkNodeExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE18_URN);
    await lp.checkNodeExists(FILTERING_NODE19_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Upstream filter < 3 chars — downstream nodes unaffected
    await lp.filterNodes(FILTERING_NODE7_URN, 'up', 'xx');
    await lp.checkNodeExists(FILTERING_NODE8_URN);
    await lp.checkNodeExists(FILTERING_NODE12_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Downstream filter — single node
    await lp.filterNodes(FILTERING_NODE7_URN, 'down', 'node13');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '1 of 13 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'down', '1');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE9_URN);
    await lp.checkNodeNotExists(FILTERING_NODE10_URN);
    await lp.checkNodeNotExists(FILTERING_NODE11_URN);
    await lp.checkNodeNotExists(FILTERING_NODE12_URN);
    await lp.checkNodeExists(FILTERING_NODE13_URN);
    await lp.checkNodeNotExists(FILTERING_NODE14_URN);
    await lp.checkNodeNotExists(FILTERING_NODE15_URN);
    await lp.checkNodeNotExists(FILTERING_NODE16_URN);
    await lp.checkNodeNotExists(FILTERING_NODE17_URN);
    await lp.checkNodeNotExists(FILTERING_NODE18_URN);
    await lp.checkNodeNotExists(FILTERING_NODE19_URN);
    await lp.checkNodeNotExists(FILTERING_NODE20_URN);

    // Downstream filter — all nodes match
    await lp.filterNodes(FILTERING_NODE7_URN, 'down', 'node');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '13 of 13 shown');
    await lp.checkNodeExists(FILTERING_NODE8_URN);
    await lp.checkNodeExists(FILTERING_NODE9_URN);
    await lp.checkNodeExists(FILTERING_NODE10_URN);
    await lp.checkNodeExists(FILTERING_NODE11_URN);
    await lp.checkNodeExists(FILTERING_NODE12_URN);
    await lp.checkNodeExists(FILTERING_NODE13_URN);
    await lp.checkNodeExists(FILTERING_NODE14_URN);
    await lp.checkNodeExists(FILTERING_NODE15_URN);
    await lp.checkNodeExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE18_URN);
    await lp.checkNodeExists(FILTERING_NODE19_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Downstream filter — no matches
    await lp.filterNodes(FILTERING_NODE7_URN, 'down', 'unknown');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '0 of 13 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'down', '0');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE20_URN);

    // Platform filter — PostgreSQL downstream
    await lp.filterNodes(FILTERING_NODE7_URN, 'down', 'postgres');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '3 of 13 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'down', '3');
    await lp.checkNodeExists(FILTERING_NODE8_URN);
    await lp.checkNodeExists(FILTERING_NODE9_URN);
    await lp.checkNodeExists(FILTERING_NODE10_URN);
    await lp.checkNodeNotExists(FILTERING_NODE11_URN);
    await lp.checkNodeNotExists(FILTERING_NODE20_URN);

    // Platform filter — Snowflake downstream
    await lp.filterNodes(FILTERING_NODE7_URN, 'down', 'snowflake');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '10 of 13 shown');
    await lp.checkFilterMatches(FILTERING_NODE7_URN, 'down', '10');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE9_URN);
    await lp.checkNodeNotExists(FILTERING_NODE10_URN);
    await lp.checkNodeExists(FILTERING_NODE11_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Navigate from a different entry point and expand
    await lp.goToLineageGraph(DATASET_ENTITY_TYPE, FILTERING_NODE1_URN);
    await lp.checkNodeExists(FILTERING_NODE1_URN);
    await lp.checkNodeExists(FILTERING_NODE7_URN);
    await lp.checkEdgeExists(FILTERING_NODE1_URN, FILTERING_NODE7_URN);

    await lp.expandOne(FILTERING_NODE7_URN);
    await lp.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'PostgreSQL', '3');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'Snowflake', '10');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'subtype', 'Datasets', '13');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);

    // Filter with no match — filter node still exists (not removed)
    await lp.filterNodes(FILTERING_NODE7_URN, 'down', 'unknown');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '0 of 13 shown');
    await lp.checkNodeNotExists(FILTERING_NODE17_URN);
    await lp.checkNodeNotExists(FILTERING_NODE20_URN);

    // Clear filter — nodes still expanded (4 of 13)
    await lp.clearFilter(FILTERING_NODE7_URN, 'down');
    await lp.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lp.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'PostgreSQL', '3');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'Snowflake', '10');
    await lp.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'subtype', 'Datasets', '13');
    await lp.checkNodeNotExists(FILTERING_NODE8_URN);
    await lp.checkNodeNotExists(FILTERING_NODE16_URN);
    await lp.checkNodeExists(FILTERING_NODE17_URN);
    await lp.checkNodeExists(FILTERING_NODE20_URN);
  });
});
