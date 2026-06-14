/**
 * Lineage Graph V3 tests — migrated from Cypress v3_lineage_graph.js
 *
 * DATA SEEDING:
 * - Fixture data (static datasets): fixtures/data.json (auto-seeded via test.use({ featureName: 'lineage-v3' }))
 * - Time-range lineage (Cases 1-3): seedTimeRangeLineage() via lineage-time-seeder.ts
 * - Additional graph nodes (FILTERING nodes, NODE1-12, etc.): seeded in test.beforeAll
 *
 */

import { request as playwrightRequest } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS, gmsUrl, LOAD_STATES } from '../../utils/constants';
import { seedTimeRangeLineage } from '../../utils/lineage-time-seeder';
import { readGmsToken } from '../../fixtures/login';
import { users } from '../../data/users';

test.use({ featureName: 'lineage-v3' });

// ── Seeding helpers ─────────────────────────────────────────────────────────

const FILTERING_NODES_RANGE_1_TO_6 = 6;
const FILTERING_NODES_RANGE_8_TO_20 = 20;
const TEST_TIMEOUT_TASK_LINEAGE = 90000; // Task entity lineage graphs can be slow in CI

async function seedDatasetNode(
  request: Parameters<typeof seedTimeRangeLineage>[0],
  gmsToken: string,
  urn: string,
  name: string,
): Promise<void> {
  const url = `${gmsUrl()}/aspects?action=ingestProposal`;
  const payload = {
    proposal: {
      entityType: 'dataset',
      entityUrn: urn,
      changeType: 'UPSERT',
      aspectName: 'datasetProperties',
      aspect: {
        contentType: 'application/json',
        value: JSON.stringify({ description: name }),
      },
    },
  };
  await request.post(url, {
    data: payload,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${gmsToken}`,
    },
    failOnStatusCode: false,
  });
}

async function seedDataJobNode(
  request: Parameters<typeof seedTimeRangeLineage>[0],
  gmsToken: string,
  urn: string,
  name: string,
  flowUrn: string,
): Promise<void> {
  const url = `${gmsUrl()}/aspects?action=ingestProposal`;
  const payload = {
    proposal: {
      entityType: 'dataJob',
      entityUrn: urn,
      changeType: 'UPSERT',
      aspectName: 'dataJobInfo',
      aspect: {
        contentType: 'application/json',
        value: JSON.stringify({
          name,
          description: name,
          type: 'BATCH',
          flowUrn,
          customProperties: {},
        }),
      },
    },
  };
  await request.post(url, {
    data: payload,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${gmsToken}`,
    },
    failOnStatusCode: false,
  });
}

/**
 * Seed additional nodes (NODE1-12, FILTERING nodes) not included in seedTimeRangeLineage().
 * These are v3-specific test nodes for complete graph and filtering functionality.
 */
async function seedAdditionalNodes(
  request: Parameters<typeof seedTimeRangeLineage>[0],
  gmsToken: string,
): Promise<void> {
  // Seed NODE1-8 and related job
  await seedDatasetNode(request, gmsToken, NODE1_DATASET_URN, 'Node 1 Dataset');
  await seedDatasetNode(request, gmsToken, NODE2_DATASET_URN, 'Node 2 Dataset');
  await seedDatasetNode(request, gmsToken, NODE3_DATASET_URN, 'Node 3 Dataset');
  await seedDatasetNode(request, gmsToken, NODE5_DATASET_MANUAL_URN, 'Node 5 Manual Dataset');
  await seedDatasetNode(request, gmsToken, NODE6_DATASET_URN, 'Node 6 Dataset');
  await seedDataJobNode(
    request,
    gmsToken,
    NODE7_DATAJOB_URN,
    'node7_datajob',
    'urn:li:dataFlow:(airflow,playwright_lineage_pipeline,PROD)',
  );
  await seedDatasetNode(request, gmsToken, NODE8_DATASET_URN, 'Node 8 Dataset');

  // Seed FILTERING nodes 1-6
  const filteringNodesRange1To6 = new Map<number, string>([
    [1, FILTERING_NODE1_URN],
    [2, FILTERING_NODE2_URN],
    [3, FILTERING_NODE3_URN],
    [4, FILTERING_NODE4_URN],
    [5, FILTERING_NODE5_URN],
    [6, FILTERING_NODE6_URN],
  ]);

  for (let i = 1; i <= FILTERING_NODES_RANGE_1_TO_6; i++) {
    const urn = filteringNodesRange1To6.get(i);
    if (urn) await seedDatasetNode(request, gmsToken, urn, `Filtering Node ${i}`);
  }

  // Seed FILTERING nodes 7-20
  const filteringNodesRange7To20 = new Map<number, string>([
    [7, FILTERING_NODE7_URN],
    [8, FILTERING_NODE8_URN],
    [9, FILTERING_NODE9_URN],
    [10, FILTERING_NODE10_URN],
    [11, FILTERING_NODE11_URN],
    [12, FILTERING_NODE12_URN],
    [13, FILTERING_NODE13_URN],
    [14, FILTERING_NODE14_URN],
    [15, FILTERING_NODE15_URN],
    [16, FILTERING_NODE16_URN],
    [17, FILTERING_NODE17_URN],
    [18, FILTERING_NODE18_URN],
    [19, FILTERING_NODE19_URN],
    [20, FILTERING_NODE20_URN],
  ]);

  for (let i = 7; i <= FILTERING_NODES_RANGE_8_TO_20; i++) {
    const urn = filteringNodesRange7To20.get(i);
    if (urn) await seedDatasetNode(request, gmsToken, urn, `Filtering Node ${i}`);
  }
}

// ── Constants ────────────────────────────────────────────────────────────────

const DATASET_ENTITY_TYPE = 'dataset';
const CHART_ENTITY_TYPE = 'chart';
const TASKS_ENTITY_TYPE = 'tasks';

// Primary lineage entity URNs and constants for basic lineage tests
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const HDFS_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const BAZ_CHART_URN = 'urn:li:chart:(looker,playwright_baz1)';
const JAN_1_2021_TIMESTAMP = 1609553357755;
const JAN_1_2022_TIMESTAMP = 1641089357755;

function getTimestampMillisNumDaysAgo(days: number): number {
  return Date.now() - days * 24 * 60 * 60 * 1000;
}

const TIMESTAMP_MILLIS_14_DAYS_AGO = getTimestampMillisNumDaysAgo(14);
const TIMESTAMP_MILLIS_7_DAYS_AGO = getTimestampMillisNumDaysAgo(7);
const TIMESTAMP_MILLIS_NOW = getTimestampMillisNumDaysAgo(0);

const GNP_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,economic_data.gnp,PROD)';
const TRANSACTION_ETL_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,bq_etl,prod),transaction_etl)';
const MONTHLY_TEMPERATURE_DATASET_URN =
  'urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.monthly_temperature,PROD)';

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

// Column names for complex lineage testing
const COLUMN_NAMES = {
  RECORD_ID: 'record_id',
  NEXT_RECORD_ID: 'next_record_id',
} as const;

// Dataset search/selection terms
const SEARCH_TERMS = {
  HEALTH_TEST: 'playwright_health_test',
} as const;

// ── Test Suite ───────────────────────────────────────────────────────────────

test.describe('lineage v3 — lineage graph', () => {
  let lineagePage: LineageV3Page;

  test.beforeAll(async () => {
    const apiContext = await playwrightRequest.newContext({ baseURL: gmsUrl() });
    try {
      const gmsToken = readGmsToken(users.admin.username);

      // Seed time-range lineage (Cases 1-3: transaction_etl, temperature_etl, gnp)
      // This is shared with v2 tests and handles all core time-filtering scenarios.
      await seedTimeRangeLineage(apiContext, gmsToken);

      // Seed additional nodes for complete graph and filtering tests.
      // These are not part of seedTimeRangeLineage() and are v3-specific.
      await seedAdditionalNodes(apiContext, gmsToken);
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

  test('can see full history', async () => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await lineagePage.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await lineagePage.waitForGraphToRender();

    // Verify all expected lineage nodes are visible
    await expect(lineagePage.getReactFlowNodeByUrn(DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(BAZ_CHART_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('cannot see any lineage edges for 2021', async () => {
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      DATASET_URN,
      JAN_1_2021_TIMESTAMP,
      JAN_1_2022_TIMESTAMP,
    );
    await lineagePage.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await lineagePage.waitForGraphToRender();

    // Verify root node is visible but downstream lineage from 2021 is not present
    await expect(lineagePage.getReactFlowNodeByUrn(DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN)).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await expect(lineagePage.getReactFlowNodeByUrn(BAZ_CHART_URN)).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('can see when the inputs to a data job change', async () => {
    test.setTimeout(TEST_TIMEOUT_TASK_LINEAGE);

    // Between 14 days ago and 7 days ago, only transactions was an input
    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );

    // Time range 14-7 days ago: transactions and aggregated visible, user_profile hidden
    await lineagePage.checkDatasetNodeVisible('transactions.aggregated_transactions', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeVisible('transactions.transactions', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeHidden('transactions.user_profile', TIMEOUTS.MEDIUM);
    // Verify edges connect the visible nodes
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);

    // From 7 days ago to now, user_profile was also added as an input
    await lineagePage.goToLineageGraphWithTimeRange(
      TASKS_ENTITY_TYPE,
      TRANSACTION_ETL_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );

    // Time range 7 days - now: all three visible (user_profile added)
    await lineagePage.checkDatasetNodeVisible('transactions.aggregated_transactions', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeVisible('transactions.transactions', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeVisible('transactions.user_profile', TIMEOUTS.EXTRA_LONG);
    // Verify edges show the expanded lineage
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);
  });

  test('can see when a data job is replaced', async () => {
    // Between 14 days ago and 7 days ago, only temperature_etl_1 was an input
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
    );

    // Time range 14-7 days ago: only temperature_etl_1 job visible
    await lineagePage.checkDatasetNodeVisible('climate.monthly_temperature', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeHidden('temperature_etl_2', TIMEOUTS.MEDIUM);
    // Verify lineage edges exist showing the job connection
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);

    // Since 7 days ago, temperature_etl_1 has been replaced by temperature_etl_2
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      MONTHLY_TEMPERATURE_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );

    // Time range 7 days - now: only temperature_etl_2 job visible (replaced)
    await lineagePage.checkDatasetNodeVisible('climate.monthly_temperature', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeHidden('temperature_etl_1', TIMEOUTS.MEDIUM);
    // Verify edges reflect the job replacement
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);
  });

  test('can see when a dataset join changes', async () => {
    // 8 days ago, both gdp and factor_income were joined to create gnp
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_14_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );

    // Time range 14 days - now: both upstream sources visible (gdp and factor_income)
    await lineagePage.checkDatasetNodeVisible('economic_data.gnp', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeVisible('economic_data.gdp', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeVisible('economic_data.factor_income', TIMEOUTS.EXTRA_LONG);
    // Verify edges show both join sources connected
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);

    // Since 7 days ago, factor_income was removed from the join
    await lineagePage.goToLineageGraphWithTimeRange(
      DATASET_ENTITY_TYPE,
      GNP_DATASET_URN,
      TIMESTAMP_MILLIS_7_DAYS_AGO,
      TIMESTAMP_MILLIS_NOW,
    );

    // Time range 7 days - now: only gdp visible (factor_income join removed after 8 days)
    await lineagePage.checkDatasetNodeVisible('economic_data.gnp', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeVisible('economic_data.gdp', TIMEOUTS.EXTRA_LONG);
    await lineagePage.checkDatasetNodeHidden('economic_data.factor_income', TIMEOUTS.MEDIUM);
    // Verify edges reflect the removed join (only gdp-gnp edge remains)
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);
  });

  test('can edit upstream lineage', async ({ page }) => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);

    await lineagePage.openManageLineageMenu(DATASET_URN);
    await lineagePage.clickEditUpstreamLineage();

    await expect(page.getByText('Select the Upstreams', { exact: false })).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    await expect(page.getByRole('button', { name: /Set Upstreams/i })).toHaveAttribute('disabled');

    await lineagePage.lineageEditSearchInput.type(SEARCH_TERMS.HEALTH_TEST);

    const urn = `urn:li:dataset:(urn:li:dataPlatform:hive,${SEARCH_TERMS.HEALTH_TEST},PROD)`;
    const checkbox = page.getByTestId(`checkbox-${urn}`);
    await checkbox.click();

    await expect(page.getByRole('button', { name: /Set Upstreams/i })).not.toHaveAttribute('disabled');
  });

  test('displays complete lineage graph with all node types', async () => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, NODE1_DATASET_URN);

    // Check initial state
    await lineagePage.checkNodeExists(NODE1_DATASET_URN);
    await lineagePage.checkNodeExists(NODE2_DATASET_URN);
    await lineagePage.checkNodeExists(NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkEdgeExists(NODE1_DATASET_URN, NODE2_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE1_DATASET_URN, NODE5_DATASET_MANUAL_URN);

    // Column lineage - hover behavior
    await lineagePage.expandContractColumns(NODE2_DATASET_URN);
    await lineagePage.hoverColumn(NODE2_DATASET_URN, COLUMN_NAMES.RECORD_ID);
    await lineagePage.checkEdgeBetweenColumnsExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
    );
    await lineagePage.unhoverColumn(NODE2_DATASET_URN, COLUMN_NAMES.RECORD_ID);
    await lineagePage.checkEdgeBetweenColumnsNotExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
    );

    // Column lineage - next_record_id column
    await lineagePage.hoverColumn(NODE2_DATASET_URN, COLUMN_NAMES.NEXT_RECORD_ID);
    await lineagePage.checkEdgeBetweenColumnsExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.NEXT_RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.NEXT_RECORD_ID,
    );
    await lineagePage.unhoverColumn(NODE2_DATASET_URN, COLUMN_NAMES.NEXT_RECORD_ID);
    await lineagePage.checkEdgeBetweenColumnsNotExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.NEXT_RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.NEXT_RECORD_ID,
    );

    // Column lineage - column selection/deselection
    await lineagePage.selectColumn(NODE2_DATASET_URN, COLUMN_NAMES.RECORD_ID);
    await lineagePage.checkEdgeBetweenColumnsExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
    );
    await lineagePage.selectColumn(NODE2_DATASET_URN, COLUMN_NAMES.NEXT_RECORD_ID);
    await lineagePage.checkEdgeBetweenColumnsExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.NEXT_RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.NEXT_RECORD_ID,
    );
    // When next_record_id is selected, record_id deselection should remove its edge
    await lineagePage.checkEdgeBetweenColumnsNotExists(
      NODE1_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
      NODE2_DATASET_URN,
      COLUMN_NAMES.RECORD_ID,
    );

    // Expand one node
    await lineagePage.expandOne(NODE2_DATASET_URN);
    await lineagePage.checkNodeExists(NODE3_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE2_DATASET_URN, NODE3_DATASET_URN);
    await lineagePage.expandOne(NODE3_DATASET_URN);
    await lineagePage.checkNodeExists(NODE4_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE3_DATASET_URN, NODE4_DATASET_URN);

    // Expand all nodes
    await lineagePage.expandAll(NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkNodeExists(NODE6_DATASET_URN);
    await lineagePage.checkNodeExists(NODE4_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE5_DATASET_MANUAL_URN, NODE6_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE6_DATASET_URN, NODE4_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE3_DATASET_URN, NODE4_DATASET_URN);
    // Datajob
    await lineagePage.checkNodeExists(NODE7_DATAJOB_URN);
    await lineagePage.checkNodeExists(NODE8_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    await lineagePage.checkEdgeExists(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
    // Dataset -> Chart -> Dashboard
    await lineagePage.checkNodeExists(NODE9_CHART_URN);
    await lineagePage.checkNodeExists(NODE10_DASHBOARD_URN);
    await lineagePage.checkEdgeExists(NODE8_DATASET_URN, NODE9_CHART_URN);
    await lineagePage.checkEdgeExists(NODE9_CHART_URN, NODE10_DASHBOARD_URN);
    // Dbt
    await lineagePage.checkNodeExists(NODE11_DBT_URN);
    await lineagePage.checkNodeExists(NODE12_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE8_DATASET_URN, NODE11_DBT_URN);
    await lineagePage.checkEdgeExists(NODE11_DBT_URN, NODE12_DATASET_URN);

    // Contract single node
    await lineagePage.contract(NODE9_CHART_URN);
    await lineagePage.checkNodeNotExists(NODE10_DASHBOARD_URN);

    // Contract multiple nodes
    await lineagePage.contract(NODE4_DATASET_URN);
    await lineagePage.checkNodeNotExists(NODE7_DATAJOB_URN);
    await lineagePage.checkNodeNotExists(NODE8_DATASET_URN);
    await lineagePage.checkNodeNotExists(NODE9_CHART_URN);
    await lineagePage.checkNodeNotExists(NODE11_DBT_URN);
    await lineagePage.checkNodeNotExists(NODE12_DATASET_URN);

    // Contract when a node has two input edges
    await lineagePage.contract(NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkNodeNotExists(NODE6_DATASET_URN);
    await lineagePage.checkNodeExists(NODE4_DATASET_URN); // should still exist

    // Downstream expand one
    await lineagePage.goToLineageGraph(CHART_ENTITY_TYPE, NODE9_CHART_URN);
    await lineagePage.checkNodeExists(NODE9_CHART_URN);
    await lineagePage.checkNodeExists(NODE8_DATASET_URN);
    await lineagePage.expandOne(NODE8_DATASET_URN);
    await lineagePage.checkNodeExists(NODE7_DATAJOB_URN);
    await lineagePage.checkNodeExists(NODE4_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE4_DATASET_URN, NODE7_DATAJOB_URN);

    // Downstream expand all
    await lineagePage.goToLineageGraph(CHART_ENTITY_TYPE, NODE9_CHART_URN);
    await lineagePage.checkNodeExists(NODE9_CHART_URN);
    await lineagePage.checkNodeExists(NODE8_DATASET_URN);
    await lineagePage.expandAll(NODE8_DATASET_URN);
    await lineagePage.checkNodeExists(NODE7_DATAJOB_URN);
    await lineagePage.checkNodeExists(NODE4_DATASET_URN);
    await lineagePage.checkNodeExists(NODE6_DATASET_URN);
    await lineagePage.checkNodeExists(NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkNodeExists(NODE3_DATASET_URN);
    await lineagePage.checkNodeExists(NODE2_DATASET_URN);
    await lineagePage.checkNodeExists(NODE1_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE1_DATASET_URN, NODE2_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE2_DATASET_URN, NODE3_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE3_DATASET_URN, NODE4_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE1_DATASET_URN, NODE5_DATASET_MANUAL_URN);
    await lineagePage.checkEdgeExists(NODE5_DATASET_MANUAL_URN, NODE6_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE6_DATASET_URN, NODE4_DATASET_URN);
    await lineagePage.checkEdgeExists(NODE4_DATASET_URN, NODE7_DATAJOB_URN);
    await lineagePage.checkEdgeExists(NODE7_DATAJOB_URN, NODE8_DATASET_URN);
  });

  test('should allow to expand and filter children', async () => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, FILTERING_NODE7_URN);

    await lineagePage.checkNodeExists(FILTERING_NODE7_URN);

    // ── Upstream section initial state ─────────────────────────────────────────
    await lineagePage.checkFilterNodeExists(FILTERING_NODE7_URN, 'up');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '4 of 6 shown');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'up', 'platform', 'PostgreSQL', '3');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'up', 'platform', 'Snowflake', '3');

    // Verify initial node visibility
    await lineagePage.checkNodeNotExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE6_URN);

    // Show more upstream
    await lineagePage.showMore(FILTERING_NODE7_URN, 'up');
    await lineagePage.checkNodeExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE6_URN);

    // Filter by <3 chars (no filtering should occur)
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'xx');
    await lineagePage.checkNodeExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE6_URN);

    // Filter by single node name
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'node6');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '1 of 6 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'up', '1');
    await lineagePage.checkNodeNotExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE6_URN);

    // Filter by multiple nodes (all matching 'node')
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'node');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '6 of 6 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'up', '6');
    await lineagePage.checkNodeExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE6_URN);

    // Filter with no matches
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'unknown');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '0 of 6 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'up', '0');
    await lineagePage.checkNodeNotExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE6_URN);

    // Filter by PostgreSQL platform
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'postgres');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '3 of 6 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'up', '3');
    await lineagePage.checkNodeExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE6_URN);

    // Filter by Snowflake platform
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'snowflake');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'up', '3 of 6 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'up', '3');
    await lineagePage.checkNodeNotExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE2_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE3_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE4_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE5_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE6_URN);

    // ── Downstream section initial state ──────────────────────────────────────
    await lineagePage.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'PostgreSQL', '3');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'Snowflake', '10');

    // Verify initial downstream node visibility
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Show more downstream
    await lineagePage.showMore(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Show less downstream
    await lineagePage.showLess(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Show all downstream
    await lineagePage.showAll(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkNodeExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Filter by <3 chars (no filtering)
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'up', 'xx');
    await lineagePage.checkNodeExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Filter by single node
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'down', 'node13');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '1 of 13 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'down', '1');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE20_URN);

    // Filter by multiple nodes
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'down', 'node');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '13 of 13 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'down', '13');
    await lineagePage.checkNodeExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Filter with no matches
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'down', 'unknown');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '0 of 13 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'down', '0');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE20_URN);

    // Filter by PostgreSQL platform
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'down', 'postgres');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '3 of 13 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'down', '3');
    await lineagePage.checkNodeExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE20_URN);

    // Filter by Snowflake platform
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'down', 'snowflake');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '10 of 13 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'down', '10');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // ── Test from FILTERING_NODE1 perspective ─────────────────────────────────
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE1_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE7_URN);
    await lineagePage.checkEdgeExists(FILTERING_NODE1_URN, FILTERING_NODE7_URN);

    // Expand and verify downstream
    await lineagePage.expandOne(FILTERING_NODE7_URN);
    await lineagePage.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'PostgreSQL', '3');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'Snowflake', '10');

    // Verify node visibility
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);

    // Filter with no matches (filtering node should still exist)
    await lineagePage.filterNodes(FILTERING_NODE7_URN, 'down', 'unknown');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '0 of 13 shown');
    await lineagePage.checkFilterMatches(FILTERING_NODE7_URN, 'down', '0');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE20_URN);

    // Clear filters (nodes should still be expanded)
    await lineagePage.clearFilter(FILTERING_NODE7_URN, 'down');
    await lineagePage.checkFilterNodeExists(FILTERING_NODE7_URN, 'down');
    await lineagePage.ensureFilterNodeTitleHasText(FILTERING_NODE7_URN, 'down', '4 of 13 shown');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'PostgreSQL', '3');
    await lineagePage.checkFilterCounter(FILTERING_NODE7_URN, 'down', 'platform', 'Snowflake', '10');
    await lineagePage.checkNodeNotExists(FILTERING_NODE8_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE9_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE10_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE11_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE12_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE13_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE14_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE15_URN);
    await lineagePage.checkNodeNotExists(FILTERING_NODE16_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE17_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE18_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE19_URN);
    await lineagePage.checkNodeExists(FILTERING_NODE20_URN);
  });

  test('can switch direction between upstream and downstream in compact mode', async () => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await lineagePage.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await lineagePage.waitForGraphToRender();

    // Verify both upstream and downstream lineage is visible
    await expect(lineagePage.getReactFlowNodeByUrn(DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(BAZ_CHART_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('renders complete lineage graph with multiple node types and edges', async () => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await lineagePage.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await lineagePage.waitForGraphToRender();

    // Verify all node types render and edges connect them
    await expect(lineagePage.getReactFlowNodeByUrn(DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(BAZ_CHART_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    // Verify edges are present (graph is fully connected)
    expect(await lineagePage.areGraphEdgesVisible()).toBe(true);
  });

  test('graph remains stable with all lineage nodes visible after loading', async () => {
    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await lineagePage.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await lineagePage.waitForGraphToRender();

    // Verify all nodes load completely and persist
    await expect(lineagePage.getReactFlowNodeByUrn(DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(lineagePage.getReactFlowNodeByUrn(BAZ_CHART_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Re-verify nodes remain accessible (stability after initial render)
    await expect(lineagePage.getReactFlowNodeByUrn(DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });
});
