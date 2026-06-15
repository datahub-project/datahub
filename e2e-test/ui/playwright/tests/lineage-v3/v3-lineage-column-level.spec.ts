/**
 * Column-Level Lineage V3 tests — migrated from Cypress v3_lineage_column_level.js
 *
 * DATA SEEDING: Static datasets from fixtures/data.json (auto-seeded via test.use below)
 *
 * Comprehensive test for column expansion/contraction behavior across multiple datasets and columns.
 * Verifies default state, expansion, contraction, and multi-node interactions.
 * Full parity with Cypress v3_lineage_column_level.js (44 assertions across 3 datasets, 11 columns).
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';
import type { Locator } from '@playwright/test';

test.use({ featureName: 'lineage-v3' });

// Datasets with schema columns for comprehensive column-level lineage testing
const ENTITY_TYPE = 'dataset';
const HDFS_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,playwright_column_level_hdfs,PROD)';
const HIVE_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_column_level_hive,PROD)';
const LOGGING_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_column_level_logging,PROD)';

// Use Hive as the starting dataset since all three are connected to it
const START_DATASET_URN = HIVE_DATASET_URN;

// Column names to verify in each dataset (full coverage matching Cypress)
const HDFS_COLUMNS = [
  'shipment_info',
  'shipment_info.date',
  'shipment_info.target',
  'shipment_info.destination',
  'shipment_info.geo_info',
];

const HIVE_COLUMNS = ['field_foo', 'field_bar'];

const LOGGING_COLUMNS = ['event_name', 'event_data', 'timestamp', 'browser'];

test.describe('column-level lineage graph V3 test', () => {
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

  // Helper: Verify columns are in a specific visibility state
  async function verifyColumnsVisibility(
    nodeLocator: Locator,
    columns: string[],
    shouldBeVisible: boolean,
  ): Promise<void> {
    for (const column of columns) {
      const columnLocator = lineagePage.getColumnLocatorInNode(nodeLocator, column);
      if (shouldBeVisible) {
        await expect(columnLocator).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
      } else {
        await expect(columnLocator).not.toBeVisible({ timeout: TIMEOUTS.SHORT });
      }
    }
  }

  // Helper: Expand/contract columns and verify new state
  async function expandContractAndVerify(
    urn: string,
    node: Locator,
    columns: string[],
    shouldBeVisible: boolean,
  ): Promise<void> {
    await lineagePage.expandContractColumns(urn);
    await verifyColumnsVisibility(node, columns, shouldBeVisible);
  }

  test('navigate to lineage graph view and verify that column-level lineage is showing correctly', async ({ page }) => {
    await lineagePage.goToLineageGraph(ENTITY_TYPE, START_DATASET_URN);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Get node locators for all three datasets
    const hdfsNode = lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN);
    const hiveNode = lineagePage.getReactFlowNodeByUrn(HIVE_DATASET_URN);
    const loggingNode = lineagePage.getReactFlowNodeByUrn(LOGGING_DATASET_URN);

    // Phase 1: Verify columns are hidden by default for all datasets
    await verifyColumnsVisibility(hdfsNode, HDFS_COLUMNS, false);
    await verifyColumnsVisibility(hiveNode, HIVE_COLUMNS, false);
    await verifyColumnsVisibility(loggingNode, LOGGING_COLUMNS, false);

    // Phase 2: Expand HDFS and verify columns appear
    await expandContractAndVerify(HDFS_DATASET_URN, hdfsNode, HDFS_COLUMNS, true);

    // Phase 3: Expand Hive and verify columns appear
    await expandContractAndVerify(HIVE_DATASET_URN, hiveNode, HIVE_COLUMNS, true);

    // Phase 4: Expand Logging and verify columns appear
    await expandContractAndVerify(LOGGING_DATASET_URN, loggingNode, LOGGING_COLUMNS, true);

    // Phase 5: Contract Hive and verify columns disappear
    await expandContractAndVerify(HIVE_DATASET_URN, hiveNode, HIVE_COLUMNS, false);

    // Phase 6: Verify HDFS and Logging columns still visible after Hive collapse
    await verifyColumnsVisibility(hdfsNode, HDFS_COLUMNS, true);
    await verifyColumnsVisibility(loggingNode, LOGGING_COLUMNS, true);

    // Phase 7: Contract all remaining datasets
    await lineagePage.expandContractColumns(HDFS_DATASET_URN);
    await lineagePage.expandContractColumns(LOGGING_DATASET_URN);

    // Phase 8: Re-expand Hive and verify columns appear again
    await expandContractAndVerify(HIVE_DATASET_URN, hiveNode, HIVE_COLUMNS, true);

    // Phase 9: Verify other datasets remain collapsed
    await verifyColumnsVisibility(hdfsNode, HDFS_COLUMNS, false);
    await verifyColumnsVisibility(loggingNode, LOGGING_COLUMNS, false);
  });
});
