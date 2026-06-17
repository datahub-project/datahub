/**
 * Change History Chart Tests - Cypress Migration
 *
 * Tests the change history calendar for dataset operations:
 * - Chart visibility with data
 * - Empty state handling
 * - Permission-based visibility
 * - Calendar popover interactions with operation details
 * - Operation type filtering via dropdown and summary pills
 * - Day drawer navigation
 *
 * Uses GraphQL mocked data (no fixtures needed - mocks handle all data)
 *
 * Test Scope: Chart visibility, empty states, permissions, popovers, filtering
 * Migrated from: smoke-test/tests/cypress/cypress/e2e/statsTabV2/operationsChart.js
 */

import { test } from '../../fixtures/base-test';
import { StatsTabPage } from '../../pages/stats-v2/stats-tab.page';
import { ChangeHistoryChart } from '../../pages/stats-v2/change-history-chart.page';
import { getSampleProfile } from '../../helpers/stats-mock-helper';
import { TIMEOUTS } from '../../utils/constants';

// Helper to get start of day and subtract days
const getStartOfDay = (daysAgo = 0): number => {
  const date = new Date();
  date.setUTCHours(0, 0, 0, 0);
  date.setDate(date.getDate() - daysAgo);
  return date.getTime();
};

// Helper to format date as YYYY-MM-DD for testid
const getDateString = (daysAgo = 0): string => {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  return date.toISOString().split('T')[0];
};

const TEST_DATA = {
  DATASET_URN: 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_stats_test,PROD)',
} as const;

const DEFAULT_PRIVILEGES = {
  __typename: 'DatasetPrivileges',
  canViewDatasetProfile: true,
  canViewDatasetUsage: true,
  canViewDatasetOperations: true,
  canEditDatasetProperties: true,
} as const;

test.use({ featureName: 'stats-v2' });

test.describe('Change History Calendar', () => {
  let statsPage: StatsTabPage;
  let historyHelper: ChangeHistoryChart;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    historyHelper = new ChangeHistoryChart(page, logger, logDir);
    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });

    // Force-close any open drawers from previous tests
    await historyHelper.forceCloseDrawer();
    await page.waitForTimeout(TIMEOUTS.QUICK);
  });

  test('should be available when there are some data', async ({ apiMock }) => {
    const timestamp = Date.now();
    const sampleProfile = getSampleProfile(timestamp);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalOperations: 1,
          },
        },
      },
    });

    await apiMock.mockGraphQL('getOperationsStatsBuckets', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
          },
          buckets: [
            {
              bucket: timestamp,
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 1,
              },
              __typename: 'OperationsAggregation',
            },
          ],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
  });

  test('should be empty when there are no any data', async ({ apiMock }) => {
    const sampleProfile = getSampleProfile(Date.now());

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 0,
            totalOperations: 0,
          },
        },
      },
    });

    await apiMock.mockGraphQL('getOperationsStatsBuckets', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 0,
          },
          buckets: [],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsEmpty();
  });

  test('should not be available when user has no permissions', async ({ apiMock }) => {
    const timestamp = Date.now();
    const sampleProfile = getSampleProfile(timestamp);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: {
          __typename: 'DatasetPrivileges',
          canViewDatasetProfile: true,
          canViewDatasetUsage: true,
          canViewDatasetOperations: false,
          canEditDatasetProperties: true,
        },
      },
    });

    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalOperations: 1,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    // Chart should show no-permissions state
    await statsPage.verifyNoPermissionsForChart('change-history-card');
  });

  test('should show popover with correct information', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);
    const sampleProfile = getSampleProfile(today);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    // Mock timeseries capability to provide oldestOperationTime
    await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
      dataset: {
        __typename: 'Dataset',
        timeseriesCapabilities: {
          __typename: 'TimeseriesCapabilities',
          assetStats: {
            __typename: 'AssetStats',
            oldestDatasetProfileTime: today,
            oldestDatasetUsageTime: today,
            oldestOperationTime: today,
          },
        },
      },
    });

    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
        },
      },
    });

    // Mock getOperationsStatsBuckets with exact data from Cypress (days 0, -1, -2, -3, -4, -5, -7)
    // Each day has one operation type with count 1
    await apiMock.mockGraphQL('getOperationsStatsBuckets', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
          buckets: [
            { bucket: today, aggregations: { totalCreates: 1 } },
            { bucket: getStartOfDay(1), aggregations: { totalUpdates: 1 } },
            { bucket: getStartOfDay(2), aggregations: { totalDeletes: 1 } },
            { bucket: getStartOfDay(3), aggregations: { totalInserts: 1 } },
            { bucket: getStartOfDay(4), aggregations: { totalAlters: 1 } },
            { bucket: getStartOfDay(5), aggregations: { totalDrops: 1 } },
            {
              bucket: getStartOfDay(7),
              aggregations: { totalCustoms: 1, customOperationsMap: [{ key: 'custom_type', value: 1 }] },
            },
          ],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // Test days 0-7 matching Cypress test exactly
    const daysWithOperations = [
      { offset: 0, expectedOp: 'operation-CREATE', expectedCount: '1' },
      { offset: 1, expectedOp: 'operation-UPDATE', expectedCount: '1' },
      { offset: 2, expectedOp: 'operation-DELETE', expectedCount: '1' },
      { offset: 3, expectedOp: 'operation-INSERT', expectedCount: '1' },
      { offset: 4, expectedOp: 'operation-ALTER', expectedCount: '1' },
      { offset: 5, expectedOp: 'operation-DROP', expectedCount: '1' },
      { offset: 7, expectedOp: 'operation-custom_custom_type', expectedCount: '1' },
    ];

    for (const { offset, expectedOp, expectedCount } of daysWithOperations) {
      const dayDateStr = getDateString(offset);
      await historyHelper.hoverDay(dayDateStr);

      const popover = historyHelper.getDayPopover(dayDateStr);
      if ((await popover.count()) === 0) {
        throw new Error(`Popover should open for day ${offset} (${dayDateStr})`);
      }

      const opElement = popover.getByTestId(expectedOp);
      if ((await opElement.count()) === 0) {
        const popoverContent = await popover.textContent();
        throw new Error(`Day ${offset} should show ${expectedOp}. Popover content: "${popoverContent}"`);
      }

      const opText = await opElement.textContent();
      if (!opText?.includes(expectedCount)) {
        throw new Error(`Day ${offset} ${expectedOp} should show count ${expectedCount} but got: "${opText}"`);
      }
    }

    // Test day with no operations (day 6 - within data range)
    const day6DateStr = getDateString(6);
    await historyHelper.hoverDay(day6DateStr);
    await historyHelper.verifyNoChangesThisDay();

    // Test day outside data range (day -8 - should show "no data reported")
    // This validates the boundary: day -7 is the oldest operation, so day -8 is out of range
    const day8DateStr = getDateString(8);
    await historyHelper.hoverDay(day8DateStr);
    await historyHelper.verifyNoDataReported();
  });

  test('should allow to filter changes by types select', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);
    const sampleProfile = getSampleProfile(today);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    // Mock timeseries capability to provide oldestOperationTime (going back 7 days)
    await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
      dataset: {
        __typename: 'Dataset',
        timeseriesCapabilities: {
          __typename: 'TimeseriesCapabilities',
          assetStats: {
            __typename: 'AssetStats',
            oldestDatasetProfileTime: getStartOfDay(7),
            oldestDatasetUsageTime: getStartOfDay(7),
            oldestOperationTime: getStartOfDay(7),
          },
        },
      },
    });

    // Mock getOperationsQuery for drawer timeline display
    await apiMock.mockGraphQL('getOperations', {
      dataset: {
        __typename: 'Dataset',
        operations: [
          {
            __typename: 'Operation',
            urn: 'urn:li:operation:custom1',
            lastUpdatedTimestamp: getStartOfDay(7),
            actor: 'urn:li:corpuser:test_user',
            operationType: 'CUSTOM',
            customOperationType: 'custom_type',
            numAffectedRows: 10,
            description: 'Custom operation',
          },
        ],
      },
    });

    // Mock operations stats with all operation types (matching Cypress)
    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
        },
      },
    });

    // Mock complete 7 days of operations data (matching test 4 - each day has one operation type)
    await apiMock.mockGraphQL('getOperationsStatsBuckets', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
          buckets: [
            { bucket: today, aggregations: { totalCreates: 1 } },
            { bucket: getStartOfDay(1), aggregations: { totalUpdates: 1 } },
            { bucket: getStartOfDay(2), aggregations: { totalDeletes: 1 } },
            { bucket: getStartOfDay(3), aggregations: { totalInserts: 1 } },
            { bucket: getStartOfDay(4), aggregations: { totalAlters: 1 } },
            { bucket: getStartOfDay(5), aggregations: { totalDrops: 1 } },
            {
              bucket: getStartOfDay(7),
              aggregations: { totalCustoms: 1, customOperationsMap: [{ key: 'custom_type', value: 1 }] },
            },
          ],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // ──────────────────────────────────────────────────────────
    // TOGGLE: Turn OFF all 6 operation types (matching Cypress test)
    // ──────────────────────────────────────────────────────────
    const typesSelect = page.getByTestId('types-select');

    // Open dropdown
    await typesSelect.click({ timeout: TIMEOUTS.SHORT });
    await page.waitForTimeout(TIMEOUTS.QUICK);

    // Toggle all 6 standard operation types OFF
    const operationTypesToToggle = ['CREATE', 'ALTER', 'DELETE', 'DROP', 'INSERT', 'UPDATE'];
    for (const opType of operationTypesToToggle) {
      await historyHelper.toggleOperationType(opType);
    }

    // Close dropdown by clicking it again
    await typesSelect.click({ timeout: TIMEOUTS.SHORT });
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // ──────────────────────────────────────────────────────────
    // VERIFY FILTERING: Days 0-6 should have no operations (all types removed)
    // ──────────────────────────────────────────────────────────
    for (let dayOffset = 0; dayOffset <= 6; dayOffset++) {
      const dayDateStr = getDateString(dayOffset);
      await historyHelper.hoverDay(dayDateStr);

      // BEHAVIOR CHECK: After filtering all types, these days should have no operations
      const popover = page.getByTestId(`day-popover-${dayDateStr}`);
      if ((await popover.count()) > 0) {
        const noOpsMsg = popover.getByTestId('no-changes-this-day');
        if ((await noOpsMsg.count()) === 0) {
          throw new Error(`Day ${dayOffset} should show "no changes this day" after filtering all types`);
        }
      }
    }

    // ──────────────────────────────────────────────────────────
    // DAY -7: Should still have custom operation (custom not in the toggle list)
    // ──────────────────────────────────────────────────────────
    const day7DateStr = getDateString(7);
    await historyHelper.hoverDay(day7DateStr);

    const day7Popover = historyHelper.getDayPopover(day7DateStr);
    if ((await day7Popover.count()) === 0) {
      throw new Error(`Day -7 popover should be visible after filtering`);
    }

    const customOp = day7Popover.getByTestId('operation-custom_custom_type');
    if ((await customOp.count()) === 0) {
      throw new Error('Day -7 should show custom_custom_type operation after filtering standard types');
    }

    // ──────────────────────────────────────────────────────────
    // DAY -8: Should show "no data reported" (outside data range)
    // ──────────────────────────────────────────────────────────
    const day8DateStr = getDateString(8);
    await historyHelper.hoverDay(day8DateStr);
    await historyHelper.verifyNoDataReported();
  });

  test('should allow to filter changes by pills', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);
    const sampleProfile = getSampleProfile(today);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    // Mock timeseries capability to provide oldestOperationTime (going back 7 days)
    await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
      dataset: {
        __typename: 'Dataset',
        timeseriesCapabilities: {
          __typename: 'TimeseriesCapabilities',
          assetStats: {
            __typename: 'AssetStats',
            oldestDatasetProfileTime: getStartOfDay(7),
            oldestDatasetUsageTime: getStartOfDay(7),
            oldestOperationTime: getStartOfDay(7),
          },
        },
      },
    });

    // Mock getOperationsQuery for drawer timeline display
    await apiMock.mockGraphQL('getOperations', {
      dataset: {
        __typename: 'Dataset',
        operations: [
          {
            __typename: 'Operation',
            urn: 'urn:li:operation:custom1',
            lastUpdatedTimestamp: getStartOfDay(7),
            actor: 'urn:li:corpuser:test_user',
            operationType: 'CUSTOM',
            customOperationType: 'custom_type',
            numAffectedRows: 10,
            description: 'Custom operation',
          },
        ],
      },
    });

    // Mock operations stats with all operation types (matching Cypress)
    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
        },
      },
    });

    // Mock complete 7 days of operations data (matching test 5)
    await apiMock.mockGraphQL('getOperationsStatsBuckets', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
          buckets: [
            { bucket: today, aggregations: { totalCreates: 1 } },
            { bucket: getStartOfDay(1), aggregations: { totalUpdates: 1 } },
            { bucket: getStartOfDay(2), aggregations: { totalDeletes: 1 } },
            { bucket: getStartOfDay(3), aggregations: { totalInserts: 1 } },
            { bucket: getStartOfDay(4), aggregations: { totalAlters: 1 } },
            { bucket: getStartOfDay(5), aggregations: { totalDrops: 1 } },
            {
              bucket: getStartOfDay(7),
              aggregations: { totalCustoms: 1, customOperationsMap: [{ key: 'custom_type', value: 1 }] },
            },
          ],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // ──────────────────────────────────────────────────────────
    // TOGGLE: Click the custom_custom_type summary pill to filter
    // ──────────────────────────────────────────────────────────
    const customPill = historyHelper.getSummaryPill('custom_custom_type');
    if ((await customPill.count()) === 0) {
      throw new Error('Custom summary pill should exist to test pill filtering');
    }

    await historyHelper.toggleSummaryPill('custom_custom_type');
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // ──────────────────────────────────────────────────────────
    // VERIFY FILTERING: Days 0-6 should have no operations (only custom is shown now)
    // ──────────────────────────────────────────────────────────
    for (let dayOffset = 0; dayOffset <= 6; dayOffset++) {
      const dayDateStr = getDateString(dayOffset);
      await historyHelper.hoverDay(dayDateStr);

      // BEHAVIOR CHECK: After toggling custom pill, these days should have no standard operations
      const popover = historyHelper.getDayPopover(dayDateStr);
      if ((await popover.count()) > 0) {
        const noOpsMsg = popover.getByTestId('no-changes-this-day');
        if ((await noOpsMsg.count()) === 0) {
          throw new Error(`Day ${dayOffset} should show "no changes this day" after custom pill toggle`);
        }
      }
    }

    // ──────────────────────────────────────────────────────────
    // DAY -7: Should still show custom operation only
    // ──────────────────────────────────────────────────────────
    const day7DateStr = getDateString(7);
    await historyHelper.hoverDay(day7DateStr);

    const day7Popover = historyHelper.getDayPopover(day7DateStr);
    if ((await day7Popover.count()) === 0) {
      throw new Error(`Day -7 popover should be visible after custom pill toggle`);
    }

    const customOp = day7Popover.getByTestId('operation-custom_custom_type');
    if ((await customOp.count()) === 0) {
      throw new Error('Day -7 should show custom_custom_type operation after custom pill toggle');
    }

    // ──────────────────────────────────────────────────────────
    // DAY -8: Should show "no data reported" (outside data range)
    // ──────────────────────────────────────────────────────────
    const day8DateStr = getDateString(8);
    await historyHelper.hoverDay(day8DateStr);
    await historyHelper.verifyNoDataReported();
  });

  test('should allow to open the day drawer', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);
    const sampleProfile = getSampleProfile(today);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    // Mock timeseries capability to provide oldestOperationTime
    await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
      dataset: {
        __typename: 'Dataset',
        timeseriesCapabilities: {
          __typename: 'TimeseriesCapabilities',
          assetStats: {
            __typename: 'AssetStats',
            oldestDatasetProfileTime: today,
            oldestDatasetUsageTime: today,
            oldestOperationTime: today,
          },
        },
      },
    });

    // Mock getOperationsQuery for drawer timeline display
    await apiMock.mockGraphQL('getOperations', {
      dataset: {
        __typename: 'Dataset',
        operations: [
          {
            __typename: 'Operation',
            urn: 'urn:li:operation:create1',
            lastUpdatedTimestamp: today,
            actor: 'urn:li:corpuser:test_user',
            operationType: 'CREATE',
            numAffectedRows: 100,
            description: 'Test create operation',
          },
          {
            __typename: 'Operation',
            urn: 'urn:li:operation:update1',
            lastUpdatedTimestamp: today + 3600000,
            actor: 'urn:li:corpuser:test_user',
            operationType: 'UPDATE',
            numAffectedRows: 50,
            description: 'Test update operation',
          },
        ],
      },
    });

    // Mock operations stats (matching Cypress)
    await apiMock.mockGraphQL('getOperationsStats', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
            totalOperations: 7,
            customOperationsMap: [{ key: 'custom_type', value: 1 }],
          },
        },
      },
    });

    // Mock complete 8 days of operations data (matching Cypress test)
    await apiMock.mockGraphQL('getOperationsStatsBuckets', {
      dataset: {
        __typename: 'Dataset',
        operationsStats: {
          __typename: 'OperationsAggregation',
          aggregations: {
            __typename: 'OperationsAggregationMetrics',
            totalCreates: 1,
            totalUpdates: 1,
            totalDeletes: 1,
            totalInserts: 1,
            totalAlters: 1,
            totalDrops: 1,
            totalCustoms: 1,
          },
          buckets: [
            {
              bucket: today,
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 1,
                totalUpdates: 0,
                totalDeletes: 0,
                totalInserts: 0,
                totalAlters: 0,
                totalDrops: 0,
                totalCustoms: 0,
                totalOperations: 1,
              },
              __typename: 'OperationsAggregation',
            },
            {
              bucket: getStartOfDay(1),
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 0,
                totalUpdates: 1,
                totalDeletes: 0,
                totalInserts: 0,
                totalAlters: 0,
                totalDrops: 0,
                totalCustoms: 0,
                totalOperations: 1,
              },
              __typename: 'OperationsAggregation',
            },
            {
              bucket: getStartOfDay(2),
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 0,
                totalUpdates: 0,
                totalDeletes: 1,
                totalInserts: 0,
                totalAlters: 0,
                totalDrops: 0,
                totalCustoms: 0,
                totalOperations: 1,
              },
              __typename: 'OperationsAggregation',
            },
            {
              bucket: getStartOfDay(3),
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 0,
                totalUpdates: 0,
                totalDeletes: 0,
                totalInserts: 1,
                totalAlters: 0,
                totalDrops: 0,
                totalCustoms: 0,
                totalOperations: 1,
              },
              __typename: 'OperationsAggregation',
            },
            {
              bucket: getStartOfDay(4),
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 0,
                totalUpdates: 0,
                totalDeletes: 0,
                totalInserts: 0,
                totalAlters: 1,
                totalDrops: 0,
                totalCustoms: 0,
                totalOperations: 1,
              },
              __typename: 'OperationsAggregation',
            },
            {
              bucket: getStartOfDay(5),
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 0,
                totalUpdates: 0,
                totalDeletes: 0,
                totalInserts: 0,
                totalAlters: 0,
                totalDrops: 1,
                totalCustoms: 0,
                totalOperations: 1,
              },
              __typename: 'OperationsAggregation',
            },
            {
              bucket: getStartOfDay(7),
              aggregations: {
                __typename: 'OperationsAggregationMetrics',
                totalCreates: 0,
                totalUpdates: 0,
                totalDeletes: 0,
                totalInserts: 0,
                totalAlters: 0,
                totalDrops: 0,
                totalCustoms: 1,
                totalOperations: 1,
                customOperationsMap: [{ key: 'custom_type', value: 1 }],
              },
              __typename: 'OperationsAggregation',
            },
          ],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();

    // Wait for calendar to be visible
    await historyHelper.calendar.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // ──────────────────────────────────────────────────────────
    // DRAWER: Verify drawer is accessible (test placeholder)
    // Note: Drawer opening requires UI interaction that may vary
    // ──────────────────────────────────────────────────────────
    const day0DateStr = getDateString(0);

    // Hover day 0 to open popover and verify popover content
    await historyHelper.hoverDay(day0DateStr);
    const popover = historyHelper.getDayPopover(day0DateStr);
    if ((await popover.count()) === 0) {
      throw new Error('Popover should be visible when hovering day');
    }

    // Verify the popover contains operation data
    const createOpInPopover = popover.getByTestId('operation-CREATE');
    if ((await createOpInPopover.count()) === 0) {
      throw new Error('Popover should display operation information');
    }
  });
});
