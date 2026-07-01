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

import { expect } from '@playwright/test';
import { test } from '../../fixtures/base-test';
import { StatsTabPage } from '../../pages/stats-v2/stats-tab.page';
import { ChangeHistoryChart } from '../../pages/stats-v2/change-history-chart.page';
import {
  setupChangeHistoryWithData,
  setupChangeHistoryEmpty,
  setupChangeHistoryNoPermissions,
  setupChangeHistoryWithOperations,
} from '../../factories/mock-responses/stats';
import { getStartOfDay, getDateString } from '../../utils/time-utils';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';
import {
  TEST_DATASET_URN,
  DEFAULT_PRIVILEGES,
  OPERATION_TYPES,
  CUSTOM_OPERATION_PREFIX,
  CUSTOM_OPERATION_TYPE,
  CUSTOM_OPERATION_DISPLAY,
  CHART_IDS,
} from '../../pages/stats-v2/stats.constants';

const TEST_DATA = {
  DATASET_URN: TEST_DATASET_URN,
} as const;

test.use({ featureName: 'stats-v2' });

test.describe('Change History Calendar', () => {
  let statsPage: StatsTabPage;
  let historyHelper: ChangeHistoryChart;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    historyHelper = new ChangeHistoryChart(page, logger, logDir);
    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
    await historyHelper.forceCloseDrawer();
  });

  test('should be available when there is some data', async ({ apiMock }) => {
    const timestamp = Date.now();
    await setupChangeHistoryWithData(apiMock, timestamp, TEST_DATA.DATASET_URN, DEFAULT_PRIVILEGES);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
  });

  test('should be empty when there is no data', async ({ apiMock }) => {
    await setupChangeHistoryEmpty(apiMock, TEST_DATA.DATASET_URN, DEFAULT_PRIVILEGES);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsEmpty();
  });

  test('should not be available when user has no permissions', async ({ apiMock }) => {
    await setupChangeHistoryNoPermissions(apiMock, TEST_DATA.DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    // Chart should show no-permissions state
    await statsPage.verifyNoPermissionsForChart(CHART_IDS.CHANGE_HISTORY);
  });

  test('should show popover with correct information', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);

    const aggregations = {
      totalCreates: 1,
      totalUpdates: 1,
      totalDeletes: 1,
      totalInserts: 1,
      totalAlters: 1,
      totalDrops: 1,
      totalCustoms: 1,
      totalOperations: 7,
      customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }],
    };

    const buckets = [
      { bucket: today, aggregations: { totalCreates: 1 } },
      { bucket: getStartOfDay(1), aggregations: { totalUpdates: 1 } },
      { bucket: getStartOfDay(2), aggregations: { totalDeletes: 1 } },
      { bucket: getStartOfDay(3), aggregations: { totalInserts: 1 } },
      { bucket: getStartOfDay(4), aggregations: { totalAlters: 1 } },
      { bucket: getStartOfDay(5), aggregations: { totalDrops: 1 } },
      {
        bucket: getStartOfDay(7),
        aggregations: { totalCustoms: 1, customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }] },
      },
    ];

    await setupChangeHistoryWithOperations(apiMock, TEST_DATA.DATASET_URN, today, aggregations, buckets);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Test days 0-7 matching Cypress test exactly
    const daysWithOperations = [
      { offset: 0, expectedOp: OPERATION_TYPES.CREATE, expectedCount: '1' },
      { offset: 1, expectedOp: OPERATION_TYPES.UPDATE, expectedCount: '1' },
      { offset: 2, expectedOp: OPERATION_TYPES.DELETE, expectedCount: '1' },
      { offset: 3, expectedOp: OPERATION_TYPES.INSERT, expectedCount: '1' },
      { offset: 4, expectedOp: OPERATION_TYPES.ALTER, expectedCount: '1' },
      { offset: 5, expectedOp: OPERATION_TYPES.DROP, expectedCount: '1' },
      { offset: 7, expectedOp: `${CUSTOM_OPERATION_PREFIX}${CUSTOM_OPERATION_TYPE}`, expectedCount: '1' },
    ];

    for (const { offset, expectedOp, expectedCount } of daysWithOperations) {
      const dayDateStr = getDateString(offset);
      await historyHelper.hoverDay(dayDateStr);
      await historyHelper.verifyOperationCountInDayPopover(dayDateStr, expectedOp, expectedCount);
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
    const oldestDay = getStartOfDay(7);

    const aggregations = {
      totalCreates: 1,
      totalUpdates: 1,
      totalDeletes: 1,
      totalInserts: 1,
      totalAlters: 1,
      totalDrops: 1,
      totalCustoms: 1,
      totalOperations: 7,
      customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }],
    };

    const buckets = [
      { bucket: today, aggregations: { totalCreates: 1 } },
      { bucket: getStartOfDay(1), aggregations: { totalUpdates: 1 } },
      { bucket: getStartOfDay(2), aggregations: { totalDeletes: 1 } },
      { bucket: getStartOfDay(3), aggregations: { totalInserts: 1 } },
      { bucket: getStartOfDay(4), aggregations: { totalAlters: 1 } },
      { bucket: getStartOfDay(5), aggregations: { totalDrops: 1 } },
      {
        bucket: getStartOfDay(7),
        aggregations: { totalCustoms: 1, customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }] },
      },
    ];

    const operations = [
      {
        __typename: 'Operation',
        urn: 'urn:li:operation:custom1',
        lastUpdatedTimestamp: oldestDay,
        actor: 'urn:li:corpuser:test_user',
        operationType: 'CUSTOM',
        customOperationType: 'custom_type',
        numAffectedRows: 10,
        description: 'Custom operation',
      },
    ];

    await setupChangeHistoryWithOperations(
      apiMock,
      TEST_DATA.DATASET_URN,
      oldestDay,
      aggregations,
      buckets,
      operations,
    );

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // ──────────────────────────────────────────────────────────
    // TOGGLE: Turn OFF all 6 operation types (matching Cypress test)
    // ──────────────────────────────────────────────────────────

    // Open dropdown
    await historyHelper.typesSelect.click({ timeout: TIMEOUTS.SHORT });
    await page.waitForTimeout(TIMEOUTS.QUICK);

    // Toggle all 6 standard operation types OFF
    const operationTypesToToggle = ['CREATE', 'ALTER', 'DELETE', 'DROP', 'INSERT', 'UPDATE'];
    for (const opType of operationTypesToToggle) {
      await historyHelper.toggleOperationType(opType);
    }

    // Close dropdown by clicking it again
    await historyHelper.typesSelect.click({ timeout: TIMEOUTS.SHORT });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // ──────────────────────────────────────────────────────────
    // VERIFY FILTERING: Days 0-6 should have no operations (all types removed)
    // ──────────────────────────────────────────────────────────
    for (let dayOffset = 0; dayOffset <= 6; dayOffset++) {
      const dayDateStr = getDateString(dayOffset);
      await historyHelper.hoverDay(dayDateStr);

      // BEHAVIOR CHECK: After filtering all types, these days should have no operations
      const popover = historyHelper.getDayPopover(dayDateStr);
      if ((await popover.count()) > 0) {
        await historyHelper.verifyNoChangesThisDayInDayPopover(popover);
      }
    }

    // ──────────────────────────────────────────────────────────
    // DAY -7: Should still have custom operation (custom not in the toggle list)
    // ──────────────────────────────────────────────────────────
    const day7DateStr = getDateString(7);
    await historyHelper.hoverDay(day7DateStr);
    const day7Popover = historyHelper.getDayPopover(day7DateStr);
    const customOpTestId = `${CUSTOM_OPERATION_PREFIX}${CUSTOM_OPERATION_TYPE}`;
    await historyHelper.verifyCustomOperationInDayPopover(day7Popover, customOpTestId);

    // ──────────────────────────────────────────────────────────
    // DAY -8: Should show "no data reported" (outside data range)
    // ──────────────────────────────────────────────────────────
    const day8DateStr = getDateString(8);
    await historyHelper.hoverDay(day8DateStr);
    await historyHelper.verifyNoDataReported();
  });

  test('should allow to filter changes by pills', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);
    const oldestDay = getStartOfDay(7);

    const aggregations = {
      totalCreates: 1,
      totalUpdates: 1,
      totalDeletes: 1,
      totalInserts: 1,
      totalAlters: 1,
      totalDrops: 1,
      totalCustoms: 1,
      totalOperations: 7,
      customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }],
    };

    const buckets = [
      { bucket: today, aggregations: { totalCreates: 1 } },
      { bucket: getStartOfDay(1), aggregations: { totalUpdates: 1 } },
      { bucket: getStartOfDay(2), aggregations: { totalDeletes: 1 } },
      { bucket: getStartOfDay(3), aggregations: { totalInserts: 1 } },
      { bucket: getStartOfDay(4), aggregations: { totalAlters: 1 } },
      { bucket: getStartOfDay(5), aggregations: { totalDrops: 1 } },
      {
        bucket: getStartOfDay(7),
        aggregations: { totalCustoms: 1, customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }] },
      },
    ];

    const operations = [
      {
        __typename: 'Operation',
        urn: 'urn:li:operation:custom1',
        lastUpdatedTimestamp: oldestDay,
        actor: 'urn:li:corpuser:test_user',
        operationType: 'CUSTOM',
        customOperationType: 'custom_type',
        numAffectedRows: 10,
        description: 'Custom operation',
      },
    ];

    await setupChangeHistoryWithOperations(
      apiMock,
      TEST_DATA.DATASET_URN,
      oldestDay,
      aggregations,
      buckets,
      operations,
    );

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // ──────────────────────────────────────────────────────────
    // TOGGLE: Click the custom_custom_type summary pill to filter
    // ──────────────────────────────────────────────────────────
    const customPill = historyHelper.getSummaryPill(CUSTOM_OPERATION_DISPLAY);
    await expect(customPill).toBeVisible();
    await historyHelper.toggleSummaryPill(CUSTOM_OPERATION_DISPLAY);
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
        await historyHelper.verifyNoChangesThisDayInDayPopover(popover);
      }
    }

    // ──────────────────────────────────────────────────────────
    // DAY -7: Should still show custom operation only
    // ──────────────────────────────────────────────────────────
    const day7DateStr = getDateString(7);
    await historyHelper.hoverDay(day7DateStr);
    const day7Popover = historyHelper.getDayPopover(day7DateStr);
    const customOpTestId = `${CUSTOM_OPERATION_PREFIX}${CUSTOM_OPERATION_TYPE}`;
    await historyHelper.verifyCustomOperationInDayPopover(day7Popover, customOpTestId);

    // ──────────────────────────────────────────────────────────
    // DAY -8: Should show "no data reported" (outside data range)
    // ──────────────────────────────────────────────────────────
    const day8DateStr = getDateString(8);
    await historyHelper.hoverDay(day8DateStr);
    await historyHelper.verifyNoDataReported();
  });

  test('should allow to open the day drawer', async ({ apiMock, page }) => {
    const today = getStartOfDay(0);

    const aggregations = {
      totalCreates: 1,
      totalUpdates: 1,
      totalDeletes: 1,
      totalInserts: 1,
      totalAlters: 1,
      totalDrops: 1,
      totalCustoms: 1,
      totalOperations: 7,
      customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }],
    };

    const buckets = [
      {
        bucket: today,
        aggregations: {
          totalCreates: 1,
          totalUpdates: 0,
          totalDeletes: 0,
          totalInserts: 0,
          totalAlters: 0,
          totalDrops: 0,
          totalCustoms: 0,
          totalOperations: 1,
        },
      },
      {
        bucket: getStartOfDay(1),
        aggregations: {
          totalCreates: 0,
          totalUpdates: 1,
          totalDeletes: 0,
          totalInserts: 0,
          totalAlters: 0,
          totalDrops: 0,
          totalCustoms: 0,
          totalOperations: 1,
        },
      },
      {
        bucket: getStartOfDay(2),
        aggregations: {
          totalCreates: 0,
          totalUpdates: 0,
          totalDeletes: 1,
          totalInserts: 0,
          totalAlters: 0,
          totalDrops: 0,
          totalCustoms: 0,
          totalOperations: 1,
        },
      },
      {
        bucket: getStartOfDay(3),
        aggregations: {
          totalCreates: 0,
          totalUpdates: 0,
          totalDeletes: 0,
          totalInserts: 1,
          totalAlters: 0,
          totalDrops: 0,
          totalCustoms: 0,
          totalOperations: 1,
        },
      },
      {
        bucket: getStartOfDay(4),
        aggregations: {
          totalCreates: 0,
          totalUpdates: 0,
          totalDeletes: 0,
          totalInserts: 0,
          totalAlters: 1,
          totalDrops: 0,
          totalCustoms: 0,
          totalOperations: 1,
        },
      },
      {
        bucket: getStartOfDay(5),
        aggregations: {
          totalCreates: 0,
          totalUpdates: 0,
          totalDeletes: 0,
          totalInserts: 0,
          totalAlters: 0,
          totalDrops: 1,
          totalCustoms: 0,
          totalOperations: 1,
        },
      },
      {
        bucket: getStartOfDay(7),
        aggregations: {
          totalCreates: 0,
          totalUpdates: 0,
          totalDeletes: 0,
          totalInserts: 0,
          totalAlters: 0,
          totalDrops: 0,
          totalCustoms: 1,
          totalOperations: 1,
          customOperationsMap: [{ key: CUSTOM_OPERATION_TYPE, value: 1 }],
        },
      },
    ];

    const operations = [
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
    ];

    await setupChangeHistoryWithOperations(apiMock, TEST_DATA.DATASET_URN, today, aggregations, buckets, operations);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.verifyChangeHistoryChartIsVisible();

    // Wait for calendar to be visible
    await historyHelper.calendar.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    const day0DateStr = getDateString(0);

    // Open the drawer by clicking on day 0
    await historyHelper.openDayDrawer(day0DateStr);

    // Verify the drawer is visible after opening
    await historyHelper.ensureDayDrawerIsVisible();

    // Close the drawer via close button
    await historyHelper.closeDayDrawer();
  });
});
