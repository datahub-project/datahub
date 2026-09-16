/**
 * Charts Tests - Cypress Migration
 *
 * Tests statistics charts with comprehensive time range filtering:
 * - Row Count, Query Count, Storage Size charts
 * - Time filter visibility rules based on data age
 * - Empty state handling
 * - Permission-based visibility
 *
 * Uses GraphQL mocked data (no fixtures needed - mocks handle all data)
 *
 * Test Scope: Chart visibility, time range options, empty states, permissions
 * Migrated from: smoke-test/tests/cypress/cypress/e2e/statsTabV2/charts.js
 */

import { test } from '../../fixtures/base-test';
import { StatsTabPage } from '../../pages/stats-v2/stats-tab.page';
import {
  setupChartsData,
  setupChartsDataEmpty,
  setupChartsDataNoPermissions,
  getNormalizedTimestamp,
} from '../../factories/mock-responses/stats';
import { TEST_DATASET_URN, TIME_RANGES, CHART_IDS, TIMESTAMP_OFFSETS } from '../../pages/stats-v2/stats.constants';

test.use({ featureName: 'stats-v2' });

test.describe('Statistics Charts', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
  });

  test('should be available when there is some data', async ({ apiMock }) => {
    const timestamp = getNormalizedTimestamp();
    await setupChartsData(apiMock, timestamp, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // All three main charts should be visible
    await statsPage.verifyRowCountChartIsVisible();
    await statsPage.verifyQueryCountChartIsVisible();
    await statsPage.verifyStorageSizeChartIsVisible();
  });

  test('should be empty when there is no data', async ({ apiMock }) => {
    await setupChartsDataEmpty(apiMock, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // All charts should show empty state
    await statsPage.verifyRowCountChartIsEmpty();
    await statsPage.verifyQueryCountChartIsEmpty();
    await statsPage.verifyStorageSizeChartIsEmpty();
  });

  test('should hide time filter when there is no data', async ({ apiMock }) => {
    await setupChartsDataEmpty(apiMock, TEST_DATASET_URN);
    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Time range selector should not exist when no data
    await statsPage.verifyTimeRangeSelectorDoesNotExist();
  });

  test('should hide time filter when there is only one option', async ({ apiMock }) => {
    const timestamp = getNormalizedTimestamp();
    await setupChartsData(apiMock, timestamp, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);
    await statsPage.verifyTimeRangeSelectorDoesNotExist();
  });

  test('should show time filter with all options when a year of data is available', async ({ apiMock }) => {
    await setupChartsData(apiMock, TIMESTAMP_OFFSETS.ONE_YEAR_AGO, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TIME_RANGES.MONTH);

    // All options should be available
    await statsPage.verifyTimeRangeOptionsExist([
      TIME_RANGES.WEEK,
      TIME_RANGES.MONTH,
      TIME_RANGES.QUARTER,
      TIME_RANGES.HALF_OF_YEAR,
      TIME_RANGES.YEAR,
    ]);
  });

  test('should show time filter with expected options when more than 6 months of data is available', async ({
    apiMock,
  }) => {
    await setupChartsData(apiMock, TIMESTAMP_OFFSETS.SIX_MONTHS_AGO, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    await statsPage.verifyTimeRangeSelectorExists();
    await statsPage.verifyTimeRangeSelected(TIME_RANGES.MONTH);
    await statsPage.verifyTimeRangeOptionsExist([
      TIME_RANGES.WEEK,
      TIME_RANGES.MONTH,
      TIME_RANGES.QUARTER,
      TIME_RANGES.HALF_OF_YEAR,
      TIME_RANGES.YEAR,
    ]);
  });

  test('should show time filter with expected options when more than 3 months of data is available', async ({
    apiMock,
  }) => {
    await setupChartsData(apiMock, TIMESTAMP_OFFSETS.THREE_MONTHS_AGO, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TIME_RANGES.MONTH);

    // Available options
    await statsPage.verifyTimeRangeOptionsExist([
      TIME_RANGES.WEEK,
      TIME_RANGES.MONTH,
      TIME_RANGES.QUARTER,
      TIME_RANGES.HALF_OF_YEAR,
    ]);

    // Year should not be available
    await statsPage.verifyTimeRangeOptionsDoNotExist([TIME_RANGES.YEAR]);
  });

  test('should show time filter with expected options when more than 1 month of data is available', async ({
    apiMock,
  }) => {
    await setupChartsData(apiMock, TIMESTAMP_OFFSETS.ONE_MONTH_ONE_WEEK_AGO, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TIME_RANGES.MONTH);

    // Available options
    await statsPage.verifyTimeRangeOptionsExist([TIME_RANGES.WEEK, TIME_RANGES.MONTH, TIME_RANGES.QUARTER]);

    // Half year and year should not be available
    await statsPage.verifyTimeRangeOptionsDoNotExist([TIME_RANGES.HALF_OF_YEAR, TIME_RANGES.YEAR]);
  });

  test('should show time filter with expected options when more than 1 week of data is available', async ({
    apiMock,
  }) => {
    await setupChartsData(apiMock, TIMESTAMP_OFFSETS.ONE_WEEK_ONE_DAY_AGO, TEST_DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TIME_RANGES.MONTH);

    // Only week and month should be available
    await statsPage.verifyTimeRangeOptionsExist([TIME_RANGES.WEEK, TIME_RANGES.MONTH]);

    // Quarter, half year, and year should not be available
    await statsPage.verifyTimeRangeOptionsDoNotExist([TIME_RANGES.QUARTER, TIME_RANGES.HALF_OF_YEAR, TIME_RANGES.YEAR]);
  });

  test('should not be available when user has no permissions', async ({ apiMock }) => {
    await setupChartsDataNoPermissions(apiMock, TEST_DATASET_URN);
    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // All charts should show no permissions message
    await statsPage.verifyNoPermissionsForChart(CHART_IDS.ROW_COUNT);
    await statsPage.verifyNoPermissionsForChart(CHART_IDS.QUERY_COUNT);
    await statsPage.verifyNoPermissionsForChart(CHART_IDS.STORAGE_SIZE);
  });
});
