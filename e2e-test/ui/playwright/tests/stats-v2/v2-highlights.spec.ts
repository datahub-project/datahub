/**
 * Highlights (Stats Cards) Tests - Cypress Migration
 *
 * Tests the summary statistics cards displaying Latest and Last Month stats.
 * Verifies card values, visibility, view buttons, and empty state handling.
 *
 * Uses GraphQL mocked data (no fixtures needed - mocks handle all data)
 *
 * Test Scope: Card visibility, numeric values, view buttons, empty state behavior
 * Migrated from: smoke-test/tests/cypress/cypress/e2e/statsTabV2/highlights.js
 */

import { test } from '../../fixtures/base-test';
import { StatsTabPage } from '../../pages/stats-v2/stats-tab.page';
import {
  setupBasicDataset,
  setupUsageStats,
  setupOperationsStats,
  setupEmptyDatasetForStats,
} from '../../factories/mock-responses/stats';
import type { ApiMocker } from '../../fixtures/mocking.fixture';
import {
  TEST_DATASET_URN,
  DEFAULT_PRIVILEGES,
  EXPECTED_STATS,
  LATEST_STATS_CARDS,
  LAST_MONTH_STATS_CARDS,
} from '../../pages/stats-v2/stats.constants';

test.use({ featureName: 'stats-v2' });

async function setupDataWithStats(apiMock: ApiMocker) {
  const timestamp = Date.now();
  await setupBasicDataset(apiMock, timestamp, TEST_DATASET_URN, DEFAULT_PRIVILEGES);
  await setupUsageStats(apiMock, timestamp);
  await setupOperationsStats(apiMock, timestamp, false);
}

test.describe('Highlight Stats Cards', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
  });

  test('should show values when the data is available', async ({ apiMock }) => {
    await setupDataWithStats(apiMock);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Latest stats (Rows, Columns)
    await statsPage.verifyLatestStatsVisible();
    await statsPage.verifyLatestStatsCard(LATEST_STATS_CARDS.ROWS, EXPECTED_STATS.ROWS);
    await statsPage.verifyLatestStatsCardButtonVisible(LATEST_STATS_CARDS.ROWS);
    await statsPage.verifyLatestStatsCard(LATEST_STATS_CARDS.COLUMNS, EXPECTED_STATS.COLUMNS);
    await statsPage.verifyLatestStatsCardButtonVisible(LATEST_STATS_CARDS.COLUMNS);

    // Last month stats (Users, Queries)
    await statsPage.verifyLastMonthStatsVisible();
    await statsPage.verifyLastMonthStatsCard(LAST_MONTH_STATS_CARDS.USERS, EXPECTED_STATS.USERS);
    await statsPage.verifyLastMonthStatsCardButtonVisible(LAST_MONTH_STATS_CARDS.USERS);
    await statsPage.verifyLastMonthStatsCard(LAST_MONTH_STATS_CARDS.QUERIES, EXPECTED_STATS.QUERIES);
    await statsPage.verifyLastMonthStatsCardButtonVisible(LAST_MONTH_STATS_CARDS.QUERIES);

    // Changes card
    await statsPage.verifyChangesCardVisible();
    await statsPage.verifyChangesCardValue(EXPECTED_STATS.CHANGES);
    await statsPage.verifyChangesCardButtonVisible();
  });

  test('should not show values when the data is not available', async ({ apiMock }) => {
    const timestamp = Date.now();
    await setupEmptyDatasetForStats(apiMock, timestamp, TEST_DATASET_URN, DEFAULT_PRIVILEGES);

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    // Rows card - should not show value when profile is empty
    // The card shows "No Data" because rowCount is null
    await statsPage.verifyLatestStatsCardDoesNotContain(LATEST_STATS_CARDS.ROWS, EXPECTED_STATS.ROWS);

    // Columns card - should not show value when fields are empty
    await statsPage.verifyLatestStatsCardDoesNotContain(LATEST_STATS_CARDS.COLUMNS, '5');

    // Users card - should show "0" when uniqueUserCount is 0
    await statsPage.verifyLastMonthStatsCard(LAST_MONTH_STATS_CARDS.USERS, '0');

    // Queries card - should show "0" when totalSqlQueries is 0
    await statsPage.verifyLastMonthStatsCard(LAST_MONTH_STATS_CARDS.QUERIES, '0');

    // Changes card - should show "0" when totalCreates is 0
    await statsPage.verifyChangesCardValue('0');
  });
});
