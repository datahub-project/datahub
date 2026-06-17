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
import { getEmptyProfile } from '../../factories/mock-responses/stats';
import type { ApiMocker } from '../../fixtures/mocking.fixture';
import { TEST_DATASET_URN, DEFAULT_PRIVILEGES, EXPECTED_STATS } from './stats-constants';
import { setupBasicDataset, setupUsageStats, setupOperationsStats } from '../../factories/mock-responses/stats';

const TEST_DATA = {
  DATASET_URN: TEST_DATASET_URN,
} as const;

test.use({ featureName: 'stats-v2' });

async function setupDataWithStats(apiMock: ApiMocker) {
  const timestamp = Date.now();
  await setupBasicDataset(apiMock, timestamp, TEST_DATASET_URN, DEFAULT_PRIVILEGES);
  await setupUsageStats(apiMock, timestamp);
  await setupOperationsStats(apiMock, timestamp, false);
}

test.describe('Highlight Stats Cards', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
  });

  test('should show values when the data is available', async ({ apiMock }) => {
    await setupDataWithStats(apiMock);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Latest stats (Rows, Columns)
    await statsPage.verifyLatestStatsVisible();
    await statsPage.verifyLatestStatsCard('rows-card', EXPECTED_STATS.ROWS);
    await statsPage.verifyLatestStatsCardButtonVisible('rows-card');
    await statsPage.verifyLatestStatsCard('columns-card', EXPECTED_STATS.COLUMNS);
    await statsPage.verifyLatestStatsCardButtonVisible('columns-card');

    // Last month stats (Users, Queries)
    await statsPage.verifyLastMonthStatsVisible();
    await statsPage.verifyLastMonthStatsCard('users-card', EXPECTED_STATS.USERS);
    await statsPage.verifyLastMonthStatsCardButtonVisible('users-card');
    await statsPage.verifyLastMonthStatsCard('queries-card', EXPECTED_STATS.QUERIES);
    await statsPage.verifyLastMonthStatsCardButtonVisible('queries-card');

    // Changes card
    await statsPage.verifyChangesCardVisible();
    await statsPage.verifyChangesCardValue(EXPECTED_STATS.CHANGES);
    await statsPage.verifyChangesCardButtonVisible();
  });

  test('should not show values when the data is not available', async ({ apiMock }) => {
    const timestamp = Date.now();
    const emptyProfile = getEmptyProfile(timestamp);

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [emptyProfile],
        latestPartitionProfile: [],
        privileges: DEFAULT_PRIVILEGES,
      },
    });

    // Mock empty usage stats - returns 0 for users and queries
    await apiMock.mockGraphQL('getLastMonthUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          aggregations: {
            __typename: 'UsageAggregationMetrics',
            totalSqlQueries: 0,
            uniqueUserCount: 0,
            users: [],
            fields: [],
          },
        },
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: [],
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

    await apiMock.mockGraphQL('getDatasetSchema', {
      dataset: {
        __typename: 'Dataset',
        schemaMetadata: {
          __typename: 'SchemaMetadata',
          fields: [],
        },
        siblingsSearch: {
          __typename: 'SearchResults',
          searchResults: [
            {
              entity: {
                __typename: 'Dataset',
                schemaMetadata: {
                  __typename: 'SchemaMetadata',
                  fields: [],
                },
              },
            },
          ],
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Rows card - should not show value when profile is empty
    // The card shows "No Data" because rowCount is null
    await statsPage.verifyLatestStatsCardDoesNotContain('rows-card', EXPECTED_STATS.ROWS);

    // Columns card - should not show value when fields are empty
    await statsPage.verifyLatestStatsCardDoesNotContain('columns-card', '5');

    // Users card - should show "0" when uniqueUserCount is 0
    await statsPage.verifyLastMonthStatsCard('users-card', '0');

    // Queries card - should show "0" when totalSqlQueries is 0
    await statsPage.verifyLastMonthStatsCard('queries-card', '0');

    // Changes card - should show "0" when totalCreates is 0
    await statsPage.verifyChangesCardValue('0');
  });
});
