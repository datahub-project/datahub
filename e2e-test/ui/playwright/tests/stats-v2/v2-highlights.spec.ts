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
import { getSampleProfile, getEmptyProfile } from '../../helpers/stats-mock-helper';
import type { ApiMocker } from '../../fixtures/mocking.fixture';

const TEST_DATA = {
  DATASET_URN: 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_stats_test,PROD)',
} as const;

test.use({ featureName: 'stats-v2' });

// Helper to set up mocks inline in each test
async function setupDataWithStats(apiMock: ApiMocker) {
  const timestamp = Date.now();
  const sampleProfile = getSampleProfile(timestamp);

  // Mock getDataset with profile to enable stats tab
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
        canViewDatasetOperations: true,
        canEditDatasetProperties: true,
      },
    },
  });

  // Mock detail endpoints with actual data
  await apiMock.mockGraphQL('getDataProfiles', {
    dataset: {
      __typename: 'Dataset',
      datasetProfiles: [sampleProfile],
    },
  });

  await apiMock.mockGraphQL('getDataProfiles', {
    dataset: {
      __typename: 'Dataset',
      datasetProfiles: [sampleProfile],
    },
  });

  await apiMock.mockGraphQL('getDatasetTimeseriesCapability', {
    dataset: {
      __typename: 'Dataset',
      timeseriesCapabilities: {
        __typename: 'TimeseriesCapabilities',
        assetStats: {
          __typename: 'AssetStats',
          oldestDatasetProfileTime: timestamp,
        },
      },
    },
  });

  await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: {
        __typename: 'UsageAggregation',
        buckets: [
          {
            bucket: timestamp,
            metrics: {
              __typename: 'UsageAggregationMetrics',
              totalSqlQueries: 25,
            },
            __typename: 'UsageAggregation',
          },
        ],
      },
    },
  });

  await apiMock.mockGraphQL('getLastMonthUsageAggregations', {
    dataset: {
      __typename: 'Dataset',
      usageStats: {
        __typename: 'UsageAggregation',
        aggregations: {
          __typename: 'UsageAggregationMetrics',
          totalSqlQueries: 25,
          uniqueUserCount: 5,
          users: [
            {
              count: 1,
              userEmail: 'user1@example.com',
              user: {
                __typename: 'CorpUser',
                urn: 'urn:li:corpuser:user1',
                username: 'user1',
                type: 'CORP_USER',
                properties: {
                  __typename: 'CorpUserProperties',
                  displayName: 'User One',
                  firstName: 'User',
                  lastName: 'One',
                  fullName: 'User One',
                },
                editableProperties: {
                  __typename: 'CorpUserEditableProperties',
                  displayName: 'User One',
                  pictureLink: '',
                },
              },
              __typename: 'UsageAggregationUser',
            },
            {
              count: 1,
              userEmail: 'user2@example.com',
              user: {
                __typename: 'CorpUser',
                urn: 'urn:li:corpuser:user2',
                username: 'user2',
                type: 'CORP_USER',
                properties: {
                  __typename: 'CorpUserProperties',
                  displayName: 'User Two',
                  firstName: 'User',
                  lastName: 'Two',
                  fullName: 'User Two',
                },
                editableProperties: {
                  __typename: 'CorpUserEditableProperties',
                  displayName: 'User Two',
                  pictureLink: '',
                },
              },
              __typename: 'UsageAggregationUser',
            },
            {
              count: 1,
              userEmail: 'user3@example.com',
              user: {
                __typename: 'CorpUser',
                urn: 'urn:li:corpuser:user3',
                username: 'user3',
                type: 'CORP_USER',
                properties: {
                  __typename: 'CorpUserProperties',
                  displayName: 'User Three',
                  firstName: 'User',
                  lastName: 'Three',
                  fullName: 'User Three',
                },
                editableProperties: {
                  __typename: 'CorpUserEditableProperties',
                  displayName: 'User Three',
                  pictureLink: '',
                },
              },
              __typename: 'UsageAggregationUser',
            },
            {
              count: 1,
              userEmail: 'user4@example.com',
              user: {
                __typename: 'CorpUser',
                urn: 'urn:li:corpuser:user4',
                username: 'user4',
                type: 'CORP_USER',
                properties: {
                  __typename: 'CorpUserProperties',
                  displayName: 'User Four',
                  firstName: 'User',
                  lastName: 'Four',
                  fullName: 'User Four',
                },
                editableProperties: {
                  __typename: 'CorpUserEditableProperties',
                  displayName: 'User Four',
                  pictureLink: '',
                },
              },
              __typename: 'UsageAggregationUser',
            },
            {
              count: 1,
              userEmail: 'user5@example.com',
              user: {
                __typename: 'CorpUser',
                urn: 'urn:li:corpuser:user5',
                username: 'user5',
                type: 'CORP_USER',
                properties: {
                  __typename: 'CorpUserProperties',
                  displayName: 'User Five',
                  firstName: 'User',
                  lastName: 'Five',
                  fullName: 'User Five',
                },
                editableProperties: {
                  __typename: 'CorpUserEditableProperties',
                  displayName: 'User Five',
                  pictureLink: '',
                },
              },
              __typename: 'UsageAggregationUser',
            },
          ],
          fields: [],
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
}

test.describe('Highlight Stats Cards', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
  });

  test('should show values when the data is available', async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
    await setupDataWithStats(apiMock);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Latest stats (Rows, Columns)
    await statsPage.verifyLatestStatsVisible();
    await statsPage.verifyLatestStatsCard('rows-card', '100');
    await statsPage.verifyLatestStatsCardButtonVisible('rows-card');
    await statsPage.verifyLatestStatsCard('columns-card', '7');
    await statsPage.verifyLatestStatsCardButtonVisible('columns-card');

    // Last month stats (Users, Queries)
    await statsPage.verifyLastMonthStatsVisible();
    await statsPage.verifyLastMonthStatsCard('users-card', '5');
    await statsPage.verifyLastMonthStatsCardButtonVisible('users-card');
    await statsPage.verifyLastMonthStatsCard('queries-card', '25');
    await statsPage.verifyLastMonthStatsCardButtonVisible('queries-card');

    // Changes card
    await statsPage.verifyChangesCardVisible();
    await statsPage.verifyChangesCardValue('1');
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
        privileges: {
          __typename: 'DatasetPrivileges',
          canViewDatasetProfile: true,
          canViewDatasetUsage: true,
          canViewDatasetOperations: true,
          canEditDatasetProperties: true,
        },
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
    await statsPage.verifyLatestStatsCardDoesNotContain('rows-card', '100');

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
