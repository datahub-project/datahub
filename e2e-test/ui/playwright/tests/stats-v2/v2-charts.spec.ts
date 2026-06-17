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
  getSampleProfile,
  getSampleUsageStats,
  setupChartsData,
  setupChartsDataEmpty,
  setupChartsDataNoPermissions,
} from '../../factories/mock-responses/stats';
import { TEST_DATASET_URN, TIME_RANGES } from './stats-constants';

const TEST_DATA = {
  DATASET_URN: TEST_DATASET_URN,
  TIME_RANGES,
} as const;

test.use({ featureName: 'stats-v2' });

test.describe('Statistics Charts', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
  });

  test('should be available when there are some data', async ({ apiMock }) => {
    const timestamp = Math.floor(Date.now() / 1000) * 1000;
    await setupChartsData(apiMock, timestamp, TEST_DATA.DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // All three main charts should be visible
    await statsPage.verifyRowCountChartIsVisible();
    await statsPage.verifyQueryCountChartIsVisible();
    await statsPage.verifyStorageSizeChartIsVisible();
  });

  test('should be empty when there are no any data', async ({ apiMock }) => {
    await setupChartsDataEmpty(apiMock, TEST_DATA.DATASET_URN);

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // All charts should show empty state
    await statsPage.verifyRowCountChartIsEmpty();
    await statsPage.verifyQueryCountChartIsEmpty();
    await statsPage.verifyStorageSizeChartIsEmpty();
  });

  test('should hide time filter when there are no data', async ({ apiMock }) => {
    await setupChartsDataEmpty(apiMock, TEST_DATA.DATASET_URN);
    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Time range selector should not exist when no data
    await statsPage.verifyTimeRangeSelectorDoesNotExist();
  });

  test('should hide time filter when there is only one option', async ({ apiMock }) => {
    const timestamp = Math.floor(Date.now() / 1000) * 1000;
    const sampleProfile = getSampleProfile(timestamp);
    const sampleUsageStats = getSampleUsageStats(timestamp);

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

    await apiMock.mockGraphQL('getDataProfiles', {
      dataset: {
        __typename: 'Dataset',
        datasetProfiles: [sampleProfile],
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: sampleUsageStats.buckets,
        },
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
            oldestDatasetUsageTime: timestamp,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Time range selector should not exist when only current day has data
    await statsPage.verifyTimeRangeSelectorDoesNotExist();
  });

  test('should show time filter with all options when a year of data available', async ({ apiMock }) => {
    const now = Date.now();
    const oneYearAgo = now - 365 * 24 * 60 * 60 * 1000;
    const timestamp = Math.floor(oneYearAgo / 1000) * 1000;

    const sampleProfile = getSampleProfile(timestamp);
    const sampleUsageStats = getSampleUsageStats(timestamp);

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

    await apiMock.mockGraphQL('getDataProfiles', {
      dataset: {
        __typename: 'Dataset',
        datasetProfiles: [sampleProfile],
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: sampleUsageStats.buckets,
        },
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
            oldestDatasetUsageTime: timestamp,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TEST_DATA.TIME_RANGES.MONTH);

    // All options should be available
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.WEEK);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.MONTH);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.QUARTER);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.HALF_OF_YEAR);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.YEAR);
  });

  test('should show time filter with expected options when more than 6 months of data available', async ({
    apiMock,
  }) => {
    const now = Date.now();
    const sixMonthsAgo = now - (6 * 30 + 7) * 24 * 60 * 60 * 1000;
    const timestamp = Math.floor(sixMonthsAgo / 1000) * 1000;

    const sampleProfile = getSampleProfile(timestamp);
    const sampleUsageStats = getSampleUsageStats(timestamp);

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

    await apiMock.mockGraphQL('getDataProfiles', {
      dataset: {
        __typename: 'Dataset',
        datasetProfiles: [sampleProfile],
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: sampleUsageStats.buckets,
        },
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
            oldestDatasetUsageTime: timestamp,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    await statsPage.verifyTimeRangeSelectorExists();
    await statsPage.verifyTimeRangeSelected(TEST_DATA.TIME_RANGES.MONTH);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.WEEK);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.MONTH);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.QUARTER);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.HALF_OF_YEAR);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.YEAR);
  });

  test('should show time filter with expected options when more than 3 months of data available', async ({
    apiMock,
  }) => {
    const now = Date.now();
    const threeMonthsAgo = now - 90 * 24 * 60 * 60 * 1000;
    const timestamp = Math.floor(threeMonthsAgo / 1000) * 1000;

    const sampleProfile = getSampleProfile(timestamp);
    const sampleUsageStats = getSampleUsageStats(timestamp);

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

    await apiMock.mockGraphQL('getDataProfiles', {
      dataset: {
        __typename: 'Dataset',
        datasetProfiles: [sampleProfile],
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: sampleUsageStats.buckets,
        },
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
            oldestDatasetUsageTime: timestamp,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TEST_DATA.TIME_RANGES.MONTH);

    // Available options
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.WEEK);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.MONTH);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.QUARTER);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.HALF_OF_YEAR);

    // Year should not be available
    await statsPage.verifyTimeRangeOptionDoesNotExist(TEST_DATA.TIME_RANGES.YEAR);
  });

  test('should show time filter with expected options when more than 1 month of data available', async ({
    apiMock,
  }) => {
    const now = Date.now();
    const oneMonthAndOneWeekAgo = now - (30 + 7) * 24 * 60 * 60 * 1000;
    const timestamp = Math.floor(oneMonthAndOneWeekAgo / 1000) * 1000;

    const sampleProfile = getSampleProfile(timestamp);
    const sampleUsageStats = getSampleUsageStats(timestamp);

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

    await apiMock.mockGraphQL('getDataProfiles', {
      dataset: {
        __typename: 'Dataset',
        datasetProfiles: [sampleProfile],
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: sampleUsageStats.buckets,
        },
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
            oldestDatasetUsageTime: timestamp,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TEST_DATA.TIME_RANGES.MONTH);

    // Available options
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.WEEK);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.MONTH);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.QUARTER);

    // Half year and year should not be available
    await statsPage.verifyTimeRangeOptionDoesNotExist(TEST_DATA.TIME_RANGES.HALF_OF_YEAR);
    await statsPage.verifyTimeRangeOptionDoesNotExist(TEST_DATA.TIME_RANGES.YEAR);
  });

  test('should show time filter with expected options when more than 1 week of data available', async ({ apiMock }) => {
    const now = Date.now();
    const oneWeekAndOneDayAgo = now - (7 + 1) * 24 * 60 * 60 * 1000;
    const timestamp = Math.floor(oneWeekAndOneDayAgo / 1000) * 1000;

    const sampleProfile = getSampleProfile(timestamp);
    const sampleUsageStats = getSampleUsageStats(timestamp);

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

    await apiMock.mockGraphQL('getDataProfiles', {
      dataset: {
        __typename: 'Dataset',
        datasetProfiles: [sampleProfile],
      },
    });

    await apiMock.mockGraphQL('getTimeRangeUsageAggregations', {
      dataset: {
        __typename: 'Dataset',
        usageStats: {
          __typename: 'UsageAggregation',
          buckets: sampleUsageStats.buckets,
        },
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
            oldestDatasetUsageTime: timestamp,
          },
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // Selector should exist
    await statsPage.verifyTimeRangeSelectorExists();

    // Default selection should be MONTH
    await statsPage.verifyTimeRangeSelected(TEST_DATA.TIME_RANGES.MONTH);

    // Only week and month should be available
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.WEEK);
    await statsPage.verifyTimeRangeOptionExists(TEST_DATA.TIME_RANGES.MONTH);

    // Quarter, half year, and year should not be available
    await statsPage.verifyTimeRangeOptionDoesNotExist(TEST_DATA.TIME_RANGES.QUARTER);
    await statsPage.verifyTimeRangeOptionDoesNotExist(TEST_DATA.TIME_RANGES.HALF_OF_YEAR);
    await statsPage.verifyTimeRangeOptionDoesNotExist(TEST_DATA.TIME_RANGES.YEAR);
  });

  test('should not be available when user has no permissions', async ({ apiMock }) => {
    await setupChartsDataNoPermissions(apiMock, TEST_DATA.DATASET_URN);
    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    // All charts should show no permissions message
    await statsPage.verifyNoPermissionsForChart('row-count-card');
    await statsPage.verifyNoPermissionsForChart('query-count-card');
    await statsPage.verifyNoPermissionsForChart('storage-size-card');
  });
});
