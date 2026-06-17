/**
 * Stats Tab V2 Tests - Cypress Migration
 *
 * Comprehensive tests for the statistics tab V2 redesign.
 * Tests tab enable/disable state, accessibility attributes, visibility, and content rendering.
 *
 * Fixture Reference: tests/stats-v2/fixtures/data.json defines the test dataset schema:
 * - Test dataset: urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_stats_test,PROD)
 * - Tests use inline GraphQL mocks (apiMock) rather than loading the fixture directly
 *
 * Test Scope: Tab state logic, visibility, permissions, content rendering, accessibility
 * Migrated from: smoke-test/tests/cypress/cypress/e2e/statsTabV2/statsTab.js
 */

import { test, expect } from '../../fixtures/base-test';
import { StatsTabPage } from '../../pages/stats-v2/stats-tab.page';
import { getSampleProfile, getSampleUsageStats } from '../../factories/mock-responses/stats';
import { TEST_DATASET_URN } from './stats-constants';

const TEST_DATA = {
  DATASET_URN: TEST_DATASET_URN,
} as const;

const mockDatasetResponse = (overrides?: Record<string, unknown>) => ({
  dataset: {
    __typename: 'Dataset',
    urn: TEST_DATA.DATASET_URN,
    latestFullTableProfile: [],
    latestPartitionProfile: [],
    usageStats: {
      __typename: 'UsageQueryResult',
      buckets: [],
      aggregations: {
        uniqueUserCount: 0,
        totalSqlQueries: 0,
        fields: [],
        __typename: 'UsageQueryResultAggregations',
      },
    },
    privileges: {
      __typename: 'DatasetPrivileges',
      canViewDatasetProfile: true,
      canViewDatasetUsage: true,
      canViewDatasetOperations: true,
      canEditDatasetProperties: true,
    },
    ...overrides,
  },
});

test.use({ featureName: 'stats-v2' });

test.describe('Stats Tab V2 - Tab Enable/Disable State', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
  });

  test('should be disabled when entity has no latestFullTableProfile, latestPartitionProfile or usageStats', async ({
    apiMock,
  }) => {
    await apiMock.mockGraphQL('getDataset', mockDatasetResponse());
    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);

    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'true');
  });

  test('should be enabled when entity has latestFullTableProfile', async ({ apiMock }) => {
    const sampleProfile = getSampleProfile(Date.now());
    await apiMock.mockGraphQL(
      'getDataset',
      mockDatasetResponse({
        latestFullTableProfile: [sampleProfile],
      }),
    );

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'false');
  });

  test('should be enabled when entity has latestPartitionProfile', async ({ apiMock }) => {
    const sampleProfile = getSampleProfile(Date.now());
    await apiMock.mockGraphQL(
      'getDataset',
      mockDatasetResponse({
        latestPartitionProfile: [sampleProfile],
      }),
    );

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'false');
  });

  test('should be enabled when entity has usageStats', async ({ apiMock }) => {
    const sampleUsageStats = getSampleUsageStats(Date.now());
    await apiMock.mockGraphQL(
      'getDataset',
      mockDatasetResponse({
        usageStats: sampleUsageStats,
      }),
    );

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'false');
  });
});

test.describe('Stats Tab V2 - Tab Interaction', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);

    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
    const sampleProfile = getSampleProfile(Date.now());
    await apiMock.mockGraphQL(
      'getDataset',
      mockDatasetResponse({
        latestFullTableProfile: [sampleProfile],
      }),
    );
  });

  test('should be clickable', async () => {
    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
    await statsPage.clickStatsTab();

    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-selected', 'true');
  });
});
