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
import {
  getSampleProfile,
  getSampleUsageStats,
  getMinimalDatasetMock,
  getDatasetMockWithProfile,
} from '../../factories/mock-responses/stats';
import { TEST_DATASET_URN } from '../../pages/stats-v2/stats.constants';

test.use({ featureName: 'stats-v2' });

test.describe('Stats Tab V2 - Tab Enable/Disable State', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
  });

  test('should be disabled when entity has no latestFullTableProfile, latestPartitionProfile or usageStats', async ({
    apiMock,
  }) => {
    await apiMock.mockGraphQL('getDataset', { dataset: getMinimalDatasetMock(TEST_DATASET_URN) });
    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);

    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'true');
  });

  test('should be enabled when entity has latestFullTableProfile', async ({ apiMock }) => {
    const timestamp = Date.now();
    await apiMock.mockGraphQL('getDataset', {
      dataset: getDatasetMockWithProfile(TEST_DATASET_URN, timestamp),
    });

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);
    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'false');
  });

  test('should be enabled when entity has latestPartitionProfile', async ({ apiMock }) => {
    const timestamp = Date.now();
    const profile = getSampleProfile(timestamp);
    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        ...getMinimalDatasetMock(TEST_DATASET_URN),
        latestPartitionProfile: [profile],
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);
    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'false');
  });

  test('should be enabled when entity has usageStats', async ({ apiMock }) => {
    const timestamp = Date.now();
    const usageStats = getSampleUsageStats(timestamp);
    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        ...getMinimalDatasetMock(TEST_DATASET_URN),
        usageStats,
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);
    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-disabled', 'false');
  });
});

test.describe('Stats Tab V2 - Tab Interaction', () => {
  let statsPage: StatsTabPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);

    await apiMock.setFeatureFlags({ showStatsTabRedesign: true });
    const timestamp = Date.now();
    await apiMock.mockGraphQL('getDataset', {
      dataset: getDatasetMockWithProfile(TEST_DATASET_URN, timestamp),
    });
  });

  test('should be clickable', async () => {
    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);
    await statsPage.clickStatsTab();

    await expect(statsPage.getStatsTabElement()).toHaveAttribute('aria-selected', 'true');
  });
});
