/**
 * Column Statistics Tests - Cypress Migration
 *
 * Tests the column statistics table and column-level interactions.
 * Verifies column stats display, field values, and drawer navigation.
 *
 * Uses GraphQL mocked data (no fixtures needed - mocks handle all data)
 * Expected values extracted from mock data, not hardcoded.
 *
 * Test Scope: Column rendering, detailed field values, drawer navigation
 * Migrated from: smoke-test/tests/cypress/cypress/e2e/statsTabV2/columnStats.js
 */

import { test } from '../../fixtures/base-test';
import { StatsTabPage } from '../../pages/stats-v2/stats-tab.page';
import { getSampleProfile, getSchemaMetadata, getExpectedColumnStats } from '../../helpers/stats-mock-helper';

const TEST_DATA = {
  DATASET_URN: 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_stats_test,PROD)',
  COLUMNS: {
    USER_ID: 'user_id',
    USER_NAME: 'user_name',
    EMAIL: 'email',
    CREATED_AT: 'created_at',
    UPDATED_AT: 'updated_at',
    ORDER_COUNT: 'order_count',
    LIFETIME_VALUE: 'lifetime_value',
  },
} as const;

test.use({ featureName: 'stats-v2' });

test.describe('Column Statistics Table', () => {
  let statsPage: StatsTabPage;
  let expectedStats: Record<string, Record<string, string>>;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    expectedStats = getExpectedColumnStats();

    // Feature flag is already enabled by default in test environment - not needed
    // await apiMock.setFeatureFlags({ showStatsTabRedesign: true });

    const timestamp = Date.now();
    const sampleProfile = getSampleProfile(timestamp);
    const schemaMetadata = getSchemaMetadata();

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        __typename: 'Dataset',
        urn: TEST_DATA.DATASET_URN,
        latestFullTableProfile: [sampleProfile],
        latestPartitionProfile: [],
        schemaMetadata,
        privileges: {
          __typename: 'DatasetPrivileges',
          canViewDatasetProfile: true,
          canViewDatasetUsage: true,
          canViewDatasetOperations: true,
          canEditDatasetProperties: true,
        },
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATA.DATASET_URN);
  });

  test('should show column stats in the table', async () => {
    // Verify all 7 columns with field values extracted from mock data
    for (const [, columnName] of Object.entries(TEST_DATA.COLUMNS)) {
      await statsPage.verifyColumnFields(columnName, expectedStats[columnName]);
    }
  });

  test('should open the column drawer by clicking on row', async () => {
    await statsPage.clickColumnRow(TEST_DATA.COLUMNS.USER_ID);
    await statsPage.verifySchemaFieldDrawerOpen();
  });
});
