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
import { getFullDatasetMock, getExpectedColumnStats, getSchemaMetadata } from '../../factories/mock-responses/stats';
import { TEST_DATASET_URN, COLUMN_NAMES } from '../../pages/stats-v2/stats.constants';

test.use({ featureName: 'stats-v2' });

test.describe('Column Statistics Table', () => {
  let statsPage: StatsTabPage;
  let expectedStats: Record<string, Record<string, string>>;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    statsPage = new StatsTabPage(page, logger, logDir);
    expectedStats = getExpectedColumnStats();

    const timestamp = Date.now();
    const schema = getSchemaMetadata();

    await apiMock.mockGraphQL('getDataset', {
      dataset: {
        ...getFullDatasetMock(TEST_DATASET_URN, timestamp),
        schemaMetadata: schema,
      },
    });

    await statsPage.navigateToDatasetStats(TEST_DATASET_URN);
  });

  test('should show column stats in the table', async () => {
    // Verify all 7 columns with field values extracted from mock data
    for (const [, columnName] of Object.entries(COLUMN_NAMES)) {
      await statsPage.verifyColumnFields(columnName, expectedStats[columnName]);
    }
  });

  test('should open the column drawer by clicking on row', async () => {
    await statsPage.clickColumnRow(COLUMN_NAMES.USER_ID);
    await statsPage.verifySchemaFieldDrawerOpen();
  });
});
