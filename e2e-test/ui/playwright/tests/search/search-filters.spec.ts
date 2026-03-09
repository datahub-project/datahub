import { test, expect } from '../../fixtures/test-context';

/**
 * Search Filters V2 Tests
 *
 * These tests use shared authentication from auth.setup.ts
 * Tests start already authenticated - no login needed
 */
test.describe('Search Filters V2', () => {
    // Seed test data once before all search tests

  test.beforeEach(async ({ searchPage }) => {
    // Navigate to home page (already authenticated via shared state)
    await searchPage.navigateToHome();
  });

  test('should show search filters v2 by default', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.expectFiltersV2Visible();
    await searchPage.expectFiltersV1NotVisible();
  });

  test('should add and remove multiple filters with no issues', async ({ searchPage }) => {
    await searchPage.searchAndWait('*', 2000);

    // Use SearchPage methods to select filters properly
    // First, select Type=Datasets filter
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.expectActiveFilter('Datasets');

    // Then, select Platform=HDFS filter
    await searchPage.selectFilterOption('Platform', 'HDFS');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.expectActiveFilter('HDFS');

    // Verify both filters are active
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('HDFS');

    // Remove Datasets filter
    await searchPage.removeActiveFilter('Datasets');
    await searchPage.expectUrlNotContains('filter__entityType');
    await searchPage.expectActiveFilterNotVisible('Datasets');

    // Verify HDFS filter is still active
    await searchPage.expectActiveFilter('HDFS');

    // Clear all filters
    await searchPage.clearAllFilters();
    await searchPage.expectUrlNotContains('filter_platform');
    await searchPage.expectActiveFilterNotVisible('HDFS');
  });

  test('should filter by type and verify results', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Datasets');

    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectPaginationVisible();
  });

  test('should filter by platform and verify results', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Platform', 'Hive');

    await searchPage.expectUrlContains('filter_platform');
    await searchPage.expectActiveFilter('Hive');
    await searchPage.expectPaginationVisible();
  });

  test('should handle multiple active filters correctly', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectActiveFilter('Datasets');

    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectActiveFilter('Hive');

    await searchPage.expectActiveFilter('DATASET');

    await searchPage.removeActiveFilter('Datasets');
    await searchPage.expectActiveFilterNotVisible('Datasets');
    await searchPage.expectActiveFilter('Hive');
  });
});
