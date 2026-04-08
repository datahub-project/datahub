/**
 * Search Filters V2 tests.
 *
 * Uses base-test — authenticated context via loginFixture, no login step
 * needed. SearchPage constructed in beforeEach with logger.
 */

import { test, expect } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search-page';

test.describe('Search Filters V2', () => {
  let searchPage: SearchPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test('should show search filters v2 by default', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.expectFiltersV2Visible();
    await searchPage.expectFiltersV1NotVisible();
  });

  test('should add and remove multiple filters with no issues', async () => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.expectActiveFilter('Datasets');

    await searchPage.selectFilterOption('Platform', 'HDFS');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.expectActiveFilter('HDFS');

    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('HDFS');

    await searchPage.removeActiveFilter('Datasets');
    await searchPage.expectUrlNotContains('filter__entityType');
    await searchPage.expectActiveFilterNotVisible('Datasets');

    await searchPage.expectActiveFilter('HDFS');

    await searchPage.clearAllFilters();
    await searchPage.expectUrlNotContains('filter_platform');
    await searchPage.expectActiveFilterNotVisible('HDFS');
  });

  test('should filter by type and verify results', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectPaginationVisible();
  });

  test('should filter by platform and verify results', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.expectActiveFilter('Hive');
    await searchPage.expectPaginationVisible();
  });

  test('should handle multiple active filters correctly', async () => {
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
