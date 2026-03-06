import { test, expect } from '../../fixtures/test-context';

/**
 * Search Filters V2 Tests
 *
 * These tests use shared authentication from auth.setup.ts
 * Tests start already authenticated - no login needed
 */
test.describe('Search Filters V2', () => {
  test.beforeEach(async ({ searchPage }) => {
    // Navigate to home page (already authenticated via shared state)
    await searchPage.navigateToHome();
  });

  test('should show search filters v2 by default', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.expectFiltersV2Visible();
    await searchPage.expectFiltersV1NotVisible();
  });

  test('should add and remove multiple filters with no issues', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.moreFiltersDropdown.click();

    const taggedWithFilter = page.locator('[data-testid="filter-dropdown-Tagged-With"], [data-testid="more-filter-Tagged-With"]').first();
    await taggedWithFilter.click();

    await searchPage.searchInModal('Playwright');

    const PlaywrightOption = page.locator('[data-testid="filter-option-Playwright"]');
    await PlaywrightOption.click({ force: true });

    await searchPage.updateFiltersButton.click({ force: true });

    await searchPage.expectUrlContains('filter_tags___false___EQUAL___0=urn%3Ali%3Atag%3APlaywright');

    const typeFilter = page.locator('[data-testid="filter-dropdown-Type"]');
    await typeFilter.click({ force: true });

    const datasetsOption = page.locator('[data-testid="filter-option-Datasets"]');
    await datasetsOption.click({ force: true });

    await searchPage.updateFiltersButton.click({ force: true });

    await searchPage.expectUrlContains('filter__entityType');

    await searchPage.expectTextVisible('SamplePlaywrightHdfsDataset');

    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('Playwright');

    await searchPage.removeActiveFilter('Datasets');

    await searchPage.expectUrlNotContains('filter__entityType');
    await searchPage.expectActiveFilterNotVisible('Datasets');

    await searchPage.clearAllFilters();

    await searchPage.expectUrlNotContains('filter_tags___false___EQUAL___0=urn%3Ali%3Atag%3APlaywright');
    await searchPage.expectActiveFilterNotVisible('Playwright');
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

    await searchPage.expectActiveFilter('Datasets');

    await searchPage.removeActiveFilter('Datasets');
    await searchPage.expectActiveFilterNotVisible('Datasets');
    await searchPage.expectActiveFilter('Hive');
  });
});
