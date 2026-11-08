/**
 * SearchV2 specific tests.
 *
 * Uses base-test — authenticated context via loginFixture, no login step
 * needed. SearchPage constructed in beforeEach with logger.
 */

import { test, expect } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';
import { GLOBAL_FEATURE_FLAGS } from '../../utils/test-feature-flags';

test.use({ featureName: 'search' });

test.describe('SearchV2 Features', () => {
  let searchPage: SearchPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(GLOBAL_FEATURE_FLAGS);
    searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test('should display SearchV2 interface by default', async () => {
    await searchPage.search('*');
    await searchPage.expectFiltersV2Visible();
    await searchPage.expectFiltersV1NotVisible();
  });

  test('should perform autocomplete search', async () => {
    await searchPage.searchInput.fill('playwright');
    await expect(searchPage.autocompleteDropdown).toBeVisible();
  });

  test('should show no results message when search returns empty', async () => {
    await searchPage.search('zzznonexistentqueryyy123');
    await searchPage.expectNoResults();
  });

  test('should clear search input using clear button', async () => {
    // Click to focus first — the clear button only renders when the input is focused
    await searchPage.searchInput.click();
    await searchPage.searchInput.fill('test query');
    await expect(searchPage.clearButton).toBeVisible({ timeout: 5000 });
    await searchPage.clearButton.click();
    await expect(searchPage.searchInput).toHaveValue('');
  });

  test('should navigate through filter dropdowns', async ({ page }) => {
    await searchPage.search('*');
    const typeFilterDropdown = page.getByTestId('filter-dropdown-Type');
    await typeFilterDropdown.click();
    await expect(searchPage.filterDropdownMenu).toBeVisible();
    await expect(searchPage.updateFiltersButton).toBeVisible();
  });

  test('should toggle filters using More Filters dropdown', async () => {
    await searchPage.search('*');
    await expect(searchPage.moreFiltersDropdown).toBeVisible({ timeout: 10000 });
    // Fails unless at least one more-filter option becomes visible
    await searchPage.openMoreFilters();
  });

  test('should display active filters with correct test IDs', async ({ page }) => {
    await searchPage.search('*');
    await searchPage.selectFilterOption('Type', 'Datasets');
    const activeFilter = page.getByTestId('active-filter-_entityType␞typeNames');
    await expect(activeFilter).toBeVisible();
    const activeFilterValue = page.getByTestId('active-filter-value-_entityType␞typeNames-DATASET');
    await expect(activeFilterValue).toBeVisible();
  });

  test('should remove individual filters using remove button', async ({ page }) => {
    await searchPage.search('*');
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('Hive');

    const removeTypeFilterButton = page.getByTestId('remove-filter-_entityType␞typeNames');
    await removeTypeFilterButton.click();
    await searchPage.expectActiveFilterNotVisible('Datasets');
    await searchPage.expectActiveFilter('Hive');
  });

  test('should clear all filters using Clear All button', async () => {
    await searchPage.search('*');
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('Hive');
    await searchPage.clearAllFilters();
    await searchPage.expectActiveFilterNotVisible('Datasets');
    await searchPage.expectActiveFilterNotVisible('Hive');
  });

  test('should persist search query in URL', async ({ page }) => {
    await searchPage.search('playwright');
    await expect(page).toHaveURL(/.*query=playwright.*/);
  });

  test('should expand and collapse filter facets', async ({ page }) => {
    await searchPage.search('*');
    // eslint-disable-next-line playwright/no-raw-locators -- data-testid prefix selector (^=); getByTestId requires exact match
    const expandFacetIcon = page.locator('[data-testid^="expand-facet-"]').first();
    const facetCount = await expandFacetIcon.count();
    if (facetCount > 0) {
      await expandFacetIcon.click();
      await page.waitForLoadState('networkidle');
    }
  });

  test('should display search results with pagination', async ({ page }) => {
    await searchPage.search('*');
    await searchPage.expectHasResults();
    await searchPage.expectPaginationVisible();
    // The total is locale-formatted once it passes 999 ("of 5,146 results"), and may carry a "+" when capped.
    await expect(page.getByText(/of [0-9][0-9,.]*\+? result/)).toBeVisible();
  });

  test('should handle filter option selection with checkboxes', async () => {
    await searchPage.search('*');
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectUrlContains('filter__entityType');
  });

  test('should filter search within filter dropdown', async () => {
    await searchPage.search('*');
    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectActiveFilter('Hive');
  });

  test('should maintain search state when clicking entity result and navigating back', async ({ page }) => {
    await searchPage.search('playwright');
    await searchPage.selectFilterOption('Type', 'Datasets');
    const searchUrl = page.url();
    await searchPage.clickEntityResult();
    await page.waitForLoadState('networkidle');
    await page.goBack();
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(searchUrl);
    await searchPage.expectActiveFilter('Datasets');
  });
});
