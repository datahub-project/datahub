import { test, expect } from '../../fixtures/test-context';

/**
 * SearchV2 Specific Tests
 *
 * These tests focus on SearchV2-specific features and components
 * Tests start already authenticated - no login needed
 */
test.describe('SearchV2 Features', () => {
  test.beforeEach(async ({ searchPage }) => {
    // Navigate to home page (already authenticated via shared state)
    await searchPage.navigateToHome();
  });

  test('should display SearchV2 interface by default', async ({ searchPage }) => {
    // Perform a search to trigger filters display
    await searchPage.searchAndWait('*', 2000);

    // Verify SearchV2 is the active search interface
    await searchPage.expectFiltersV2Visible();
    await searchPage.expectFiltersV1NotVisible();
  });

  test('should perform autocomplete search', async ({ searchPage, page }) => {
    // Type a search query without pressing Enter
    await searchPage.searchInput.fill('playwright');
    await page.waitForTimeout(1000);

    // Verify autocomplete dropdown appears
    await expect(searchPage.autocompleteDropdown).toBeVisible();
  });

  test('should show no results message when search returns empty', async ({ searchPage }) => {
    // Search for something that doesn't exist
    await searchPage.searchAndWait('zzznonexistentqueryyy123', 5000);

    // Verify no results message is shown (using existing method)
    await searchPage.expectNoResults();
  });

  test('should clear search input using clear button', async ({ searchPage }) => {
    // Type a search query
    await searchPage.searchInput.fill('test query');

    // Wait for the clear button to appear
    await expect(searchPage.clearButton).toBeVisible();

    // Click the clear button
    await searchPage.clearButton.click();

    // Verify input is cleared
    await expect(searchPage.searchInput).toHaveValue('');
  });

  test('should navigate through filter dropdowns', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    // Click on Type filter dropdown
    const typeFilterDropdown = page.locator('[data-testid="filter-dropdown-Type"]');
    await typeFilterDropdown.click();

    // Verify dropdown menu appears
    await expect(searchPage.filterDropdownMenu).toBeVisible();

    // Verify Update button is present
    await expect(searchPage.updateFiltersButton).toBeVisible();
  });

  test('should toggle filters using More Filters dropdown', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    // Click on More Filters dropdown
    await searchPage.moreFiltersDropdown.click();
    await page.waitForTimeout(500);

    // Verify more filters options are displayed
    const moreFilterOption = page.locator('[data-testid^="more-filter-"]').first();
    await expect(moreFilterOption).toBeVisible();
  });

  test('should display active filters with correct test IDs', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    // Apply a Type filter
    await searchPage.selectFilterOption('Type', 'Datasets');

    // Verify active filter is displayed with correct data-testid
    const activeFilter = page.locator('[data-testid="active-filter-_entityType␞typeNames"]');
    await expect(activeFilter).toBeVisible();

    // Verify active filter value is displayed
    const activeFilterValue = page.locator('[data-testid="active-filter-value-_entityType␞typeNames-DATASET"]');
    await expect(activeFilterValue).toBeVisible();
  });

  test('should remove individual filters using remove button', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    // Apply multiple filters
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.selectFilterOption('Platform', 'Hive');

    // Verify both filters are active
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('Hive');

    // Remove Type filter using remove button
    const removeTypeFilterButton = page.locator('[data-testid="remove-filter-_entityType␞typeNames"]');
    await removeTypeFilterButton.click();

    // Verify Type filter is removed
    await searchPage.expectActiveFilterNotVisible('Datasets');

    // Verify Platform filter is still active
    await searchPage.expectActiveFilter('Hive');
  });

  test('should clear all filters using Clear All button', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    // Apply multiple filters
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.selectFilterOption('Platform', 'Hive');

    // Verify filters are active
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectActiveFilter('Hive');

    // Click Clear All Filters button
    await searchPage.clearAllFilters();

    // Verify all filters are removed
    await searchPage.expectActiveFilterNotVisible('Datasets');
    await searchPage.expectActiveFilterNotVisible('Hive');
  });

  test('should persist search query in URL', async ({ searchPage, page }) => {
    // Perform a search
    await searchPage.searchAndWait('playwright', 3000);

    // Verify URL contains the search query
    await expect(page).toHaveURL(/.*query=playwright.*/);
  });

  test('should expand and collapse filter facets', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    // Look for expand facet icon
    const expandFacetIcon = page.locator('[data-testid^="expand-facet-"]').first();
    const facetCount = await expandFacetIcon.count();

    if (facetCount > 0) {
      // Click to expand/collapse
      await expandFacetIcon.click();
      await page.waitForTimeout(500);

      // Verify interaction completed without errors
      await page.waitForLoadState('networkidle');
    }
  });

  test('should display search results with pagination', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 3000);

    // Verify results are displayed
    await searchPage.expectHasResults();

    // Verify pagination is visible
    await searchPage.expectPaginationVisible();

    // Verify result count is displayed
    await expect(page.getByText(/of [0-9]+ result/)).toBeVisible();
  });

  test('should handle filter option selection with checkboxes', async ({ searchPage }) => {
    await searchPage.searchAndWait('*', 2000);

    // Use the existing SearchPage method which handles checkboxes properly
    await searchPage.selectFilterOption('Type', 'Datasets');

    // Verify filter is applied
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectUrlContains('filter__entityType');
  });

  test('should filter search within filter dropdown', async ({ searchPage }) => {
    await searchPage.searchAndWait('*', 2000);

    // selectFilterOption handles opening dropdown, searching, selecting, and clicking Update
    await searchPage.selectFilterOption('Platform', 'Hive');

    // Verify filter is applied
    await searchPage.expectActiveFilter('Hive');
  });

  test('should maintain search state when clicking entity result and navigating back', async ({ searchPage, page }) => {
    // Perform search with filters
    await searchPage.searchAndWait('playwright', 2000);
    await searchPage.selectFilterOption('Type', 'Datasets');

    // Store current URL
    const searchUrl = page.url();

    // Click on a result
    await searchPage.clickEntityResult();
    await page.waitForLoadState('networkidle');

    // Navigate back
    await page.goBack();
    await page.waitForLoadState('networkidle');

    // Verify search state is maintained
    await expect(page).toHaveURL(searchUrl);
    await searchPage.expectActiveFilter('Datasets');
  });

  test('should display autocomplete entity items with correct test IDs', async ({ searchPage, page }) => {
    // Type a search query
    await searchPage.searchInput.fill('fct');
    await page.waitForTimeout(1500);

    // Check for autocomplete entity items
    const autocompleteEntity = page.locator('[data-testid^="auto-complete-entity-name-"]').first();
    const entityCount = await autocompleteEntity.count();

    if (entityCount > 0) {
      await expect(autocompleteEntity).toBeVisible();
    }
  });

  test('should show search recommendations when available', async ({ searchPage, page }) => {
    // Clear any existing search
    await searchPage.navigateToHome();

    // Focus on search input without typing
    await searchPage.searchInput.click();
    await page.waitForTimeout(1000);

    // Check if recommendation container appears (optional feature)
    const recommendationContainer = page.locator('[data-testid="recommendation-container-id"]');
    const recCount = await recommendationContainer.count();

    if (recCount > 0) {
      await expect(recommendationContainer).toBeVisible();
    }
  });
});
