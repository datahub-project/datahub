/**
 * Global search discovery for Semantic Models and Metrics.
 *
 * Prerequisites: fixtures/data.json (featureName: 'metrics')
 */

import { test, expect } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';
import { MetricsPage } from '../../pages/metrics.page';
import { TIMEOUTS } from '../../utils/constants';
import { METRICS_FEATURE_FLAGS, NAMES, SEMANTIC_MODEL_ORDERS_URN, TOTAL_REVENUE_URN } from './constants';

test.use({ featureName: 'metrics' });

test.describe('Metrics global search', () => {
  let searchPage: SearchPage;
  let metricsPage: MetricsPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(METRICS_FEATURE_FLAGS);
    searchPage = new SearchPage(page, logger, logDir);
    metricsPage = new MetricsPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test('finds semantic model by name and opens Metrics-wrapped detail', async ({ page }) => {
    await searchPage.searchAndWait(NAMES.ORDERS_MODEL, 4000);
    await searchPage.expectHasResults();
    await searchPage.openResultByUrn(SEMANTIC_MODEL_ORDERS_URN);

    await expect(page).toHaveURL(/\/semanticModel\//, { timeout: TIMEOUTS.LONG });
    await metricsPage.expectEntityNamed(NAMES.ORDERS_MODEL);
    await expect(metricsPage.sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('finds metric by name and opens Metrics-wrapped detail', async ({ page }) => {
    await searchPage.searchAndWait(NAMES.TOTAL_REVENUE, 4000);
    await searchPage.expectHasResults();
    await searchPage.openResultByUrn(TOTAL_REVENUE_URN);

    await expect(page).toHaveURL(/\/metric\//, { timeout: TIMEOUTS.LONG });
    await metricsPage.expectEntityNamed(NAMES.TOTAL_REVENUE);
    await expect(metricsPage.sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('filters search results by Semantic Models and Metrics types', async ({ page }) => {
    await searchPage.searchAndWait('PW ', 4000);
    await searchPage.expectHasResults();

    const hasTypeFilter = await searchPage.isFilterAvailable('Type');
    test.skip(!hasTypeFilter, 'Type filter not available in this environment');

    const typeDropdown = page.getByTestId('filter-dropdown-Type');
    await typeDropdown.click();
    const semanticOption = page.getByRole('checkbox', { name: /Semantic Model/i }).first();
    const metricOption = page.getByRole('checkbox', { name: /^Metrics?$/i }).first();

    if (await semanticOption.isVisible().catch(() => false)) {
      await semanticOption.click();
      await searchPage.updateFiltersButton.click();
      await expect(page.getByText(NAMES.ORDERS_MODEL, { exact: false }).first()).toBeVisible({
        timeout: TIMEOUTS.LONG,
      });
    } else if (await metricOption.isVisible().catch(() => false)) {
      await metricOption.click();
      await searchPage.updateFiltersButton.click();
      await expect(page.getByText(NAMES.TOTAL_REVENUE, { exact: false }).first()).toBeVisible({
        timeout: TIMEOUTS.LONG,
      });
    } else {
      throw new Error('Type filter opened but neither Semantic Model nor Metric option was visible');
    }
  });

  test('restores search state after opening a result and navigating back', async ({ page }) => {
    await searchPage.searchAndWait(NAMES.TOTAL_REVENUE, 4000);
    await searchPage.expectHasResults();
    const searchUrl = page.url();

    await searchPage.openResultByUrn(TOTAL_REVENUE_URN);
    await expect(page).toHaveURL(/\/metric\//, { timeout: TIMEOUTS.LONG });

    await page.goBack();
    await expect(page).toHaveURL(searchUrl, { timeout: TIMEOUTS.LONG });
    await expect(page.getByText(NAMES.TOTAL_REVENUE, { exact: false }).first()).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
  });

  test('shows autocomplete suggestions for metrics entities', async ({ page }) => {
    await searchPage.searchInput.fill(NAMES.ORDERS_MODEL);
    // eslint-disable-next-line playwright/no-wait-for-timeout -- debounce for autocomplete
    await page.waitForTimeout(1000);
    await expect(searchPage.autocompleteDropdown).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(page.getByTestId(`autocomplete-item-${SEMANTIC_MODEL_ORDERS_URN}`)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
  });
});
