/**
 * Search Filters — Within operator for hierarchical Domain filters.
 *
 * Seeds nested domains + a dataset only in the child domain. Selecting the
 * parent Domain with Equals should miss it; Within (DESCENDANTS_INCL) should
 * include it and round-trip via the URL.
 */

import { expect, test } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';

test.use({ featureName: 'search' });

const PARENT_DOMAIN = 'PlaywrightWithinParent';
const DATASET_NAME = 'fct_playwright_within_nested';

test.describe('Search Filters — Within operator', () => {
  let searchPage: SearchPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test('Domain Within uses DESCENDANTS_INCL and matches nested child entities', async ({ page }) => {
    await searchPage.searchAndWait(DATASET_NAME, 3000);
    await searchPage.expectFiltersV2Visible();

    // Dataset is only in the child domain — searching by name alone should find it.
    await searchPage.expectTextVisible(DATASET_NAME);

    await searchPage.selectFilterOption('Domain', PARENT_DOMAIN);
    await searchPage.expectActiveFilter(PARENT_DOMAIN);

    // Domain filters default to Within (DESCENDANTS_INCL), which includes nested children.
    await searchPage.expectActiveFilterOperator('domains', 'within');
    await searchPage.expectUrlContains('DESCENDANTS_INCL');
    await searchPage.expectTextVisible(DATASET_NAME);
    await searchPage.expectHasResults();

    // Switching to Equals on the parent should miss the child-only dataset.
    await searchPage.selectActiveFilterOperator('domains', 'equals');
    await searchPage.expectActiveFilterOperator('domains', 'equals');
    await expect(page.getByText('of 0 results')).toBeVisible({ timeout: 15000 });

    // Reload preserves Within via the URL condition.
    await searchPage.selectActiveFilterOperator('domains', 'within');
    await page.reload();
    await page.waitForLoadState('networkidle');
    await searchPage.dismissOnboardingOverlays();
    await searchPage.expectActiveFilterOperator('domains', 'within');
    await searchPage.expectUrlContains('DESCENDANTS_INCL');
    await searchPage.expectTextVisible(DATASET_NAME);
  });
});
