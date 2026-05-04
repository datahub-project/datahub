/**
 * Search Functionality tests.
 *
 * Uses base-test — authenticated context via loginFixture, no login step
 * needed. SearchPage constructed in beforeEach with logger.
 */

import { test, expect } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';

test.use({ featureName: 'search' });

test.describe('Search Functionality', () => {
  let searchPage: SearchPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test('should search all entities and see results', async () => {
    await searchPage.searchAndWait('*', 5000);
    await searchPage.expectHasResults();
  });

  test('should search with impossible query and find 0 results', async () => {
    await searchPage.searchAndWait('zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq', 5000);
    await searchPage.expectNoResults();
  });

  test('should search, find a result, and visit the dataset page', async ({ page }) => {
    test.setTimeout(45000);
    await searchPage.searchAndWait('fct_playwright_users_created', 5000);

    // Wait for results — seeder fixture guarantees data is present.
    await expect(page.getByText(/of [0-9]+ result/)).toBeVisible({ timeout: 30000 });

    await searchPage.clickResult('fct_playwright_users_created');

    await page.waitForURL(/.*dataset.*fct_playwright_users_created.*/);
    await page.waitForLoadState('networkidle');

    // The V2 entity page shows the platform as an icon (not text), so we verify
    // the entity type and schema fields that are rendered as visible text.
    await searchPage.expectTextVisible('Dataset');
    await searchPage.expectTextVisible('fct_playwright_users_created');
    await searchPage.expectTextVisible('user_id');
    await searchPage.expectTextVisible('Id of the user');
  });

  test('should filter by glossary term', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Glossary-Term', 'PlaywrightTerm');
    await searchPage.expectUrlContains('filter_glossaryTerms');
    await searchPage.expectActiveFilter('PlaywrightTerm');
    await searchPage.expectHasResults();
  });

  test('should search and filter by glossary term', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Glossary-Term', 'PlaywrightTerm');
    await searchPage.expectUrlContains('filter_glossaryTerms');
    await searchPage.expectHasResults();
  });

  test('should combine glossary term filter with text search', async () => {
    await searchPage.searchAndWait('playwright', 2000);
    await searchPage.selectFilterOption('Glossary-Term', 'PlaywrightTerm');
    await searchPage.expectUrlContains('filter_glossaryTerms');
    await searchPage.expectHasResults();
    await searchPage.expectActiveFilter('PlaywrightTerm');
  });

  test('should filter by multiple values using glossary terms', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Glossary-Term', 'PlaywrightTerm');
    await searchPage.expectUrlContains('filter_glossaryTerms');
    await searchPage.expectHasResults();
    await searchPage.expectActiveFilter('PlaywrightTerm');
  });
});
