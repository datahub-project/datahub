import { test, expect } from '../../fixtures/test-context';

/**
 * Search Functionality Tests
 *
 * These tests use shared authentication from auth.setup.ts
 * Tests start already authenticated - no login needed
 */
test.describe('Search Functionality', () => {
  test.beforeEach(async ({ searchPage }) => {
    // Navigate to home page (already authenticated via shared state)
    await searchPage.navigateToHome();
  });

  test('should search all entities and see results', async ({ searchPage }) => {
    await searchPage.searchAndWait('*', 5000);
    await searchPage.expectHasResults();
  });

  test('should search with impossible query and find 0 results', async ({ searchPage }) => {
    await searchPage.searchAndWait('zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq', 5000);
    await searchPage.expectNoResults();
  });

  test('should search, find a result, and visit the dataset page', async ({ searchPage, page }) => {
    await page.goto('/search?filter_entity=DATASET&filter_tags=urn%3Ali%3Atag%3APlaywright&page=1&query=users created');

    await searchPage.expectResultsCount(/of 1 result/);
    await searchPage.expectTextVisible('Playwright');

    await searchPage.clickResult('fct_Playwright_users_created');

    await searchPage.expectTextVisible('Hive');
    await searchPage.expectTextVisible('Dataset');
    await searchPage.expectTextVisible('fct_Playwright_users_created');
    await searchPage.expectTextVisible('user_id');
    await searchPage.expectTextVisible('Id of the user');
    await searchPage.expectTextVisible('table containing all the users created on a single day');
  });

  test('should search and get glossary term facets with proper labels', async ({ searchPage, datasetPage, page }) => {
    await page.goto('/dataset/urn:li:dataset:(urn:li:dataPlatform:hive,Playwright_logging_events,PROD)');

    await searchPage.expectTextVisible('Playwright_logging_events');

    await page.getByText('Add Term').click();

    const termOption = page.getByText('PlaywrightTerm');
    await termOption.click();

    await searchPage.expectTextVisible('PlaywrightTerm');

    await page.goto('/search?query=Playwright');
    await searchPage.expectTextVisible('PlaywrightTerm');
  });

  test('should search by a specific term using advanced search', async ({ searchPage, page }) => {
    await searchPage.navigateToHome();
    await searchPage.searchAndWait('*', 2000);

    await searchPage.clickAdvanced();
    await searchPage.clickAddFilter();

    await searchPage.selectFilter('Column Glossary Term');

    const termOption = page.getByText('PlaywrightColumnInfo');
    await termOption.click();

    await searchPage.expectTextVisible('SamplePlaywrightHdfsDataset');
    await searchPage.expectTextVisible('Playwright_logging_events');

    await searchPage.expectResultsCount(/Showing 1 - [2-4] of [2-4]/);
  });

  test('should search by AND-ing two concepts using advanced search', async ({ searchPage, page }) => {
    await searchPage.navigateToHome();
    await searchPage.searchAndWait('*', 2000);

    await searchPage.clickAdvanced();
    await searchPage.clickAddFilter();

    await searchPage.selectFilter('Column Glossary Term');

    const termOption = page.getByText('PlaywrightColumnInfo');
    await termOption.click();

    await searchPage.clickAddFilter();

    await searchPage.selectAdvancedFilter('description');

    await searchPage.fillTextFilter('log event');

    await searchPage.expectTextVisible('Playwright_logging_events');
  });

  test('should search by OR-ing two concepts using advanced search', async ({ searchPage, page }) => {
    await searchPage.navigateToHome();
    await searchPage.searchAndWait('*', 2000);

    await searchPage.clickAdvanced();
    await searchPage.clickAddFilter();

    await searchPage.selectFilter('Column Glossary Term');

    const termOption = page.getByText('PlaywrightColumnInfo');
    await termOption.click();

    await searchPage.clickAddFilter();

    await searchPage.selectAdvancedFilter('description');

    await searchPage.fillTextFilter('log event');

    await searchPage.clickAllFilters();
    await searchPage.clickAnyFilter();

    await searchPage.expectTextVisible('Playwright_logging_events');
    await searchPage.expectTextVisible('fct_Playwright_users_created_no_tag');
    await searchPage.expectTextVisible('SamplePlaywrightHdfsDataset');
  });
});
