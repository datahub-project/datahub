import { test, expect } from '../../fixtures/test-context';

/**
 * Query and Filter Search Tests
 *
 * These tests use shared authentication from auth.setup.ts
 * Tests start already authenticated - no login needed
 */
test.describe('Query and Filter Search', () => {
  test.beforeEach(async ({ searchPage }) => {
    // Navigate to home page (already authenticated via shared state)
    await searchPage.navigateToHome();
  });

  test('should filter by type: Dashboards', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Dashboards');

    await searchPage.expectUrlContains('filter__entityType');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Dashboard');
  });

  test('should filter by type: ML Models', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'ML Models');

    await searchPage.expectUrlContains('filter__entityType');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('ML Model');
  });

  test('should filter by type: Pipelines', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Pipelines');

    await searchPage.expectUrlContains('filter__entityType');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Pipeline');
  });

  test('should filter by type: Glossary Terms', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Glossary Terms');

    await searchPage.expectUrlContains('filter__entityType');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Glossary Term');
  });

  test('should filter by platform: Hive', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Platform', 'Hive');

    await searchPage.expectUrlContains('filter_platform');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Hive');
  });

  test('should filter by platform: HDFS', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Platform', 'HDFS');

    await searchPage.expectUrlContains('filter_platform');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('HDFS');
  });

  test('should filter by platform: Airflow', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Platform', 'Airflow');

    await searchPage.expectUrlContains('filter_platform');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Airflow');
  });

  test('should filter by tag through more filters', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOptionThroughMoreFilters('Tagged-With', 'PlaywrightFeatureTag');

    await searchPage.expectUrlContains('filter_tags');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Tags');

    const tagLocator = page.locator('[data-testid="tag-PlaywrightFeatureTag"]');
    await expect(tagLocator).toBeVisible();

    await searchPage.expectTextVisible('PlaywrightFeatureTag');
  });

  test('should combine multiple filters and verify results', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectActiveFilter('Datasets');

    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectActiveFilter('Hive');

    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.expectUrlContains('filter_platform');

    await searchPage.clickEntityResult();

    await searchPage.expectTextVisible('Dataset');
    await searchPage.expectTextVisible('Hive');
  });

  test('should preserve filters when navigating back', async ({ searchPage, page }) => {
    await searchPage.searchAndWait('*', 2000);

    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectActiveFilter('Datasets');

    const urlBeforeClick = page.url();

    await searchPage.clickEntityResult();

    await page.goBack();

    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectUrlContains('filter__entityType');
  });
});
