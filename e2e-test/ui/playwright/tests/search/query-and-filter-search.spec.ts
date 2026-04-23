/**
 * Query and Filter Search tests.
 *
 * Uses base-test so every test runs with a pre-authenticated context
 * (no login step needed in beforeEach). SearchPage is instantiated directly
 * in beforeEach with the logger from the fixture — no test-context.ts needed.
 *
 * The `user` defaults to resolvedUsers.admin (set in loginFixture). Override
 * per suite with test.use({ user: resolvedUsers.reader }) if needed.
 */

import { test, expect } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';

test.use({ featureName: 'search' });

test.describe('Query and Filter Search', () => {
  let searchPage: SearchPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test('should filter by type: Dashboards', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'Dashboards');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('Dashboard');
  });

  test('should filter by type: ML Models', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'ML Models');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('ML Model');
  });

  test('should filter by type: Pipelines', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'Pipelines');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('Pipeline');
  });

  test('should filter by type: Glossary Terms', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'Glossary Terms');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('Glossary Term');
  });

  test('should filter by platform: Hive', async ({ page }) => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.clickEntityResult();
    await page.waitForLoadState('networkidle');
    // The V2 entity page shows the platform as an icon (not text), so we verify
    // that the entity page was reached. Hive datasets show as "Dataset" entity type.
    await searchPage.expectTextVisible('Dataset');
  });

  test('should filter by platform: HDFS', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Platform', 'HDFS');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('HDFS');
  });

  test('should filter by platform: Airflow', async () => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Platform', 'Airflow');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('Airflow');
  });

  test('should filter by tag', async ({ page }) => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Tag', 'PlaywrightFeatureTag');
    await searchPage.expectUrlContains('filter_tags');
    await searchPage.clickEntityResult();
    await searchPage.expectTextVisible('Tags');

    // Verify the specific tag element is visible (requires page directly).
    const tagLocator = page.locator('[data-testid="tag-PlaywrightFeatureTag"]');
    await expect(tagLocator).toBeVisible();
    await searchPage.expectTextVisible('PlaywrightFeatureTag');
  });

  test('should combine multiple filters and verify results', async ({ page }) => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectActiveFilter('Datasets');
    await searchPage.selectFilterOption('Platform', 'Hive');
    await searchPage.expectActiveFilter('Hive');
    await searchPage.expectUrlContains('filter__entityType');
    await searchPage.expectUrlContains('filter_platform');
    await searchPage.clickEntityResult();
    await page.waitForLoadState('networkidle');
    // The V2 entity page shows the platform as an icon (not text), so we verify
    // the entity type text. Hive datasets are of type "Dataset".
    await searchPage.expectTextVisible('Dataset');
  });

  test('should preserve filters when navigating back', async ({ page }) => {
    await searchPage.searchAndWait('*', 2000);
    await searchPage.selectFilterOption('Type', 'Datasets');
    await searchPage.expectActiveFilter('Datasets');

    await searchPage.clickEntityResult();
    await page.goBack();

    await searchPage.expectActiveFilter('Datasets');
    await searchPage.expectUrlContains('filter__entityType');
  });
});
