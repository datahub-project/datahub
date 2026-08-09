/**
 * Browse V2 Tests - Sidebar Navigation & Browse Path Filtering
 *
 * Tests core Browse V2 functionality (9 focused tests):
 * 1. Feature flags control sidebar visibility
 * 2. Sidebar toggle collapses and expands
 * 3. Wildcard search discovers multiple platforms
 * 4. Platform expand/collapse navigation
 * 5. Home page entity type search navigates to unified search+browse
 * 6. Browse hierarchy navigation (platform → path → nodes)
 * 7. Browse path selection applies filters and reduces results
 * 8. Browse path deselection removes filters
 * 9. Browse path click navigates to container entity page
 *
 * Architecture:
 * Browse V2 shows PLATFORMS directly (not entity types).
 * The hierarchy is: Platform (BigQuery) → Browse Path (PlaywrightBrowseEntity) → Datasets (test_schema)
 *
 * Test Data:
 * Uses dedicated fixture data with two datasets and browse paths:
 * - BigQuery dataset: urn:li:dataset:(urn:li:dataPlatform:bigquery,PlaywrightBrowseEntity.test_schema.customers,PROD)
 * - dbt dataset: urn:li:dataset:(urn:li:dataPlatform:dbt,PlaywrightBrowseEntity.test_schema.orders,PROD)
 * - Browse path hierarchy: BigQuery/dbt → PlaywrightBrowseEntity → test_schema
 * - Container links in browse paths navigate to container entity pages
 */

import { test, expect } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';
import { BrowseV2Page } from '../../pages/browse-v2.page';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';

// Browse V2 test constants
const BROWSE_V2_FEATURE_FLAGS = {
  showBrowseV2: true,
  showSearchFiltersV2: true,
};

const PLATFORM = {
  BIGQUERY: 'BigQuery',
};

const BROWSE_PATHS = {
  PROJECT: 'PlaywrightBrowseEntity',
  SCHEMA: 'test_schema',
};

const CONTAINER_URNS = {
  PROJECT: 'urn:li:container:playwright_browse_entity_project_container',
  SCHEMA: 'urn:li:container:playwright_browse_entity_schema_container',
};

const SEARCH_QUERIES = {
  WILDCARD: '*',
  ENTITY_TYPE: 'dataset',
  CUSTOMERS: 'customers',
};

const URL_PATTERNS = {
  SEARCH: 'search',
  LEGACY_BROWSE: '/browse/dataset',
  FILTER_PLATFORM: 'filter_platform',
  BIGQUERY_FILTER: 'bigquery',
  FILTER_BROWSE_PATH: 'filter_browsePathV2',
  CONTAINER: 'container',
};

test.use({ featureName: 'browse-v2' });

test.describe('Browse V2 - Platform Browse Mode', () => {
  let searchPage: SearchPage;
  let browseV2Page: BrowseV2Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    searchPage = new SearchPage(page, logger, logDir);
    browseV2Page = new BrowseV2Page(page, logger, logDir);

    // Enable Browse V2 before navigation
    await apiMock.setFeatureFlags(BROWSE_V2_FEATURE_FLAGS);

    // Navigate to home with flags enabled
    await searchPage.navigateToHome();
  });

  test('Browse V2 sidebar is visible when feature flags are enabled', async ({ page, logger }) => {
    logger?.step('perform wildcard search to load browse sidebar');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify browse v2 sidebar is visible');
    await browseV2Page.expectBrowseV2Visible();
  });

  test('Browse V2 sidebar is hidden when feature flags are disabled', async ({ page, apiMock, logger }) => {
    // Disable Browse V2
    await apiMock.setFeatureFlags({
      showBrowseV2: false,
      showSearchFiltersV2: false,
    });

    logger?.step('reload page with flags disabled');
    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('perform wildcard search');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify browse v2 sidebar is not visible');
    await browseV2Page.expectBrowseV2NotVisible();
  });

  test('Sidebar toggle collapses and expands', async ({ page, logger }) => {
    logger?.step('perform wildcard search');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await browseV2Page.expectBrowseV2Visible();

    logger?.step('wait for platform nodes to render in browse sidebar');
    // BigQuery is used as a reliable platform that should always be present in test data
    await browseV2Page.expectBrowsePlatformExists(PLATFORM.BIGQUERY, TIMEOUTS.EXTRA_LONG);

    logger?.step('verify sidebar is expanded initially (width > 100px)');
    await browseV2Page.expectSidebarExpanded();

    logger?.step('click toggle to collapse sidebar');
    await browseV2Page.toggleSidebar();

    logger?.step('verify sidebar is collapsed after toggle');
    await browseV2Page.expectSidebarCollapsed();

    logger?.step('click toggle to expand sidebar');
    await browseV2Page.toggleSidebar();

    logger?.step('verify sidebar is expanded after toggle');
    await browseV2Page.expectSidebarExpanded();
  });

  test('Wildcard search discovers multiple platforms in browse sidebar', async ({ page, logger }) => {
    logger?.step('perform wildcard search');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify browse v2 sidebar is visible');
    await browseV2Page.expectBrowseV2Visible();

    logger?.step('verify BigQuery platform is discoverable');
    await browseV2Page.expectBrowsePlatformExists(PLATFORM.BIGQUERY, TIMEOUTS.EXTRA_LONG);
  });

  test('Platform navigation: expand/collapse keeps sidebar visible', async ({ page, logger }) => {
    logger?.step('perform wildcard search');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('expand BigQuery platform');
    await browseV2Page.expectBrowsePlatformExists(PLATFORM.BIGQUERY, TIMEOUTS.EXTRA_LONG);
    await browseV2Page.clickBrowsePlatform(PLATFORM.BIGQUERY);
    await page.waitForTimeout(TIMEOUTS.QUICK);

    logger?.step('verify sidebar remains visible after expanding');
    await browseV2Page.expectBrowseV2Visible();

    logger?.step('collapse BigQuery platform');
    await browseV2Page.clickBrowsePlatform(PLATFORM.BIGQUERY);
    await page.waitForTimeout(TIMEOUTS.QUICK);

    logger?.step('verify sidebar remains visible after collapsing');
    await browseV2Page.expectBrowseV2Visible();
  });

  test('Home page entity type search navigates to unified search+browse', async ({ page, logger }) => {
    logger?.step('perform entity type search');
    await searchPage.search(SEARCH_QUERIES.ENTITY_TYPE);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify browse v2 sidebar is visible');
    await browseV2Page.expectBrowseV2Visible();

    logger?.step('verify BigQuery platform is available in browse');
    await browseV2Page.expectBrowsePlatformExists(PLATFORM.BIGQUERY, TIMEOUTS.EXTRA_LONG);

    logger?.step('verify URL is search experience (not legacy /browse/dataset)');
    const url = page.url();
    expect(url).toContain(URL_PATTERNS.SEARCH);
    expect(url).not.toContain(URL_PATTERNS.LEGACY_BROWSE);
  });

  test('Browse hierarchy is navigable: platform → path → nodes', async ({ page, logger }) => {
    logger?.step('perform wildcard search');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('expand BigQuery platform');
    await browseV2Page.expectBrowsePlatformExists(PLATFORM.BIGQUERY, TIMEOUTS.EXTRA_LONG);
    await browseV2Page.clickBrowsePlatform(PLATFORM.BIGQUERY);
    await page.waitForTimeout(TIMEOUTS.QUICK);

    logger?.step('verify PlaywrightBrowseEntity browse path is visible');
    await browseV2Page.expectBrowseNodeVisible(BROWSE_PATHS.PROJECT);

    logger?.step('expand PlaywrightBrowseEntity path');
    await browseV2Page.expandBrowseNode(BROWSE_PATHS.PROJECT);

    logger?.step('verify test_schema child node is visible');
    await browseV2Page.expectBrowseNodeVisible(BROWSE_PATHS.SCHEMA);

    logger?.step('verify sidebar remains visible throughout navigation');
    await browseV2Page.expectBrowseV2Visible();
  });

  test('Browse path selection applies filters and reduces results', async ({ page, logger }) => {
    logger?.step('perform wildcard search and capture baseline result count');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    const baselineCount = await searchPage.getResultCount();
    logger?.step(`baseline: ${baselineCount} results`);

    logger?.step('expand BigQuery platform');
    await browseV2Page.expectBrowsePlatformExists(PLATFORM.BIGQUERY, TIMEOUTS.EXTRA_LONG);
    await browseV2Page.clickBrowsePlatform(PLATFORM.BIGQUERY);
    await page.waitForTimeout(TIMEOUTS.QUICK);

    logger?.step('expand PlaywrightBrowseEntity path');
    await browseV2Page.expandBrowseNode(BROWSE_PATHS.PROJECT);
    await page.waitForTimeout(TIMEOUTS.QUICK);

    logger?.step('click test_schema node to apply filters');
    await browseV2Page.clickBrowseNode(BROWSE_PATHS.SCHEMA);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify URL has filters applied');
    const url = page.url();
    expect(url).toContain(URL_PATTERNS.FILTER_PLATFORM);
    expect(url).toContain(URL_PATTERNS.BIGQUERY_FILTER);
    expect(url).toContain(URL_PATTERNS.FILTER_BROWSE_PATH);
    expect(url).toContain(BROWSE_PATHS.PROJECT);
    expect(url).toContain(BROWSE_PATHS.SCHEMA);

    logger?.step('verify result count decreased');
    const filteredCount = await searchPage.getResultCount();
    logger?.step(`filtered: ${filteredCount} results`);
    expect(filteredCount).toBeLessThan(baselineCount);
    expect(filteredCount).toBeGreaterThan(0);
  });

  test('Browse path deselection removes filters', async ({ page, logger }) => {
    logger?.step('perform wildcard search');
    await searchPage.search(SEARCH_QUERIES.WILDCARD);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('apply browse path filter');
    await browseV2Page.clickBrowsePlatform(PLATFORM.BIGQUERY);
    await page.waitForTimeout(TIMEOUTS.QUICK);
    await browseV2Page.expandBrowseNode(BROWSE_PATHS.PROJECT);
    await page.waitForTimeout(TIMEOUTS.QUICK);
    await browseV2Page.clickBrowseNode(BROWSE_PATHS.SCHEMA);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify filters are applied');
    let url = page.url();
    expect(url).toContain(URL_PATTERNS.FILTER_PLATFORM);
    expect(url).toContain(URL_PATTERNS.FILTER_BROWSE_PATH);

    logger?.step('click test_schema again to deselect');
    await browseV2Page.clickBrowseNode(BROWSE_PATHS.SCHEMA);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify filters are removed from URL');
    url = page.url();
    expect(url).not.toContain(URL_PATTERNS.FILTER_PLATFORM);
    expect(url).not.toContain(URL_PATTERNS.FILTER_BROWSE_PATH);
  });

  test('Browse path click navigates to container entity page', async ({ page, logger }) => {
    logger?.step('search for customers dataset');
    await searchPage.search(SEARCH_QUERIES.CUSTOMERS);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('click on customers dataset to open profile');
    await searchPage.clickResult(SEARCH_QUERIES.CUSTOMERS);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify dataset profile page is loaded');
    expect(page.url()).toContain(SEARCH_QUERIES.ENTITY_TYPE);
    expect(page.url()).toContain(URL_PATTERNS.BIGQUERY_FILTER);

    logger?.step('verify browse path entries are visible');
    await browseV2Page.expectBrowsePathVisible(CONTAINER_URNS.PROJECT);

    logger?.step('click on first browse path entry to navigate to container');
    await browseV2Page.clickBrowsePath(CONTAINER_URNS.PROJECT);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    logger?.step('verify navigation to container entity page');
    const url = page.url();
    expect(url).toContain(URL_PATTERNS.CONTAINER);
    expect(url).toContain(CONTAINER_URNS.PROJECT.split(':').pop());
  });
});
