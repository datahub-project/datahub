/**
 * Metrics home + sidebar browse coverage.
 *
 * Prerequisites:
 * - fixtures/data.json (featureName: 'metrics')
 * - Live GMS should have METRICS_ENABLED=true; client flag is also mocked via apiMock
 */

import { test, expect } from '../../fixtures/base-test';
import { MetricsPage } from '../../pages/metrics.page';
import { TIMEOUTS } from '../../utils/constants';
import {
  EXPECTED_COUNTS,
  METRICS_FEATURE_FLAGS,
  METRICS_FEATURE_FLAGS_OFF,
  NAMES,
  SEMANTIC_MODEL_ORDERS_URN,
  TOTAL_REVENUE_URN,
  CHILD_REVENUE_URN,
  PAYMENT_VOLUME_URN,
} from './constants';

test.use({ featureName: 'metrics' });

test.describe('Metrics home and sidebar', () => {
  let metricsPage: MetricsPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(METRICS_FEATURE_FLAGS);
    metricsPage = new MetricsPage(page, logger, logDir);
  });

  test('hides Metrics nav and route when flag is off', async ({ page, apiMock }) => {
    await apiMock.setFeatureFlags(METRICS_FEATURE_FLAGS_OFF);
    await page.goto('/');
    await expect(metricsPage.navMetricsItem).toBeHidden({ timeout: TIMEOUTS.MEDIUM });

    await page.goto('/metrics');
    await expect(metricsPage.metricsPage).toBeHidden({ timeout: TIMEOUTS.MEDIUM });
    await expect(metricsPage.sidebar).toBeHidden({ timeout: TIMEOUTS.MEDIUM });
  });

  test('shows Metrics nav and populated home when flag is on', async ({ page }) => {
    await page.goto('/');
    await expect(metricsPage.navMetricsItem).toBeVisible({ timeout: TIMEOUTS.LONG });

    await metricsPage.openMetricsFromNav();
    await metricsPage.expectHomeVisible();
    await metricsPage.expectSummaryCounts(
      EXPECTED_COUNTS.SEMANTIC_MODELS,
      EXPECTED_COUNTS.ROOT_METRICS,
      EXPECTED_COUNTS.PLATFORMS,
    );
    await metricsPage.expectRecentListsVisible();

    // Seeded entities may not be in the "recent" window on dirty instances; find via sidebar search.
    await metricsPage.searchSidebar(NAMES.TOTAL_REVENUE);
    await expect(metricsPage.sidebarSearchMetric(TOTAL_REVENUE_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await metricsPage.clearSidebarSearch();
  });

  test('navigates from recent semantic model to detail profile', async ({ page }) => {
    await metricsPage.navigateToHome();
    // Prefer the seeded model when it appears in Recent; otherwise any recent row.
    const seededRecent = metricsPage.recentModel(SEMANTIC_MODEL_ORDERS_URN);
    const recentRow = (await seededRecent.isVisible().catch(() => false))
      ? seededRecent
      : page.getByTestId(/^recent-model-/).first();
    await expect(recentRow).toBeVisible({ timeout: TIMEOUTS.LONG });
    await recentRow.click();
    await expect(page).toHaveURL(new RegExp(`/semanticModel/`));
    await expect(metricsPage.entityHeader).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(metricsPage.sidebar).toBeVisible();
  });

  test('expands model → metric → child metric and selects rows', async ({ page }) => {
    // Navigating to a model-backed metric injects the owning model into the sidebar tree.
    await metricsPage.navigateToMetric(TOTAL_REVENUE_URN);
    await expect(metricsPage.sidebarMetric(TOTAL_REVENUE_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await expect(metricsPage.sidebarModel(SEMANTIC_MODEL_ORDERS_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });

    await metricsPage.expandSidebarMetric(TOTAL_REVENUE_URN);
    await expect(metricsPage.sidebarMetric(CHILD_REVENUE_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });

    await metricsPage.clickSidebarMetric(CHILD_REVENUE_URN);
    await expect(page).toHaveURL(new RegExp(`/metric/`));
    await metricsPage.expectEntityNamed(NAMES.CHILD_REVENUE);
  });

  test('sidebar search finds a metric and clear restores the tree', async () => {
    await metricsPage.navigateToHome();
    await metricsPage.searchSidebar(NAMES.PAYMENT_VOLUME);
    await expect(metricsPage.sidebarSearchMetric(PAYMENT_VOLUME_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await metricsPage.clearSidebarSearch();
    await expect(metricsPage.sidebarTree).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('groups sidebar by platform and domain', async () => {
    await metricsPage.navigateToHome();

    await metricsPage.setGroupBy('platform');
    await expect(metricsPage.platformGroups()).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(
      metricsPage
        .platformGroups()
        .getByText(/Snowflake/i)
        .first(),
    ).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await expect(
      metricsPage
        .platformGroups()
        .getByText(/BigQuery/i)
        .first(),
    ).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });

    await metricsPage.setGroupBy('domain');
    await expect(metricsPage.domainGroups()).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(
      metricsPage
        .domainGroups()
        .getByText(/PW Metrics Sales/i)
        .first(),
    ).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await expect(
      metricsPage
        .domainGroups()
        .getByText(/PW Metrics Finance/i)
        .first(),
    ).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });

    await metricsPage.setGroupBy('semantic_model');
    await expect(metricsPage.sidebarTree).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(metricsPage.modelsSection).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('sidebar home link returns to Metrics landing', async () => {
    await metricsPage.navigateToMetric(TOTAL_REVENUE_URN);
    await metricsPage.expectEntityNamed(NAMES.TOTAL_REVENUE);
    await metricsPage.clickSidebarHome();
    await metricsPage.expectHomeVisible();
  });
});
