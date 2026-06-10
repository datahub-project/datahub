/**
 * Analytics Tests — Verify that user interactions (tab clicks) are tracked and visible in analytics.
 *
 * Core Behavior:
 * - User opens a chart and clicks a tab (Dashboards)
 * - This interaction is tracked by the system
 * - Analytics page shows the tracked "dashboards" interaction in the Tab Views chart
 *
 * Test Data:
 * Uses global Playwright test data (test-data/data.json):
 * - Chart: urn:li:chart:(looker,playwright_baz1) — "Baz Chart 1"
 * - Pre-seeded in all test environments
 */

import { test } from '../../fixtures/base-test';
import { AnalyticsPage } from '../../pages/analytics.page';

test.describe('Analytics', () => {
  let analyticsPage: AnalyticsPage;

  test.beforeEach(async ({ logger, logDir, page }) => {
    analyticsPage = new AnalyticsPage(page, logger, logDir);
  });

  test('can view analytics dashboard with Data Landscape Summary', async () => {
    await analyticsPage.goToAnalytics();
    await analyticsPage.verifyLandscapeSummaryVisible();
  });

  test('can track tab interactions and see them in analytics', async () => {
    const chartUrn = 'urn:li:chart:(looker,playwright_baz1)';
    const chartTitle = 'Baz Chart 1';

    await analyticsPage.goToChart(chartUrn);
    await analyticsPage.verifyChartTitle(chartTitle);
    await analyticsPage.clickTab('Dashboards');
    await analyticsPage.goToAnalytics();
    await analyticsPage.verifyTabViewsChartVisible();
    await analyticsPage.verifyTabInteractionTracked('dashboards');
  });
});
