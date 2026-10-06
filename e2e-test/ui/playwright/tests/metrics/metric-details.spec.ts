/**
 * Metric detail page coverage (SQL, related metrics, ancestry expansion).
 *
 * Prerequisites: fixtures/data.json (featureName: 'metrics')
 */

import { test, expect } from '../../fixtures/base-test';
import { MetricsPage } from '../../pages/metrics.page';
import { TIMEOUTS } from '../../utils/constants';
import {
  CHILD_REVENUE_URN,
  DOUBLE_REVENUE_URN,
  METRICS_FEATURE_FLAGS,
  NAMES,
  ORDER_COUNT_URN,
  SEMANTIC_MODEL_ORDERS_URN,
  TOTAL_REVENUE_URN,
} from './constants';

test.use({ featureName: 'metrics' });

test.describe('Metric details', () => {
  let metricsPage: MetricsPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(METRICS_FEATURE_FLAGS);
    metricsPage = new MetricsPage(page, logger, logDir);
  });

  test('loads profile and auto-expands owning semantic model in sidebar', async () => {
    await metricsPage.navigateToMetric(TOTAL_REVENUE_URN);
    await metricsPage.expectEntityNamed(NAMES.TOTAL_REVENUE);
    await expect(metricsPage.sidebar).toBeVisible();
    await expect(metricsPage.sidebarModel(SEMANTIC_MODEL_ORDERS_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    // Owning model auto-expands so the metric row is visible.
    await expect(metricsPage.sidebarMetric(TOTAL_REVENUE_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
  });

  test('shows SQL expression and related metrics classifications', async ({ page }) => {
    await metricsPage.navigateToMetric(TOTAL_REVENUE_URN);
    await metricsPage.openSummaryTab();
    await metricsPage.expectMetricModulesVisible();
    await expect(page.getByTestId('sql-code-block')).toContainText('SUM(ORDERS.amount)', {
      timeout: TIMEOUTS.LONG,
    });
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, NAMES.ORDER_COUNT);
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, 'Related To');
  });

  test('shows Derived From on derived metric', async () => {
    await metricsPage.navigateToMetric(DOUBLE_REVENUE_URN);
    await metricsPage.expectEntityNamed(NAMES.DOUBLE_REVENUE);
    await metricsPage.openSummaryTab();
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, NAMES.TOTAL_REVENUE);
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, 'Derived From');
  });

  test('shows Child of on nested metric and expands ancestry', async () => {
    await metricsPage.navigateToMetric(CHILD_REVENUE_URN);
    await metricsPage.expectEntityNamed(NAMES.CHILD_REVENUE);
    await expect(metricsPage.sidebarMetric(CHILD_REVENUE_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await expect(metricsPage.sidebarMetric(TOTAL_REVENUE_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await metricsPage.openSummaryTab();
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, NAMES.TOTAL_REVENUE);
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, 'Child of');
  });

  test('related metrics module navigates to linked metric', async ({ page }) => {
    await metricsPage.navigateToMetric(TOTAL_REVENUE_URN);
    await metricsPage.openSummaryTab();
    await metricsPage.expectModuleContains(metricsPage.relatedMetricsModule, NAMES.ORDER_COUNT);
    await metricsPage.clickModuleItem(metricsPage.relatedMetricsModule, NAMES.ORDER_COUNT);
    await expect(page).toHaveURL(new RegExp(`/metric/`));
    await metricsPage.expectEntityNamed(NAMES.ORDER_COUNT);
    expect(decodeURIComponent(page.url())).toContain(ORDER_COUNT_URN);
  });

  test('exposes Properties and Lineage tabs', async () => {
    await metricsPage.navigateToMetric(TOTAL_REVENUE_URN);
    await expect(metricsPage.propertiesTab).toBeVisible();
    await expect(metricsPage.lineageTab).toBeVisible();
    await metricsPage.openPropertiesTab();
    await metricsPage.openLineageTab();
  });
});
