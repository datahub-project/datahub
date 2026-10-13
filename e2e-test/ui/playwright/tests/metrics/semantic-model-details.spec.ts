/**
 * Semantic Model detail page coverage (Summary modules, Definition, tabs).
 *
 * Prerequisites: fixtures/data.json (featureName: 'metrics')
 */

import { test, expect } from '../../fixtures/base-test';
import { MetricsPage } from '../../pages/metrics.page';
import { TIMEOUTS } from '../../utils/constants';
import { METRICS_FEATURE_FLAGS, NAMES, ORDERS_LOGICAL_URN, SEMANTIC_MODEL_ORDERS_URN } from './constants';

test.use({ featureName: 'metrics' });

test.describe('Semantic Model details', () => {
  let metricsPage: MetricsPage;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(METRICS_FEATURE_FLAGS);
    metricsPage = new MetricsPage(page, logger, logDir);
    await metricsPage.navigateToSemanticModel(SEMANTIC_MODEL_ORDERS_URN);
  });

  test('loads profile with sidebar selection and summary modules', async () => {
    await metricsPage.expectEntityNamed(NAMES.ORDERS_MODEL);
    await expect(metricsPage.sidebar).toBeVisible();

    await metricsPage.openSummaryTab();
    await metricsPage.expectSemanticModelModulesVisible();
    await metricsPage.expectModuleContains(metricsPage.datasetsModule, NAMES.ORDERS_ALIAS);
    await metricsPage.expectModuleContains(metricsPage.metricsModule, NAMES.TOTAL_REVENUE);
    await metricsPage.expectModuleContains(metricsPage.dimensionsModule, 'order_id');
    await metricsPage.expectModuleContains(metricsPage.relationshipsModule, NAMES.ORDERS_ALIAS);
    await metricsPage.expectModuleContains(metricsPage.relationshipsModule, NAMES.CUSTOMERS_ALIAS);
  });

  test('definition tab shows native definition', async () => {
    await metricsPage.openDefinitionTab();
    await expect(metricsPage.definitionCode).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(metricsPage.definitionCode).toContainText('pw_orders_model');
    await expect(metricsPage.definitionCopy).toBeVisible();
  });

  test('navigates from metrics module to metric detail', async ({ page }) => {
    await metricsPage.openSummaryTab();
    await metricsPage.expectModuleContains(metricsPage.metricsModule, NAMES.TOTAL_REVENUE);
    await metricsPage.clickModuleItem(metricsPage.metricsModule, NAMES.TOTAL_REVENUE);
    await expect(page).toHaveURL(/\/metric\//);
    await metricsPage.expectEntityNamed(NAMES.TOTAL_REVENUE);
  });

  test('exposes Properties and Lineage tabs', async () => {
    await expect(metricsPage.propertiesTab).toBeVisible();
    await expect(metricsPage.lineageTab).toBeVisible();
    await metricsPage.openPropertiesTab();
    await metricsPage.openLineageTab();
  });

  test('datasets module links to member dataset', async ({ page }) => {
    await metricsPage.openSummaryTab();
    await metricsPage.expectModuleContains(metricsPage.datasetsModule, NAMES.ORDERS_ALIAS);
    await metricsPage.clickModuleItem(metricsPage.datasetsModule, NAMES.ORDERS_ALIAS);
    await expect(page).toHaveURL(new RegExp(`/dataset/`), { timeout: TIMEOUTS.LONG });
    expect(decodeURIComponent(page.url())).toContain(ORDERS_LOGICAL_URN);
  });
});
