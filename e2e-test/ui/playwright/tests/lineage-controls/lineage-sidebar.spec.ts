/**
 * The lineage sidebar — clicking a node on the graph selects it and opens its entity profile in
 * a sidebar beside the canvas.
 *
 * DATA SEEDING: static datasets from fixtures/data.json (auto-seeded via test.use below).
 *
 *   raw ──▶ [model (dbt)] ──▶ mart ──▶ chart
 *
 * The graph is opened on `mart` so that all three kinds of node are one hop away: a warehouse
 * dataset, a dbt model (drawn as a transformation node) and a Looker chart. Each has to be able
 * to fill the sidebar, and selecting a new node has to replace what the last one put there.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { BASE_FEATURE_FLAGS, CHART_URN, MART_URN, MODEL_DBT_URN } from './constants';

test.use({ featureName: 'lineage-controls' });

test.describe('lineage sidebar', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags(BASE_FEATURE_FLAGS);

    await lineagePage.navigateToDatasetLineage(MART_URN);
    await lineagePage.waitForGraphToRender();
    await lineagePage.checkNodeExists(MODEL_DBT_URN);
    await lineagePage.checkNodeExists(CHART_URN);
    await lineagePage.waitForViewportToSettle();
  });

  test('opens for a dataset, a dbt model and a chart, one at a time', async () => {
    await expect(lineagePage.lineageSidebar).toHaveCount(0);

    await lineagePage.clickNode(MART_URN);
    await lineagePage.checkSidebarShows('mart');

    await lineagePage.clickNode(MODEL_DBT_URN);
    await lineagePage.checkSidebarShows('model');

    await lineagePage.clickNode(CHART_URN);
    await lineagePage.checkSidebarShows('Lineage Controls Chart');

    // One node is selected at a time, so one sidebar is open at a time
    await expect(lineagePage.lineageSidebar).toHaveCount(1);
  });
});
