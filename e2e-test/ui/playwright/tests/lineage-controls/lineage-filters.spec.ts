/**
 * The graph's filters panel — "hide transformations" and "show hidden edges" (ghost entities).
 *
 * DATA SEEDING: static datasets from fixtures/data.json (auto-seeded via test.use below).
 *
 * Two of the seeded chains are used here:
 *
 *   transformations:  raw ──metric──▶ [model (dbt)] ──metric──▶ mart
 *   ghost:            ghost_src ──value──▶ ghost_dst (soft deleted, so drawn as a ghost)
 *
 * Hiding transformations takes the dbt model off the graph without taking its column lineage
 * with it: the arrow that ran raw.metric ──▶ model.metric ──▶ mart.metric is redrawn as a single
 * hop straight from raw.metric to mart.metric. Ghost entities are the same story for
 * soft-deleted nodes, which are off the graph until the toggle puts them back.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';
import { BASE_FEATURE_FLAGS, GHOST_DST_URN, GHOST_SRC_URN, MART_URN, MODEL_DBT_URN, RAW_URN } from './constants';

test.use({ featureName: 'lineage-controls' });

test.describe('lineage graph filters', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags(BASE_FEATURE_FLAGS);
  });

  /** Open a dataset's lineage graph, wait for it to settle, and expand its columns. */
  async function openLineageAndExpandColumns(homeUrn: string): Promise<void> {
    await lineagePage.navigateToDatasetLineage(homeUrn);
    await lineagePage.waitForGraphToRender();
    // The minimap floats over the bottom-right of the canvas; after fitView a column can land
    // under it, which intercepts pointer events and blocks hovers. Let hovers pass through it.
    await lineagePage.page.addStyleTag({ content: '.react-flow__minimap { pointer-events: none !important; }' });
    await lineagePage.expandContractColumns(homeUrn);
    await lineagePage.waitForViewportToSettle();
  }

  test.describe('hide transformations', () => {
    test('takes the dbt model off the graph and puts it back', async () => {
      await lineagePage.navigateToDatasetLineage(RAW_URN);
      await lineagePage.waitForGraphToRender();
      await lineagePage.checkNodeExists(MODEL_DBT_URN);
      await lineagePage.checkNodeExists(MART_URN);

      await lineagePage.setFilter('hide-transformations', true);
      await lineagePage.checkNodeNotExists(MODEL_DBT_URN);
      // Hiding the transformation collapses the chain rather than truncating it: what the model
      // fed still has to be reachable from what fed the model.
      await lineagePage.checkNodeExists(MART_URN);
      await lineagePage.checkEdgeExists(RAW_URN, MART_URN);

      await lineagePage.setFilter('hide-transformations', false);
      await lineagePage.checkNodeExists(MODEL_DBT_URN);
      await lineagePage.checkEdgeExists(RAW_URN, MODEL_DBT_URN);
      await lineagePage.checkEdgeExists(MODEL_DBT_URN, MART_URN);
    });

    test('keeps drawing column lineage through the hidden transformation', async () => {
      await openLineageAndExpandColumns(RAW_URN);

      // With the model shown, the column path is drawn in two hops, through it
      await lineagePage.hoverColumn(RAW_URN, 'metric');
      await expect(lineagePage.getColumnEdge(RAW_URN, 'metric', MODEL_DBT_URN, 'metric')).toBeAttached({
        timeout: TIMEOUTS.MEDIUM,
      });
      await expect(lineagePage.getColumnEdge(MODEL_DBT_URN, 'metric', MART_URN, 'metric')).toBeAttached({
        timeout: TIMEOUTS.MEDIUM,
      });

      await lineagePage.setFilter('hide-transformations', true);
      await lineagePage.checkNodeNotExists(MODEL_DBT_URN);

      // The readout only lives as long as the hover, and toggling a filter re-lays out the graph,
      // which can slide the column out from under the cursor: re-hover and retry.
      const collapsedEdge = lineagePage.getColumnEdge(RAW_URN, 'metric', MART_URN, 'metric');
      await expect(async () => {
        await lineagePage.hoverColumn(RAW_URN, 'metric');
        await expect(collapsedEdge).toBeAttached({ timeout: TIMEOUTS.SHORT });
      }).toPass({ timeout: TIMEOUTS.EXTRA_LONG });
      await lineagePage.checkEdgeHasArrowMarker(collapsedEdge);
    });
  });

  test.describe('show hidden edges', () => {
    test('keeps a soft-deleted downstream off the graph until the toggle puts it back', async () => {
      await lineagePage.navigateToDatasetLineage(GHOST_SRC_URN);
      await lineagePage.waitForGraphToRender();
      await lineagePage.checkNodeExists(GHOST_SRC_URN);
      await lineagePage.checkNodeNotExists(GHOST_DST_URN);

      await lineagePage.setFilter('show-ghost-entities', true);
      await lineagePage.checkNodeExists(GHOST_DST_URN);
      await lineagePage.checkEdgeExists(GHOST_SRC_URN, GHOST_DST_URN);

      await lineagePage.setFilter('show-ghost-entities', false);
      await lineagePage.checkNodeNotExists(GHOST_DST_URN);
    });

    test('draws column lineage into a soft-deleted downstream once it is shown', async () => {
      await lineagePage.navigateToDatasetLineage(GHOST_SRC_URN);
      await lineagePage.waitForGraphToRender();
      await lineagePage.setFilter('show-ghost-entities', true);
      await lineagePage.checkNodeExists(GHOST_DST_URN);

      await lineagePage.page.addStyleTag({ content: '.react-flow__minimap { pointer-events: none !important; }' });
      await lineagePage.expandContractColumns(GHOST_SRC_URN);
      await lineagePage.waitForViewportToSettle();

      const ghostEdge = lineagePage.getColumnEdge(GHOST_SRC_URN, 'value', GHOST_DST_URN, 'value');
      await expect(async () => {
        await lineagePage.hoverColumn(GHOST_SRC_URN, 'value');
        await expect(ghostEdge).toBeAttached({ timeout: TIMEOUTS.SHORT });
      }).toPass({ timeout: TIMEOUTS.EXTRA_LONG });
    });
  });
});
