/**
 * Column lineage controls — the `n / total` readouts beside a hovered or selected column.
 *
 * DATA SEEDING: static datasets from fixtures/data.json (auto-seeded via test.use below).
 *
 * The seeded graph is a warehouse table with a dbt sibling, which is drawn folded into it:
 *
 *   orders (snowflake)  ──order_mode──▶  order_details (snowflake)
 *     └── orders (dbt), its sibling, emitting only `order_id ──▶ order_id` onto itself
 *
 * So `order_mode` has column lineage the graph can draw, and `order_id` has none: its only column
 * lineage is to the same column on its own sibling, which is never drawn as a node of its own.
 *
 * Every case runs with `hideDbtSourceInLineage` both on and off. That flag decides whether a dbt
 * source is merged into its warehouse sibling, which changes the shape the sibling data arrives
 * in, so the controls have to read the same either way.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'column-lineage-controls' });

const WAREHOUSE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_column_controls.orders,PROD)';
const DOWNSTREAM_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_column_controls.order_details,PROD)';

const LINEAGE_COLUMN = 'order_mode'; // Has lineage to the downstream table
const SIBLING_ONLY_COLUMN = 'order_id'; // Only "lineage" is to the same column on its dbt sibling

for (const hideDbtSourceInLineage of [true, false]) {
  test.describe(`column lineage controls, hideDbtSourceInLineage=${hideDbtSourceInLineage}`, () => {
    let lineagePage: LineageV3Page;

    test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
      lineagePage = new LineageV3Page(page, logger, logDir);
      await apiMock.setFeatureFlags({
        themeV2Enabled: true,
        themeV2Default: true,
        showNavBarRedesign: true,
        hideDbtSourceInLineage,
        // The controls stand in for lineage filter nodes, so they only render without them
        showLineageFilterNodes: false,
      });
      await lineagePage.navigateToDatasetLineage(WAREHOUSE_URN);
      await lineagePage.waitForGraphToRender();
      await lineagePage.expandContractColumns(WAREHOUSE_URN);
      // The graph fits its viewport on a delay after entity data loads; if that lands after a
      // hover, it slides the column out from under the cursor and everything hover-driven unmounts
      await lineagePage.waitForViewportToSettle();
    });

    test('says nothing while the node is showing all of its lineage', async ({ page }) => {
      await lineagePage.hoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
      // The column's edge is drawn on hover, marking the point the hover has been taken in
      await lineagePage.checkEdgeBetweenColumnsExists(WAREHOUSE_URN, LINEAGE_COLUMN, DOWNSTREAM_URN, LINEAGE_COLUMN);

      // The node has nothing to hide downstream, so neither does its column
      await expect(page.getByTestId(`column-lineage-control-${LINEAGE_COLUMN}-DOWNSTREAM`)).toHaveCount(0);
    });

    test('reports how much of a column lineage is on the graph once the node contracts', async ({ page }) => {
      await lineagePage.contract(WAREHOUSE_URN);
      // Contracting takes the downstream node off the graph and re-lays out what is left, which
      // moves the column out from under the cursor
      await lineagePage.checkNodeNotExists(DOWNSTREAM_URN);

      // One downstream column exists, and contracting took it off the graph. The readout only
      // lives as long as the hover, and any relayout can steal the hover by moving the column out
      // from under the cursor, so re-hover and retry rather than asserting on a single hover.
      const control = page.getByTestId(`column-lineage-control-${LINEAGE_COLUMN}-DOWNSTREAM`);
      await expect(async () => {
        await lineagePage.hoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
        await expect(control).toHaveText(/0 \/ 1/, { timeout: TIMEOUTS.SHORT });
      }).toPass({ timeout: TIMEOUTS.EXTRA_LONG });
    });

    test('leaves a column whose only lineage is to its sibling out of it', async ({ page }) => {
      // Nothing renders in response to hovering the column under test, so take the graph through a
      // hover it does react to first: by the time that edge comes and goes, hovers are being read
      await lineagePage.hoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
      await lineagePage.checkEdgeBetweenColumnsExists(WAREHOUSE_URN, LINEAGE_COLUMN, DOWNSTREAM_URN, LINEAGE_COLUMN);
      await lineagePage.unhoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
      await lineagePage.checkEdgeBetweenColumnsNotExists(WAREHOUSE_URN, LINEAGE_COLUMN, DOWNSTREAM_URN, LINEAGE_COLUMN);

      await lineagePage.hoverColumn(WAREHOUSE_URN, SIBLING_ONLY_COLUMN);

      // A sibling is drawn folded into this node, so that lineage can never be drawn: the
      // column has none, and gets no control in either direction
      await expect(page.getByTestId(`column-lineage-control-${SIBLING_ONLY_COLUMN}-DOWNSTREAM`)).toHaveCount(0);
      await expect(page.getByTestId(`column-lineage-control-${SIBLING_ONLY_COLUMN}-UPSTREAM`)).toHaveCount(0);
    });

    test('highlights nothing elsewhere for a column whose only lineage is to its sibling', async ({ page }) => {
      // Columns light up by taking on a background; every other column on the graph is transparent
      const countHighlighted = () =>
        page.evaluate((homeUrn) => {
          const home = document.querySelector(`[data-testid="lineage-node-${homeUrn}"]`);
          const columns = document.querySelectorAll(
            '[data-testid^="column-"]:not([data-testid^="column-lineage-control-"])',
          );
          const highlighted = Array.from(columns).filter(
            (element) => getComputedStyle(element).backgroundColor !== 'rgba(0, 0, 0, 0)',
          );
          return {
            home: highlighted.filter((element) => home?.contains(element)).length,
            elsewhere: highlighted.filter((element) => !home?.contains(element)).length,
          };
        }, WAREHOUSE_URN);

      // As above: take the graph through a hover it does react to first, so that the interaction
      // under test is read by a graph that has already been through the whole cycle once
      await lineagePage.hoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
      await lineagePage.checkEdgeBetweenColumnsExists(WAREHOUSE_URN, LINEAGE_COLUMN, DOWNSTREAM_URN, LINEAGE_COLUMN);
      await lineagePage.unhoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
      await expect.poll(countHighlighted, { timeout: TIMEOUTS.MEDIUM }).toEqual({ home: 0, elsewhere: 0 });

      // Hovering the column first, as a user does on the way to clicking it: drawing a column's
      // edges moves the nodes apart, which slides the column out from under an immediate click
      await lineagePage.hoverColumn(WAREHOUSE_URN, SIBLING_ONLY_COLUMN);
      await lineagePage.selectColumn(WAREHOUSE_URN, SIBLING_ONLY_COLUMN);
      // Long enough for the graph to have lit something up, there being nothing to wait for when
      // the expected outcome is that nothing happens
      // eslint-disable-next-line playwright/no-wait-for-timeout
      await page.waitForTimeout(TIMEOUTS.SHORT);

      // A sibling is drawn folded into this node, so that lineage can never be drawn: nothing
      // outside the column's own node lights up
      expect((await countHighlighted()).elsewhere).toEqual(0);
    });
  });
}
