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
        });

        test('says nothing while the node is showing all of its lineage', async ({ page }) => {
            await lineagePage.hoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);
            await page.waitForTimeout(TIMEOUTS.SHORT);

            // The node has nothing to hide downstream, so neither does its column
            await expect(page.getByTestId(`column-lineage-control-${LINEAGE_COLUMN}-DOWNSTREAM`)).toHaveCount(0);
        });

        test('reports how much of a column lineage is on the graph once the node contracts', async ({ page }) => {
            await lineagePage.contract(WAREHOUSE_URN);
            // Contracting re-lays out the graph, which moves the column out from under the cursor
            await page.waitForTimeout(TIMEOUTS.MEDIUM);
            await lineagePage.hoverColumn(WAREHOUSE_URN, LINEAGE_COLUMN);

            // One downstream column exists, and contracting took it off the graph
            const control = page.getByTestId(`column-lineage-control-${LINEAGE_COLUMN}-DOWNSTREAM`);
            await expect(control).toHaveText(/0 \/ 1/, { timeout: TIMEOUTS.MEDIUM });
        });

        test('leaves a column whose only lineage is to its sibling out of it', async ({ page }) => {
            await lineagePage.hoverColumn(WAREHOUSE_URN, SIBLING_ONLY_COLUMN);
            await page.waitForTimeout(TIMEOUTS.SHORT);

            // A sibling is drawn folded into this node, so that lineage can never be drawn: the
            // column has none, and gets no control in either direction
            await expect(page.getByTestId(`column-lineage-control-${SIBLING_ONLY_COLUMN}-DOWNSTREAM`)).toHaveCount(0);
            await expect(page.getByTestId(`column-lineage-control-${SIBLING_ONLY_COLUMN}-UPSTREAM`)).toHaveCount(0);
        });

        test('highlights nothing elsewhere for a column whose only lineage is to its sibling', async ({ page }) => {
            await lineagePage.selectColumn(WAREHOUSE_URN, SIBLING_ONLY_COLUMN);
            await page.waitForTimeout(TIMEOUTS.SHORT);

            const highlightedElsewhere = await page.evaluate((homeUrn) => {
                const home = document.querySelector(`[data-testid="lineage-node-${homeUrn}"]`);
                return Array.from(document.querySelectorAll('[data-testid^="column-"]')).filter(
                    (element) =>
                        !home?.contains(element) && getComputedStyle(element).backgroundColor !== 'rgba(0, 0, 0, 0)',
                ).length;
            }, WAREHOUSE_URN);

            expect(highlightedElsewhere).toEqual(0);
        });
    });
}
