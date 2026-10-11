/**
 * Column lineage counts for columns read by charts, when some of those charts are hidden by
 * lineage pagination.
 *
 * DATA SEEDING: static entities from fixtures/data.json (auto-seeded via test.use below).
 *
 * The seeded graph is a Looker explore read by 6 charts through their `inputFields`:
 *
 *   pets (explore)  ──customer_id──▶  every chart          (fan-out: 4 drawn, 2 behind the "4/6" control)
 *                   ──field_0k─────▶  chart k only         (drawn or hidden, depending on pagination order)
 *
 * The `n / total` readout beside a hovered column compares what the graph draws against the counts
 * fetched for the column. A chart consuming a column is a `consumesField` edge to the chart itself,
 * not to a schema field, so the counts query has to ask for charts for `total` to be right.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'column-lineage-hidden' });

const EXPLORE_URN = 'urn:li:dataset:(urn:li:dataPlatform:looker,pw_column_hidden.explore.pets,PROD)';
const CHART_IDS = ['01', '02', '03', '04', '05', '06'];
const chartUrn = (id: string) => `urn:li:chart:(looker,pw_column_hidden_chart_${id})`;

const FAN_OUT_COLUMN = 'customer_id'; // Read by every chart, so always partly hidden

test.describe('column lineage counts with hidden chart consumers', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags({
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
      // The column readouts stand in for lineage filter nodes, so they only render without them
      showLineageFilterNodes: false,
    });
    await lineagePage.navigateToDatasetLineage(EXPLORE_URN);
    await lineagePage.waitForGraphToRender();
    await expect(page.getByTestId(`children-shown-${EXPLORE_URN}-DOWNSTREAM`)).toHaveText('4/6', {
      timeout: TIMEOUTS.LONG,
    });
    await lineagePage.expandContractColumns(EXPLORE_URN);
    // The graph fits its viewport on a delay after entity data loads; if that lands after a
    // hover, it slides the column out from under the cursor and everything hover-driven unmounts
    await lineagePage.waitForViewportToSettle();
  });

  /**
   * Hover a column and wait for its downstream readout to settle on `text`. The readout only lives
   * as long as the hover, and a relayout can steal the hover by moving the column out from under
   * the cursor, so re-hover and retry rather than asserting on a single hover.
   */
  async function hoverUntilReadout(column: string, text: RegExp) {
    const control = lineagePage.page.getByTestId(`column-lineage-control-${column}-DOWNSTREAM`);
    await expect(async () => {
      await lineagePage.hoverColumn(EXPLORE_URN, column);
      await expect(control).toHaveText(text, { timeout: TIMEOUTS.SHORT });
    }).toPass({ timeout: TIMEOUTS.EXTRA_LONG });
  }

  test('counts every chart reading the column, drawn or not', async () => {
    // 4 charts are on the graph; the total has to come from the counts query, which sees all 6
    await hoverUntilReadout(FAN_OUT_COLUMN, /4 \/ 6/);
    for (const id of CHART_IDS) {
      if (await lineagePage.getReactFlowNode(chartUrn(id)).isVisible()) {
        await expect(
          lineagePage.getColumnEdge(EXPLORE_URN, FAN_OUT_COLUMN, chartUrn(id), FAN_OUT_COLUMN),
        ).toBeAttached();
      }
    }
  });

  test('reads 0 / 1 for a column whose only chart is hidden, and 1 / 1 once it is drawn', async () => {
    let hiddenColumns = 0;
    for (const id of CHART_IDS) {
      const column = `field_${id}`;
      const chartShown = await lineagePage.getReactFlowNode(chartUrn(id)).isVisible();
      await hoverUntilReadout(column, chartShown ? /1 \/ 1/ : /0 \/ 1/);
      if (!chartShown) hiddenColumns += 1;
      await lineagePage.unhoverColumn(EXPLORE_URN, column);
    }
    expect(hiddenColumns).toBe(2);
  });
});
