/**
 * Column-level lineage highlighting — the arrows drawn between columns when a column is
 * hovered or selected, including arrows routed through transformational nodes (dbt models,
 * data jobs) and query nodes.
 *
 * DATA SEEDING: static datasets from fixtures/data.json (auto-seeded via test.use below).
 *
 * The seeded graph is three independent chains, each with a different kind of middle node:
 *
 *   query:    cll_source ──amount──▶ cll_staging ──amount──[query]────▶ cll_final.total
 *   dbt:      cll_raw ──metric──▶ [cll_model (dbt)] ──metric──▶ cll_mart.metric
 *   datajob:  cll_input ──val──▶ [transform_task (airflow)] ──val──▶ cll_output.val
 *
 * dbt models and data jobs are drawn as transformation nodes; queries as via nodes. In every
 * case, highlighting a column must draw arrow segments to and from the middle node, e.g. for
 * the dbt chain: raw.metric ──▶ model.metric and model.metric ──▶ mart.metric.
 *
 * Regression coverage for:
 *   (i)  column arrows through transformational nodes not rendering at all
 *   (ii) hover-drawn arrows intermittently disappearing (edge-set races)
 */

import type { Locator } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'lineage-cll' });

// Query chain
const SOURCE_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.source,PROD)';
const STAGING_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.staging,PROD)';
const FINAL_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.final,PROD)';
const QUERY_URN = 'urn:li:query:playwright-cll-query';

// dbt chain
const RAW_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.raw,PROD)';
const MODEL_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,playwright_cll.model,PROD)';
const MART_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.mart,PROD)';

// datajob chain
const INPUT_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.input,PROD)';
const JOB_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,playwright_cll_flow,PROD),transform_task)';
const OUTPUT_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.output,PROD)';

// query-mismatch chain: the fine-grained edge's query differs from its table edge's (which has none)
const MISMATCH_SRC_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.mismatch_src,PROD)';
const MISMATCH_DST_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_cll.mismatch_dst,PROD)';
const PHANTOM_QUERY_URN = 'urn:li:query:playwright-cll-phantom';

test.describe('column-level lineage highlighting', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags({
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  /** Open a dataset's lineage graph, verify the given nodes rendered, and expand its columns. */
  async function openLineageAndExpandColumns(homeUrn: string, expectedNodeUrns: string[]): Promise<void> {
    await lineagePage.navigateToDatasetLineage(homeUrn);
    await lineagePage.waitForGraphToRender();
    // The minimap floats over the bottom-right of the canvas; after fitView a column can land
    // under it, which intercepts pointer events and blocks hovers. Let hovers pass through it.
    await lineagePage.page.addStyleTag({ content: '.react-flow__minimap { pointer-events: none !important; }' });
    for (const urn of expectedNodeUrns) {
      await expect(lineagePage.getReactFlowNode(urn)).toBeAttached({ timeout: TIMEOUTS.LONG });
    }
    await lineagePage.expandContractColumns(homeUrn);
    // The graph fits its viewport on a delay after entity data loads; if that lands after a
    // hover, it slides the column out from under the cursor and everything hover-driven unmounts
    await lineagePage.waitForViewportToSettle();
  }

  /** Assert every given arrow segment is in the DOM, with an arrowhead marker. */
  async function checkArrowsDrawn(edges: Locator[]): Promise<void> {
    for (const edge of edges) {
      await expect(edge).toBeAttached({ timeout: TIMEOUTS.MEDIUM });
      await lineagePage.checkEdgeHasArrowMarker(edge);
    }
  }

  /** Assert none of the given arrow segments are in the DOM. */
  async function checkArrowsCleared(edges: Locator[]): Promise<void> {
    for (const edge of edges) {
      await expect(edge).not.toBeAttached({ timeout: TIMEOUTS.SHORT });
    }
  }

  test.describe('through a dbt model', () => {
    // raw.metric ──▶ model.metric ──▶ mart.metric; the dbt model is drawn as a
    // transformation node, and its column refs are real columns on the model
    const dbtPathEdges = () => [
      lineagePage.getColumnEdge(RAW_URN, 'metric', MODEL_URN, 'metric'),
      lineagePage.getColumnEdge(MODEL_URN, 'metric', MART_URN, 'metric'),
    ];

    test('hovering a column draws arrows to and from the dbt model', async ({ page }) => {
      await openLineageAndExpandColumns(RAW_URN, [MODEL_URN, MART_URN]);

      await lineagePage.hoverColumn(RAW_URN, 'metric');
      await checkArrowsDrawn(dbtPathEdges());

      await lineagePage.unhoverColumn(RAW_URN, 'metric');
      await page.mouse.move(0, 0);
      await checkArrowsCleared(dbtPathEdges());
    });

    test('selecting a column draws persistent arrows to and from the dbt model', async ({ page }) => {
      await openLineageAndExpandColumns(RAW_URN, [MODEL_URN, MART_URN]);

      // Hovering first, as a user does on the way to clicking: drawing a column's edges can
      // re-lay out the graph, which slides the column out from under an immediate click
      await lineagePage.hoverColumn(RAW_URN, 'metric');
      await lineagePage.selectColumn(RAW_URN, 'metric');
      await checkArrowsDrawn(dbtPathEdges());

      // Selection must survive the pointer leaving the column (unlike hover highlighting)
      await lineagePage.unhoverColumn(RAW_URN, 'metric');
      await page.mouse.move(0, 0);
      await checkArrowsDrawn(dbtPathEdges());
    });

    test('hovering the downstream column draws the upstream path through the dbt model', async () => {
      await openLineageAndExpandColumns(MART_URN, [MODEL_URN, RAW_URN]);

      await lineagePage.hoverColumn(MART_URN, 'metric');
      await checkArrowsDrawn(dbtPathEdges());
    });
  });

  test.describe('through a data job', () => {
    // input.val ──▶ [transform_task] ──▶ output.val; the airflow task is drawn as a
    // transformation node, and the column edges route through its operation ref
    const dataJobPathEdges = () => [
      lineagePage.getColumnToOperationEdge(INPUT_URN, 'val', JOB_URN),
      lineagePage.getOperationToColumnEdge(JOB_URN, OUTPUT_URN, 'val'),
    ];

    test('hovering a column draws arrows to and from the data job', async ({ page }) => {
      await openLineageAndExpandColumns(INPUT_URN, [JOB_URN, OUTPUT_URN]);

      await lineagePage.hoverColumn(INPUT_URN, 'val');
      await checkArrowsDrawn(dataJobPathEdges());

      await lineagePage.unhoverColumn(INPUT_URN, 'val');
      await page.mouse.move(0, 0);
      await checkArrowsCleared(dataJobPathEdges());
    });

    test('selecting a column draws persistent arrows to and from the data job', async ({ page }) => {
      await openLineageAndExpandColumns(INPUT_URN, [JOB_URN, OUTPUT_URN]);

      await lineagePage.hoverColumn(INPUT_URN, 'val');
      await lineagePage.selectColumn(INPUT_URN, 'val');
      await checkArrowsDrawn(dataJobPathEdges());

      await lineagePage.unhoverColumn(INPUT_URN, 'val');
      await page.mouse.move(0, 0);
      await checkArrowsDrawn(dataJobPathEdges());
    });

    test('hovering the downstream column draws the upstream path through the data job', async () => {
      await openLineageAndExpandColumns(OUTPUT_URN, [JOB_URN, INPUT_URN]);

      await lineagePage.hoverColumn(OUTPUT_URN, 'val');
      await checkArrowsDrawn(dataJobPathEdges());
    });
  });

  test.describe('through a query missing from the graph', () => {
    // The fine-grained edge routes through a query that is not the table edge's query, so the
    // graph never draws a node for it. The arrow must be drawn directly between the columns.
    const directEdge = () => lineagePage.getColumnEdge(MISMATCH_SRC_URN, 'score', MISMATCH_DST_URN, 'score');

    test('hovering a column draws the arrow directly between the columns', async ({ page }) => {
      await openLineageAndExpandColumns(MISMATCH_SRC_URN, [MISMATCH_DST_URN]);
      await expect(lineagePage.getReactFlowNode(PHANTOM_QUERY_URN)).not.toBeAttached();

      await lineagePage.hoverColumn(MISMATCH_SRC_URN, 'score');
      await checkArrowsDrawn([directEdge()]);
      // No segments are drawn to the query, as it has no node to attach to
      await expect(
        lineagePage.getColumnToOperationEdge(MISMATCH_SRC_URN, 'score', PHANTOM_QUERY_URN),
      ).not.toBeAttached();

      await lineagePage.unhoverColumn(MISMATCH_SRC_URN, 'score');
      await page.mouse.move(0, 0);
      await checkArrowsCleared([directEdge()]);
    });

    test('selecting a column draws the direct arrow persistently', async ({ page }) => {
      await openLineageAndExpandColumns(MISMATCH_SRC_URN, [MISMATCH_DST_URN]);

      await lineagePage.hoverColumn(MISMATCH_SRC_URN, 'score');
      await lineagePage.selectColumn(MISMATCH_SRC_URN, 'score');
      await checkArrowsDrawn([directEdge()]);

      await lineagePage.unhoverColumn(MISMATCH_SRC_URN, 'score');
      await page.mouse.move(0, 0);
      await checkArrowsDrawn([directEdge()]);
    });
  });

  test.describe('through a query node', () => {
    // source.amount ──▶ staging.amount ──[query]──▶ final.total
    const queryPathEdges = () => [
      lineagePage.getColumnEdge(SOURCE_URN, 'amount', STAGING_URN, 'amount'),
      lineagePage.getColumnToOperationEdge(STAGING_URN, 'amount', QUERY_URN),
      lineagePage.getOperationToColumnEdge(QUERY_URN, FINAL_URN, 'total'),
    ];

    test('hovering a column draws arrows to and from the correct entities, through the query node', async ({
      page,
    }) => {
      await openLineageAndExpandColumns(STAGING_URN, [SOURCE_URN, QUERY_URN, FINAL_URN]);

      await lineagePage.hoverColumn(STAGING_URN, 'amount');
      await checkArrowsDrawn(queryPathEdges());

      await lineagePage.unhoverColumn(STAGING_URN, 'amount');
      await page.mouse.move(0, 0);
      await checkArrowsCleared(queryPathEdges());
    });

    test('hover arrows come back on every hover, not just the first', async ({ page }) => {
      await openLineageAndExpandColumns(STAGING_URN, [SOURCE_URN, QUERY_URN, FINAL_URN]);

      // Regression: hover-drawn arrows raced the graph's own edge updates and intermittently
      // never appeared (or vanished mid-hover). Cycle hover a few times; each cycle must draw
      // the full path and clear it again.
      for (let cycle = 0; cycle < 3; cycle++) {
        await lineagePage.hoverColumn(STAGING_URN, 'amount');
        await checkArrowsDrawn(queryPathEdges());

        // Physically move the pointer off the column so the next hover is a real re-entry
        await lineagePage.unhoverColumn(STAGING_URN, 'amount');
        await page.mouse.move(0, 0);
        await checkArrowsCleared(queryPathEdges());
      }
    });

    test('selecting a column draws persistent arrows to and from the correct entities', async ({ page }) => {
      await openLineageAndExpandColumns(STAGING_URN, [SOURCE_URN, QUERY_URN, FINAL_URN]);

      await lineagePage.hoverColumn(STAGING_URN, 'amount');
      await checkArrowsDrawn(queryPathEdges());
      await lineagePage.selectColumn(STAGING_URN, 'amount');
      await checkArrowsDrawn(queryPathEdges());

      // Selection must survive the pointer leaving the column (unlike hover highlighting)
      await lineagePage.unhoverColumn(STAGING_URN, 'amount');
      await page.mouse.move(0, 0);
      await checkArrowsDrawn(queryPathEdges());

      // Deselecting clears the arrows
      await lineagePage.selectColumn(STAGING_URN, 'amount');
      await checkArrowsCleared(queryPathEdges());
    });

    test('hovering the downstream column draws the upstream path through the query node', async () => {
      await openLineageAndExpandColumns(FINAL_URN, [QUERY_URN, STAGING_URN]);

      await lineagePage.hoverColumn(FINAL_URN, 'total');
      // Only one degree of lineage is drawn by default, so cll_source is not on this graph;
      // assert the two segments through the query node
      await checkArrowsDrawn([
        lineagePage.getColumnToOperationEdge(STAGING_URN, 'amount', QUERY_URN),
        lineagePage.getOperationToColumnEdge(QUERY_URN, FINAL_URN, 'total'),
      ]);
    });
  });
});
