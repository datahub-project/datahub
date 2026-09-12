/**
 * Semantic Model and Metric lineage topologies.
 *
 * Covers bounding-box membership, canonical Metric → SMD → physical chains,
 * derived metrics, standalone metrics, multi-input metrics, and a BI boundary hop.
 *
 * Prerequisites: fixtures/data.json (featureName: 'metrics')
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';
import {
  CUSTOMERS_LOGICAL_URN,
  DOUBLE_REVENUE_URN,
  METRICS_FEATURE_FLAGS,
  ORDERS_CHART_URN,
  ORDERS_DASHBOARD_URN,
  ORDERS_LOGICAL_URN,
  PHYS_ORDERS_URN,
  PHYS_STANDALONE_URN,
  REVENUE_PER_CUSTOMER_URN,
  SEMANTIC_MODEL_ORDERS_URN,
  STANDALONE_EVENT_COUNT_URN,
  TOTAL_REVENUE_URN,
} from './constants';

test.use({ featureName: 'metrics' });

test.describe('Metrics lineage topologies', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(METRICS_FEATURE_FLAGS);
    lineagePage = new LineageV3Page(page, logger, logDir);
  });

  async function loadLineageGraph(entityType: string, urn: string): Promise<void> {
    await lineagePage.goToLineageGraph(entityType, urn);
    await lineagePage.waitForGraphToRender();
  }

  test('semantic model root draws members inside a bounding box', async () => {
    await loadLineageGraph('semanticModel', SEMANTIC_MODEL_ORDERS_URN);

    await expect(lineagePage.getPrimaryNode(SEMANTIC_MODEL_ORDERS_URN)).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
    await lineagePage.checkNodeExists(ORDERS_LOGICAL_URN);
    await lineagePage.checkNodeExists(CUSTOMERS_LOGICAL_URN);
    await lineagePage.checkNodeExists(TOTAL_REVENUE_URN);
  });

  test('expanding a semantic model member reveals physical and BI lineage', async () => {
    test.setTimeout(90000);
    // Confirm the member is present on the Semantic Model bounding-box graph, then open the
    // member as lineage home. Default 1-hop already surfaces physical upstream + chart;
    // expand the chart for the dashboard hop.
    await loadLineageGraph('semanticModel', SEMANTIC_MODEL_ORDERS_URN);
    await lineagePage.checkNodeExists(ORDERS_LOGICAL_URN);

    await loadLineageGraph('dataset', ORDERS_LOGICAL_URN);

    await lineagePage.checkNodeExists(PHYS_ORDERS_URN);
    await lineagePage.checkEdgeExists(PHYS_ORDERS_URN, ORDERS_LOGICAL_URN);
    await lineagePage.checkNodeExists(ORDERS_CHART_URN);
    await lineagePage.checkEdgeExists(ORDERS_LOGICAL_URN, ORDERS_CHART_URN);

    await lineagePage.expandOne(ORDERS_CHART_URN);
    await lineagePage.checkNodeExists(ORDERS_DASHBOARD_URN);
    await lineagePage.checkEdgeExists(ORDERS_CHART_URN, ORDERS_DASHBOARD_URN);
  });

  test('model-backed metric shows logical and physical upstream path', async () => {
    await loadLineageGraph('metric', TOTAL_REVENUE_URN);

    await lineagePage.checkNodeExists(TOTAL_REVENUE_URN);
    await lineagePage.checkNodeExists(ORDERS_LOGICAL_URN);
    await lineagePage.checkEdgeExists(ORDERS_LOGICAL_URN, TOTAL_REVENUE_URN);

    await lineagePage.expandOne(ORDERS_LOGICAL_URN);
    await lineagePage.checkNodeExists(PHYS_ORDERS_URN);
    await lineagePage.checkEdgeExists(PHYS_ORDERS_URN, ORDERS_LOGICAL_URN);
  });

  test('derived metric shows base metric lineage', async () => {
    await loadLineageGraph('metric', DOUBLE_REVENUE_URN);

    await lineagePage.checkNodeExists(DOUBLE_REVENUE_URN);
    await lineagePage.checkNodeExists(TOTAL_REVENUE_URN);
    await lineagePage.checkEdgeExists(TOTAL_REVENUE_URN, DOUBLE_REVENUE_URN);
    // Dataset path retained on the derived metric
    await lineagePage.checkNodeExists(ORDERS_LOGICAL_URN);
    await lineagePage.checkEdgeExists(ORDERS_LOGICAL_URN, DOUBLE_REVENUE_URN);
  });

  test('standalone metric links directly to a physical dataset', async () => {
    await loadLineageGraph('metric', STANDALONE_EVENT_COUNT_URN);

    await lineagePage.checkNodeExists(STANDALONE_EVENT_COUNT_URN);
    await lineagePage.checkNodeExists(PHYS_STANDALONE_URN);
    await lineagePage.checkEdgeExists(PHYS_STANDALONE_URN, STANDALONE_EVENT_COUNT_URN);
  });

  test('multi-input metric keeps dataset and metric upstreams', async () => {
    await loadLineageGraph('metric', REVENUE_PER_CUSTOMER_URN);

    await lineagePage.checkNodeExists(REVENUE_PER_CUSTOMER_URN);
    await lineagePage.checkNodeExists(ORDERS_LOGICAL_URN);
    await lineagePage.checkNodeExists(CUSTOMERS_LOGICAL_URN);
    await lineagePage.checkNodeExists(TOTAL_REVENUE_URN);
    await lineagePage.checkEdgeExists(ORDERS_LOGICAL_URN, REVENUE_PER_CUSTOMER_URN);
    await lineagePage.checkEdgeExists(CUSTOMERS_LOGICAL_URN, REVENUE_PER_CUSTOMER_URN);
    await lineagePage.checkEdgeExists(TOTAL_REVENUE_URN, REVENUE_PER_CUSTOMER_URN);
  });
});
