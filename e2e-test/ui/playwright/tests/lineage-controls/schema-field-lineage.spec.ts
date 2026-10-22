/**
 * A schema field's own lineage graph — the view reached from a column, at
 * /schemaField/{urn}/Lineage, where every node is a column rather than a table.
 *
 * DATA SEEDING: static datasets from fixtures/data.json (auto-seeded via test.use below).
 *
 *   sf_a ──breed──▶ sf_b ──breed──▶ sf_c
 *
 * Opened on sf_a.breed, the graph draws one hop by default: sf_b.breed. Expanding that node has
 * to reach sf_c.breed, exactly as expanding a table node does on the table graph.
 */

import { test } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { BASE_FEATURE_FLAGS, SF_A_URN, SF_B_URN, SF_C_URN, schemaField } from './constants';

test.use({ featureName: 'lineage-controls' });

const A_BREED = schemaField(SF_A_URN, 'breed');
const B_BREED = schemaField(SF_B_URN, 'breed');
const C_BREED = schemaField(SF_C_URN, 'breed');

test.describe('schema field lineage graph', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await apiMock.setFeatureFlags(BASE_FEATURE_FLAGS);
  });

  test('draws the field graph and expands it a hop at a time', async () => {
    await lineagePage.goToSchemaFieldLineage(A_BREED);
    await lineagePage.waitForGraphToRender();

    await lineagePage.checkNodeExists(A_BREED);
    await lineagePage.checkNodeExists(B_BREED);
    await lineagePage.checkEdgeExists(A_BREED, B_BREED);
    // Only one hop is drawn to begin with
    await lineagePage.checkNodeNotExists(C_BREED);

    await lineagePage.expandOne(B_BREED);
    await lineagePage.checkNodeExists(C_BREED);
    await lineagePage.checkEdgeExists(B_BREED, C_BREED);

    await lineagePage.contract(B_BREED);
    await lineagePage.checkNodeNotExists(C_BREED);
  });
});
