/**
 * Lineage Edit Tests
 *
 * Covers editing lineage from the graph's Manage Lineage menu all the way through saving:
 * - Add an upstream, see the manual edge, and see it survive a reload
 * - Add a downstream the same way
 * - Remove each edge again and see it stay gone after a reload
 *
 * The existing lineage specs stop once the edit modal opens; these tests save the change.
 *
 * Seeds (tests/lineage-edit/fixtures/data.json): pw_lineage_edit_upstream_subject, pw_lineage_edit_upstream and
 * pw_lineage_edit_downstream_subject, pw_lineage_edit_downstream, plus one seeded "anchor" edge per subject that
 * the tests never touch. Edges between the subjects and the datasets they add are cleared over GraphQL before
 * and after every test, so a failed run cannot leave lineage behind for the next one.
 */

import type { Page } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';
import { THEME_V2_FLAGS } from '../../utils/test-feature-flags';
import { DomainTag, TestTag } from '../../utils/test-tags';

test.use({ featureName: 'lineage-edit' });

const DATASET_ENTITY_TYPE = 'dataset';
const UPSTREAM_SUBJECT_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_upstream_subject,PROD)';
const DOWNSTREAM_SUBJECT_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_downstream_subject,PROD)';
const UPSTREAM_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_upstream,PROD)';
const DOWNSTREAM_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_downstream,PROD)';
const UPSTREAM_ANCHOR_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_anchor_upstream,PROD)';
const DOWNSTREAM_ANCHOR_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_anchor_downstream,PROD)';
const UPSTREAM_NAME = 'pw_lineage_edit_upstream';
const DOWNSTREAM_NAME = 'pw_lineage_edit_downstream';

interface Edge {
  upstreamUrn: string;
  downstreamUrn: string;
}

// Seeded and never edited: once an anchor edge is drawn, the graph has loaded the subject's lineage.
const UPSTREAM_ANCHOR_EDGE: Edge = { upstreamUrn: UPSTREAM_ANCHOR_URN, downstreamUrn: UPSTREAM_SUBJECT_URN };
const DOWNSTREAM_ANCHOR_EDGE: Edge = { upstreamUrn: DOWNSTREAM_SUBJECT_URN, downstreamUrn: DOWNSTREAM_ANCHOR_URN };

const UPDATE_LINEAGE_MUTATION = `
  mutation updateLineage($input: UpdateLineageInput!) {
    updateLineage(input: $input)
  }
`;

// Run serially: with the two tests editing lineage at the same time, an edge saved by one was
// intermittently never returned by the graph after a reload. Alone they pass reliably.
test.describe.configure({ mode: 'serial' });

test.describe('lineage edit', { tag: [TestTag.P0, DomainTag.Catalog] }, () => {
  let lineagePage: LineageV3Page;
  let graphql: GraphQLHelper;

  async function clearEdge(edge: Edge): Promise<void> {
    const response = await graphql.executeQuery(UPDATE_LINEAGE_MUTATION, {
      input: { edgesToAdd: [], edgesToRemove: [edge] },
    });
    // GraphQL reports a failed mutation in the body of a 200 response, so a leftover edge would otherwise
    // go unnoticed until the next run.
    expect(response.errors, `updateLineage failed: ${JSON.stringify(response.errors)}`).toBeUndefined();
    expect((response.data as { updateLineage?: boolean } | undefined)?.updateLineage).toBe(true);
  }

  /**
   * Reload until the graph shows the edge (or no longer shows it). Lineage edges come from the graph
   * index, which catches up shortly after a save, so a single reload can briefly see stale state. The
   * subject's anchor edge is required first: an empty canvas is also what the graph shows before its
   * lineage has loaded, so without it an absence check would pass on a page that is still loading.
   */
  async function expectEdgeAfterReload(page: Page, anchor: Edge, edge: Edge, present: boolean): Promise<void> {
    await expect(async () => {
      await page.reload();
      await lineagePage.checkEdgeExists(anchor.upstreamUrn, anchor.downstreamUrn);
      if (present) {
        await lineagePage.checkEdgeExists(edge.upstreamUrn, edge.downstreamUrn);
      } else {
        await lineagePage.checkEdgeNotExists(edge.upstreamUrn, edge.downstreamUrn);
      }
    }).toPass({ timeout: TIMEOUTS.EXTRA_LONG * 2 });
  }

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    graphql = new GraphQLHelper(page);
    await apiMock.setFeatureFlags(THEME_V2_FLAGS);
    await page.goto('/');
  });

  test('adds an upstream, keeps it after a reload, then removes it', async ({ page }) => {
    // Each test owns its edge and resets it before and after, so a failed run cannot leave lineage
    // behind for the next one.
    const edge: Edge = { upstreamUrn: UPSTREAM_URN, downstreamUrn: UPSTREAM_SUBJECT_URN };
    await clearEdge(edge);
    try {
      await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, UPSTREAM_SUBJECT_URN);
      await expectEdgeAfterReload(page, UPSTREAM_ANCHOR_EDGE, edge, false);

      await lineagePage.openManageLineageMenu(UPSTREAM_SUBJECT_URN);
      await lineagePage.clickEditUpstreamLineage();
      await lineagePage.searchInLineageEditModal(UPSTREAM_NAME);
      await lineagePage.toggleLineageEditResult(UPSTREAM_URN);
      await lineagePage.clickSetUpstreams();

      await lineagePage.checkEdgeExists(UPSTREAM_URN, UPSTREAM_SUBJECT_URN);

      await expectEdgeAfterReload(page, UPSTREAM_ANCHOR_EDGE, edge, true);
      // Manually added edges are drawn dashed; checked after the reload so it reads settled server state.
      await lineagePage.checkEdgeIsManual(UPSTREAM_URN, UPSTREAM_SUBJECT_URN, true);

      await lineagePage.openManageLineageMenu(UPSTREAM_SUBJECT_URN);
      await lineagePage.clickEditUpstreamLineage();
      await lineagePage.expectCurrentLineageContains(UPSTREAM_NAME);
      await lineagePage.searchInLineageEditModal(UPSTREAM_NAME);
      await lineagePage.toggleLineageEditResult(UPSTREAM_URN);
      await lineagePage.clickSetUpstreams();

      await lineagePage.checkEdgeNotExists(UPSTREAM_URN, UPSTREAM_SUBJECT_URN);
      await expectEdgeAfterReload(page, UPSTREAM_ANCHOR_EDGE, edge, false);
    } finally {
      await clearEdge(edge);
    }
  });

  test('adds a downstream, keeps it after a reload, then removes it', async ({ page }) => {
    const edge: Edge = { upstreamUrn: DOWNSTREAM_SUBJECT_URN, downstreamUrn: DOWNSTREAM_URN };
    await clearEdge(edge);
    try {
      await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DOWNSTREAM_SUBJECT_URN);
      await expectEdgeAfterReload(page, DOWNSTREAM_ANCHOR_EDGE, edge, false);

      await lineagePage.openManageLineageMenu(DOWNSTREAM_SUBJECT_URN);
      await lineagePage.clickEditDownstreamLineage();
      await lineagePage.searchInLineageEditModal(DOWNSTREAM_NAME);
      await lineagePage.toggleLineageEditResult(DOWNSTREAM_URN);
      await lineagePage.clickSetDownstreams();

      await lineagePage.checkEdgeExists(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN);

      await expectEdgeAfterReload(page, DOWNSTREAM_ANCHOR_EDGE, edge, true);
      // Manually added edges are drawn dashed; checked after the reload so it reads settled server state.
      await lineagePage.checkEdgeIsManual(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN, true);

      await lineagePage.openManageLineageMenu(DOWNSTREAM_SUBJECT_URN);
      await lineagePage.clickEditDownstreamLineage();
      await lineagePage.expectCurrentLineageContains(DOWNSTREAM_NAME);
      await lineagePage.searchInLineageEditModal(DOWNSTREAM_NAME);
      await lineagePage.toggleLineageEditResult(DOWNSTREAM_URN);
      await lineagePage.clickSetDownstreams();

      await lineagePage.checkEdgeNotExists(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN);
      await expectEdgeAfterReload(page, DOWNSTREAM_ANCHOR_EDGE, edge, false);
    } finally {
      await clearEdge(edge);
    }
  });
});
