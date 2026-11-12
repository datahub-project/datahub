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
 * Seeds (tests/lineage-edit/fixtures/data.json): pw_lineage_edit_subject, pw_lineage_edit_upstream and
 * pw_lineage_edit_downstream. Edges between them are cleared over GraphQL before and after every test,
 * so a failed run cannot leave lineage behind for the next one.
 */

import type { Page } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';

test.use({ featureName: 'lineage-edit' });

const DATASET_ENTITY_TYPE = 'dataset';
const UPSTREAM_SUBJECT_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_upstream_subject,PROD)';
const DOWNSTREAM_SUBJECT_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_downstream_subject,PROD)';
const UPSTREAM_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_upstream,PROD)';
const DOWNSTREAM_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_lineage_edit_downstream,PROD)';
const UPSTREAM_NAME = 'pw_lineage_edit_upstream';
const DOWNSTREAM_NAME = 'pw_lineage_edit_downstream';

const BASE_FEATURE_FLAGS = {
  themeV2Enabled: true,
  themeV2Default: true,
  showNavBarRedesign: true,
};

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

  async function clearEdge(edge: { upstreamUrn: string; downstreamUrn: string }): Promise<void> {
    await graphql.executeQuery(UPDATE_LINEAGE_MUTATION, { input: { edgesToAdd: [], edgesToRemove: [edge] } });
  }

  /**
   * Reload until the graph shows the edge (or no longer shows it). Lineage edges come from the graph
   * index, which catches up shortly after a save, so a single reload can briefly see stale state.
   */
  async function expectEdgeAfterReload(
    page: Page,
    upstreamUrn: string,
    downstreamUrn: string,
    present: boolean,
  ): Promise<void> {
    await expect(async () => {
      await page.reload();
      if (present) {
        await lineagePage.checkEdgeExists(upstreamUrn, downstreamUrn);
      } else {
        await lineagePage.checkEdgeNotExists(upstreamUrn, downstreamUrn);
      }
    }).toPass({ timeout: TIMEOUTS.EXTRA_LONG * 2 });
  }

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    graphql = new GraphQLHelper(page);
    await apiMock.setFeatureFlags(BASE_FEATURE_FLAGS);
    await page.goto('/');
  });

  test('adds an upstream, keeps it after a reload, then removes it', async ({ page }) => {
    // Each test owns its edge and resets it before and after, so a failed run cannot leave lineage
    // behind for the next one.
    const edge = { upstreamUrn: UPSTREAM_URN, downstreamUrn: UPSTREAM_SUBJECT_URN };
    await clearEdge(edge);
    try {
      await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, UPSTREAM_SUBJECT_URN);
      await lineagePage.checkEdgeNotExists(UPSTREAM_URN, UPSTREAM_SUBJECT_URN);

      await lineagePage.openManageLineageMenu(UPSTREAM_SUBJECT_URN);
      await lineagePage.clickEditUpstreamLineage();
      await lineagePage.searchInLineageEditModal(UPSTREAM_NAME);
      await page.getByTestId(`checkbox-${UPSTREAM_URN}`).click();
      await page.getByRole('button', { name: /Set Upstreams/i }).click();

      await lineagePage.checkEdgeExists(UPSTREAM_URN, UPSTREAM_SUBJECT_URN);

      await expectEdgeAfterReload(page, UPSTREAM_URN, UPSTREAM_SUBJECT_URN, true);
      // Manually added edges are drawn dashed; checked after the reload so it reads settled server state.
      await lineagePage.checkEdgeIsManual(UPSTREAM_URN, UPSTREAM_SUBJECT_URN, true);

      await lineagePage.openManageLineageMenu(UPSTREAM_SUBJECT_URN);
      await lineagePage.clickEditUpstreamLineage();
      await lineagePage.expectCurrentLineageContains(UPSTREAM_NAME);
      await lineagePage.searchInLineageEditModal(UPSTREAM_NAME);
      await page.getByTestId(`checkbox-${UPSTREAM_URN}`).click();
      await page.getByRole('button', { name: /Set Upstreams/i }).click();

      await lineagePage.checkEdgeNotExists(UPSTREAM_URN, UPSTREAM_SUBJECT_URN);
      await expectEdgeAfterReload(page, UPSTREAM_URN, UPSTREAM_SUBJECT_URN, false);
    } finally {
      await clearEdge(edge);
    }
  });

  test('adds a downstream, keeps it after a reload, then removes it', async ({ page }) => {
    const edge = { upstreamUrn: DOWNSTREAM_SUBJECT_URN, downstreamUrn: DOWNSTREAM_URN };
    await clearEdge(edge);
    try {
      await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, DOWNSTREAM_SUBJECT_URN);
      await lineagePage.checkEdgeNotExists(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN);

      await lineagePage.openManageLineageMenu(DOWNSTREAM_SUBJECT_URN);
      await lineagePage.clickEditDownstreamLineage();
      await lineagePage.searchInLineageEditModal(DOWNSTREAM_NAME);
      await page.getByTestId(`checkbox-${DOWNSTREAM_URN}`).click();
      await page.getByRole('button', { name: /Set Downstreams/i }).click();

      await lineagePage.checkEdgeExists(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN);
      // Manually added edges are drawn dashed; checked after the reload so it reads settled server state.
      await lineagePage.checkEdgeIsManual(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN, true);

      await expectEdgeAfterReload(page, DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN, true);

      await lineagePage.openManageLineageMenu(DOWNSTREAM_SUBJECT_URN);
      await lineagePage.clickEditDownstreamLineage();
      await lineagePage.expectCurrentLineageContains(DOWNSTREAM_NAME);
      await lineagePage.searchInLineageEditModal(DOWNSTREAM_NAME);
      await page.getByTestId(`checkbox-${DOWNSTREAM_URN}`).click();
      await page.getByRole('button', { name: /Set Downstreams/i }).click();

      await lineagePage.checkEdgeNotExists(DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN);
      await expectEdgeAfterReload(page, DOWNSTREAM_SUBJECT_URN, DOWNSTREAM_URN, false);
    } finally {
      await clearEdge(edge);
    }
  });
});
