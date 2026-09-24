/**
 * Data product lineage — home bounding box members.
 *
 * Fixture data (24 member datasets, lineage between them, and the data product that owns them) is
 * seeded from fixtures/data.json via test.use({ featureName: 'data-product-lineage' }). The member
 * count exceeds DATA_PRODUCT_MEMBER_PAGE_SIZE so seeding is paginated, and most members form a
 * lineage chain, so members left out of the fetched page are still pulled into the box by the
 * neighbour that was fetched — the box therefore holds more members than a page.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'data-product-lineage' });

const DATA_PRODUCT_URN = 'urn:li:dataProduct:pw_dpbx_product';
const TOTAL_MEMBERS = 24;
/** Mirrors DATA_PRODUCT_MEMBER_PAGE_SIZE in the app. */
const MEMBER_PAGE_SIZE = 20;
const MEMBER_TESTID_PREFIX = 'lineage-node-urn:li:dataset:(urn:li:dataPlatform:snowflake,pw_dpbx.pw_dpbx_';

/** A bounding box renders its wrapper and its header card under the same testid. */
const node = (page: import('@playwright/test').Page, urn: string) =>
  // eslint-disable-next-line playwright/no-nth-methods -- encapsulated: the box's two wrappers share a testid
  page.getByTestId(`lineage-node-${urn}`).first();

test.describe('Data product lineage bounding box', () => {
  let lineagePage: LineageV3Page;

  /** Member nodes drawn in the graph, by the data-testid NodeWrapper puts on each. */
  const memberNodes = (page: import('@playwright/test').Page) =>
    // eslint-disable-next-line playwright/no-raw-locators -- prefix match on the node testid; getByTestId has no "starts with" form
    page.locator(`[data-testid^="${MEMBER_TESTID_PREFIX}"]`);

  // eslint-disable-next-line playwright/no-nth-methods -- encapsulated: any one drawn member will do
  const firstMemberNode = (page: import('@playwright/test').Page) => memberNodes(page).first();

  /** Members keep arriving in batches as their entities load; wait for the graph to stop growing. */
  const waitForGraphSettled = async (page: import('@playwright/test').Page) => {
    let previous = -1;
    await expect(async () => {
      const current = await memberNodes(page).count();
      const settled = current > 0 && current === previous;
      previous = current;
      expect(settled).toBe(true);
    }).toPass({ intervals: [1500], timeout: TIMEOUTS.EXTRA_LONG * 2 });
  };

  test.beforeEach(async ({ page, logger, logDir }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);
    await lineagePage.goToLineageGraph('dataProduct', DATA_PRODUCT_URN);
    await expect(node(page, DATA_PRODUCT_URN)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await waitForGraphSettled(page);
  });

  test('draws the product members inside the home bounding box', async ({ page }) => {
    await expect(node(page, DATA_PRODUCT_URN)).toBeVisible();
    await expect(firstMemberNode(page)).toBeVisible();
  });

  test('member count reports the members actually drawn, not the page size', async ({ page }) => {
    const drawn = await memberNodes(page).count();

    // Lineage pulls in members past the fetched page, so the box holds more than a page of them
    expect(drawn).toBeGreaterThan(MEMBER_PAGE_SIZE);
    await expect(page.getByText(`${drawn} / ${TOTAL_MEMBERS} assets`)).toBeVisible();
  });
});
