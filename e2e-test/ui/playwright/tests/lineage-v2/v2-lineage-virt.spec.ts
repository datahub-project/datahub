/**
 * Lineage V2 — virtualisation interop regression tests.
 *
 * Two DOM-boundary interactions need explicit coverage as virt becomes the
 * default at scale:
 *
 *   1. Column-edge highlighting — edge geometry comes from the React Flow
 *      store, not the DOM, so column hover should attach the highlight even
 *      when the off-screen endpoint is virtualised away.
 *   2. Screenshot capture — html-to-image walks the live DOM, so the button
 *      flips `forceMountAll` during capture and reverts afterwards.
 */

import { expect, test } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

const NODE1_URN = 'urn:li:dataset:(urn:li:dataPlatform:postgres,playwright_lineage.node1_dataset,PROD)';
const NODE2_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,playwright_lineage.node2_dataset,PROD)';
const COLUMN_NAME = 'record_id';

test.describe('lineage_graph virtualisation regression', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  test('column-edge highlight renders under virt-on', async ({ page, logger, logDir }) => {
    // Smoke test only — the 3-node fixture is too tightly packed to
    // reliably pan one endpoint off-screen without also unmounting the
    // hover target. Off-screen coverage is in the screenshot test below.
    test.setTimeout(60_000);

    const lp = new LineageV2Page(page, logger, logDir);
    await lp.setPerfFlags('virt');
    await lp.goToLineageGraph('dataset', NODE2_URN);

    await lp.checkNodeExists(NODE2_URN);
    await lp.checkNodeExists(NODE1_URN);
    await lp.expandContractColumns(NODE2_URN);
    await lp.hoverColumn(NODE2_URN, COLUMN_NAME);
    await lp.checkEdgeBetweenColumnsExists(NODE1_URN, COLUMN_NAME, NODE2_URN, COLUMN_NAME);
  });

  test('screenshot capture force-mounts every node under virt-on', async ({ page, logger, logDir }) => {
    test.setTimeout(60_000);

    const lp = new LineageV2Page(page, logger, logDir);
    await lp.setPerfFlags('virt');
    await lp.goToLineageGraph('dataset', NODE2_URN);
    await lp.checkNodeExists(NODE2_URN);
    await lp.checkNodeExists(NODE1_URN);

    // Pan NODE1 off the left edge so virt unmounts it.
    await lp.sustainedPan('right', 700);
    await page.waitForTimeout(200);
    await expect(page.locator(`[data-testid="rf__node-${NODE1_URN}"]`)).toHaveCount(0);

    // Neuter html-to-image's synthetic download so the test doesn't
    // trigger a real browser download dialog mid-run.
    await page.evaluate(() => {
      const proto = HTMLAnchorElement.prototype as HTMLAnchorElement & { __origClick?: () => void };
      if (!proto.__origClick) {
        proto.__origClick = proto.click;
        proto.click = function noopClick() {
          /* swallowed by virt regression test */
        };
      }
    });

    // Watch for NODE1 to remount: the screenshot button flips
    // `forceMountAll=true` before `toPng` walks the DOM.
    const seenAllMountedPromise = page.waitForFunction(
      (node1Selector) => document.querySelector(node1Selector) !== null,
      `[data-testid="rf__node-${NODE1_URN}"]`,
      { timeout: 10_000 },
    );

    // The camera icon is the only selector stable across collapsed
    // / expanded controls panels.
    const screenshotButton = page.locator('button:has([aria-label="camera"])').first();
    await screenshotButton.click();

    await seenAllMountedPromise;

    // After capture, forceMountAll flips back and NODE1 unmounts again.
    await page.waitForTimeout(300);
    await expect(page.locator(`[data-testid="rf__node-${NODE1_URN}"]`)).toHaveCount(0);
  });
});
