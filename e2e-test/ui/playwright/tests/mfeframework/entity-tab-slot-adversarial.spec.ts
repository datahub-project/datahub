/**
 * MFE framework — how the host behaves when a slot-placed remote misbehaves.
 *
 * Each test drives the entity page with a mocked /mfe/config and a remoteEntry.js that is slow, unreachable,
 * oversized, or steers the page. The host must keep its own chrome working in every case.
 */

import { test, expect } from '../../fixtures/base-test';
import { MFEFrameworkPage } from '../../pages/mfe-framework.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'mfeframework' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,mfe_slot_test_dataset,PROD)';
const TAB_LABEL = 'Rogue Stub';
const TAB_NAME = 'Rogue';
const HOST_LOAD_TIMEOUT_MS = 5000;

const TAB_MFE = `subNavigationMode: false
microFrontends:
  - id: rogue-stub
    label: ${TAB_LABEL}
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: rogueMFE/mount
    flags:
      enabled: true
      showInNav: false
    placement:
      slot: entity.detail.tab
      tabName: "${TAB_NAME}"`;

/** Module Federation `var` remote whose mount runs the given body (a JS source string using `el` and `ctx`). */
const remoteWithMount = (body: string) => `
window.rogueMFE = {
  init: function() {},
  get: function(module) {
    return Promise.resolve(() => function mount(el, ctx) { ${body} return () => {}; });
  }
};`;

const WIDE_REMOTE = remoteWithMount(`
  const bar = document.createElement('div');
  bar.setAttribute('data-testid', 'rogue-wide');
  bar.style.cssText = 'width:4000px;height:24px;background:#8080ff';
  el.replaceChildren(bar);`);

const TALL_REMOTE = remoteWithMount(`
  for (let i = 1; i <= 150; i++) {
    const row = document.createElement('div');
    row.setAttribute('data-testid', 'rogue-row');
    row.style.cssText = 'height:40px';
    row.textContent = 'row ' + i;
    el.appendChild(row);
  }`);

const NAVIGATE_REMOTE = remoteWithMount(`window.location.assign('/glossary');`);

test.describe('MFE Framework — misbehaving entity.detail.tab remotes', () => {
  let mfePage: MFEFrameworkPage;

  test.beforeEach(async ({ page }) => {
    mfePage = new MFEFrameworkPage(page);
    await mfePage.mockFetchForMFEConfig(TAB_MFE);
  });

  test('a remote that takes longer than the host timeout shows the not-available state', async ({ page }) => {
    await page.route('**/remoteEntry.js', async (route) => {
      await new Promise((resolve) => {
        setTimeout(resolve, HOST_LOAD_TIMEOUT_MS + 3000);
      });
      await route.fulfill({ status: 200, contentType: 'application/javascript', body: WIDE_REMOTE });
    });
    await mfePage.gotoDataset(DATASET_URN);
    await mfePage.entityTabHeader(TAB_NAME).click();

    await expect(mfePage.errorMessage(TAB_LABEL)).toBeVisible({ timeout: HOST_LOAD_TIMEOUT_MS + TIMEOUTS.LONG });
    await expect(mfePage.entityHeader()).toBeVisible();
  });

  test('an unreachable remote (connection refused) shows the not-available state', async ({ page }) => {
    await page.route('**/remoteEntry.js', (route) => route.abort('connectionrefused'));
    await mfePage.gotoDataset(DATASET_URN);
    await mfePage.entityTabHeader(TAB_NAME).click();

    await expect(mfePage.errorMessage(TAB_LABEL)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.entityHeader()).toBeVisible();
  });

  test('content wider than the pane does not add horizontal scroll to the page', async ({ page }) => {
    await mfePage.mockRemoteEntry(200, WIDE_REMOTE);
    await mfePage.gotoDataset(DATASET_URN);
    await mfePage.entityTabHeader(TAB_NAME).click();
    await expect(page.getByTestId('rogue-wide')).toBeAttached({ timeout: TIMEOUTS.LONG });

    const overflow = await page.evaluate(() => ({
      page: document.documentElement.scrollWidth - document.documentElement.clientWidth,
      body: document.body.scrollWidth - document.body.clientWidth,
    }));
    expect(overflow.page).toBe(0);
    expect(overflow.body).toBe(0);
    // the sidebar and header are still where they belong
    await expect(mfePage.navSidebar()).toBeInViewport();
    await expect(mfePage.entityHeader()).toBeInViewport();
  });

  test('content taller than the pane scrolls inside the tab, not the whole page', async ({ page }) => {
    await mfePage.mockRemoteEntry(200, TALL_REMOTE);
    await mfePage.gotoDataset(DATASET_URN);
    await mfePage.entityTabHeader(TAB_NAME).click();
    await expect(page.getByText('row 1', { exact: true })).toBeVisible({ timeout: TIMEOUTS.LONG });

    const pageScroll = await page.evaluate(
      () => document.documentElement.scrollHeight - document.documentElement.clientHeight,
    );
    expect(pageScroll).toBe(0);
    await page.getByText('row 150', { exact: true }).scrollIntoViewIfNeeded();
    await expect(page.getByText('row 150', { exact: true })).toBeInViewport();
    await expect(mfePage.entityHeader()).toBeInViewport();
    await expect(mfePage.entityTabHeader(TAB_NAME)).toBeInViewport();
  });

  test('a remote that navigates the window steers the host (same-origin, not sandboxed)', async ({ page }) => {
    await mfePage.mockRemoteEntry(200, NAVIGATE_REMOTE);
    await mfePage.gotoDataset(DATASET_URN);
    await mfePage.entityTabHeader(TAB_NAME).click();

    // Documents current behaviour: MFEs run in the host's JS context and can call window.location.
    await expect(page).toHaveURL(/\/glossary/, { timeout: TIMEOUTS.LONG });
  });
});
