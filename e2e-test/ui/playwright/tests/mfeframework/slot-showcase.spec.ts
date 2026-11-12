/**
 * MFE framework — every slot at once.
 *
 * One placeholder micro frontend is placed into each slot DataHub exposes. It renders nothing but the name
 * of the slot it was mounted into (taken from `ctx.slot`), so each test shows where a slot sits on the page
 * and proves the host mounted the placeholder there with the right context. The screenshots double as a
 * visual reference for where an MFE can be added.
 *
 * Seeds one dataset (with columns) from tests/mfeframework/fixtures/data.json.
 */

import type { Locator, Page, TestInfo } from '@playwright/test';

import { test, expect } from '../../fixtures/base-test';
import { MFEFrameworkPage } from '../../pages/mfe-framework.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'mfeframework', viewport: { width: 1600, height: 1000 } });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,mfe_slot_test_dataset,PROD)';
const COLUMN = 'col_a';

const entry = (id: string, slot: string, extra = '') => `
  - id: ${id}
    label: Placeholder ${slot}
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: placeholderMFE/mount
    flags:
      enabled: true
      showInNav: false
    placement:
      slot: ${slot}
      contractVersion: "1.0.0"
      entityTypes: [dataset]
${extra}`;

const SHOWCASE_MFE = `subNavigationMode: false
microFrontends:
  - id: placeholder-page
    label: Placeholder App
    path: /placeholder-app
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: placeholderMFE/mount
    flags:
      enabled: true
      showInNav: true
    navIcon: AppWindow
${entry('placeholder-tab', 'entity.detail.tab', '      after: Summary\n')}${entry('placeholder-summary-top', 'entity.summary.top')}${entry(
  'placeholder-summary-bottom',
  'entity.summary.bottom',
)}${entry('placeholder-sidebar', 'entity.sidebar.section', '      after: notes\n')}${entry(
  'placeholder-column',
  'schemaField.drawer.section',
)}`;

/** A placeholder remote: a dashed box naming the slot it was mounted into. */
const PLACEHOLDER_REMOTE = `
window.placeholderMFE = {
  init: function() {},
  get: function() {
    return Promise.resolve(() => function mount(el, ctx) {
      var box = document.createElement('div');
      box.setAttribute('data-testid', 'placeholder-' + ctx.slot);
      box.style.cssText = 'margin:4px;padding:18px 16px;border:2px dashed #7c5cff;border-radius:10px;' +
        'background:#f5f2ff;color:#4b2fd6;font:600 15px/1.3 ui-monospace,Menlo,monospace;text-align:center';
      box.textContent = ctx.slot;
      el.replaceChildren(box);
      return function () { el.replaceChildren(); };
    });
  }
};`;

/** Attach a screenshot once `target` has stopped moving (drawers slide in), so the report shows the slot in place. */
async function attachSlotScreenshot(page: Page, testInfo: TestInfo, name: string, target: Locator): Promise<void> {
  await target.scrollIntoViewIfNeeded();
  await expect
    .poll(async () => {
      const a = await target.boundingBox();
      await page.waitForTimeout(150);
      const b = await target.boundingBox();
      return !!a && !!b && a.x === b.x && a.y === b.y;
    })
    .toBe(true);
  await testInfo.attach(name, { body: await page.screenshot(), contentType: 'image/png' });
}

test.describe('MFE Framework — a placeholder in every slot', () => {
  let mfePage: MFEFrameworkPage;

  test.beforeEach(async ({ page }) => {
    mfePage = new MFEFrameworkPage(page);
    await mfePage.mockFetchForMFEConfig(SHOWCASE_MFE);
    await mfePage.mockRemoteEntry(200, PLACEHOLDER_REMOTE);
  });

  const placeholder = (slot: string) => mfePage.page.getByTestId(`placeholder-${slot}`);

  test('nav.page: a full page reached from the left navigation', async ({ page }, testInfo) => {
    await page.goto('/mfe/placeholder-app');
    await expect(placeholder('nav.page')).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.mfeItemByName('Placeholder App')).toBeVisible();
    await attachSlotScreenshot(page, testInfo, 'nav.page', placeholder('nav.page'));
  });

  test('entity.detail.tab: a tab on the entity page, placed right after Summary', async ({ page }, testInfo) => {
    await mfePage.gotoDataset(DATASET_URN, 'mfe-placeholder-tab');
    await expect(placeholder('entity.detail.tab')).toBeVisible({ timeout: TIMEOUTS.LONG });
    const tabs = await page
      .getByTestId(/-entity-tab-header$/)
      .evaluateAll((els) => els.map((el) => el.getAttribute('data-testid')));
    expect(tabs.indexOf('mfe-placeholder-tab-entity-tab-header')).toBe(tabs.indexOf('Summary-entity-tab-header') + 1);
    await attachSlotScreenshot(page, testInfo, 'entity.detail.tab', placeholder('entity.detail.tab'));
  });

  test('entity.summary.top and entity.summary.bottom: inline on the Summary tab', async ({ page }, testInfo) => {
    await mfePage.gotoDataset(DATASET_URN, 'Summary');
    const top = placeholder('entity.summary.top');
    const bottom = placeholder('entity.summary.bottom');
    await expect(top).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(bottom).toBeAttached();
    const topBox = await top.boundingBox();
    const bottomBox = await bottom.boundingBox();
    expect(topBox && bottomBox && topBox.y < bottomBox.y).toBe(true);
    await attachSlotScreenshot(page, testInfo, 'entity.summary.top', top);
    await attachSlotScreenshot(page, testInfo, 'entity.summary.bottom', bottom);
  });

  test('entity.sidebar.section: a section of the right-hand sidebar', async ({ page }, testInfo) => {
    await mfePage.gotoDataset(DATASET_URN, 'Summary');
    await expect(placeholder('entity.sidebar.section')).toBeVisible({ timeout: TIMEOUTS.LONG });
    await attachSlotScreenshot(page, testInfo, 'entity.sidebar.section', placeholder('entity.sidebar.section'));
  });

  test('schemaField.drawer.section: a section of the column drawer', async ({ page }, testInfo) => {
    await mfePage.gotoDataset(DATASET_URN, 'Columns');
    await page.getByText(COLUMN, { exact: true }).click();
    await expect(placeholder('schemaField.drawer.section')).toBeVisible({ timeout: TIMEOUTS.LONG });
    await attachSlotScreenshot(page, testInfo, 'schemaField.drawer.section', placeholder('schemaField.drawer.section'));
  });
});
