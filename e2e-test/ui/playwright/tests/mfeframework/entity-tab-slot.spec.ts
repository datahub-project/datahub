/**
 * MFE framework — typed slot placement (`entity.detail.tab`).
 *
 * An MFE declared with `placement.slot: entity.detail.tab` renders as a tab on the entity profile
 * page and receives a typed `EntityDetailTabContext` through `mount(el, ctx)`. These tests drive the
 * host with a mocked /mfe/config and a stub remoteEntry.js that echoes the context it receives.
 *
 * Seeds one dataset from tests/mfeframework/fixtures/data.json.
 */

import { test, expect } from '../../fixtures/base-test';
import { MFEFrameworkPage } from '../../pages/mfe-framework.page';
import { TIMEOUTS } from '../../utils/constants';

test.use({ featureName: 'mfeframework' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,mfe_slot_test_dataset,PROD)';

const TAB_ID = 'access-stub';
const TAB_LABEL = 'Access Stub';
const TAB_NAME = 'One Click Access';
const NAV_LABEL = 'Nav Stub Playwright';
const NAV_PATH = '/nav-stub-mfe';
const SLOT_CONTRACT_VERSION = '1.0.0';

const tabEntry = (overrides = '') => `
  - id: ${TAB_ID}
    label: ${TAB_LABEL}
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: slotStubMFE/mount
    flags:
      enabled: true
      showInNav: false
    placement:
      slot: entity.detail.tab
      tabName: "${TAB_NAME}"
${overrides}`;

const NAV_ENTRY = `
  - id: nav-stub
    label: ${NAV_LABEL}
    path: ${NAV_PATH}
    remoteEntry: http://localhost:3002/remoteEntry.js
    module: slotStubMFE/mount
    flags:
      enabled: true
      showInNav: true
    navIcon: HandWaving`;

const yaml = (...entries: string[]) => `subNavigationMode: false\nmicroFrontends:${entries.join('')}`;

const TAB_MFE = yaml(tabEntry());
const TAB_MFE_DATASET_ONLY = yaml(tabEntry('      entityTypes: [dataset]\n'));
const TAB_MFE_CHART_ONLY = yaml(tabEntry('      entityTypes: [chart]\n'));
const TAB_MFE_DISABLED = yaml(tabEntry().replace('enabled: true', 'enabled: false'));
const MIXED = yaml(NAV_ENTRY, tabEntry());

/**
 * Stub remote in the same `window.<remoteName>` shape as a webpack Module Federation `var` remote.
 * `mount` echoes the typed context into the container so tests can assert what the host passed.
 */
const REMOTE_ENTRY_ECHO_CTX = `
window.slotStubMFE = {
  init: function() {},
  get: function(module) {
    return Promise.resolve(() => {
      return function mount(containerElement, ctx) {
        if (containerElement) {
          const pre = document.createElement('pre');
          pre.setAttribute('data-testid', 'mfe-slot-ctx');
          pre.textContent = JSON.stringify(ctx);
          containerElement.replaceChildren(pre);
        }
        return () => {};
      };
    });
  }
};`;

test.describe('MFE Framework — entity.detail.tab slot', () => {
  let mfePage: MFEFrameworkPage;

  test.beforeEach(async ({ page }) => {
    mfePage = new MFEFrameworkPage(page);
  });

  test('renders the placed MFE as a tab named by placement.tabName', async () => {
    await mfePage.mockFetchForMFEConfig(TAB_MFE);
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.gotoDataset(DATASET_URN);

    await expect(mfePage.entityTabHeader(TAB_NAME)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.entityTabHeader(TAB_NAME)).toContainText(TAB_NAME);
  });

  test('passes a typed EntityDetailTabContext to mount(el, ctx)', async () => {
    await mfePage.mockFetchForMFEConfig(TAB_MFE);
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.gotoDataset(DATASET_URN);

    await mfePage.entityTabHeader(TAB_NAME).click();
    await expect(mfePage.slotContainer()).toBeVisible({ timeout: TIMEOUTS.LONG });

    const ctx = await mfePage.readSlotCtx();
    expect(ctx.slot).toBe('entity.detail.tab');
    expect(ctx.version).toBe(SLOT_CONTRACT_VERSION);
    expect(ctx.entity).toEqual({ urn: DATASET_URN, type: 'DATASET' });
    // principal is optional in the base contract, but the host fills it for an authenticated session
    expect((ctx.principal as { user: string }).user).toMatch(/^urn:li:corpuser:/);
  });

  test('loads the remote lazily — no remoteEntry request until the tab is opened', async () => {
    const requests = mfePage.trackRemoteEntryRequests();
    await mfePage.mockFetchForMFEConfig(TAB_MFE);
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.gotoDataset(DATASET_URN);

    await expect(mfePage.entityTabHeader(TAB_NAME)).toBeVisible({ timeout: TIMEOUTS.LONG });
    expect(requests()).toBe(0);

    await mfePage.entityTabHeader(TAB_NAME).click();
    await expect(mfePage.slotCtx()).toBeVisible({ timeout: TIMEOUTS.LONG });
    expect(requests()).toBeGreaterThan(0);
  });

  test('honours placement.entityTypes as a coarse filter', async () => {
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);

    await mfePage.mockFetchForMFEConfig(TAB_MFE_CHART_ONLY);
    await mfePage.gotoDataset(DATASET_URN);
    await expect(mfePage.entityHeader()).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.entityTabHeader(TAB_NAME)).toHaveCount(0);
  });

  test('matches placement.entityTypes case-insensitively against the entity type', async () => {
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.mockFetchForMFEConfig(TAB_MFE_DATASET_ONLY);
    await mfePage.gotoDataset(DATASET_URN);

    await expect(mfePage.entityTabHeader(TAB_NAME)).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('hides the tab and never fetches the remote when flags.enabled is false', async () => {
    const requests = mfePage.trackRemoteEntryRequests();
    await mfePage.mockFetchForMFEConfig(TAB_MFE_DISABLED);
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.gotoDataset(DATASET_URN);

    await expect(mfePage.entityHeader()).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.entityTabHeader(TAB_NAME)).toHaveCount(0);
    expect(requests()).toBe(0);
  });

  test('shows the not-available error inside the tab when the remote fails, and the host page keeps working', async ({
    page,
  }) => {
    await mfePage.mockFetchForMFEConfig(TAB_MFE);
    await mfePage.mockRemoteEntry(503, 'Service Unavailable');
    await mfePage.gotoDataset(DATASET_URN);

    await mfePage.entityTabHeader(TAB_NAME).click();
    await expect(mfePage.errorMessage(TAB_LABEL)).toBeVisible({ timeout: TIMEOUTS.LONG });

    // the failure stays inside the slot: the host's own tabs still render for this entity
    await mfePage.gotoDataset(DATASET_URN, 'Columns');
    await expect(page).toHaveURL(/\/Columns/);
    await expect(page.getByText('col_a')).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('deep-links to the slot tab by name', async () => {
    await mfePage.mockFetchForMFEConfig(TAB_MFE);
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.gotoDataset(DATASET_URN, TAB_NAME);

    const ctx = await mfePage.readSlotCtx();
    expect((ctx.entity as { urn: string }).urn).toBe(DATASET_URN);
  });

  test('keeps nav.page MFEs in the sidebar and keeps slot MFEs out of it', async ({ page }) => {
    await mfePage.mockRemoteEntry(200, REMOTE_ENTRY_ECHO_CTX);
    await mfePage.setupMFEFramework(MIXED);

    await expect(mfePage.navSidebar().getByText(NAV_LABEL)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(mfePage.navSidebar().getByText(TAB_LABEL)).toHaveCount(0);
    await expect(mfePage.navSidebar().getByText(TAB_NAME)).toHaveCount(0);

    // nav.page entries still route and mount, now with a nav.page context
    await mfePage.clickMFEItem(NAV_LABEL);
    await mfePage.waitForMFENavigation(NAV_PATH);
    const ctx = await mfePage.readSlotCtx();
    expect(ctx.slot).toBe('nav.page');
    expect(ctx.version).toBe(SLOT_CONTRACT_VERSION);

    // a slot-placed entry has no /mfe route of its own
    await page.goto(`/mfe/${TAB_ID}`);
    await expect(mfePage.notFoundPage()).toBeVisible({ timeout: TIMEOUTS.LONG });
  });
});
