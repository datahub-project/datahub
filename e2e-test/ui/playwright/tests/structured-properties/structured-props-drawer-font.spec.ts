/**
 * Structured-Properties drawer font regression.
 *
 * AntD's <Drawer> portals its content to document.body, escaping the app-wide
 * `.themeV2 *` Mulish override (defined in src/AppV2.less). Any drawer whose
 * styled-components wrapper doesn't explicitly set font-family falls through
 * to AntD's base @font-family — which is set to Manrope in
 * src/conf/theme/global-variables.less — producing a visible font mismatch
 * against the rest of the app.
 *
 * Two surfaces are affected in this repo:
 *   - The shared alchemy <Drawer> at src/alchemy-components/components/Drawer
 *   - The local StyledDrawer at src/app/govern/structuredProperties/styledComponents.ts
 *
 * This test opens the Structured-Properties "Create" drawer (which uses the
 * govern-local StyledDrawer) and asserts every chrome element resolves to
 * Mulish.
 *
 * Expected to FAIL before the fix and PASS after.
 *
 * Standalone — does not use the project's base-test fixtures because their
 * pre-test login uses waitUntil:'networkidle', which never settles when the
 * MFE loader keeps fetching.
 */

import { test, expect, type Locator, type Page } from '@playwright/test';

const MULISH_RE = /^Mulish(\s*,|$)/;
const USERNAME = process.env.DATAHUB_ADMIN_USERNAME || process.env.ADMIN_USERNAME || 'datahub';
const PASSWORD = process.env.DATAHUB_ADMIN_PASSWORD || process.env.ADMIN_PASSWORD || 'datahub';

test.describe('Structured-Properties Create drawer — font family', () => {
  test('drawer chrome renders in Mulish', async ({ page }) => {
    test.setTimeout(120_000);

    // Suppress the welcome modal so it doesn't block clicks.
    await page.addInitScript(() => {
      localStorage.setItem('skipWelcomeModal', 'true');
    });

    await page.goto('/', { waitUntil: 'domcontentloaded' });
    if (page.url().includes('/login')) {
      await page.getByTestId('username').fill(USERNAME);
      await page.getByTestId('password').fill(PASSWORD);
      await page.getByTestId('sign-in').click();
      await page.waitForURL((url) => !url.pathname.includes('login'), {
        waitUntil: 'domcontentloaded',
        timeout: 30_000,
      });
    }

    // Dismiss any blocking modal/onboarding overlays.
    for (const sel of ['.ant-modal-close', 'button:has-text("Skip Tour")', 'button:has-text("Got it")']) {
      // eslint-disable-next-line playwright/no-raw-locators -- dynamic selector loop includes Ant Design class names with no data-testid equivalent
      const m = page.locator(sel).first();
      if ((await m.count()) && (await m.isVisible().catch(() => false))) {
        await m.click().catch(() => {});
      }
    }
    await page.keyboard.press('Escape').catch(() => {});

    // ── Navigate to Structured Properties + open the Create drawer ───────────
    await page.goto('/structured-properties', { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('domcontentloaded');

    const createBtn = page.getByRole('button', { name: /^Create/i }).first();
    await expect(createBtn).toBeVisible({ timeout: 15_000 });
    await createBtn.click();

    // Wait for the drawer chrome to render.
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design drawer content class; no data-testid or ARIA role equivalent
    const drawer = page.locator('.ant-drawer-content').first();
    await expect(drawer).toBeVisible({ timeout: 10_000 });

    // ── Assertions ───────────────────────────────────────────────────────────
    // After the fix, every chrome element inside the portaled drawer must
    // resolve to Mulish — not the Manrope it falls through to today.
    await expectMulish(page, '.ant-drawer-header', 'drawer header chrome');
    await expectMulish(page, '.ant-drawer-title', 'drawer title wrapper');
    await expectMulish(page, '.ant-drawer-body', 'drawer body container');
  });
});

async function expectMulish(page: Page, selector: string, label: string) {
  // eslint-disable-next-line playwright/no-raw-locators -- utility helper accepts arbitrary CSS selectors including Ant Design class names
  await expectMulishElement(page.locator(selector).first(), label);
}

async function expectMulishElement(el: Locator, label: string) {
  const family = await el.evaluate((node) => getComputedStyle(node as HTMLElement).fontFamily);
  expect(family, `${label} — expected Mulish, got: ${family}`).toMatch(MULISH_RE);
}
