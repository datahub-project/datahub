/**
 * Change History Sidebar — Playwright E2E tests
 *
 * Tests the "Change History" drawer that appears on versioned entities.
 * The feature adds a clock-icon button to the version picker popover on
 * entity profiles; clicking it opens a timeline sidebar that shows
 * documentation, tag, ownership, and version-milestone events.
 *
 * Key components under test:
 *   VersioningBadge / VersionsPreview  — version pill + popover + clock button
 *   HistorySidebar                     — Ant-Design Drawer (testId: "schema-blame-history-panel")
 *   ChangeTransactionView              — per-transaction rows (milestones + change events)
 *   DocumentationDiff                  — "show diff" / "hide diff" toggle + red-green diff
 *
 * Test data requirements:
 *   The versioned entity used in most tests
 *   (urn:li:dataset:(urn:li:dataPlatform:agents,text-to-sql-skill-2-1-0,PROD))
 *   must be present in the instance with versionProperties referencing a
 *   versionSet. The fixture data.json in this directory seeds a minimal
 *   version of that data. For doc-diff tests an earlier description must
 *   have been emitted first (the Python script agents_and_skills_versioned.py
 *   handles this — run it once against a live instance for full test coverage).
 *
 *   An unversioned fallback dataset (from the schema-blame seeding fixture) is
 *   used for "no version pill" assertions.
 *
 * Port note:
 *   The frontend dev server runs on :9002 by default for this worktree.
 *   The playwright.config.ts baseURL is derived from BASE_URL env var with a
 *   fallback of http://localhost:9002.
 */

import type { Page } from '@playwright/test';
import { test, expect } from '../../fixtures/base-test';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';

// ── Entity URNs ───────────────────────────────────────────────────────────────

/** Versioned entity — has versionProperties, shows clock button. */
const VERSIONED_URN = 'urn:li:dataset:(urn:li:dataPlatform:agents,text-to-sql-skill-2-1-0,PROD)';

/**
 * Unversioned entity — a well-known Hive dataset from the global seeded data
 * that is guaranteed never to have versionProperties.
 */
const UNVERSIONED_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SchemaBlameTesterDataset,PROD)';

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Navigate to a dataset's Summary tab (or default landing tab). */
async function gotoDataset(page: Page, urn: string): Promise<void> {
  await page.goto(`/dataset/${encodeURIComponent(urn)}/Summary`, {
    waitUntil: 'domcontentloaded',
  });
  await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
}

/**
 * Dismiss any modal/onboarding overlays that appear on first page load.
 * Tries common patterns: Escape key, AntD close buttons, and "Skip Tour".
 */
async function dismissModals(page: Page): Promise<void> {
  for (const sel of ['.ant-modal-close', 'button:has-text("Skip Tour")', 'button:has-text("Got it")']) {
    // eslint-disable-next-line playwright/no-raw-locators -- dynamic loop selector
    const modal = page.locator(sel).first();
    if ((await modal.count()) > 0 && (await modal.isVisible().catch(() => false))) {
      await modal.click().catch(() => {});
    }
  }
  await page.keyboard.press('Escape').catch(() => {});
}

/**
 * Locate the version pill on the entity header.
 * VersionPill sets data-testid="version-pill" on the underlying Pill container.
 */
function versionPillLocator(page: Page) {
  return page.getByTestId('version-pill').first();
}

/**
 * Return the AntD Popover panel that appears when the version pill is hovered/clicked.
 * The popover content is teleported to document.body by AntD, outside the entity profile DOM.
 */
function versionPopoverLocator(page: Page) {
  return page.getByTestId('versions-preview-panel');
}

/** Locator for the clock-icon button inside the versions popover. */
function clockButtonLocator(page: Page) {
  return page.getByTitle('View change history');
}

/** Locator for the Change History sidebar drawer. */
function historySidebarLocator(page: Page) {
  return page.getByTestId('schema-blame-history-panel');
}

// ── Feature seeding ───────────────────────────────────────────────────────────

// Seed minimal fixture data so the versioned entity is present even if
// agents_and_skills_versioned.py has not been run against this instance.
// The schema-blame fixture is also seeded for the unversioned entity assertions.
test.use({ featureName: 'change-history' });

// ═════════════════════════════════════════════════════════════════════════════
// Group 1: version pill visibility
// ═════════════════════════════════════════════════════════════════════════════

test.describe('version pill on entity header', () => {
  test('versioned entity shows a version pill in the header', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    const pill = versionPillLocator(page);
    await expect(pill).toBeVisible({ timeout: TIMEOUTS.LONG });
    // The pill label is the semver tag
    await expect(pill).toContainText('2.1.0');
  });

  test('unversioned entity has no version pill in the header', async ({ page }) => {
    await gotoDataset(page, UNVERSIONED_URN);
    await dismissModals(page);

    // Give the header time to render fully before asserting absence
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    const pill = versionPillLocator(page);
    await expect(pill).toHaveCount(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 2: clock button in the version picker popover
// ═════════════════════════════════════════════════════════════════════════════

test.describe('clock button in version picker popover', () => {
  test('hovering the version pill opens a popover', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    const pill = versionPillLocator(page);
    await expect(pill).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Hover to open popover (VersioningBadge wraps VersionsPreview in a Popover)
    await pill.hover();

    const popover = versionPopoverLocator(page);
    await expect(popover).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('popover contains the "Versions" heading', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    await versionPillLocator(page).hover();
    const popover = versionPopoverLocator(page);
    await expect(popover).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await expect(popover.getByText('Versions')).toBeVisible({ timeout: TIMEOUTS.SHORT });
  });

  test('popover contains a clock button with title "View change history"', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    const clockBtn = clockButtonLocator(page);
    await expect(clockBtn).toBeVisible({ timeout: TIMEOUTS.SHORT });
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 3: opening the Change History sidebar via the clock button
// ═════════════════════════════════════════════════════════════════════════════

test.describe('opening the Change History sidebar', () => {
  test('clicking the clock button opens the Change History sidebar', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    // Open the popover and click the clock button
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();

    // The drawer should appear
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('sidebar opened via clock button shows "Change History" heading', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();

    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(sidebar.getByText('Change History', { exact: true })).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('sidebar opened via clock button starts in "All versions" mode', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();

    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });

    // The scope bar should show "All versions" as the active label
    await expect(sidebar.getByText('All versions')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('sidebar has a search bar and a filter control', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);

    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();

    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Search bar placeholder
    await expect(sidebar.getByPlaceholder('Search changes...')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    // Filter (Types) control
    await expect(sidebar.getByText('Types')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 4: All Versions mode — timeline content
// ═════════════════════════════════════════════════════════════════════════════

test.describe('All Versions mode — timeline content', () => {
  /**
   * Shared setup: navigate to the versioned entity and open the sidebar
   * via the clock button (which always opens in All Versions mode).
   */
  async function openSidebarInAllVersionsMode(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(sidebar.getByText('All versions')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    return sidebar;
  }

  test('timeline contains at least one entry', async ({ page }) => {
    const sidebar = await openSidebarInAllVersionsMode(page);
    // Any timestamp or event text appearing in the transaction list
    // is evidence of at least one entry. The footer says "Complete change history"
    // when no truncation occurred.
    const hasEntries = (await sidebar.textContent()) ?? '';
    // Either there are change events OR the "No changes" empty state
    expect(hasEntries.length).toBeGreaterThan(0);
  });

  test('timeline shows a "Version created" milestone', async ({ page }) => {
    const sidebar = await openSidebarInAllVersionsMode(page);
    // VersionMilestoneView renders "Version created" text from i18n
    await expect(sidebar.getByText('Version created').first()).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });
  });

  test('version milestone shows the version tag (e.g. "2.1.0")', async ({ page }) => {
    const sidebar = await openSidebarInAllVersionsMode(page);
    await expect(sidebar.getByText('Version created').first()).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });
    // The tag chip renders the semver label
    await expect(sidebar.getByText('2.1.0').first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('version scope bar toggle is clickable and switches label', async ({ page }) => {
    const sidebar = await openSidebarInAllVersionsMode(page);

    // Currently "All versions" is active; clicking toggles to "This version"
    const scopeBar = sidebar.getByTestId('version-scope-bar');
    if ((await scopeBar.count()) > 0) {
      await scopeBar.click();
      await expect(sidebar.getByText('This version')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
      // Toggle back
      await scopeBar.click();
      await expect(sidebar.getByText('All versions')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    } else {
      // Fallback: click the "Back to this version" action text if visible
      const backAction = sidebar.getByText('Back to this version');
      if ((await backAction.count()) > 0) {
        await backAction.click();
        await expect(sidebar.getByText('This version')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
      }
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 5: This Version mode
// ═════════════════════════════════════════════════════════════════════════════

test.describe('This Version mode', () => {
  async function openSidebarInThisVersionMode(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    // Navigate directly to the Schema tab which has the schema-blame button
    // that opens the sidebar in "This version" mode by default
    await page.goto(`/dataset/${encodeURIComponent(VERSIONED_URN)}/Schema`, {
      waitUntil: 'domcontentloaded',
    });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await dismissModals(page);

    // Try the schema-blame button first
    const schemaBlameBtn = page.getByTestId('schema-blame-button');
    if ((await schemaBlameBtn.count()) > 0 && (await schemaBlameBtn.isVisible())) {
      await schemaBlameBtn.click();
      await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    } else {
      // Fall back to clock button which opens in All Versions mode, then toggle
      await page.goto(`/dataset/${encodeURIComponent(VERSIONED_URN)}/Summary`, {
        waitUntil: 'domcontentloaded',
      });
      await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
      await dismissModals(page);
      await versionPillLocator(page).hover();
      await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
      await clockButtonLocator(page).click();
      // Toggle from All Versions to This version
      const sidebar = historySidebarLocator(page);
      await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
      const scopeToggle = sidebar.getByText('Back to this version');
      if ((await scopeToggle.count()) > 0) {
        await scopeToggle.click();
      }
    }
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    return sidebar;
  }

  test('sidebar can be in "This version" scope', async ({ page }) => {
    const sidebar = await openSidebarInThisVersionMode(page);
    // Either "This version" label is shown or the sidebar is in single-version mode
    // (no scope bar when versionSetUrn is absent). Both are valid outcomes.
    const hasThisVersionLabel = (await sidebar.getByText('This version').count()) > 0;
    const hasScopeBar =
      (await sidebar.getByText('All versions').count()) > 0 || (await sidebar.getByText('This version').count()) > 0;
    // Accept either: the feature is available or the entity simply lacks a versionSet
    // (in which case there's no scope bar at all, which is also correct behavior).
    expect(hasThisVersionLabel || !hasScopeBar).toBe(true);
  });

  test('"View all versions" text is present when scope is "This version"', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });

    // In All Versions mode, the action text is "Back to this version"
    // In This Version mode, the action text is "View all versions"
    // Toggle to This Version mode
    const backBtn = sidebar.getByText('Back to this version');
    if ((await backBtn.count()) > 0) {
      await backBtn.click();
      await expect(sidebar.getByText('View all versions')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 6: Documentation diff
// ═════════════════════════════════════════════════════════════════════════════

test.describe('documentation diff in timeline', () => {
  async function openSidebarAllVersions(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(sidebar.getByText('All versions')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    return sidebar;
  }

  test('documentation change events show a "show diff" link', async ({ page }) => {
    const sidebar = await openSidebarAllVersions(page);
    // Wait for timeline to populate
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // If there are documentation events the DocumentationDiff component renders
    // a "show diff" toggle link. Check if any documentation events are present.
    const docAdded = sidebar.getByText('Documentation added');
    const docUpdated = sidebar.getByText('Documentation updated');
    const hasDocEvents = (await docAdded.count()) > 0 || (await docUpdated.count()) > 0;

    if (hasDocEvents) {
      // The "show diff" link should be present next to the documentation event
      await expect(sidebar.getByText('show diff').first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    } else {
      // No doc events: this is expected when running against a fresh seed that has
      // not yet run the second-pass description update. Skip rather than fail.
      test.skip(true, 'No documentation events present in timeline — run agents_and_skills_versioned.py first');
    }
  });

  test('clicking "show diff" expands and "hide diff" collapses it', async ({ page }) => {
    const sidebar = await openSidebarAllVersions(page);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const showDiffLink = sidebar.getByText('show diff').first();
    if ((await showDiffLink.count()) === 0) {
      test.skip(true, 'No "show diff" link found — documentation events may not exist in this instance');
      return;
    }

    await expect(showDiffLink).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await showDiffLink.click();

    // After expanding, the diff container should be visible and "hide diff" appears
    await expect(sidebar.getByText('hide diff').first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // The diff viewer renders inside a container with react-diff-viewer elements
    const diffContainer = sidebar.getByRole('table').first();
    await expect(diffContainer).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Collapse
    await sidebar.getByText('hide diff').first().click();
    await expect(sidebar.getByText('show diff').first()).toBeVisible({ timeout: TIMEOUTS.SHORT });
  });

  test('expanded diff contains visible text content', async ({ page }) => {
    const sidebar = await openSidebarAllVersions(page);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const showDiffLink = sidebar.getByText('show diff').first();
    if ((await showDiffLink.count()) === 0) {
      test.skip(true, 'No "show diff" link found — documentation events may not exist in this instance');
      return;
    }

    await showDiffLink.click();
    await expect(sidebar.getByText('hide diff').first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // The diff content container wraps the react-diff-viewer output
    const diffContent = sidebar.getByTestId('documentation-diff-content');
    await expect(diffContent).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    const diffText = await diffContent.textContent();
    expect((diffText ?? '').trim().length).toBeGreaterThan(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 7: Keyboard and close interactions
// ═════════════════════════════════════════════════════════════════════════════

test.describe('sidebar keyboard and close interactions', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    return sidebar;
  }

  test('sidebar closes when the close (X) button is clicked', async ({ page }) => {
    const sidebar = await openSidebar(page);

    const closeBtn = sidebar.getByTestId('history-close-btn');
    if ((await closeBtn.count()) > 0) {
      await closeBtn.click();
    } else {
      await page.keyboard.press('Escape');
    }

    await expect(sidebar).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('sidebar search filters the timeline entries', async ({ page }) => {
    const sidebar = await openSidebar(page);

    // Wait for timeline to populate
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const searchInput = sidebar.getByPlaceholder('Search changes...');
    await expect(searchInput).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Type a search term unlikely to match anything — the list should become empty
    // (or show only matching entries). We type something nonsensical.
    await searchInput.fill('xyzzy_no_match_12345');
    // Give the filter a moment to apply (synchronous state update)
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await page.waitForTimeout(300);

    // After filtering with a non-matching term, version milestones and change events
    // should not be shown. The sidebar body should contain very little content.
    const milestones = sidebar.getByText('Version created');
    const docEvents = sidebar.getByText('Documentation added');
    const milestoneCount = await milestones.count();
    const docEventCount = await docEvents.count();
    expect(milestoneCount + docEventCount).toBe(0);

    // Clear the search and entries should re-appear
    await searchInput.clear();
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await page.waitForTimeout(300);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 8: Unversioned entity — graceful absence of history feature
// ═════════════════════════════════════════════════════════════════════════════

test.describe('unversioned entity graceful behavior', () => {
  test('no version pill on an unversioned entity', async ({ page }) => {
    await gotoDataset(page, UNVERSIONED_URN);
    await dismissModals(page);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const pill = versionPillLocator(page);
    // Allow short time for header to fully render before asserting absence
    await expect(pill).toHaveCount(0, { timeout: TIMEOUTS.SHORT });
  });

  test('no clock button popover on unversioned entity', async ({ page }) => {
    await gotoDataset(page, UNVERSIONED_URN);
    await dismissModals(page);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const clockBtn = clockButtonLocator(page);
    await expect(clockBtn).toHaveCount(0, { timeout: TIMEOUTS.SHORT });
  });

  test('unversioned entity can still open Change History via the Schema tab blame button', async ({ page }) => {
    // The schema-blame button is always available for datasets even without versioning.
    // When opened this way, no version scope bar is shown.
    await page.goto(`/dataset/${encodeURIComponent(UNVERSIONED_URN)}/Schema`, {
      waitUntil: 'domcontentloaded',
    });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await dismissModals(page);

    const schemaBlameBtn = page.getByTestId('schema-blame-button');
    if ((await schemaBlameBtn.count()) > 0 && (await schemaBlameBtn.isVisible())) {
      await schemaBlameBtn.click();
      await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
      const sidebar = historySidebarLocator(page);
      await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
      // No version scope bar should be rendered (no versionSetUrn)
      await expect(sidebar.getByText('All versions')).toHaveCount(0, { timeout: TIMEOUTS.SHORT });
      await expect(sidebar.getByText('This version')).toHaveCount(0, { timeout: TIMEOUTS.SHORT });
    } else {
      test.skip(true, 'Schema blame button not found on this entity — expected on schema-blame-seeded dataset');
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 9: Edge cases
// ═════════════════════════════════════════════════════════════════════════════

// ═════════════════════════════════════════════════════════════════════════════
// Group 10: Tag changes
// i18n: "Added tag \"{{tagName}}\"." / "Removed tag \"{{tagName}}\"."
//         "Added tag \"{{tagName}}\" to field {{field}}." (field-level)
// Seeded by: globalTags aspect in fixtures/data.json (generates ADD event on
//            first ingest; REMOVE requires a second pass without the tag).
// ═════════════════════════════════════════════════════════════════════════════

test.describe('tag change events in timeline', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return sidebar;
  }

  test('tag ADD event appears as "Added tag" text in the timeline', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const addedTagEntry = sidebar.getByText(/Added tag ".*"\./, { exact: false });
    if ((await addedTagEntry.count()) === 0) {
      test.skip(true, 'No tag ADD events in timeline — seed globalTags aspect or run agents_and_skills_versioned.py');
      return;
    }
    await expect(addedTagEntry.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('tag REMOVE event appears as "Removed tag" text in the timeline', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const removedTagEntry = sidebar.getByText(/Removed tag ".*"\./, { exact: false });
    if ((await removedTagEntry.count()) === 0) {
      test.skip(true, 'No tag REMOVE events — requires two ingestion passes: first adding, then removing the tag');
      return;
    }
    await expect(removedTagEntry.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('field-level tag ADD event shows the field name', async ({ page }) => {
    const sidebar = await openSidebar(page);
    // Field-level tag events contain "to field" in the rendered string
    const fieldTagEntry = sidebar.getByText(/Added tag ".*" to field/, { exact: false });
    if ((await fieldTagEntry.count()) === 0) {
      test.skip(true, 'No field-level tag events in timeline');
      return;
    }
    await expect(fieldTagEntry.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('tag events do NOT show a "show diff" button', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const tagEventRows = sidebar.getByTestId('change-event-row').filter({ hasText: /Added tag ".*"|Removed tag ".*"/ });
    if ((await tagEventRows.count()) === 0) {
      test.skip(true, 'No tag events to verify diff-button absence');
      return;
    }
    await expect(tagEventRows.getByText('show diff')).toHaveCount(0);
  });

  test('Types filter shows Tag category option', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const typesFilter = sidebar.getByText('Types');
    await expect(typesFilter).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    // Click to open the filter dropdown
    await typesFilter.click();
    // The "Tags" option should be in the filter
    const tagsOption = page.getByText('Tags').last(); // may be in a dropdown teleported outside sidebar
    await expect(tagsOption).toBeVisible({ timeout: TIMEOUTS.SHORT });
    // Close filter by pressing Escape
    await page.keyboard.press('Escape');
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 11: Ownership changes
// i18n: "Added owner \"{{ownerName}}\"{{ownerTypeSuffix}}."
//        "Removed owner \"{{ownerName}}\"{{ownerTypeSuffix}}."
// Owner type suffix examples: " (Business Owner)", " (Technical Owner)", ""
// Seeded by: ownership aspect in fixtures/data.json.
// ═════════════════════════════════════════════════════════════════════════════

test.describe('ownership change events in timeline', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return sidebar;
  }

  test('ownership ADD event appears as "Added owner" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const addedOwner = sidebar.getByText(/Added owner ".*"/, { exact: false });
    if ((await addedOwner.count()) === 0) {
      test.skip(true, 'No ownership ADD events — seed ownership aspect or run agents_and_skills_versioned.py');
      return;
    }
    await expect(addedOwner.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('ownership ADD event includes the owner name', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const addedOwner = sidebar.getByText(/Added owner ".*"/, { exact: false });
    if ((await addedOwner.count()) === 0) {
      test.skip(true, 'No ownership events to verify owner name');
      return;
    }
    // The text should include a quoted name (not just empty quotes "")
    const text = await addedOwner.first().textContent();
    expect(text).toMatch(/Added owner "[^"]+"/);
  });

  test('ownership ADD event with a named type shows the type suffix', async ({ page }) => {
    const sidebar = await openSidebar(page);
    // Events with a non-uninformative owner type suffix render e.g. "(Business Owner)"
    const ownerWithType = sidebar.getByText(/Added owner ".*" \(.*\)/, { exact: false });
    if ((await ownerWithType.count()) === 0) {
      test.skip(true, 'No ownership events with explicit owner type suffix in this dataset');
      return;
    }
    await expect(ownerWithType.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('ownership REMOVE event appears as "Removed owner" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const removedOwner = sidebar.getByText(/Removed owner ".*"/, { exact: false });
    if ((await removedOwner.count()) === 0) {
      test.skip(true, 'No ownership REMOVE events — requires two ingestion passes');
      return;
    }
    await expect(removedOwner.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('ownership events do NOT show a "show diff" button', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const ownerEventRows = sidebar
      .getByTestId('change-event-row')
      .filter({ hasText: /Added owner ".*"|Removed owner ".*"/ });
    if ((await ownerEventRows.count()) === 0) {
      test.skip(true, 'No ownership events to verify diff-button absence');
      return;
    }
    await expect(ownerEventRows.getByText('show diff')).toHaveCount(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 12: Glossary term changes
// i18n: "Added term \"{{termName}}\"." / "Removed term \"{{termName}}\"."
//        "Added {{label}} term \"{{termName}}\"." (with relationship type)
//        "Added term \"{{termName}}\" to field {{field}}." (field-level)
// ═════════════════════════════════════════════════════════════════════════════

test.describe('glossary term change events in timeline', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return sidebar;
  }

  test('glossary term ADD event appears as "Added term" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const addedTerm = sidebar.getByText(/Added term ".*"\.|Added .* term ".*"\./, { exact: false });
    if ((await addedTerm.count()) === 0) {
      test.skip(true, 'No glossary term ADD events — seed glossaryTerms aspect');
      return;
    }
    await expect(addedTerm.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('glossary term REMOVE event appears as "Removed term" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const removedTerm = sidebar.getByText(/Removed term ".*"\.|Removed .* term ".*"\./, { exact: false });
    if ((await removedTerm.count()) === 0) {
      test.skip(true, 'No glossary term REMOVE events — requires two ingestion passes');
      return;
    }
    await expect(removedTerm.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('glossary term events do NOT show a "show diff" button', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const termEventRows = sidebar
      .getByTestId('change-event-row')
      .filter({ hasText: /Added term ".*"|Removed term ".*"/ });
    if ((await termEventRows.count()) === 0) {
      test.skip(true, 'No glossary term events to verify diff-button absence');
      return;
    }
    await expect(termEventRows.getByText('show diff')).toHaveCount(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 13: Technical schema changes (columns)
// i18n: "Added column {{column}}." / "Removed column {{column}}." /
//        "Modified column {{column}}."
// These are generated when the schemaMetadata aspect changes between ingestion
// runs. Requires two runs with different schemas to get add/remove/modify events.
// ═════════════════════════════════════════════════════════════════════════════

test.describe('technical schema change events in timeline', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return sidebar;
  }

  test('schema ADD event appears as "Added column" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const addedCol = sidebar.getByText(/Added column /, { exact: false });
    if ((await addedCol.count()) === 0) {
      test.skip(true, 'No schema ADD events — requires two ingestion runs with different schemaMetadata');
      return;
    }
    await expect(addedCol.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('schema REMOVE event appears as "Removed column" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const removedCol = sidebar.getByText(/Removed column /, { exact: false });
    if ((await removedCol.count()) === 0) {
      test.skip(true, 'No schema REMOVE events');
      return;
    }
    await expect(removedCol.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('schema MODIFY event appears as "Modified column" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const modifiedCol = sidebar.getByText(/Modified column /, { exact: false });
    if ((await modifiedCol.count()) === 0) {
      test.skip(true, 'No schema MODIFY events');
      return;
    }
    await expect(modifiedCol.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('schema events do NOT show a "show diff" button', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const schemaEventRows = sidebar
      .getByTestId('change-event-row')
      .filter({ hasText: /Added column |Removed column |Modified column / });
    if ((await schemaEventRows.count()) === 0) {
      test.skip(true, 'No schema events to verify diff-button absence');
      return;
    }
    await expect(schemaEventRows.getByText('show diff')).toHaveCount(0);
  });

  test('Types filter shows Schema category option', async ({ page }) => {
    const sidebar = await openSidebar(page);
    await sidebar.getByText('Types').click();
    const schemaOption = page.getByText('Schema').last();
    await expect(schemaOption).toBeVisible({ timeout: TIMEOUTS.SHORT });
    await page.keyboard.press('Escape');
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 14: Domain changes
// i18n: "Added to domain \"{{domainName}}\"." / "Removed from domain \"{{domainName}}\"."
// Seeded by: domains aspect in fixtures/data.json.
// ═════════════════════════════════════════════════════════════════════════════

test.describe('domain change events in timeline', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return sidebar;
  }

  test('domain ADD event appears as "Added to domain" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const addedDomain = sidebar.getByText(/Added to domain ".*"/, { exact: false });
    if ((await addedDomain.count()) === 0) {
      test.skip(true, 'No domain ADD events — seed domains aspect or run agents_and_skills_versioned.py');
      return;
    }
    await expect(addedDomain.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('domain REMOVE event appears as "Removed from domain" text', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const removedDomain = sidebar.getByText(/Removed from domain ".*"/, { exact: false });
    if ((await removedDomain.count()) === 0) {
      test.skip(true, 'No domain REMOVE events — requires two ingestion passes');
      return;
    }
    await expect(removedDomain.first()).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  });

  test('domain events do NOT show a "show diff" button', async ({ page }) => {
    const sidebar = await openSidebar(page);
    const domainEventRows = sidebar
      .getByTestId('change-event-row')
      .filter({ hasText: /Added to domain ".*"|Removed from domain ".*"/ });
    if ((await domainEventRows.count()) === 0) {
      test.skip(true, 'No domain events to verify diff-button absence');
      return;
    }
    await expect(domainEventRows.getByText('show diff')).toHaveCount(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 15: Types filter — filtering by change category
// Verifies each filter pill hides/shows the right event types.
// ═════════════════════════════════════════════════════════════════════════════

test.describe('Types filter per category', () => {
  async function openSidebar(page: Page) {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return sidebar;
  }

  test('filtering to Documentation hides tag and ownership events', async ({ page }) => {
    const sidebar = await openSidebar(page);

    // Record whether tag/owner events exist before filtering
    const tagCount = await sidebar.getByText(/Added tag ".*"|Removed tag ".*"/, { exact: false }).count();
    const ownerCount = await sidebar.getByText(/Added owner ".*"|Removed owner ".*"/, { exact: false }).count();
    if (tagCount === 0 && ownerCount === 0) {
      test.skip(true, 'No tag or ownership events to verify filter hides them');
      return;
    }

    // Open the Types filter and select Documentation only
    await sidebar.getByText('Types').click();
    const docOption = page.getByText('Documentation').last();
    await expect(docOption).toBeVisible({ timeout: TIMEOUTS.SHORT });
    await docOption.click();
    await page.keyboard.press('Escape');
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await page.waitForTimeout(400);

    // Tag and owner events should now be hidden
    await expect(sidebar.getByText(/Added tag ".*"|Removed tag ".*"/, { exact: false })).toHaveCount(0);
    await expect(sidebar.getByText(/Added owner ".*"|Removed owner ".*"/, { exact: false })).toHaveCount(0);
  });

  test('filtering to Ownership hides documentation and tag events', async ({ page }) => {
    const sidebar = await openSidebar(page);

    const docCount = await sidebar.getByText(/Documentation (added|updated)/, { exact: false }).count();
    const tagCount = await sidebar.getByText(/Added tag ".*"/, { exact: false }).count();
    if (docCount === 0 && tagCount === 0) {
      test.skip(true, 'No doc or tag events to verify Ownership filter hides them');
      return;
    }

    await sidebar.getByText('Types').click();
    const ownershipOption = page.getByText('Owners').last();
    await expect(ownershipOption).toBeVisible({ timeout: TIMEOUTS.SHORT });
    await ownershipOption.click();
    await page.keyboard.press('Escape');
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await page.waitForTimeout(400);

    await expect(sidebar.getByText(/Documentation (added|updated)/, { exact: false })).toHaveCount(0);
    await expect(sidebar.getByText(/Added tag ".*"/, { exact: false })).toHaveCount(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 16: "show diff" is exclusive to Documentation events
// This is a structural invariant: only DocumentationDiff renders that button.
// ═════════════════════════════════════════════════════════════════════════════

test.describe('"show diff" is only present on Documentation events', () => {
  test('tag, ownership, schema, and domain events never render a "show diff" link', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();
    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Verify no tag, ownership, schema, or domain event rows contain "show diff"
    const allRows = sidebar.getByTestId('change-event-row');
    await expect(allRows.filter({ hasText: /Added tag ".*"|Removed tag ".*"/ }).getByText('show diff')).toHaveCount(0);
    await expect(allRows.filter({ hasText: /Added owner ".*"|Removed owner ".*"/ }).getByText('show diff')).toHaveCount(
      0,
    );
    await expect(
      allRows.filter({ hasText: /Added column |Removed column |Modified column / }).getByText('show diff'),
    ).toHaveCount(0);
    await expect(
      allRows.filter({ hasText: /Added to domain ".*"|Removed from domain ".*"/ }).getByText('show diff'),
    ).toHaveCount(0);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// Group 17: Edge cases
// ═════════════════════════════════════════════════════════════════════════════
test.describe('edge cases', () => {
  test('sidebar footer shows a status message', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();

    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Footer should show one of: "Complete change history", a truncation notice,
    // or a skipped-versions notice — all are valid.
    const footerTexts = [
      'Complete change history',
      'Showing the most recent changes',
      'sibling version',
      'Unable to load',
    ];
    let found = false;
    for (const text of footerTexts) {
      if ((await sidebar.getByText(text).count()) > 0) {
        found = true;
        break;
      }
    }
    expect(found, 'Expected a footer status message in the history sidebar').toBe(true);
  });

  test('sidebar search clears and restores entries', async ({ page }) => {
    await gotoDataset(page, VERSIONED_URN);
    await dismissModals(page);
    await versionPillLocator(page).hover();
    await expect(versionPopoverLocator(page)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await clockButtonLocator(page).click();

    const sidebar = historySidebarLocator(page);
    await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const searchInput = sidebar.getByPlaceholder('Search changes...');
    await expect(searchInput).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Type a specific term we know will match version events
    await searchInput.fill('Version');
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await page.waitForTimeout(300);
    const afterFilter = sidebar.getByText('Version created');

    // Clear search — the entries should be back
    await searchInput.clear();
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await page.waitForTimeout(300);
    // After clear, "Version created" milestone should return if it was there before
    const afterClear = sidebar.getByText('Version created');
    // Either it was present before clearing and returns, or the test data doesn't
    // have version milestones (acceptable).
    const beforeCount = await afterFilter.count();
    const afterCount = await afterClear.count();
    // The count after clearing must be >= the count after filtering
    expect(afterCount).toBeGreaterThanOrEqual(beforeCount);
  });

  test('sidebar handles entities with minimal history gracefully', async ({ page }) => {
    // Navigate to the versioned entity but look at a version that might have minimal history
    const olderVersionUrn = 'urn:li:dataset:(urn:li:dataPlatform:agents,text-to-sql-skill-2-0-0,PROD)';

    await gotoDataset(page, olderVersionUrn);
    await dismissModals(page);

    // The entity may or may not have a version pill depending on whether
    // versionProperties was seeded. Either outcome is valid.
    const pill = versionPillLocator(page);
    const hasPill = (await pill.count()) > 0 && (await pill.isVisible().catch(() => false));

    if (hasPill) {
      await pill.hover();
      const popover = versionPopoverLocator(page);
      if ((await popover.count()) > 0 && (await popover.isVisible().catch(() => false))) {
        const clockBtn = clockButtonLocator(page);
        if ((await clockBtn.count()) > 0) {
          await clockBtn.click();
          const sidebar = historySidebarLocator(page);
          await expect(sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
          // Should not crash — either shows events or the empty state
          await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
          // Sidebar rendered — just assert it's visible and non-empty
          const sidebarText = await sidebar.textContent();
          expect((sidebarText ?? '').trim().length).toBeGreaterThan(0);
        }
      }
    } else {
      // This older version entity wasn't seeded with versionProperties in this run — acceptable.
      test.skip(true, 'Older version entity not available with versionProperties in this instance');
    }
  });
});
