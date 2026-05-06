/**
 * Glossary Sidebar Navigation tests — migrated from Cypress e2e/glossaryV2/v2_glossary_navigation.js
 *
 * Tests the move and sidebar navigation behaviour for glossary entities:
 *   1. Create a root-level Term Group and two Terms inside it.
 *   2. Move each Term back into the same Term Group (confirming move messages).
 *   3. Switch between Terms and verify the Properties tab persists as the active tab.
 *   4. Move the Term Group under the parent node (PlaywrightNode).
 *   5. Delete both Terms and the Term Group, verifying the sidebar clears.
 *
 * Each test run uses a unique timestamp-based suffix so tests are fully independent
 * and can run in parallel without colliding on entity names.
 *
 * Prerequisites: `PlaywrightNode` glossary node must exist (seeded via fixtures/data.json).
 */

import { test, expect } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';

test.use({ featureName: 'glossary' });

// Serial: tests build on state created in earlier tests within this suite.
test.describe.configure({ mode: 'serial' });

const runId = Date.now();
const TERM_GROUP_NAME = `PlaywrightNavGroup${runId}`;
const TERM_NAME = `PlaywrightNavTerm${runId}`;
const SECOND_TERM_NAME = `PlaywrightNavTerm2${runId}`;
const PARENT_NODE = 'PlaywrightNode';

test.describe('glossary sidebar navigation', () => {
  test('create term group and terms, move to group, verify sidebar', async ({ page, logger, logDir }) => {
    test.setTimeout(120000);

    const glossaryPage = new GlossaryPage(page, logger, logDir);

    // ── Step 1: Create root-level Term Group ────────────────────────────────
    await glossaryPage.navigateToGlossary();
    await glossaryPage.createTermGroup(TERM_GROUP_NAME);

    await glossaryPage.navigateToGlossary();
    await glossaryPage.expectSidebarContains(TERM_GROUP_NAME);

    // ── Step 2: Create first Term inside the group ──────────────────────────
    await glossaryPage.clickSidebarItem(TERM_GROUP_NAME);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.createTermInContentsTab(TERM_NAME);

    // ── Step 3: Move first Term back into the same Term Group ───────────────
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await expect(page.getByText('Created Glossary Term!')).not.toBeVisible({ timeout: 5000 });
    await glossaryPage.moveCurrentEntityTo(TERM_GROUP_NAME);
    await expect(page.getByText('Moved Glossary Term!')).toBeVisible({ timeout: 15000 });

    // Verify the term appears under the group in the sidebar.
    await glossaryPage.clickSidebarItem(TERM_GROUP_NAME);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.expectTextVisible(TERM_NAME);

    // ── Step 4: Create second Term ──────────────────────────────────────────
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await expect(page.getByText('Moved Glossary Term!')).not.toBeVisible({ timeout: 5000 });
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.createTermInContentsTab(SECOND_TERM_NAME);

    // ── Step 5: Move second Term back into the same Term Group ──────────────
    await glossaryPage.navigateToGlossaryTerm(SECOND_TERM_NAME);
    await glossaryPage.moveCurrentEntityTo(TERM_GROUP_NAME);
    await expect(page.getByText('Moved Glossary Term!')).toBeVisible({ timeout: 15000 });

    // Verify the second term appears under the group.
    await glossaryPage.clickSidebarItem(TERM_GROUP_NAME);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.expectTextVisible(SECOND_TERM_NAME);

    // ── Step 6: Switch between terms; Properties tab should stay active ──────
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await glossaryPage.clickEntityTabByName('Properties');
    await glossaryPage.expectPropertiesTabActive();

    await glossaryPage.navigateToGlossaryTerm(SECOND_TERM_NAME);
    await glossaryPage.expectPropertiesTabActive();

    // ── Step 7: Move the Term Group under PlaywrightNode ─────────────────────
    await glossaryPage.navigateToGlossary();
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.moveCurrentEntityTo(PARENT_NODE);
    await expect(page.getByText('Moved Term Group!')).toBeVisible({ timeout: 15000 });

    // Verify the Term Group now appears under PlaywrightNode.
    await glossaryPage.clickSidebarItem(PARENT_NODE);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.expectTextVisible(TERM_GROUP_NAME);

    // ── Step 8: Delete first Term ─────────────────────────────────────────────
    await glossaryPage.navigateToGlossary();
    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await glossaryPage.deleteCurrentEntity();
    await expect(page.getByText('Deleted Glossary Term!')).toBeVisible({ timeout: 15000 });

    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.expectTextNotPresent(TERM_NAME);

    // ── Step 9: Delete second Term ─────────────────────────────────────────────
    await glossaryPage.navigateToGlossary();
    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.navigateToGlossaryTerm(SECOND_TERM_NAME);
    await glossaryPage.deleteCurrentEntity();
    await expect(page.getByText('Deleted Glossary Term!')).toBeVisible({ timeout: 15000 });

    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.expectTextNotPresent(SECOND_TERM_NAME);

    // ── Step 10: Delete the Term Group ─────────────────────────────────────────
    await glossaryPage.navigateToGlossary();
    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.deleteCurrentEntity();
    await expect(page.getByText('Deleted Term Group!')).toBeVisible({ timeout: 15000 });

    // Verify the sidebar no longer shows the group or either term.
    await glossaryPage.navigateToGlossary();
    await glossaryPage.expectSidebarNotContains(TERM_NAME);
    await glossaryPage.expectSidebarNotContains(TERM_GROUP_NAME);
  });
});
