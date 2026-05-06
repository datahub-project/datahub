/**
 * Glossary Term V2 tests — migrated from PlaywrightGlossaryTag e2e/glossaryV2/v2_glossaryTerm.js
 *
 * Tests glossary term operations from the entity profile:
 *   1. Search related entities by query (batch add + search by name).
 *   2. Apply tag filters on related entities.
 *   3. Use advanced search to filter by tag, then delete the term.
 *
 * Each test run uses a unique timestamp-based suffix so tests are independent
 * and can run in parallel without colliding on entity names.
 *
 * Prerequisites:
 *   - `PlaywrightNode` glossary node exists (seeded via fixtures/data.json).
 *   - A dataset with tag "PlaywrightGlossaryTag" exists in the test environment.
 *   - A dataset named "PlaywrightGlossaryTaggedDataset" tagged with "PlaywrightGlossaryTag" exists (seeded via fixtures/data.json).
 *
 * Note: Tests in this file share a term created in the first test (serial mode).
 * The term is deleted in the last test.
 */

import { test, expect } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';

test.use({ featureName: 'glossary' });

// Serial mode: tests share the same term created in test 1 and deleted in test 3.
test.describe.configure({ mode: 'serial' });

const runId = Date.now();
const TERM_NAME = `GlossaryNewTerm${runId}`;
const PARENT_NODE = 'PlaywrightNode';

test.describe('glossary term entity operations', () => {
  test.beforeEach(async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();
    // Wait for the sidebar to stabilise before each test.
    await expect(glossaryPage.sidebarContainer).toBeVisible({ timeout: 15000 });
  });

  test('can search related entities by query', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);

    // Navigate to the parent node and create the term.
    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.createTermInContentsTab(TERM_NAME);
    // Wait for the term to appear in the refreshed Contents list before clicking.
    await expect(page.getByText(TERM_NAME).first()).toBeVisible({ timeout: 15000 });
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);

    // Batch-add the term to the seeded dataset that carries PlaywrightGlossaryTag,
    // so test 3 can filter related assets by that tag and reliably find it.
    await glossaryPage.batchAddToEntityBySearch('PlaywrightGlossaryTaggedDataset');

    // Switch to Related Assets tab.
    await glossaryPage.navigateToEntityRelatedAssetsTab();
    await glossaryPage.expectPreviewEntitiesVisible();

    // Read the title of the first related asset, then search for it.
    const firstTitle = await page
      .locator('[data-testid^="preview-urn:"] [data-testid="entity-title"]')
      .first()
      .innerText();

    await glossaryPage.searchWithinEntityPage(firstTitle);
    await page.waitForLoadState('networkidle');

    await expect(page.locator('[data-testid^="preview-urn:"] [data-testid="entity-title"]').first()).toHaveText(
      firstTitle,
      { timeout: 15000 },
    );
  });

  test('can apply filters on related entities', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);

    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await glossaryPage.navigateToEntityRelatedAssetsTab();
    await glossaryPage.expectPreviewEntitiesVisible();

    // Open the basic facet filter panel.
    await glossaryPage.clickFacetFilterIcon();
    // Wait for the tag facet checkboxes to appear.
    const tagCheckbox = page.locator('input.ant-checkbox-input[data-testid^="facet-tags-urn:li:tag:"]').first();
    await expect(tagCheckbox).toBeVisible({ timeout: 10000 });

    // Click the first tag checkbox to filter.
    await tagCheckbox.click();

    // After filtering, results should still be present.
    await glossaryPage.expectPreviewEntitiesVisible();

    // Get the title of the first result and navigate to it.
    const firstEntityTitle = page.locator('[data-testid^="preview-urn:"] [data-testid="entity-title"]').first();
    await expect(firstEntityTitle).toBeVisible({ timeout: 15000 });
    const entityName = await firstEntityTitle.innerText();
    await firstEntityTitle.click();
    await page.waitForLoadState('networkidle');

    // The entity name should appear on the entity page.
    await expect(page.getByText(entityName).first()).toBeVisible({ timeout: 15000 });
  });

  test('can search related entities by a specific tag using advanced search, then delete term', async ({
    page,
    logger,
    logDir,
  }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);

    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await glossaryPage.navigateToEntityRelatedAssetsTab();
    await glossaryPage.expectPreviewEntitiesVisible();

    await glossaryPage.filterRelatedAssetsByTag('PlaywrightGlossaryTag');
    await glossaryPage.expectPreviewEntitiesVisible();

    // Search for a known entity and verify it appears.
    await glossaryPage.searchWithinEntityPage('PlaywrightGlossaryTaggedDataset');
    await glossaryPage.expectPreviewEntitiesVisible();
    await expect(page.getByText('PlaywrightGlossaryTaggedDataset').filter({ visible: true }).first()).toBeVisible({
      timeout: 15000,
    });

    // Delete the term to clean up.
    await glossaryPage.navigateToGlossaryTerm(PARENT_NODE);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await glossaryPage.deleteCurrentEntity();
    await expect(page.getByText('Deleted Glossary Term!')).toBeVisible({ timeout: 15000 });
  });
});
