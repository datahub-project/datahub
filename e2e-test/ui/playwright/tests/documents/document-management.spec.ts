/**
 * Document management tests — migrated from Cypress e2e/documents/document_management.js
 *
 * Covers: document CRUD, sidebar navigation (collapse/expand, search),
 * document hierarchy (move/nesting), and document deletion with cascade.
 *
 * All tests run in parallel with independent document instances and cleanup.
 * Feature flags required: contextDocumentsEnabled=true, showNavBarRedesign=true,
 * showHomePageRedesign=true.
 */

import { test, expect } from '../../fixtures/base-test';
import { withRandomSuffix } from '../../utils/random';
import { DocumentPage } from '../../pages/entity/document.page';
import { TIMEOUTS, LOAD_STATES, KEYS } from '../../utils/constants';
import { waitForWritesToSync } from '../../utils/writes-sync';

// Test constants
const STATUS_PUBLISHED = 'PUBLISHED';
const TYPE_RUNBOOK = 'Runbook';

test.use({ featureName: 'documents' });

test.describe('Document Management', () => {
  // Create → move → verify (and cascade delete) need more than the default 60s.
  test.setTimeout(120000);

  let documentPage: DocumentPage;

  test.beforeEach(async ({ apiMock, page }) => {
    // Enable features required for document management UI
    // contextDocumentsEnabled: activates document entity and sidebar
    // showNavBarRedesign/showHomePageRedesign: uses updated UI components
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
    documentPage = new DocumentPage(page);
    await documentPage.navigateToDocuments();
  });

  // ── CRUD Operations ───────────────────────────────────────────────────────

  test('should create a new document via Context Documents page', async ({ cleanup }) => {
    const docTitle = withRandomSuffix('doc');

    const docUrn = await documentPage.createDocumentWithTitle(docTitle);
    cleanup.track(docUrn);
    await documentPage.expectSidebarVisible();
    await documentPage.expectTitleInput(docTitle);
  });

  test('should create multiple documents via different UI paths', async ({ cleanup }) => {
    const base = withRandomSuffix('doc');
    const doc1Title = `${base}_first`;
    const doc2Title = `${base}_second`;

    // Create first document via main create action
    const docUrn = await documentPage.createDocumentWithTitle(doc1Title);
    cleanup.track(docUrn);
    await documentPage.expectSidebarVisible();
    await documentPage.expectTitleInput(doc1Title);

    // Create second document via sidebar create button (different UI path)
    const doc2Urn = await documentPage.createNewDocumentViaButton();
    cleanup.track(doc2Urn);
    await documentPage.navigateToDocument(doc2Urn);
    await documentPage.setDocumentTitle(doc2Title);
    await documentPage.expectTitleInput(doc2Title);

    await cleanup.flush();
  });

  test('should update document title and verify persistence', async ({ page, cleanup, gmsToken }) => {
    const initialTitle = withRandomSuffix('doc');
    const updatedTitle = `${initialTitle}_updated`;

    const docUrn = await documentPage.createDocumentWithTitle(initialTitle);
    cleanup.track(docUrn);
    await documentPage.navigateToDocument(docUrn);
    await documentPage.setDocumentTitle(updatedTitle);

    // Ensure the title write is consumed and indexed before the reload re-reads it
    await waitForWritesToSync(page.request, { gmsToken });
    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectTitleInput(updatedTitle);

    await cleanup.flush();
  });

  test('should update document content', async ({ page, cleanup, gmsToken }) => {
    const docTitle = withRandomSuffix('content-test');
    const testContent = 'Test content';

    const docUrn = await documentPage.createDocumentWithTitle(docTitle);
    cleanup.track(docUrn);
    await documentPage.clickEditorAndType(testContent);
    await page.keyboard.press(KEYS.ESCAPE);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Ensure the content write is consumed and indexed before the reload re-reads it
    await waitForWritesToSync(page.request, { gmsToken });
    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectEditorContains(testContent);

    await cleanup.flush();
  });

  test('should update document status', async ({ page, cleanup, gmsToken }) => {
    const statusTestDoc = withRandomSuffix('status-test');
    const docUrn = await documentPage.createDocumentWithTitle(statusTestDoc);
    cleanup.track(docUrn);

    await documentPage.updateDocumentStatus(STATUS_PUBLISHED);

    // Ensure the status write is consumed and indexed before the reload re-reads it
    await waitForWritesToSync(page.request, { gmsToken });
    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectTitleInput(statusTestDoc);
    await documentPage.expectStatusContains('Published');

    await cleanup.flush();
  });

  test('should update document type', async ({ page, cleanup, gmsToken }) => {
    const typeTestDoc = withRandomSuffix('type-test');
    const docUrn = await documentPage.createDocumentWithTitle(typeTestDoc);
    cleanup.track(docUrn);

    await documentPage.updateDocumentType(TYPE_RUNBOOK);

    // Ensure the type write is consumed and indexed before the reload re-reads it
    await waitForWritesToSync(page.request, { gmsToken });
    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectTitleInput(typeTestDoc);
    await documentPage.expectTypeContains(TYPE_RUNBOOK);

    await cleanup.flush();
  });

  // ── Sidebar Navigation ─────────────────────────────────────────────────────

  test('should collapse and expand the sidebar', async ({ page, cleanup }) => {
    const docTitle = withRandomSuffix('collapse-test');
    const docUrn = await documentPage.createDocumentWithTitle(docTitle);
    cleanup.track(docUrn);

    await documentPage.expectSidebarVisible();
    await documentPage.expectSidebarContains('Documents');

    await documentPage.clickCollapseButton();
    await page.waitForTimeout(TIMEOUTS.QUICK);
    await documentPage.expectCreateButtonHidden();

    await documentPage.clickCollapseButton();
    await page.waitForTimeout(TIMEOUTS.QUICK);
    await documentPage.expectSidebarContains('Documents');
    await documentPage.expectCreateButtonEnabled();

    await cleanup.flush();
  });

  test('should search for documents using sidebar search', async ({ page, cleanup, gmsToken }) => {
    const searchTitle = withRandomSuffix('doc');

    const docUrn = await documentPage.createDocumentWithTitle(searchTitle);
    cleanup.track(docUrn);
    await documentPage.expectSidebarVisible();

    // Ensure the doc's title is indexed before searching for it by that title —
    // a fixed sleep here is a race against ES indexing, not a UI-render pause.
    await waitForWritesToSync(page.request, { gmsToken });

    await documentPage.searchForDocument(searchTitle);
    await documentPage.expectSearchResultsVisible();

    // Clear search input to verify results close when query is empty
    await documentPage.closeSearchAndVerifyClosed();

    await cleanup.flush();
  });

  // ── Hierarchy Operations ───────────────────────────────────────────────────

  test('should create two documents, move one to the other, and verify nesting', async ({ cleanup }) => {
    const base = withRandomSuffix('doc');
    const parentTitle = `${base}_parent`;
    const childTitle = `${base}_child`;

    const { parentUrn, childUrn } = await documentPage.createAndMoveChildToParent(parentTitle, childTitle);
    cleanup.track(parentUrn);
    cleanup.track(childUrn);

    // Verify hierarchy was persisted: child shows parent in breadcrumb
    await documentPage.navigateToDocument(childUrn);
    await expect(documentPage.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await documentPage.expectParentBreadcrumbVisible(parentTitle);
  });

  // ── Deletion Operations ────────────────────────────────────────────────────

  test('should create and delete a document', async ({ page, gmsToken }) => {
    const docTitle = withRandomSuffix('delete-test');
    const docUrn = await documentPage.createDocumentWithTitle(docTitle);

    await documentPage.clickActionsMenu();
    await documentPage.clickDeleteMenuItem();
    await documentPage.confirmDelete();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Ensure the delete is consumed and removed from the index before the
    // documents tree is re-queried
    await waitForWritesToSync(page.request, { gmsToken });

    await documentPage.navigateToDocuments();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await expect(documentPage.getTreeItem(docUrn)).not.toBeAttached();
  });

  test('should delete parent document and cascade to children', async ({ page, cleanup, gmsToken }) => {
    const base = withRandomSuffix('doc');
    const parentTitle = `${base}_parent`;
    const childTitle = `${base}_child`;

    const { parentUrn, childUrn } = await documentPage.createAndMoveChildToParent(parentTitle, childTitle);
    cleanup.track(parentUrn);
    cleanup.track(childUrn);

    await documentPage.navigateToDocument(parentUrn);
    await expect(documentPage.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await documentPage.expectTitleInput(parentTitle);

    // Delete parent document
    await documentPage.clickActionsMenu();
    await documentPage.clickDeleteMenuItem();
    await documentPage.confirmDelete();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Ensure the cascade delete is consumed and removed from the index before
    // the documents tree is re-queried
    await waitForWritesToSync(page.request, { gmsToken });

    await documentPage.navigateToDocuments();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Verify cascade deletion: both parent and child should be removed from tree
    await expect(documentPage.getTreeItem(parentUrn)).not.toBeAttached();
    await expect(documentPage.getTreeItem(childUrn)).not.toBeAttached();
  });
});
