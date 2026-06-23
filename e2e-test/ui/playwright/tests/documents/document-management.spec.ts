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

// Test constants
const STATUS_PUBLISHED = 'PUBLISHED';
const TYPE_RUNBOOK = 'Runbook';

test.use({ featureName: 'documents' });

test.describe('Document Management', () => {
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

  test('should update document title and verify persistence', async ({ page, cleanup }) => {
    const initialTitle = withRandomSuffix('doc');
    const updatedTitle = `${initialTitle}_updated`;

    const docUrn = await documentPage.createDocumentWithTitle(initialTitle);
    cleanup.track(docUrn);
    await documentPage.navigateToDocument(docUrn);
    await documentPage.setDocumentTitle(updatedTitle);
    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectTitleInput(updatedTitle);

    await cleanup.flush();
  });

  test('should update document content', async ({ page, cleanup }) => {
    const docTitle = withRandomSuffix('content-test');
    const testContent = 'Test content';

    const docUrn = await documentPage.createDocumentWithTitle(docTitle);
    cleanup.track(docUrn);
    await documentPage.clickEditorAndType(testContent);
    await page.keyboard.press(KEYS.ESCAPE);
    await page.waitForTimeout(TIMEOUTS.OPERATION);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectEditorContains(testContent);

    await cleanup.flush();
  });

  test('should update document status', async ({ page, cleanup }) => {
    const statusTestDoc = withRandomSuffix('status-test');
    const docUrn = await documentPage.createDocumentWithTitle(statusTestDoc);
    cleanup.track(docUrn);

    await documentPage.updateDocumentStatus(STATUS_PUBLISHED);

    await page.reload();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await documentPage.expectTitleInput(statusTestDoc);
    await documentPage.expectStatusContains('Published');

    await cleanup.flush();
  });

  test('should update document type', async ({ page, cleanup }) => {
    const typeTestDoc = withRandomSuffix('type-test');
    const docUrn = await documentPage.createDocumentWithTitle(typeTestDoc);
    cleanup.track(docUrn);

    await documentPage.updateDocumentType(TYPE_RUNBOOK);

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

  test('should search for documents using sidebar search', async ({ page, cleanup }) => {
    const searchTitle = withRandomSuffix('doc');

    const docUrn = await documentPage.createDocumentWithTitle(searchTitle);
    cleanup.track(docUrn);
    await documentPage.expectSidebarVisible();

    await page.waitForTimeout(TIMEOUTS.OPERATION);

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

  test('should create and delete a document', async ({ page }) => {
    const docTitle = withRandomSuffix('delete-test');
    const docUrn = await documentPage.createDocumentWithTitle(docTitle);

    await documentPage.clickActionsMenu();
    await documentPage.clickDeleteMenuItem();
    await documentPage.confirmDelete();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await page.waitForTimeout(TIMEOUTS.MEDIUM);

    await documentPage.navigateToDocuments();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await expect(documentPage.getTreeItem(docUrn)).not.toBeAttached();
  });

  test('should delete parent document and cascade to children', async ({ page, cleanup }) => {
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
    await page.waitForTimeout(TIMEOUTS.MEDIUM);

    await documentPage.navigateToDocuments();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    // Verify cascade deletion: both parent and child should be removed from tree
    await expect(documentPage.getTreeItem(parentUrn)).not.toBeAttached();
    await expect(documentPage.getTreeItem(childUrn)).not.toBeAttached();
  });
});
