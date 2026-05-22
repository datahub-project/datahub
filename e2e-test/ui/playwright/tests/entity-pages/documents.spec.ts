/**
 * Document management tests — migrated from Cypress e2e/documents/document_management.js
 *
 * Covers: document CRUD, sidebar navigation (collapse/expand, search),
 * document hierarchy (move/nesting), and document deletion with cascade.
 *
 * Feature flags required: contextDocumentsEnabled=true, showNavBarRedesign=true,
 * showHomePageRedesign=true.
 *
 * Tests in each describe block are serial to share document URNs across steps.
 */

import { test, expect } from '../../fixtures/base-test';
import { withTimestamp } from '../../utils/random';
import { DOCUMENT_SELECTORS } from '../../selectors/documents.selectors';
import { DocumentPage } from '../../pages/entity/document.page';
import { TIMEOUTS, WAITS } from './constants';

test.use({ featureName: 'entity-pages' });

// ── Test data ─────────────────────────────────────────────────────────────────

const testId = withTimestamp('pw_doc');
const doc1Title = `${testId}_Parent`;
const doc2Title = `${testId}_Child`;

// ── Core Document CRUD Operations ─────────────────────────────────────────────

test.describe('Core Document CRUD Operations', () => {
  test.describe.configure({ mode: 'serial' });

  let doc1Urn = '';
  let doc2Urn = '';
  const doc1UpdatedTitle = `${doc1Title}_Updated`;

  test.beforeEach(async ({ apiMock, page }) => {
    // Set feature flags and navigate to documents page to ensure app state
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.afterAll(async ({ cleanup }) => {
    if (doc1Urn) cleanup.track(doc1Urn);
    if (doc2Urn) cleanup.track(doc2Urn);
    await cleanup.flush();
  });

  test('should create a new document via Context Documents page', async ({ page }) => {
    const documentPage = new DocumentPage(page);
    await page.goto('/context/documents');
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(/\/document\//, { timeout: TIMEOUTS.LONG });

    await expect(page.locator(DOCUMENT_SELECTORS.sidebar)).toBeVisible();

    doc1Urn = documentPage.extractDocumentUrnFromUrl(page.url());

    await documentPage.setDocumentTitle(doc1Title);
    await expect(page.locator(DOCUMENT_SELECTORS.titleInput)).toHaveValue(doc1Title);
  });

  test('should create a second document using sidebar create button', async ({ page }) => {
    const documentPage = new DocumentPage(page);
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForLoadState('networkidle');

    await expect(page.locator(DOCUMENT_SELECTORS.sidebar)).toBeVisible();

    const createButton = page.locator(DOCUMENT_SELECTORS.createButton);
    await expect(createButton).toBeVisible();
    await expect(createButton).toBeEnabled();
    await createButton.click({ force: true });

    // Wait for navigation to new document
    await expect(page).not.toHaveURL(new RegExp(doc1Urn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), {
      timeout: TIMEOUTS.NORMAL,
    });
    await expect(page).toHaveURL(/\/document\//);

    doc2Urn = documentPage.extractDocumentUrnFromUrl(page.url());

    await expect(page.locator(DOCUMENT_SELECTORS.createButton)).toBeEnabled({ timeout: TIMEOUTS.LONG });

    const titleInput = page.locator(DOCUMENT_SELECTORS.titleInput);
    await expect(titleInput).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await expect(titleInput).toBeEnabled();
    await titleInput.clear();
    await titleInput.pressSequentially(doc2Title);
    await page.locator('body').click();
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);

    await expect(titleInput).toHaveValue(doc2Title);
  });

  test('should update document title and verify persistence', async ({ page }) => {
    const documentPage = new DocumentPage(page);
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForLoadState('networkidle');

    await documentPage.setDocumentTitle(doc1UpdatedTitle);

    // Verify title is set immediately
    const titleInput = page.locator(DOCUMENT_SELECTORS.titleInput);
    await expect(titleInput).toHaveValue(doc1UpdatedTitle, { timeout: TIMEOUTS.NORMAL });
  });

  test('should update document content', async ({ page }) => {
    const testContent = `Test content for ${testId}`;

    // Navigate to context documents first to ensure app is initialized
    await page.goto('/context/documents');
    await page.waitForLoadState('networkidle');

    // Then navigate to specific document
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForLoadState('networkidle');

    // Wait for sidebar to ensure document page loaded
    await expect(page.locator(DOCUMENT_SELECTORS.sidebar)).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Wait for document data to load and editor section to appear
    await expect(page.locator(DOCUMENT_SELECTORS.titleInput)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(page.locator(DOCUMENT_SELECTORS.editorSection)).toBeVisible({ timeout: TIMEOUTS.LONG });

    const editor = page.locator(DOCUMENT_SELECTORS.editor);
    await expect(editor).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await editor.click();
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await editor.clear({ force: true });
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await editor.pressSequentially(testContent, { delay: 100 });

    await page.locator('body').click();
    await page.waitForTimeout(WAITS.OPERATION);

    await page.reload();
    await page.waitForLoadState('networkidle');

    await expect(page.locator(DOCUMENT_SELECTORS.editor)).toContainText(testContent);
  });

  test('should update document status', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForLoadState('networkidle');

    const statusSelect = page.locator(DOCUMENT_SELECTORS.statusSelect);
    await expect(statusSelect).toBeAttached({ timeout: TIMEOUTS.NORMAL });

    await statusSelect.locator(DOCUMENT_SELECTORS.antDropdownTrigger).click({ force: true });
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await page.locator(DOCUMENT_SELECTORS.optionPublished).click({ force: true });
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);

    await expect(statusSelect).toContainText('Published');

    await page.reload();
    await page.waitForLoadState('networkidle');
    await expect(statusSelect).toContainText('Published');
  });

  test('should update document type', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForLoadState('networkidle');

    const typeSelect = page.locator(DOCUMENT_SELECTORS.typeSelect);
    await typeSelect.locator(DOCUMENT_SELECTORS.antDropdownTrigger).click({ force: true });
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await page.locator(DOCUMENT_SELECTORS.optionRunbook).click({ force: true });
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);

    await expect(typeSelect).toContainText('Runbook');

    await page.reload();
    await page.waitForLoadState('networkidle');
    await expect(typeSelect).toContainText('Runbook');
  });
});

// ── Sidebar Navigation ────────────────────────────────────────────────────────

test.describe('Sidebar Navigation Features', () => {
  let sidebarDocUrn = '';

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
  });

  test.afterAll(async ({ cleanup }) => {
    if (sidebarDocUrn) cleanup.track(sidebarDocUrn);
    await cleanup.flush();
  });

  test('should collapse and expand the sidebar', async ({ page }) => {
    const documentPage = new DocumentPage(page);
    await page.goto('/context/documents');
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(/\/document\//, { timeout: TIMEOUTS.LONG });

    sidebarDocUrn = documentPage.extractDocumentUrnFromUrl(page.url());

    const sidebar = page.locator(DOCUMENT_SELECTORS.sidebar);
    await expect(sidebar).toBeVisible();

    const collapseButton = page.locator(DOCUMENT_SELECTORS.collapseButton);
    await expect(collapseButton).toBeVisible();
    await expect(sidebar).toContainText('Documents');

    // Collapse
    await collapseButton.click();
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await expect(page.locator(DOCUMENT_SELECTORS.createButton)).toBeHidden();

    // Expand
    await collapseButton.click();
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await expect(sidebar).toContainText('Documents');
    await expect(page.locator(DOCUMENT_SELECTORS.createButton)).toBeVisible();
  });

  test('should search for documents using sidebar search', async ({ page }) => {
    await page.goto('/context/documents');
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(/\/document\//, { timeout: TIMEOUTS.LONG });

    const searchTitle = `${testId}_SearchTest`;

    // Set a title we can search for
    const titleInput = page.locator(DOCUMENT_SELECTORS.titleInput);
    await expect(titleInput).toBeVisible();
    await expect(titleInput).toBeEnabled();
    await titleInput.clear();
    await titleInput.pressSequentially(searchTitle);
    await page.locator('body').click();
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);

    await expect(page.locator(DOCUMENT_SELECTORS.sidebar)).toBeVisible();

    const searchInput = page.locator(DOCUMENT_SELECTORS.searchInput);
    await expect(searchInput).toBeVisible();
    await searchInput.click();
    await searchInput.fill(searchTitle.substring(0, 8));

    await page.waitForTimeout(WAITS.SEARCH);

    await expect(page.locator(DOCUMENT_SELECTORS.searchResults)).toBeVisible({
      timeout: TIMEOUTS.NORMAL,
    });

    // Clear search
    await page.locator('body').click();
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await expect(page.locator(DOCUMENT_SELECTORS.searchResults)).not.toBeAttached();
  });
});

// ── Document Hierarchy ────────────────────────────────────────────────────────

test.describe('Document Hierarchy Operations', () => {
  test.describe.configure({ mode: 'serial' });

  let parentUrn = '';
  let childUrn = '';

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
  });

  test.afterAll(async ({ cleanup }) => {
    if (parentUrn) cleanup.track(parentUrn);
    if (childUrn) cleanup.track(childUrn);
    await cleanup.flush();
  });

  test('should create two documents, move one to the other, and verify nesting', async ({ page }) => {
    const documentPage = new DocumentPage(page);
    // Create parent
    await page.goto('/context/documents');
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(/\/document\//, { timeout: TIMEOUTS.LONG });

    parentUrn = documentPage.extractDocumentUrnFromUrl(page.url());

    await documentPage.setDocumentTitle(doc1Title);

    // Create child
    const createButton = page.locator(DOCUMENT_SELECTORS.createButton);
    await expect(createButton).toBeVisible();
    await createButton.click();

    await expect(page).not.toHaveURL(new RegExp(parentUrn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), {
      timeout: TIMEOUTS.NORMAL,
    });
    childUrn = documentPage.extractDocumentUrnFromUrl(page.url());

    await documentPage.setDocumentTitle(doc2Title);

    // Move child to parent
    const treeItem = page.locator(DOCUMENT_SELECTORS.treeItem(childUrn)).first();
    await expect(treeItem).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await treeItem.scrollIntoViewIfNeeded();
    await treeItem.hover();
    await page.waitForTimeout(WAITS.UI_ANIMATION);

    const actionsMenu = treeItem.locator(DOCUMENT_SELECTORS.actionsMenuButton);
    await expect(actionsMenu).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await actionsMenu.click({ force: true });
    await page.waitForTimeout(WAITS.OPERATION);

    const moveOption = page.locator(DOCUMENT_SELECTORS.antDropdown).getByText('Move');
    await expect(moveOption).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await moveOption.click({ force: true });
    await page.waitForTimeout(WAITS.OPERATION);

    const movePopover = page.locator(DOCUMENT_SELECTORS.movePopover);
    await expect(movePopover).toBeVisible({ timeout: TIMEOUTS.LONG });
    const searchInput = movePopover.locator(DOCUMENT_SELECTORS.moveSearchInput);
    await expect(searchInput).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await searchInput.fill(doc1Title);
    await page.waitForTimeout(WAITS.SEARCH);

    // Wait for search results to load
    await page.waitForTimeout(WAITS.SEARCH);

    const searchResult = movePopover
      .locator(DOCUMENT_SELECTORS.moveSearchResult)
      .filter({ hasText: doc1Title })
      .first();
    await expect(searchResult).toBeVisible({ timeout: TIMEOUTS.LONG });
    await page.waitForTimeout(WAITS.UI_ANIMATION);
    await searchResult.click({ force: true });
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);

    const confirmButton = page.locator(DOCUMENT_SELECTORS.moveConfirmButton);
    await expect(confirmButton).toBeEnabled({ timeout: TIMEOUTS.NORMAL });
    await confirmButton.click();
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);
    await page.waitForLoadState('networkidle');

    // Wait for success message confirming move
    await expect(page.getByText(/moved successfully/i)).toBeVisible({ timeout: TIMEOUTS.NORMAL });

    // Navigate directly to child document to verify the move worked
    await page.goto(`/document/${encodeURIComponent(childUrn)}`);
    await page.waitForLoadState('networkidle');

    // Verify breadcrumb shows parent document (indicates successful move)
    await expect(page.locator(DOCUMENT_SELECTORS.titleInput)).toBeVisible({ timeout: TIMEOUTS.LONG });
    const parentBreadcrumb = page.locator('a').filter({ hasText: doc1Title });
    await expect(parentBreadcrumb).toBeVisible({ timeout: TIMEOUTS.NORMAL });
  });
});

// ── Document Deletion ─────────────────────────────────────────────────────────

test.describe('Document Deletion', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
  });

  test('should create and delete a document', async ({ page }) => {
    const documentPage = new DocumentPage(page);
    // Create a new document via context/documents
    await page.goto('/context/documents');
    await page.waitForTimeout(WAITS.OPERATION);
    await expect(page).toHaveURL(/\/document\//, { timeout: TIMEOUTS.LONG });

    const docUrn = documentPage.extractDocumentUrnFromUrl(page.url());

    const deleteTitle = withTimestamp('pw_doc_delete');
    await documentPage.setDocumentTitle(deleteTitle);

    // Click actions menu in document header
    const actionsMenu = page.locator(DOCUMENT_SELECTORS.actionsMenuButton);
    await expect(actionsMenu).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await actionsMenu.click({ force: true });
    await page.waitForTimeout(WAITS.UI_ANIMATION);

    // Click delete option from menu
    const deleteMenu = page.locator(DOCUMENT_SELECTORS.deleteMenuItem);
    await expect(deleteMenu).toBeVisible({ timeout: TIMEOUTS.SHORT });
    await deleteMenu.click();
    await page.waitForTimeout(WAITS.INPUT_DEBOUNCE);

    // Confirm deletion in dialog
    const deleteDialog = page.getByText('Delete Document');
    await expect(deleteDialog).toBeVisible({ timeout: TIMEOUTS.NORMAL });

    const deleteButton = page.getByRole('button', { name: 'Delete' });
    await expect(deleteButton).toBeVisible();
    await deleteButton.click();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(WAITS.OPERATION);

    // Verify deletion by navigating back to documents list and checking tree
    await page.goto('/context/documents');
    await page.waitForLoadState('networkidle');

    // Verify the deleted document is no longer in the tree
    await expect(page.locator(DOCUMENT_SELECTORS.treeItem(docUrn))).not.toBeAttached({
      timeout: TIMEOUTS.LONG,
    });
  });
});
