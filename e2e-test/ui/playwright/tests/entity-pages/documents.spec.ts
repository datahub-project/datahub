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
import type { Page } from '@playwright/test';
import { withTimestamp } from '../../utils/random';

test.use({ featureName: 'entity-pages' });

// ── Test data ─────────────────────────────────────────────────────────────────

const testId = withTimestamp('pw_doc');
const doc1Title = `${testId}_Parent`;
const doc2Title = `${testId}_Child`;

// ── Helpers ───────────────────────────────────────────────────────────────────

async function navigateToDocuments(page: Page): Promise<string> {
  await page.goto('/context/documents');
  await expect(page).toHaveURL(/\/document\//, { timeout: 15000 });
  const url = page.url();
  const match = url.match(/\/document\/([^/?]+)/);
  return match ? decodeURIComponent(match[1]) : '';
}

async function setDocumentTitle(page: Page, title: string): Promise<void> {
  const titleInput = page.locator('[data-testid="document-title-input"]');
  await expect(titleInput).toBeVisible({ timeout: 10000 });
  await expect(titleInput).toBeEnabled();
  await titleInput.clear();
  await titleInput.pressSequentially(title);
  await page.locator('body').click({ position: { x: 0, y: 0 } });
  await page.waitForTimeout(1500);
}

// ── Core Document CRUD Operations ─────────────────────────────────────────────

test.describe('Core Document CRUD Operations', () => {
  test.describe.configure({ mode: 'serial' });

  let doc1Urn = '';
  let doc2Urn = '';
  const doc1UpdatedTitle = `${doc1Title}_Updated`;

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ contextDocumentsEnabled: true, showNavBarRedesign: true, showHomePageRedesign: true });
  });

  test('should create a new document via Context Documents page', async ({ page }) => {
    await page.goto('/context/documents');
    await page.waitForTimeout(2000);
    await expect(page).toHaveURL(/\/document\//, { timeout: 15000 });

    await expect(page.locator('[data-testid="context-documents-sidebar"]')).toBeVisible();

    doc1Urn = await (async () => {
      const url = page.url();
      const match = url.match(/\/document\/([^/?]+)/);
      return match ? decodeURIComponent(match[1]) : '';
    })();

    await setDocumentTitle(page, doc1Title);
    await expect(page.locator('[data-testid="document-title-input"]')).toHaveValue(doc1Title);
  });

  test('should create a second document using sidebar create button', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForTimeout(2000);

    await expect(page.locator('[data-testid="context-documents-sidebar"]')).toBeVisible();

    await page.waitForTimeout(500);
    const createButton = page.locator('[data-testid="create-document-button"]');
    await expect(createButton).toBeVisible();
    await expect(createButton).toBeEnabled();
    await createButton.click({ force: true });

    // Wait for navigation to new document
    await expect(page).not.toHaveURL(new RegExp(doc1Urn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), { timeout: 10000 });
    await expect(page).toHaveURL(/\/document\//);

    doc2Urn = await (async () => {
      const url = page.url();
      const match = url.match(/\/document\/([^/?]+)/);
      return match ? decodeURIComponent(match[1]) : '';
    })();

    await expect(page.locator('[data-testid="create-document-button"]')).toBeEnabled({ timeout: 15000 });

    const titleInput = page.locator('[data-testid="document-title-input"]');
    await expect(titleInput).toBeVisible({ timeout: 10000 });
    await expect(titleInput).toBeEnabled();
    await titleInput.clear();
    await titleInput.pressSequentially(doc2Title);
    await page.locator('body').click({ position: { x: 0, y: 0 } });
    await page.waitForTimeout(1500);

    await expect(titleInput).toHaveValue(doc2Title);
  });

  test('should update document title and verify persistence', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForTimeout(1000);

    await setDocumentTitle(page, doc1UpdatedTitle);

    // Navigate away and return
    await page.goto('/');
    await page.waitForTimeout(500);
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForTimeout(1000);

    await expect(page.locator('[data-testid="document-title-input"]')).toHaveValue(doc1UpdatedTitle);
  });

  test('should update document content', async ({ page }) => {
    const testContent = `Test content for ${testId}`;

    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForTimeout(1000);

    await expect(page.locator('[data-testid="document-editor-section"]')).toBeAttached();

    const editor = page.locator('.remirror-editor[contenteditable="true"]');
    await expect(editor).toBeAttached();
    await editor.click();
    await page.waitForTimeout(300);
    await editor.clear({ force: true });
    await page.waitForTimeout(200);
    await editor.type(testContent, { delay: 100 });

    await page.locator('body').click({ position: { x: 0, y: 0 } });
    await page.waitForTimeout(3500);

    await page.reload();
    await page.waitForTimeout(1000);

    await expect(page.locator('.remirror-editor')).toContainText(testContent);
  });

  test('should update document status', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForTimeout(1000);

    const statusSelect = page.locator('[data-testid="document-status-select"]');
    await expect(statusSelect).toBeAttached({ timeout: 10000 });
    await page.waitForTimeout(1000);

    await statusSelect.locator('.ant-dropdown-trigger').click({ force: true });
    await page.waitForTimeout(500);
    await page.locator('[data-testid="option-PUBLISHED"]').click({ force: true });
    await page.waitForTimeout(1500);

    await expect(statusSelect).toContainText('Published');

    await page.reload();
    await page.waitForTimeout(1000);
    await expect(statusSelect).toContainText('Published');
  });

  test('should update document type', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(doc1Urn)}`);
    await page.waitForTimeout(1000);

    const typeSelect = page.locator('[data-testid="document-type-select"]');
    await typeSelect.locator('.ant-dropdown-trigger').click({ force: true });
    await page.waitForTimeout(500);
    await page.locator('[data-testid="option-Runbook"]').click({ force: true });
    await page.waitForTimeout(1500);

    await expect(typeSelect).toContainText('Runbook');

    await page.reload();
    await page.waitForTimeout(1000);
    await expect(typeSelect).toContainText('Runbook');
  });
});

// ── Sidebar Navigation ────────────────────────────────────────────────────────

test.describe('Sidebar Navigation Features', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ contextDocumentsEnabled: true, showNavBarRedesign: true, showHomePageRedesign: true });
  });

  test('should collapse and expand the sidebar', async ({ page }) => {
    await page.goto('/context/documents');
    await page.waitForTimeout(2000);
    await expect(page).toHaveURL(/\/document\//, { timeout: 15000 });

    const sidebar = page.locator('[data-testid="context-documents-sidebar"]');
    await expect(sidebar).toBeVisible();

    const collapseButton = page.locator('[data-testid="context-sidebar-collapse-button"]');
    await expect(collapseButton).toBeVisible();
    await expect(sidebar).toContainText('Documents');

    // Collapse
    await collapseButton.click();
    await page.waitForTimeout(500);
    await expect(page.locator('[data-testid="create-document-button"]')).not.toBeVisible();

    // Expand
    await collapseButton.click();
    await page.waitForTimeout(500);
    await expect(sidebar).toContainText('Documents');
    await expect(page.locator('[data-testid="create-document-button"]')).toBeVisible();
  });

  test('should search for documents using sidebar search', async ({ page }) => {
    await page.goto('/context/documents');
    await page.waitForTimeout(2000);
    await expect(page).toHaveURL(/\/document\//, { timeout: 15000 });

    const searchTitle = `${testId}_SearchTest`;

    // Set a title we can search for
    const titleInput = page.locator('[data-testid="document-title-input"]');
    await expect(titleInput).toBeVisible();
    await expect(titleInput).toBeEnabled();
    await titleInput.clear();
    await titleInput.pressSequentially(searchTitle);
    await page.locator('body').click({ position: { x: 0, y: 0 } });
    await page.waitForTimeout(1500);

    await expect(page.locator('[data-testid="context-documents-sidebar"]')).toBeVisible();

    const searchInput = page.locator('input[placeholder="Search documents"]');
    await expect(searchInput).toBeVisible();
    await searchInput.click();
    await searchInput.fill(searchTitle.substring(0, 8));

    await page.waitForTimeout(1500);

    await expect(
      page.locator('[data-testid="context-sidebar-search-results"]'),
    ).toBeVisible({ timeout: 10000 });

    // Clear search
    await page.locator('body').click({ position: { x: 0, y: 0 } });
    await page.waitForTimeout(500);
    await expect(page.locator('[data-testid="context-sidebar-search-results"]')).not.toBeAttached();
  });
});

// ── Document Hierarchy ────────────────────────────────────────────────────────

test.describe('Document Hierarchy Operations', () => {
  test.describe.configure({ mode: 'serial' });

  let parentUrn = '';
  let childUrn = '';

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ contextDocumentsEnabled: true, showNavBarRedesign: true, showHomePageRedesign: true });
  });

  test('should create two documents, move one to the other, and verify nesting', async ({ page }) => {
    // Create parent
    await page.goto('/context/documents');
    await page.waitForTimeout(2000);
    await expect(page).toHaveURL(/\/document\//, { timeout: 15000 });

    const url1 = page.url();
    const match1 = url1.match(/\/document\/([^/?]+)/);
    parentUrn = match1 ? decodeURIComponent(match1[1]) : '';

    await setDocumentTitle(page, doc1Title);

    // Create child
    const createButton = page.locator('[data-testid="create-document-button"]');
    await expect(createButton).toBeVisible();
    await createButton.click();

    await expect(page).not.toHaveURL(new RegExp(parentUrn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), { timeout: 10000 });
    const url2 = page.url();
    const match2 = url2.match(/\/document\/([^/?]+)/);
    childUrn = match2 ? decodeURIComponent(match2[1]) : '';

    await setDocumentTitle(page, doc2Title);

    // Move child to parent
    const treeItem = page.locator(`[data-testid="document-tree-item-${childUrn}"]`).first();
    await expect(treeItem).toBeAttached({ timeout: 5000 });
    await treeItem.scrollIntoViewIfNeeded();
    await treeItem.hover();
    await page.waitForTimeout(500);

    await treeItem.locator('[data-testid="document-actions-menu-button"]').click({ force: true });
    await page.waitForTimeout(1000);

    await page.locator('.ant-dropdown-menu').getByText('Move').click({ force: true });
    await page.waitForTimeout(2000);

    const movePopover = page.locator('[data-testid="move-document-popover"]');
    await expect(movePopover).toBeVisible({ timeout: 15000 });
    await movePopover.locator('input[placeholder="Search context..."]').pressSequentially(doc1Title);
    await page.waitForTimeout(2000);

    await movePopover
      .locator('[data-testid="move-popover-search-result-title"]')
      .filter({ hasText: doc1Title })
      .first()
      .click({ force: true });
    await page.waitForTimeout(1000);

    const confirmButton = page.locator('[data-testid="move-document-confirm-button"]');
    await expect(confirmButton).toBeEnabled({ timeout: 5000 });
    await confirmButton.click();
    await page.waitForTimeout(1500);

    await expect(page.getByText(/moved successfully/i)).toBeVisible({ timeout: 5000 });

    // Verify nesting via breadcrumb
    await page.goto(`/document/${encodeURIComponent(childUrn)}`);
    await page.waitForTimeout(1000);
    await expect(page.locator('a').filter({ hasText: doc1Title })).toBeAttached();
  });
});

// ── Document Deletion ─────────────────────────────────────────────────────────

test.describe('Document Deletion', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ contextDocumentsEnabled: true, showNavBarRedesign: true, showHomePageRedesign: true });
  });

  test('should create and delete a document', async ({ page }) => {
    await page.goto('/context/documents');
    await page.waitForTimeout(2000);
    await expect(page).toHaveURL(/\/document\//, { timeout: 15000 });

    const url = page.url();
    const match = url.match(/\/document\/([^/?]+)/);
    const docUrn = match ? decodeURIComponent(match[1]) : '';

    const deleteTitle = withTimestamp('pw_doc_delete');
    await setDocumentTitle(page, deleteTitle);

    // Hover to reveal actions
    const treeItem = page.locator(`[data-testid="document-tree-item-${docUrn}"]`).first();
    await expect(treeItem).toBeAttached({ timeout: 5000 });
    await treeItem.hover();
    await page.waitForTimeout(300);

    await treeItem.locator('[data-testid="document-actions-menu-button"]').click({ force: true });
    await page.waitForTimeout(300);

    await page.getByText('Delete').click();
    await page.waitForTimeout(500);

    await expect(page.getByText('Delete Document')).toBeVisible({ timeout: 3000 });
    await page.getByRole('button', { name: 'Delete' }).click();
    await page.waitForTimeout(2000);

    await expect(page.locator(`[data-testid="document-tree-item-${docUrn}"]`)).not.toBeAttached({ timeout: 10000 });
  });
});
