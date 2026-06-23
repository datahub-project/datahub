/**
 * DocumentPage — page object for the DataHub Document entity profile.
 *
 * Extends BasePage with document-specific navigation and CRUD operations.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS, LOAD_STATES, KEYS, DELAYS } from '../../utils/constants';

export class DocumentPage extends BasePage {
  // Document selectors in constructor
  readonly titleInput: Locator;
  readonly documentSidebar: Locator;
  readonly createButton: Locator;
  readonly collapseButton: Locator;
  readonly editorSection: Locator;
  readonly statusSelect: Locator;
  readonly typeSelect: Locator;
  readonly searchInput: Locator;
  readonly searchResults: Locator;
  readonly actionsMenuButton: Locator;
  readonly movePopover: Locator;
  readonly moveConfirmButton: Locator;

  // Factory methods for dynamic selectors
  readonly getTreeItem: (urn: string) => Locator;
  readonly getTreeItemMenuButton: (urn: string) => Locator;
  readonly getDropdownOption: (option: string) => Locator;
  readonly getEditorTextbox: () => Locator;
  readonly getMoveSearchInput: () => Locator;
  readonly getMoveOptionInDropdown: () => Locator;
  readonly getStatusSelectTrigger: () => Locator;
  readonly getTypeSelectTrigger: () => Locator;
  readonly getDropdownMenu: () => Locator;
  readonly getDeleteMenuOption: () => Locator;
  readonly getDeleteButton: () => Locator;
  readonly getDeleteConfirmDialog: () => Locator;
  readonly getParentBreadcrumb: (title: string) => Locator;
  readonly getMoveSuccessMessage: () => Locator;
  readonly getMoveSearchResultByText: (text: string) => Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    // Initialize all selectors in constructor
    this.titleInput = page.getByTestId('document-title-input');
    this.documentSidebar = page.getByTestId('context-documents-sidebar');
    this.createButton = page.getByTestId('create-document-button');
    this.collapseButton = page.getByTestId('context-sidebar-collapse-button');
    this.editorSection = page.getByTestId('document-editor-section');
    this.statusSelect = page.getByTestId('document-status-select');
    this.typeSelect = page.getByTestId('document-type-select');
    this.searchInput = page.getByPlaceholder('Search documents');
    this.searchResults = page.getByTestId('context-sidebar-search-results');
    this.actionsMenuButton = page.getByTestId('document-actions-menu-button');
    this.movePopover = page.getByTestId('move-document-popover');
    this.moveConfirmButton = page.getByTestId('move-document-confirm-button');

    // Factory methods
    this.getTreeItem = (urn: string) => page.getByTestId(`document-tree-item-${urn}`);
    this.getTreeItemMenuButton = (urn: string) => this.getTreeItem(urn).getByTestId('document-actions-menu-button');
    this.getDropdownOption = (option: string) => page.getByTestId(`option-${option}`);
    this.getEditorTextbox = () => this.editorSection.getByRole('textbox');
    this.getMoveSearchInput = () => this.movePopover.getByPlaceholder('Search context...');
    this.getMoveOptionInDropdown = () => this.getDropdownMenu().getByText('Move');
    // Ant Design's SimpleSelect uses .ant-dropdown-trigger for the dropdown trigger element.
    // This is the standard way to identify the clickable element that opens the dropdown.
    // eslint-disable-next-line playwright/no-raw-locators
    this.getStatusSelectTrigger = () => page.getByTestId('document-status-select').locator('.ant-dropdown-trigger');
    // eslint-disable-next-line playwright/no-raw-locators
    this.getTypeSelectTrigger = () => page.getByTestId('document-type-select').locator('.ant-dropdown-trigger');
    this.getDropdownMenu = () => page.getByRole('menu');
    this.getDeleteMenuOption = () => this.getDropdownMenu().getByText(/delete/i, { exact: false });
    this.getDeleteButton = () => page.getByRole('button', { name: 'Delete' });
    this.getDeleteConfirmDialog = () => page.getByText('Delete Document');
    this.getParentBreadcrumb = (title: string) => page.getByTestId('parent-breadcrumb-link').filter({ hasText: title });
    this.getMoveSuccessMessage = () => page.getByText(/moved successfully/i);
    this.getMoveSearchResultByText = (text: string) => this.movePopover.getByText(text, { exact: false });
  }

  // ── Navigation and Setup ──────────────────────────────────────────────────

  async navigateToDocuments(): Promise<string> {
    await this.page.goto('/context/documents');
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.page).toHaveURL(/\/document\//, { timeout: TIMEOUTS.LONG });
    await this.expectSidebarVisible();
    return this.extractDocumentUrnFromUrl(this.page.url());
  }

  async createNewDocumentViaButton(): Promise<string> {
    await this.expectSidebarVisible();
    await this.expectCreateButtonEnabled();
    await this.clickCreateButton();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    return this.extractDocumentUrnFromUrl(this.page.url());
  }

  async createDocumentWithTitle(title: string): Promise<string> {
    const docUrn = await this.createNewDocumentViaButton();
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
    await this.navigateToDocument(docUrn);
    await this.setDocumentTitle(title);
    return docUrn;
  }

  // ── Document URN utilities ────────────────────────────────────────────────

  extractDocumentUrnFromUrl(url: string): string {
    const match = url.match(/\/document\/([^/?]+)/);
    return match ? decodeURIComponent(match[1]) : '';
  }

  // ── Document operations ───────────────────────────────────────────────────

  async setDocumentTitle(title: string): Promise<void> {
    await expect(this.titleInput).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    // Triple-click selects all text in textarea reliably
    await this.titleInput.click({ clickCount: 3 });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    await this.titleInput.pressSequentially(title, { delay: DELAYS.SEQUENTIAL });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    // Press Enter to save the title (triggers blur/save handler)
    await this.page.keyboard.press(KEYS.ENTER);
    // Wait for title update to persist in the backend
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async expectTitleInput(title: string): Promise<void> {
    await expect(this.titleInput).toHaveValue(title);
  }

  async navigateToDocument(urn: string): Promise<void> {
    await this.navigate(`/document/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async expectSidebarVisible(): Promise<void> {
    await expect(this.documentSidebar).toBeVisible();
  }

  async expectCreateButtonEnabled(): Promise<void> {
    await expect(this.createButton).toBeEnabled();
  }

  async clickCreateButton(): Promise<void> {
    await this.createButton.click();
  }

  async expectSidebarContains(text: string): Promise<void> {
    await expect(this.documentSidebar).toContainText(text);
  }

  async expectCreateButtonHidden(): Promise<void> {
    await expect(this.createButton).toBeHidden();
  }

  async clickCollapseButton(): Promise<void> {
    await this.collapseButton.click();
  }

  async clickEditorAndType(text: string): Promise<void> {
    const editor = this.getEditorTextbox();
    await expect(editor).toBeVisible();
    await editor.click();
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    await editor.clear({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    await editor.fill(text);
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async expectEditorContains(text: string): Promise<void> {
    const editor = this.getEditorTextbox();
    await expect(editor).toContainText(text);
  }

  async updateDocumentStatus(status: string): Promise<void> {
    await expect(this.statusSelect).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    const trigger = this.getStatusSelectTrigger();
    await expect(trigger).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await trigger.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);

    const option = this.getDropdownOption(status);
    await expect(option).toBeVisible({ timeout: TIMEOUTS.LONG });
    await option.click();
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);

    // Verify optimistic update with case-insensitive match
    await expect(this.statusSelect).toContainText(new RegExp(status, 'i'));

    // Wait for network and database persistence before allowing reload
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.page.waitForTimeout(TIMEOUTS.MEDIUM);
  }

  async expectStatusContains(text: string): Promise<void> {
    await expect(this.statusSelect).toContainText(text);
  }

  async updateDocumentType(type: string): Promise<void> {
    await expect(this.typeSelect).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    const trigger = this.getTypeSelectTrigger();
    await expect(trigger).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await trigger.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);

    const option = this.getDropdownOption(type);
    await expect(option).toBeVisible({ timeout: TIMEOUTS.LONG });
    await option.click();
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);

    // Verify optimistic update with case-insensitive match
    await expect(this.typeSelect).toContainText(new RegExp(type, 'i'));

    // Wait for network and database persistence before allowing reload
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.page.waitForTimeout(TIMEOUTS.MEDIUM);
  }

  async expectTypeContains(text: string): Promise<void> {
    await expect(this.typeSelect).toContainText(text);
  }

  async searchForDocument(query: string): Promise<void> {
    await this.searchInput.click();
    await this.searchInput.fill(query);
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async expectSearchResultsVisible(): Promise<void> {
    await expect(this.searchResults).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async closeSearchAndVerifyClosed(): Promise<void> {
    await this.searchInput.clear();
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
    await expect(this.searchResults).not.toBeVisible({ timeout: TIMEOUTS.SHORT });
  }

  async clickTreeItemMenu(urn: string): Promise<void> {
    const treeItem = this.getTreeItem(urn);
    await expect(treeItem).toBeVisible();
    await treeItem.scrollIntoViewIfNeeded();
    await treeItem.hover();
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    const menuButton = this.getTreeItemMenuButton(urn);
    await expect(menuButton).toBeVisible();
    await menuButton.click();
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async clickMoveOption(): Promise<void> {
    const moveOption = this.getMoveOptionInDropdown();
    await expect(moveOption).toBeVisible();
    await moveOption.click({ force: true });
  }

  async expectMovePopoverVisible(): Promise<void> {
    await expect(this.movePopover).toBeVisible();
  }

  async searchInMovePopover(text: string): Promise<void> {
    const searchInput = this.getMoveSearchInput();
    await expect(searchInput).toBeVisible();
    await searchInput.clear();
    await searchInput.fill(text);
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async selectMoveSearchResult(text: string): Promise<void> {
    const resultElement = this.getMoveSearchResultByText(text);
    await expect(resultElement).toBeVisible({ timeout: TIMEOUTS.LONG });
    await resultElement.click({ force: true });
  }

  async clickMoveConfirmButton(): Promise<void> {
    await expect(this.moveConfirmButton).toBeEnabled();
    await this.moveConfirmButton.click();
  }

  async expectMoveSuccessMessage(): Promise<void> {
    await expect(this.getMoveSuccessMessage()).toBeVisible();
  }

  async clickActionsMenu(): Promise<void> {
    await expect(this.actionsMenuButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await expect(this.actionsMenuButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.actionsMenuButton.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async clickDeleteMenuItem(): Promise<void> {
    const deleteMenuItem = this.getDeleteMenuOption();
    await expect(deleteMenuItem).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await deleteMenuItem.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
  }

  async confirmDelete(): Promise<void> {
    const deleteDialog = this.getDeleteConfirmDialog();
    await expect(deleteDialog).toBeVisible();
    const deleteButton = this.getDeleteButton();
    await expect(deleteButton).toBeVisible();
    await deleteButton.click();
  }

  async expectParentBreadcrumbVisible(parentTitle: string): Promise<void> {
    await expect(this.getParentBreadcrumb(parentTitle)).toBeVisible();
  }

  async createAndMoveChildToParent(
    parentTitle: string,
    childTitle: string,
  ): Promise<{ parentUrn: string; childUrn: string }> {
    const parentUrn = await this.createDocumentWithTitle(parentTitle);

    await this.navigateToDocuments();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const childUrn = await this.createDocumentWithTitle(childTitle);
    await expect(this.getTreeItem(parentUrn)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.getTreeItem(childUrn)).toBeVisible({ timeout: TIMEOUTS.LONG });

    await this.clickTreeItemMenu(childUrn);
    await this.clickMoveOption();
    await this.expectMovePopoverVisible();
    await this.searchInMovePopover(parentTitle);
    await this.selectMoveSearchResult(parentTitle);
    await this.clickMoveConfirmButton();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.expectMoveSuccessMessage();

    return { parentUrn, childUrn };
  }
}
