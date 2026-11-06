/**
 * DocumentPage — page object for the DataHub Document entity profile.
 *
 * Extends BasePage with document-specific navigation and CRUD operations.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import { GraphQLHelper, GraphQLResponse } from '../../helpers/graphql-helper';
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
  private readonly graphql: GraphQLHelper;

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
  readonly getMoveSearchResultByText: (text: string) => Locator;
  readonly getDataHubSectionExpandAll: () => Locator;
  readonly getTreeItemExpandButton: (urn: string) => Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphql = new GraphQLHelper(page);

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
    this.actionsMenuButton = page.getByTestId('document-actions-menu-button').filter({ visible: true });
    this.movePopover = page.getByTestId('move-document-popover');
    this.moveConfirmButton = page.getByTestId('move-document-confirm-button');

    // Factory methods
    this.getTreeItem = (urn: string) => page.getByTestId(`document-tree-item-${urn}`);
    this.getTreeItemMenuButton = (urn: string) => this.getTreeItem(urn).getByTestId('document-actions-menu-button');
    this.getDropdownOption = (option: string) => page.getByTestId(`option-${option}`);
    this.getEditorTextbox = () => this.editorSection.getByRole('textbox');
    this.getMoveSearchInput = () => this.movePopover.getByPlaceholder('Search context...');
    this.getMoveOptionInDropdown = () =>
      // Prefer the visible open menu — Ant may leave hidden menus in the DOM.
      // eslint-disable-next-line playwright/no-raw-locators
      this.page.locator('[role="menu"]:visible').getByText(/move/i);
    // Ant Design's SimpleSelect uses .ant-dropdown-trigger for the dropdown trigger element.
    // This is the standard way to identify the clickable element that opens the dropdown.
    // eslint-disable-next-line playwright/no-raw-locators
    this.getStatusSelectTrigger = () => page.getByTestId('document-status-select').locator('.ant-dropdown-trigger');
    // eslint-disable-next-line playwright/no-raw-locators
    this.getTypeSelectTrigger = () => page.getByTestId('document-type-select').locator('.ant-dropdown-trigger');
    // eslint-disable-next-line playwright/no-raw-locators
    this.getDropdownMenu = () => page.locator('[role="menu"]:visible');
    this.getDeleteMenuOption = () => this.getDropdownMenu().getByText(/delete/i, { exact: false });
    this.getDeleteButton = () => page.getByRole('button', { name: 'Delete' });
    this.getDeleteConfirmDialog = () => page.getByText('Delete Document');
    this.getParentBreadcrumb = (title: string) => page.getByTestId('parent-breadcrumb-link').filter({ hasText: title });
    this.getMoveSearchResultByText = (text: string) => this.movePopover.getByText(text, { exact: false });
    this.getDataHubSectionExpandAll = () => page.getByTestId('document-tree-datahub-section-expand-all');
    this.getTreeItemExpandButton = (urn: string) =>
      this.getTreeItem(urn).getByRole('button', { name: /^(expand|collapse)$/i });
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
    const previousUrn = this.extractDocumentUrnFromUrl(this.page.url());
    await this.clickCreateButton();
    // Create is async (mutation + history.push). networkidle alone can resolve while we are
    // still on the previous document — then setDocumentTitle would rename that doc.
    await expect
      .poll(
        () => {
          const urn = this.extractDocumentUrnFromUrl(this.page.url());
          return urn && urn !== previousUrn ? urn : '';
        },
        { timeout: TIMEOUTS.LONG },
      )
      .not.toBe('');
    return this.extractDocumentUrnFromUrl(this.page.url());
  }

  async createDocumentWithTitle(title: string): Promise<string> {
    const docUrn = await this.createNewDocumentViaButton();
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
    await this.titleInput.pressSequentially(title, { delay: DELAYS.SEQUENTIAL });
    await expect(this.titleInput).toHaveValue(title);
    // Titles are saved through updateDocumentContents; match on the title so the
    // waiter can't resolve on a contents-only save.
    const saved = this.graphql.waitForGraphQLResponse(
      'updateDocumentContents',
      (variables) => (variables.input as { title?: string } | undefined)?.title === title,
    );
    // Press Enter to save the title (triggers blur/save handler)
    await this.page.keyboard.press(KEYS.ENTER);
    await this.expectMutationSucceeded(saved, 'updateDocumentContents');
  }

  async expectTitleInput(title: string): Promise<void> {
    await expect(this.titleInput).toHaveValue(title);
  }

  async navigateToDocument(urn: string): Promise<void> {
    await this.navigate(`/document/${encodeURIComponent(urn)}`);
    // Not networkidle: it has no timeout, and a just-created document's page can keep the network
    // busy long enough to hang the test.
    await expect(this.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectSidebarVisible(): Promise<void> {
    await expect(this.documentSidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
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
    await editor.clear({ force: true });
    const saved = this.graphql.waitForGraphQLResponse(
      'updateDocumentContents',
      (variables) => (variables.input as { contents?: unknown } | undefined)?.contents !== undefined,
    );
    await editor.fill(text);
    // By default the editor auto-saves 3s after the last keystroke. With documentExplicitSaveEnabled
    // on, it shows a Save button instead and never auto-saves, so click it when it appears. Race the
    // two so auto-save mode returns as soon as its save lands; a failed save is reported below.
    const saveButton = this.page.getByTestId('document-save-button');
    const explicitSave = await Promise.race([
      saveButton.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT }).then(
        () => true,
        () => false,
      ),
      saved.then(
        () => false,
        () => false,
      ),
    ]);
    if (explicitSave) {
      await saveButton.click();
    }
    await this.expectMutationSucceeded(saved, 'updateDocumentContents');
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

    const option = this.getDropdownOption(status);
    await expect(option).toBeVisible({ timeout: TIMEOUTS.LONG });
    const saved = this.graphql.waitForGraphQLResponse(
      'updateDocumentStatus',
      (variables) => (variables.input as { state?: string } | undefined)?.state === status,
    );
    await option.click();

    // Verify optimistic update with case-insensitive match
    await expect(this.statusSelect).toContainText(new RegExp(status, 'i'));

    // Callers reload next, so the change must be persisted first
    await this.expectMutationSucceeded(saved, 'updateDocumentStatus');
  }

  async expectStatusContains(text: string): Promise<void> {
    await expect(this.statusSelect).toContainText(text);
  }

  async updateDocumentType(type: string): Promise<void> {
    await expect(this.typeSelect).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    const trigger = this.getTypeSelectTrigger();
    await expect(trigger).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await trigger.click({ force: true });

    const option = this.getDropdownOption(type);
    await expect(option).toBeVisible({ timeout: TIMEOUTS.LONG });
    const saved = this.graphql.waitForGraphQLResponse('updateDocumentSubType');
    await option.click();

    // Verify optimistic update with case-insensitive match
    await expect(this.typeSelect).toContainText(new RegExp(type, 'i'));

    // Callers reload next, so the change must be persisted first
    await this.expectMutationSucceeded(saved, 'updateDocumentSubType');
  }

  async expectTypeContains(text: string): Promise<void> {
    await expect(this.typeSelect).toContainText(text);
  }

  async searchForDocument(query: string): Promise<void> {
    await this.searchInput.click();
    await this.searchInput.fill(query);
  }

  async expectSearchResultsVisible(): Promise<void> {
    await expect(this.searchResults).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async closeSearchAndVerifyClosed(): Promise<void> {
    await this.searchInput.clear();
    // MEDIUM leaves room for the search input's debounce before the results unmount.
    await expect(this.searchResults).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async clickTreeItemMenu(urn: string): Promise<void> {
    const treeItem = this.getTreeItem(urn);
    await expect(treeItem).toBeVisible({ timeout: TIMEOUTS.LONG });
    await treeItem.scrollIntoViewIfNeeded();
    // Reveal CSS-hover actions, then move onto ⋮ so the title Tooltip doesn't cover it.
    await treeItem.hover();
    const menuButton = this.getTreeItemMenuButton(urn);
    await expect(menuButton).toBeVisible({ timeout: TIMEOUTS.LONG });
    await menuButton.hover();
    await menuButton.click({ force: true });
    await expect(this.getDropdownMenu().filter({ visible: true }).first()).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
  }

  async clickMoveOption(): Promise<void> {
    const moveOption = this.getMoveOptionInDropdown();
    await expect(moveOption).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await moveOption.click();
  }

  async expectMovePopoverVisible(): Promise<void> {
    await expect(this.movePopover).toBeVisible();
  }

  async searchInMovePopover(text: string): Promise<void> {
    const searchInput = this.getMoveSearchInput();
    await expect(searchInput).toBeVisible();
    await searchInput.clear();
    await searchInput.fill(text);
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
    await this.toast.expectVisible(/moved successfully/i);
  }

  async clickActionsMenu(): Promise<void> {
    await expect(this.actionsMenuButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await expect(this.actionsMenuButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.actionsMenuButton.click({ force: true });
  }

  /** Click Delete in the open actions menu and wait for the confirm dialog. */
  async clickDeleteMenuItem(): Promise<void> {
    const deleteMenuItem = this.getDeleteMenuOption();
    const deleteDialog = this.getDeleteConfirmDialog();
    // A forced click can land while the menu is still animating open and be dropped, which
    // closes the menu without opening the dialog. Reopen the menu and retry until it opens.
    await expect(async () => {
      if (await deleteDialog.isVisible()) {
        return;
      }
      if (!(await deleteMenuItem.isVisible())) {
        await this.actionsMenuButton.click({ force: true });
      }
      await deleteMenuItem.click({ force: true, timeout: TIMEOUTS.SHORT });
      await expect(deleteDialog).toBeVisible({ timeout: TIMEOUTS.SHORT });
    }).toPass({ timeout: TIMEOUTS.EXTRA_LONG });
  }

  /** Confirm the delete dialog and wait for the deleteDocument mutation to finish. */
  async confirmDelete(): Promise<void> {
    const deleteDialog = this.getDeleteConfirmDialog();
    await expect(deleteDialog).toBeVisible();
    const deleteButton = this.getDeleteButton();
    await expect(deleteButton).toBeVisible();
    const deleted = this.graphql.waitForGraphQLResponse('deleteDocument');
    await deleteButton.click();
    await this.expectMutationSucceeded(deleted, 'deleteDocument');
  }

  /** Await a mutation's response and fail on GraphQL errors or a false result. */
  private async expectMutationSucceeded(response: Promise<GraphQLResponse>, mutation: string): Promise<void> {
    const json = await response;
    expect(json.errors, `${mutation} returned errors`).toBeUndefined();
    expect((json.data as Record<string, unknown> | undefined)?.[mutation], `${mutation} result`).toBe(true);
  }

  /**
   * The sidebar tree is search-backed, so a deleted document can linger until the index catches
   * up. Reload the documents page until none of `removedUrns` are in the tree. `keptUrn` is a
   * document the test did not delete: once it renders, the tree has loaded, so the absence checks
   * can't pass on a tree that is still empty.
   */
  async expectDocumentsRemovedFromTree(removedUrns: string[], keptUrn: string): Promise<void> {
    await expect(async () => {
      // Not navigateToDocuments(): its networkidle wait has no timeout and can hang right after a
      // delete, which toPass cannot interrupt. Wait for the tree's search request instead.
      const [treeSearch] = await Promise.all([
        this.graphql.waitForGraphQLResponse('searchDocuments'),
        this.page.goto('/context/documents'),
      ]);
      expect(treeSearch.errors, 'searchDocuments returned errors').toBeUndefined();
      await expect(this.getTreeItem(keptUrn)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
      for (const urn of removedUrns) {
        await expect(this.getTreeItem(urn)).not.toBeAttached({ timeout: TIMEOUTS.SHORT });
      }
    }).toPass({ timeout: TIMEOUTS.EXTRA_LONG * 2 });
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
    expect(childUrn).not.toBe(parentUrn);

    // Return to the documents list so both tree rows are mounted before Move.
    await this.navigateToDocuments();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await expect(this.getTreeItem(parentUrn)).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.getTreeItem(childUrn)).toBeVisible({ timeout: TIMEOUTS.LONG });

    await this.clickTreeItemMenu(childUrn);
    await this.clickMoveOption();
    await this.expectMovePopoverVisible();
    // Pick by URN in the browse tree. Search-by-title is flaky here: the rename often
    // hasn't hit the search index yet (row still shows "New Document"), which is unrelated
    // to filters/sort — only the destination identity matters for this helper.
    const parentRow = this.movePopover.getByTestId(`document-tree-item-${parentUrn}`);
    await expect(parentRow).toBeVisible({ timeout: TIMEOUTS.LONG });
    await parentRow.scrollIntoViewIfNeeded();
    await parentRow.click();
    await expect(this.moveConfirmButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.clickMoveConfirmButton();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.expectMoveSuccessMessage();

    return { parentUrn, childUrn };
  }

  /**
   * Create a child under `parentUrn` via the row (+) control — avoids move-popover
   * search / ES lag when we only need a nested pair for expand tests.
   */
  async createChildUnderParentViaPlus(parentUrn: string, childTitle: string): Promise<string> {
    await this.navigateToDocument(parentUrn);
    await expect(this.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    const parentRow = this.getTreeItem(parentUrn);
    await expect(parentRow).toBeVisible({ timeout: TIMEOUTS.LONG });
    await parentRow.scrollIntoViewIfNeeded();

    // Reveal CSS-hover actions, then move onto + so the title Tooltip doesn't cover it.
    await parentRow.hover();
    const createChildButton = parentRow.getByTestId('document-tree-create-child-button');
    await expect(createChildButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await createChildButton.hover();
    await createChildButton.click({ force: true });

    await expect
      .poll(() => this.extractDocumentUrnFromUrl(this.page.url()), { timeout: TIMEOUTS.LONG })
      .not.toBe(parentUrn);

    const childUrn = this.extractDocumentUrnFromUrl(this.page.url());
    expect(childUrn).toBeTruthy();
    await expect(this.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await this.setDocumentTitle(childTitle);
    return childUrn;
  }

  /** Hover the row so the Notion-style caret is clickable, then toggle expand. */
  async toggleTreeItemExpand(urn: string): Promise<void> {
    const treeItem = this.getTreeItem(urn);
    await expect(treeItem).toBeVisible({ timeout: TIMEOUTS.LONG });
    await treeItem.scrollIntoViewIfNeeded();
    const expandButton = this.getTreeItemExpandButton(urn);
    // Expand control is on the left; hover it directly (not the title) before clicking.
    await expandButton.hover({ force: true });
    await expect(expandButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    const wasExpanded = await expandButton.getAttribute('aria-expanded');
    await expandButton.click({ force: true });
    await expect(expandButton).not.toHaveAttribute('aria-expanded', wasExpanded ?? 'false', {
      timeout: TIMEOUTS.LONG,
    });
  }

  async expectTreeItemExpanded(urn: string): Promise<void> {
    const expandButton = this.getTreeItemExpandButton(urn);
    await expandButton.hover({ force: true });
    await expect(expandButton).toHaveAttribute('aria-expanded', 'true', {
      timeout: TIMEOUTS.LONG,
    });
  }

  async expectTreeItemCollapsed(urn: string): Promise<void> {
    const expandButton = this.getTreeItemExpandButton(urn);
    await expandButton.hover({ force: true });
    await expect(expandButton).toHaveAttribute('aria-expanded', 'false', {
      timeout: TIMEOUTS.LONG,
    });
  }

  async clickDataHubSectionExpandAll(): Promise<void> {
    const button = this.getDataHubSectionExpandAll();
    await expect(button).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(button).toBeEnabled({ timeout: TIMEOUTS.LONG });
    await button.click();
    // Expand-all can take a while on large libraries; wait for the control to unlock.
    await expect(button).toBeEnabled({ timeout: 60_000 });
  }

  /** If the section is already expanded, collapse-all first so expand-all has work to do. */
  async ensureDataHubSectionCollapsed(): Promise<void> {
    const button = this.getDataHubSectionExpandAll();
    await expect(button).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(button).toBeEnabled({ timeout: TIMEOUTS.LONG });
    const label = (await button.getAttribute('aria-label')) || '';
    if (/collapse/i.test(label)) {
      await button.click();
      await expect(button).toBeEnabled({ timeout: TIMEOUTS.LONG });
      await expect(button).not.toHaveAttribute('aria-label', /collapse/i, { timeout: TIMEOUTS.LONG });
    }
  }

  async expectTreeItemVisibleInSidebar(urn: string): Promise<void> {
    await expect(this.getTreeItem(urn)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectTreeItemHiddenInSidebar(urn: string): Promise<void> {
    await expect(this.getTreeItem(urn)).toBeHidden({ timeout: TIMEOUTS.LONG });
  }
}
