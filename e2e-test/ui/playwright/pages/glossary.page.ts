/**
 * GlossaryPage — page object for /glossary (Business Glossary).
 *
 * Covers the full CRUD lifecycle for Glossary Term Groups and Glossary Terms,
 * including navigation, creation, moving, deleting, and adding terms to datasets.
 */

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { GraphQLHelper } from '../helpers/graphql-helper';
import { ModalComponent } from './common/modal-component';

export class GlossaryPage extends BasePage {
  readonly modalComponent: ModalComponent;

  // ── Navigation ──────────────────────────────────────────────────────────────
  readonly sidebarContainer: Locator;
  readonly activeTabPanel: Locator;

  // ── Root-level create buttons (GlossaryContentProvider header) ──────────────
  readonly addTermGroupButtonV2: Locator;

  // ── Entity contents tab buttons (ChildrenTab) ────────────────────────────────
  readonly addTermButton: Locator;

  // ── Create modal (CreateGlossaryEntityModal) ─────────────────────────────────
  readonly createModalNameInput: Locator;
  readonly createModalSubmitButton: Locator;

  // ── Move modal (MoveGlossaryEntityModal) ─────────────────────────────────────
  readonly moveModalContainer: Locator;
  readonly moveModalSelectInput: Locator;
  readonly moveModalSubmitButton: Locator;

  // ── Three-dot entity menu (EntityDropdown) ────────────────────────────────────
  readonly entityMenuThreeDotButton: Locator;
  readonly entityMenuDeleteButton: Locator;
  readonly entityMenuMoveButton: Locator;

  // ── Search input (shared EmbeddedListSearch) ──────────────────────────────
  readonly searchInput: Locator;

  // ── Filters toggle button (EmbeddedListSearchHeader) ─────────────────────
  readonly filtersToggleButton: Locator;

  // ── Advanced search / filter panel ───────────────────────────────────────
  readonly advancedSearchButton: Locator;
  readonly addFilterButton: Locator;
  readonly addFilterTagsButton: Locator;
  readonly filterTagSelectInput: Locator;
  readonly addTagsConfirmButton: Locator;

  // ── Batch Add to Assets (EntityActions → SearchSelectModal) ─────────────────
  readonly batchAddButton: Locator;
  readonly batchAddModalSearchInput: Locator;
  readonly batchAddConfirmButton: Locator;
  readonly batchAddedToast: Locator;

  // ── Create modal headings ─────────────────────────────────────────────────
  readonly createGlossaryHeading: Locator;
  readonly createGlossaryTermHeading: Locator;

  // ── Confirmation button / toast notifications ─────────────────────────────
  readonly deleteConfirmButton: Locator;
  readonly createdTermGroupToast: Locator;
  readonly createdGlossaryTermToast: Locator;
  readonly deletedEntityToast: Locator;
  readonly movedEntityToast: Locator;

  // ── Tabs ──────────────────────────────────────────────────────────────────
  readonly propertiesTab: Locator;

  private readonly graphqlHelper: GraphQLHelper;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);

    this.modalComponent = new ModalComponent(page);

    this.sidebarContainer = page.getByTestId('glossary-browser-sidebar');
    this.activeTabPanel = page.locator('.ant-tabs-tabpane-active');

    // `:visible` needed — multiple instances may exist in the DOM simultaneously.
    this.addTermGroupButtonV2 = page.getByTestId('add-term-group-button-v2').and(page.locator(':visible'));
    this.addTermButton = page.getByTestId('add-term-button').and(page.locator(':visible'));

    this.createModalNameInput = page.getByTestId('create-glossary-entity-modal-name').locator('input');
    this.createModalSubmitButton = page.getByTestId('glossary-entity-modal-create-button');

    // Target inner visible dialog content — the AntD modal root is just a wrapper.
    this.moveModalContainer = page.getByTestId('move-glossary-entity-modal').locator('.ant-modal-content');
    this.moveModalSelectInput = this.moveModalContainer.locator('.ant-select-selector input');
    this.moveModalSubmitButton = page.getByTestId('glossary-entity-modal-move-button');

    // MoreVertOutlinedIcon is the data-testid set by EntityDropdown.tsx on the three-dot icon button.
    // `:visible` needed — multiple entity cards may render this icon simultaneously.
    this.entityMenuThreeDotButton = page.getByTestId('MoreVertOutlinedIcon').and(page.locator(':visible'));
    this.entityMenuDeleteButton = page.getByTestId('entity-menu-delete-button');
    this.entityMenuMoveButton = page.getByTestId('entity-menu-move-button');

    // Scoped to the active tab panel to avoid matching the sidebar browser's search input.
    this.searchInput = this.activeTabPanel.getByTestId('search-input');

    this.filtersToggleButton = page.getByTestId('toggle-filters-button').and(page.locator(':visible'));
    this.advancedSearchButton = page.getByTestId('search-results-advanced-search');
    this.addFilterButton = page.getByTestId('adv-search-add-filter-select');
    this.addFilterTagsButton = page.getByTestId('adv-search-add-filter-tags');
    this.filterTagSelectInput = page.locator('div.ant-select-selection-overflow input');
    this.addTagsConfirmButton = page.getByTestId('add-tag-term-from-modal-btn');

    this.batchAddButton = page.getByTestId('glossary-batch-add');
    // Scoped to modal content to avoid matching the page-level search bar.
    this.batchAddModalSearchInput = page.locator('.ant-modal-content').getByTestId('search-input');
    this.batchAddConfirmButton = page.getByTestId('search-select-modal-continue-button');
    this.batchAddedToast = page.getByText('Added Glossary Term to entities!');

    this.createGlossaryHeading = page.getByRole('heading', { name: 'Create Glossary' });
    this.createGlossaryTermHeading = page.getByRole('heading', { name: 'Create Glossary Term' });
    this.deleteConfirmButton = page.getByRole('button', { name: 'Yes' });
    this.createdTermGroupToast = page.getByText('Created Term Group!');
    this.createdGlossaryTermToast = page.getByText('Created Glossary Term!');
    this.deletedEntityToast = page.getByText(/Deleted .+!/);
    this.movedEntityToast = page.getByText(/Moved .+!/);
    this.propertiesTab = page.locator('[role="tab"]:has([data-testid="Properties-entity-tab-header"])');
  }

  // ── Dynamic locator getters ───────────────────────────────────────────────────

  getMoveDropdownOption(name: string): Locator {
    return this.page.locator('.ant-select-dropdown').getByText(name, { exact: true });
  }

  getFacetTagCheckbox(tagUrn: string): Locator {
    return this.page.getByTestId(`facet-tags-${tagUrn}`);
  }

  getBatchAddEntityCheckbox(entityUrn: string): Locator {
    return this.page.getByTestId(`checkbox-${entityUrn}`);
  }

  getContentsTabItemLocator(urn: string): Locator {
    return this.page.getByTestId(`glossary-entity-item-${urn}`);
  }

  getPreviewEntityLocator(urn: string): Locator {
    // Prefix match covers both exact `preview-{urn}` and `preview-{urn}/subpath` testids
    // that some entity types use.
    return this.page.locator(`[data-testid^="preview-${urn}"]`);
  }

  getEntityTabLocator(tabName: string): Locator {
    return this.page.getByTestId(`${tabName}-entity-tab-header`);
  }

  getTagFilterOption(tagName: string): Locator {
    return this.page.getByTestId(`tag-term-option-${tagName}`);
  }

  getSidebarTermLocator(urn: string): Locator {
    return this.sidebarContainer.getByTestId(`glossary-sidebar-term-${urn}`);
  }

  getSidebarNodeLocator(urn: string): Locator {
    return this.sidebarContainer.getByTestId(`glossary-sidebar-node-${urn}`);
  }

  // ── Navigation ───────────────────────────────────────────────────────────────

  async navigateToGlossary(): Promise<void> {
    this.logger?.step('navigate', { url: '/glossary' });
    await this.page.goto('/glossary');
    await this.waitForPageLoad();
    await expect(this.sidebarContainer).toBeVisible();
  }

  async navigateToGlossaryTermByUrn(urn: string): Promise<void> {
    this.logger?.step('navigateToGlossaryTermByUrn', { urn });
    await this.page.goto(`/glossaryTerm/${encodeURIComponent(urn)}`);
    await this.waitForPageLoad();
  }

  async navigateToGlossaryNodeByUrn(urn: string): Promise<void> {
    this.logger?.step('navigateToGlossaryNodeByUrn', { urn });
    await this.page.goto(`/glossaryNode/${encodeURIComponent(urn)}`);
    await this.waitForPageLoad();
  }

  async clickContentsTabItem(urn: string): Promise<void> {
    this.logger?.step('clickContentsTabItem', { urn });
    await this.getContentsTabItemLocator(urn).click();
    await this.waitForPageLoad();
  }

  async clickSidebarTerm(urn: string): Promise<void> {
    this.logger?.step('clickSidebarTerm', { urn });
    await this.getSidebarTermLocator(urn).click();
    await this.waitForPageLoad();
  }

  async clickSidebarNode(urn: string): Promise<void> {
    this.logger?.step('clickSidebarNode', { urn });
    await this.getSidebarNodeLocator(urn).click();
    await this.waitForPageLoad();
  }

  async openContentsTab(): Promise<void> {
    await this.openTabByName('Contents');
  }

  async openPropertiesTab(): Promise<void> {
    await this.openTabByName('Properties');
  }

  async openRelatedAssetsTab(): Promise<void> {
    await this.openTabByName('Related Assets');
  }

  async openTabByName(tabName: string): Promise<void> {
    await this.getEntityTabLocator(tabName).click();
    await this.waitForPageLoad();
  }

  // ── Glossary Term Group CRUD ─────────────────────────────────────────────────

  /**
   * Creates a root-level Term Group from the glossary home page header button.
   * Returns the URN of the created term group from the GraphQL response.
   */
  async createTermGroup(name: string): Promise<string> {
    this.logger?.step('createTermGroup', { name });
    await this.addTermGroupButtonV2.click();
    // Wait for the modal heading (h1 role) specifically to avoid matching the button text.
    await expect(this.createGlossaryHeading).toBeVisible();
    await this.createModalNameInput.fill(name);
    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createGlossaryNode');
    await this.createModalSubmitButton.click();
    await expect(this.createdTermGroupToast).toBeVisible();
    const response = await responsePromise;
    return (response.data as Record<string, string>).createGlossaryNode;
  }

  /**
   * Creates a Glossary Term inside the currently-open entity (term group) page.
   * Requires the Contents tab to already be active.
   * Returns the URN of the created term from the GraphQL response.
   */
  async createTermInContentsTab(name: string): Promise<string> {
    this.logger?.step('createTermInContentsTab', { name });
    await this.addTermButton.click();
    await expect(this.createGlossaryTermHeading).toBeVisible();
    await this.createModalNameInput.fill(name);
    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createGlossaryTerm');
    await this.createModalSubmitButton.click();
    await expect(this.createdGlossaryTermToast).toBeVisible();
    await expect(this.createdGlossaryTermToast).toBeHidden();
    const response = await responsePromise;
    return (response.data as Record<string, string>).createGlossaryTerm;
  }

  // ── Three-dot entity menu actions ────────────────────────────────────────────

  async openEntityMenu(): Promise<void> {
    await this.entityMenuThreeDotButton.click();
  }

  async deleteCurrentEntity(): Promise<void> {
    this.logger?.step('deleteCurrentEntity');
    await this.openEntityMenu();
    await this.entityMenuDeleteButton.click();
    await this.deleteConfirmButton.click();
    await expect(this.deletedEntityToast).toBeVisible();
  }

  /**
   * Moves the currently-viewed entity to a target parent using the three-dot menu.
   * @param targetName - Display name of the target group shown in the move modal tree.
   */
  async moveCurrentEntityTo(targetName: string): Promise<void> {
    this.logger?.step('moveCurrentEntityTo', { targetName });
    await this.openEntityMenu();
    // Use the entity-menu-move-button testid to avoid matching hidden DnD accessibility elements.
    await this.entityMenuMoveButton.click();
    await expect(this.moveModalContainer).toBeVisible();
    // Type into the AntD Select search input to filter results — more reliable than
    // scrolling through the GlossaryBrowser tree which accumulates entries across test runs.
    await this.moveModalSelectInput.click();
    await this.moveModalSelectInput.fill(targetName);
    // The dropdown shows search results (not the tree browser) when a query is present.
    const option = this.getMoveDropdownOption(targetName);
    await expect(option).toBeVisible();
    await option.click();
    await this.moveModalSubmitButton.click();
    await expect(this.movedEntityToast).toBeVisible();
  }

  // ── Search within an entity page ─────────────────────────────────────────────

  async searchWithinEntityPage(query: string): Promise<void> {
    this.logger?.step('searchWithinEntityPage', { query });
    await this.searchInput.click();
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
    await this.waitForPageLoad();
  }

  // ── Related-assets filtering ─────────────────────────────────────────────────

  /**
   * Opens the basic facet filter panel and clicks the checkbox for a specific tag.
   * The facet checkbox data-testid is `facet-tags-{tagUrn}`.
   */
  async applyFacetTagFilter(tagUrn: string): Promise<void> {
    this.logger?.step('applyFacetTagFilter', { tagUrn });
    await this.filtersToggleButton.click();
    await this.getFacetTagCheckbox(tagUrn).click();
  }

  /**
   * Applies an advanced-search tag filter on the Related Assets panel.
   * Navigates: filter icon → Advanced → Add Filter → Tags → selects tag → confirms.
   */
  async filterRelatedAssetsByTag(tagName: string): Promise<void> {
    this.logger?.step('filterRelatedAssetsByTag', { tagName });
    await this.filtersToggleButton.click();
    await this.advancedSearchButton.click();
    await this.addFilterButton.click();
    await this.addFilterTagsButton.click();
    // AntD Select: type into the overflow input to trigger the options search.
    await this.filterTagSelectInput.pressSequentially(tagName);
    await this.getTagFilterOption(tagName).click();
    await this.modalComponent.title.click();
    await this.addTagsConfirmButton.click();
  }

  // ── Batch add to assets ──────────────────────────────────────────────────────

  async clickBatchAddButton(): Promise<void> {
    this.logger?.step('clickBatchAddButton');
    await this.batchAddButton.click();
  }

  async searchInBatchAddModal(query: string): Promise<void> {
    this.logger?.step('searchInBatchAddModal', { query });
    // pressSequentially triggers AntD AutoComplete's onSearch callback; fill() does not.
    await this.batchAddModalSearchInput.click();
    await this.batchAddModalSearchInput.pressSequentially(query);
  }

  async selectEntityInBatchAddModal(entityUrn: string): Promise<void> {
    this.logger?.step('selectEntityInBatchAddModal', { entityUrn });
    const checkbox = this.getBatchAddEntityCheckbox(entityUrn);
    await expect(checkbox).toBeVisible();
    await checkbox.click();
  }

  async confirmBatchAdd(): Promise<void> {
    this.logger?.step('confirmBatchAdd');
    await this.batchAddConfirmButton.click();
    await expect(this.batchAddedToast).toBeVisible();
  }

  // ── Assertions ───────────────────────────────────────────────────────────────

  async expectPropertiesTabActive(): Promise<void> {
    await expect(this.propertiesTab).toHaveAttribute('aria-selected', 'true');
  }

  async expectEntityInContentsTab(urn: string): Promise<void> {
    await expect(this.getContentsTabItemLocator(urn)).toBeVisible();
  }

  async expectEntityNotInContentsTab(urn: string): Promise<void> {
    await expect(this.getContentsTabItemLocator(urn)).toBeHidden();
  }

  async expectSidebarContainsNode(urn: string): Promise<void> {
    await expect(this.getSidebarNodeLocator(urn)).toBeVisible();
  }

  async expectSidebarNotContainsNode(urn: string): Promise<void> {
    await expect(this.getSidebarNodeLocator(urn)).toBeHidden();
  }

  async expectPreviewEntityByUrn(urn: string): Promise<void> {
    await expect(this.getPreviewEntityLocator(urn)).toBeVisible();
  }
}
