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
import { ToastComponent } from './common/toast-component';

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

  // ── Create modal headings ─────────────────────────────────────────────────
  readonly createGlossaryHeading: Locator;
  readonly createGlossaryTermHeading: Locator;

  // ── Confirmation button ───────────────────────────────────────────────────
  readonly deleteConfirmButton: Locator;

  // ── Tabs ──────────────────────────────────────────────────────────────────
  readonly propertiesTab: Locator;

  private readonly graphqlHelper: GraphQLHelper;
  private readonly toast: ToastComponent;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);
    this.toast = new ToastComponent(page);

    this.modalComponent = new ModalComponent(page);

    this.sidebarContainer = page.getByTestId('glossary-browser-sidebar');
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design active tab panel CSS class; no semantic role for the active tabpanel
    this.activeTabPanel = page.locator('.ant-tabs-tabpane-active');

    // `:visible` needed — multiple instances may exist in the DOM simultaneously.
    // eslint-disable-next-line playwright/no-raw-locators -- Playwright :visible pseudo-selector; no Locator API equivalent for visibility filtering
    this.addTermGroupButtonV2 = page.getByTestId('add-term-group-button-v2').and(page.locator(':visible'));
    // eslint-disable-next-line playwright/no-raw-locators -- Playwright :visible pseudo-selector; no Locator API equivalent for visibility filtering
    this.addTermButton = page.getByTestId('add-term-button').and(page.locator(':visible'));

    this.createModalNameInput = page.getByTestId('create-glossary-entity-modal-name').getByRole('textbox');
    this.createModalSubmitButton = page.getByTestId('glossary-entity-modal-create-button');

    // Target inner visible dialog content — the AntD modal root is just a wrapper.
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design modal content CSS class; no data-testid or ARIA role on this wrapper
    this.moveModalContainer = page.getByTestId('move-glossary-entity-modal').locator('.ant-modal-content');
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design select search input; compound CSS selector with no data-testid
    this.moveModalSelectInput = this.moveModalContainer.locator('.ant-select-selector input');
    this.moveModalSubmitButton = page.getByTestId('glossary-entity-modal-move-button');

    // MoreVertOutlinedIcon is the data-testid set by EntityDropdown.tsx on the three-dot icon button.
    // `:visible` needed — multiple entity cards may render this icon simultaneously.
    // eslint-disable-next-line playwright/no-raw-locators -- Playwright :visible pseudo-selector; no Locator API equivalent for visibility filtering
    this.entityMenuThreeDotButton = page.getByTestId('MoreVertOutlinedIcon').and(page.locator(':visible'));
    this.entityMenuDeleteButton = page.getByTestId('entity-menu-delete-button');
    this.entityMenuMoveButton = page.getByTestId('entity-menu-move-button');

    // Scoped to the active tab panel to avoid matching the main nav search bar.
    this.searchInput = this.activeTabPanel.getByTestId('search-input');

    // Scoped to the active tab panel to avoid matching the second instance inside the batch-add modal.
    this.filtersToggleButton = this.activeTabPanel.getByTestId('toggle-filters-button');
    this.advancedSearchButton = page.getByTestId('search-results-advanced-search');
    this.addFilterButton = page.getByTestId('adv-search-add-filter-select');
    this.addFilterTagsButton = page.getByTestId('adv-search-add-filter-tags');
    // Alchemy SimpleSelect renders its search input in a floating dropdown popover with
    // `data-testid="dropdown-search-input"`. The dropdown must be opened (by clicking the
    // SimpleSelect container `tag-term-modal-input`) before this input is in the DOM.
    this.filterTagSelectInput = page.getByTestId('dropdown-search-input');
    this.addTagsConfirmButton = page.getByTestId('add-tag-term-from-modal-btn');

    this.batchAddButton = page.getByTestId('glossary-batch-add');
    // Scoped to modal dialog to avoid matching the page-level search bar.
    this.batchAddModalSearchInput = page.getByRole('dialog').getByTestId('search-input');
    this.batchAddConfirmButton = page.getByTestId('search-select-modal-continue-button');

    this.createGlossaryHeading = page.getByRole('heading', { name: 'Create Glossary' });
    this.createGlossaryTermHeading = page.getByRole('heading', { name: 'Create Glossary Term' });
    this.deleteConfirmButton = page.getByRole('button', { name: 'Yes' });
    this.propertiesTab = page.getByRole('tab').filter({ has: page.getByTestId('Properties-entity-tab-header') });
  }

  // ── Dynamic locator getters ───────────────────────────────────────────────────

  getMoveDropdownOption(name: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design select dropdown class; no ARIA role on the dropdown container
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
    // that some entity types use. getByTestId requires exact match, so raw locator needed.
    // eslint-disable-next-line playwright/no-raw-locators -- data-testid prefix match ([^=]); getByTestId only supports exact matches
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
    // The sidebar tree re-renders when the parent node auto-expands; allow generous wait.
    const locator = this.getSidebarTermLocator(urn);
    await expect(locator).toBeVisible({ timeout: 45000 });
    await locator.scrollIntoViewIfNeeded();
    await locator.click();
    await this.waitForPageLoad();
  }

  async clickSidebarNode(urn: string): Promise<void> {
    // ES indexing for freshly-created nodes takes >60 s in this environment.
    // Navigate directly by URL — clickSidebarNode is purely used for navigation, not to test
    // sidebar click behaviour itself.
    this.logger?.step('clickSidebarNode', { urn });
    await this.navigateToGlossaryNodeByUrn(urn);
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
    await this.toast.expectVisible('Created Term Group!');
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
    await this.toast.expectVisibleThenHidden('Created Glossary Term!');
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
    await this.toast.expectVisible(/Deleted .+!/);
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
    await this.toast.expectVisible(/Moved .+!/);
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
    await expect(this.filtersToggleButton).toBeVisible({ timeout: 15000 });
    await this.filtersToggleButton.click();
    await this.getFacetTagCheckbox(tagUrn).click();
  }

  /**
   * Applies an advanced-search tag filter on the Related Assets panel.
   * Navigates: filter icon → Advanced → Add Filter → Tags → selects tag → confirms.
   */
  async filterRelatedAssetsByTag(tagName: string): Promise<void> {
    this.logger?.step('filterRelatedAssetsByTag', { tagName });
    await expect(this.filtersToggleButton).toBeVisible({ timeout: 15000 });
    await this.filtersToggleButton.click();
    await this.advancedSearchButton.click();
    await this.addFilterButton.click();
    await this.addFilterTagsButton.click();
    // Alchemy SimpleSelect: click the trigger to open the dropdown, then type into the
    // search input inside the dropdown popover.
    await this.page.getByTestId('tag-term-modal-input').click();
    await this.filterTagSelectInput.fill(tagName);
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
    await this.toast.expectVisible('Added Glossary Term to entities!');
  }

  // ── Assertions ───────────────────────────────────────────────────────────────

  async expectPropertiesTabActive(): Promise<void> {
    await expect(this.propertiesTab).toHaveAttribute('aria-selected', 'true');
  }

  async expectEntityInContentsTab(urn: string): Promise<void> {
    // ES indexing after creation can take 60+ s. Reload and re-open Contents tab until visible.
    const locator = this.getContentsTabItemLocator(urn);
    await expect(async () => {
      if (!(await locator.isVisible())) {
        await this.page.reload();
        await this.waitForPageLoad();
        await this.openContentsTab();
      }
      await expect(locator).toBeVisible({ timeout: 5000 });
    }).toPass({ timeout: 90000, intervals: [5000] });
  }

  async expectEntityNotInContentsTab(urn: string): Promise<void> {
    await expect(this.getContentsTabItemLocator(urn)).toBeHidden({ timeout: 15000 });
  }

  async expectSidebarContainsNode(urn: string): Promise<void> {
    // ES indexing after creation can take 60+ s. Reload until the sidebar node appears.
    const locator = this.getSidebarNodeLocator(urn);
    await expect(async () => {
      await this.page.reload();
      await this.waitForPageLoad();
      await expect(locator).toBeVisible({ timeout: 5000 });
    }).toPass({ timeout: 120000, intervals: [5000] });
  }

  async expectSidebarNotContainsNode(urn: string): Promise<void> {
    await expect(this.getSidebarNodeLocator(urn)).toBeHidden({ timeout: 15000 });
  }

  async expectPreviewEntityByUrn(urn: string): Promise<void> {
    await expect(this.getPreviewEntityLocator(urn)).toBeVisible();
  }
}
