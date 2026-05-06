/**
 * GlossaryPage — page object for /glossary (Business Glossary).
 *
 * Covers the full CRUD lifecycle for Glossary Term Groups and Glossary Terms,
 * including navigation, creation, moving, deleting, and adding terms to datasets.
 *
 * Key selectors are taken directly from the React source:
 *   - GlossaryContentProvider.tsx   → glossaryPageV2, add-term-group-button-v2
 *   - GlossarySidebar.tsx           → glossary-browser-sidebar, create-glossary-button
 *   - ChildrenTab.tsx               → add-term-button
 *   - EntityTabs.tsx                → ${name}-entity-tab-header
 *   - CreateGlossaryEntityModal.tsx → create-glossary-entity-modal-name, glossary-entity-modal-create-button
 *   - MoveGlossaryEntityModal.tsx   → move-glossary-entity-modal, glossary-entity-modal-move-button
 *   - EntityDropdown.tsx            → MoreVertOutlinedIcon (three-dot), entity-menu-delete-button, entity-menu-move-button
 *   - SidebarGlossaryTermsSection.tsx → entity-profile-glossary-terms, add-terms-button
 *   - AddTagsTermsModal.tsx         → tag-term-modal-input, tag-term-option, add-tag-term-from-modal-btn
 *   - EntityActions.tsx             → glossary-batch-add
 *   - EntitySearchResults.tsx       → checkbox-{urn} (entity select checkboxes in batch-add modal)
 *   - SearchSelectModal.tsx         → search-select-modal
 *   - EmbeddedListSearch            → search-results-advanced-search, adv-search-add-filter-tags
 */

import { expect, type Locator, type Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class GlossaryPage extends BasePage {
  // ── Navigation ──────────────────────────────────────────────────────────────
  readonly glossaryPageHeader: Locator;
  readonly sidebarContainer: Locator;

  // ── Root-level create buttons (GlossaryContentProvider header) ──────────────
  readonly addTermGroupButtonV2: Locator;

  // ── Glossary sidebar (GlossarySidebar) ──────────────────────────────────────
  readonly createGlossarySidebarButton: Locator;

  // ── Entity contents tab buttons (ChildrenTab) ────────────────────────────────
  readonly addTermButton: Locator;

  // ── Create modal (CreateGlossaryEntityModal) ─────────────────────────────────
  readonly createModalNameInput: Locator;
  readonly createModalSubmitButton: Locator;

  // ── Move modal (MoveGlossaryEntityModal) ─────────────────────────────────────
  readonly moveModalContainer: Locator;
  readonly moveModalSubmitButton: Locator;

  // ── Three-dot entity menu (EntityDropdown) ────────────────────────────────────
  readonly entityMenuThreeDotButton: Locator;
  readonly entityMenuDeleteButton: Locator;
  readonly entityMenuMoveButton: Locator;

  // ── Dataset sidebar — add glossary term section ───────────────────────────────
  readonly sidebarGlossarySection: Locator;
  readonly addTermsButton: Locator;

  // ── Add tag/term modal (AddTagsTermsModal) ─────────────────────────────────
  readonly tagTermModalInput: Locator;
  readonly tagTermOption: Locator;
  readonly addTagTermConfirmButton: Locator;

  // ── Batch-add glossary term button (EntityActions) ─────────────────────────
  readonly batchAddGlossaryButton: Locator;

  // ── Batch-add modal entity results (SearchSelectModal / EntitySearchResults) ─
  readonly previewEntities: Locator;
  readonly entityCheckboxes: Locator;

  // ── Search input (shared EmbeddedListSearch) ──────────────────────────────
  readonly searchInput: Locator;

  // ── Continue button (entity selection modal) ──────────────────────────────
  readonly continueButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.glossaryPageHeader = page.locator('[data-testid="glossaryPageV2"]');
    this.sidebarContainer = page.locator('[data-testid="glossary-browser-sidebar"]');

    this.addTermGroupButtonV2 = page.locator('[data-testid="add-term-group-button-v2"]').first();
    this.createGlossarySidebarButton = page.locator('[data-testid="create-glossary-button"]');

    this.addTermButton = page.locator('[data-testid="add-term-button"]').first();

    this.createModalNameInput = page.locator('[data-testid="create-glossary-entity-modal-name"] input');
    this.createModalSubmitButton = page.locator('[data-testid="glossary-entity-modal-create-button"]');

    // The AntD modal root is a wrapper; target the inner visible dialog content.
    this.moveModalContainer = page.locator('[data-testid="move-glossary-entity-modal"] .ant-modal-content');
    this.moveModalSubmitButton = page.locator('[data-testid="glossary-entity-modal-move-button"]');

    // MoreVertOutlinedIcon is the data-testid set by EntityDropdown.tsx on the three-dot icon button.
    this.entityMenuThreeDotButton = page.locator('[data-testid="MoreVertOutlinedIcon"]').first();
    this.entityMenuDeleteButton = page.locator('[data-testid="entity-menu-delete-button"]');
    this.entityMenuMoveButton = page.locator('[data-testid="entity-menu-move-button"]');

    this.sidebarGlossarySection = page.locator('#entity-profile-glossary-terms');
    this.addTermsButton = page.locator('[data-testid="add-terms-button"]');

    // The AntD Select renders as a div wrapper; we target the inner search input for typing.
    this.tagTermModalInput = page.locator('[data-testid="tag-term-modal-input"] input');
    this.tagTermOption = page.locator('[data-testid="tag-term-option"]').first();
    this.addTagTermConfirmButton = page.locator('[data-testid="add-tag-term-from-modal-btn"]');

    this.batchAddGlossaryButton = page.locator('[data-testid="glossary-batch-add"]').first();

    // preview-urn: prefix is set by entity preview cards; checkbox- prefix by entity select checkboxes.
    this.previewEntities = page.locator('[data-testid^="preview-urn:"]');
    this.entityCheckboxes = page.locator('[data-testid^="checkbox-"]');

    this.searchInput = page.locator('[data-testid="search-input"]').last();

    this.continueButton = page.locator('#continueButton');
  }

  // ── Navigation ───────────────────────────────────────────────────────────────

  async navigateToGlossary(): Promise<void> {
    this.logger?.step('navigate', { url: '/glossary' });
    await this.page.goto('/glossary');
    await this.page.waitForLoadState('networkidle');
    await expect(this.sidebarContainer).toBeVisible({ timeout: 30000 });
  }

  async navigateToGlossaryTerm(text: string): Promise<void> {
    // Prefer clicking a visible link or text rather than a hidden sidebar span.
    // Filter to only the visible match so we don't hit hidden sidebar browser entries.
    const visibleElement = this.page.getByText(text).filter({ visible: true }).first();
    await visibleElement.waitFor({ state: 'visible', timeout: 30000 });
    await visibleElement.click();
    await this.page.waitForLoadState('networkidle');
  }

  async navigateToEntityContentsTab(): Promise<void> {
    await this.clickEntityTabByName('Contents');
  }

  async navigateToEntityPropertiesTab(): Promise<void> {
    await this.clickEntityTabByName('Properties');
  }

  async navigateToEntityRelatedAssetsTab(): Promise<void> {
    await this.clickEntityTabByName('Related Assets');
  }

  async clickEntityTabByName(tabName: string): Promise<void> {
    const tabSelector = `[data-testid="${tabName}-entity-tab-header"]`;
    await this.page.locator(tabSelector).click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Glossary Term Group CRUD ─────────────────────────────────────────────────

  /**
   * Creates a root-level Term Group from the glossary home page header button.
   * Opens the modal, types the name, and confirms.
   */
  async createTermGroup(name: string): Promise<void> {
    this.logger?.step('createTermGroup', { name });
    await this.addTermGroupButtonV2.click();
    // Wait for the modal heading (h1 role) specifically to avoid matching the button text.
    await expect(this.page.getByRole('heading', { name: 'Create Glossary' })).toBeVisible({ timeout: 15000 });
    await this.createModalNameInput.fill(name);
    await this.createModalSubmitButton.click();
    await expect(this.page.getByText(`Created Term Group!`)).toBeVisible({ timeout: 15000 });
  }

  /**
   * Creates a Glossary Term inside the currently-open entity (term group) page.
   * Requires the Contents tab to already be active.
   */
  async createTermInContentsTab(name: string): Promise<void> {
    this.logger?.step('createTermInContentsTab', { name });
    await this.addTermButton.click();
    await expect(this.page.getByRole('heading', { name: 'Create Glossary Term' })).toBeVisible({ timeout: 15000 });
    await this.createModalNameInput.fill(name);
    await this.createModalSubmitButton.click();
    // The mutation fires async; wait for the success toast which appears after ~2s.
    await expect(this.page.getByText('Created Glossary Term!')).toBeVisible({ timeout: 15000 });
  }

  // ── Three-dot entity menu actions ────────────────────────────────────────────

  /** Opens the three-dot dropdown for the currently-viewed entity. */
  async openEntityMenu(): Promise<void> {
    await this.entityMenuThreeDotButton.click();
  }

  /** Deletes the currently-viewed entity via the three-dot menu. */
  async deleteCurrentEntity(): Promise<void> {
    this.logger?.step('deleteCurrentEntity');
    await this.openEntityMenu();
    await this.page.getByText('Delete').click();
    await this.page.getByRole('button', { name: 'Yes' }).click();
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
    await expect(this.moveModalContainer).toBeVisible({ timeout: 15000 });
    // Type into the AntD Select search input to filter results — more reliable than
    // scrolling through the GlossaryBrowser tree which accumulates entries across test runs.
    const selectInput = this.moveModalContainer.locator('.ant-select-selector input');
    await selectInput.click();
    await selectInput.fill(targetName);
    // The dropdown shows search results (not the tree browser) when a query is present.
    const option = this.page.locator('.ant-select-dropdown').getByText(targetName, { exact: true }).first();
    await expect(option).toBeVisible({ timeout: 15000 });
    await option.click();
    await this.moveModalSubmitButton.click({ force: true });
  }

  // ── Dataset glossary term management ────────────────────────────────────────

  /**
   * Navigates to a dataset entity page and adds a glossary term via the sidebar.
   * @param datasetPath - URL path segment after the base URL (e.g. "dataset/urn:li:...").
   * @param datasetName - Display name to wait for after navigation.
   * @param termName    - Glossary term to search for and add.
   */
  async addGlossaryTermToDataset(datasetPath: string, datasetName: string, termName: string): Promise<void> {
    this.logger?.step('addGlossaryTermToDataset', { datasetName, termName });
    await this.page.goto(`/${datasetPath}`);
    await this.page.waitForLoadState('networkidle');
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 30000 });
    await this.sidebarGlossarySection.locator('[data-testid="add-terms-button"]').click();
    await expect(this.tagTermModalInput).toBeVisible({ timeout: 15000 });
    // AntD Select triggers search via keyboard events; pressSequentially (not fill) is required.
    await this.tagTermModalInput.pressSequentially(termName);
    await this.tagTermOption.click();
    await this.addTagTermConfirmButton.click();
    await expect(this.addTagTermConfirmButton).not.toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(termName)).toBeVisible({ timeout: 15000 });
  }

  // ── Batch add term to entities ───────────────────────────────────────────────

  /**
   * Uses the "Add Assets" batch button on a glossary term entity page to assign
   * the term to one or more datasets via the search modal.
   */
  async batchAddToFirstResult(): Promise<void> {
    this.logger?.step('batchAddToFirstResult');
    await this.batchAddGlossaryButton.click();
    await expect(this.previewEntities.first()).toBeVisible({ timeout: 30000 });
    await this.entityCheckboxes.first().click();
    await this.continueButton.click();
    await expect(this.page.getByText('Added Glossary Term to entities!')).toBeVisible({ timeout: 15000 });
  }

  /**
   * Uses the "Add Assets" batch button to assign the term to a specific entity found by search query.
   * Searches within the modal for the entity by name, waits for it to appear, then confirms.
   */
  async batchAddToEntityBySearch(query: string): Promise<void> {
    this.logger?.step('batchAddToEntityBySearch', { query });
    await this.batchAddGlossaryButton.click();
    // Wait for the modal search input to be ready.
    const modalSearchInput = this.page.locator('[data-testid="search-select-modal"] [data-testid="search-input"]');
    await expect(modalSearchInput).toBeVisible({ timeout: 30000 });
    // pressSequentially triggers the debounced search handler character by character.
    await modalSearchInput.pressSequentially(query, { delay: 50 });
    await this.page.waitForLoadState('networkidle');
    // Wait for the search results to show the specific entity.
    const entityTitle = this.previewEntities.locator('[data-testid="entity-title"]').filter({ hasText: query }).first();
    await expect(entityTitle).toBeVisible({ timeout: 15000 });
    await this.entityCheckboxes.first().click();
    await this.continueButton.click();
    await expect(this.page.getByText('Added Glossary Term to entities!')).toBeVisible({ timeout: 15000 });
  }

  // ── Search within an entity page ─────────────────────────────────────────────

  async searchWithinEntityPage(query: string): Promise<void> {
    this.logger?.step('searchWithinEntityPage', { query });
    await this.searchInput.click();
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
    await this.page.waitForLoadState('networkidle');
  }

  // ── Sidebar navigation ───────────────────────────────────────────────────────

  /** Clicks an item by name in the glossary browser sidebar. */
  async clickSidebarItem(name: string): Promise<void> {
    await this.sidebarContainer.getByText(name).click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Related-assets filtering ─────────────────────────────────────────────────

  /**
   * Opens the basic facet filter panel on the Related Assets / search-results panel.
   * .anticon-filter is the AntD CSS class applied to the filter icon button; no data-testid exists.
   */
  async clickFacetFilterIcon(): Promise<void> {
    await this.page.locator('.anticon-filter').first().click();
  }

  /**
   * Applies an advanced-search tag filter on the Related Assets panel.
   * Navigates: filter icon → Advanced → Add Filter → Tags → selects tag → confirms.
   */
  async filterRelatedAssetsByTag(tagName: string): Promise<void> {
    this.logger?.step('filterRelatedAssetsByTag', { tagName });
    await this.page.locator('[aria-label="filter"]').first().click();
    await this.page.locator('#search-results-advanced-search').click();
    await this.page.getByText('Add Filter').click();
    await this.page.getByTestId('adv-search-add-filter-tags').click();
    // AntD Select: type into the overflow input to trigger the options search.
    await this.page.locator('div.ant-select-selection-overflow input').pressSequentially(tagName);
    await this.page.locator(`[data-testid="tag-term-option-${tagName}"]`).click();
    await this.page.getByText('Add Tags').click();
    await this.addTagTermConfirmButton.click();
  }

  // ── Assertions ───────────────────────────────────────────────────────────────

  /**
   * Asserts the Properties tab is the currently selected tab.
   * Uses role="tab" ARIA selector instead of AntD-internal [data-node-key] + .ant-tabs-tab-btn.
   */
  async expectPropertiesTabActive(timeout = 10000): Promise<void> {
    await expect(this.page.getByRole('tab', { name: 'Properties' }).first()).toHaveAttribute('aria-selected', 'true', {
      timeout,
    });
  }

  async expectTextVisible(text: string, timeout = 15000): Promise<void> {
    await expect(this.page.getByText(text).filter({ visible: true }).first()).toBeVisible({ timeout });
  }

  async expectTextNotPresent(text: string, timeout = 15000): Promise<void> {
    await expect(this.page.getByText(text).first()).not.toBeVisible({ timeout });
  }

  async expectSidebarContains(name: string, timeout = 15000): Promise<void> {
    await expect(this.sidebarContainer.getByText(name)).toBeVisible({ timeout });
  }

  async expectSidebarNotContains(name: string, timeout = 15000): Promise<void> {
    await expect(this.sidebarContainer.getByText(name)).not.toBeVisible({ timeout });
  }

  async expectPreviewEntitiesVisible(timeout = 30000): Promise<void> {
    await expect(this.previewEntities.first()).toBeVisible({ timeout });
  }
}
