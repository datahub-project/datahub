import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class ViewSelectPage extends BasePage {
  readonly viewsIcon: Locator;
  readonly viewsTypeSelect: Locator;
  readonly addButton: Locator;
  readonly viewNameInput: Locator;
  readonly conditionSelect: Locator;
  readonly conditionOperatorSelect: Locator;
  readonly entitySearchInput: Locator;
  readonly dropdownSearchInput: Locator;
  readonly footerButtonUpdate: Locator;
  readonly viewBuilderSave: Locator;
  readonly closeIcon: Locator;
  readonly resultsContainer: Locator;
  readonly menuItemEdit: Locator;
  readonly menuItemSetDefault: Locator;
  readonly menuItemRemoveDefault: Locator;
  readonly menuItemDelete: Locator;
  readonly confirmDeleteYes: Locator;
  readonly viewsButton: Locator;
  readonly viewSelectItem: Locator;
  readonly entityTitle: Locator;
  readonly viewsPopover: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.viewsIcon = page.getByTestId('views-icon');
    this.viewsTypeSelect = page.getByTestId('views-type-select');
    this.addButton = page.getByTestId('create-view-button');
    this.viewNameInput = page.getByTestId('view-name-input-inner');
    this.conditionSelect = page.getByTestId('condition-select');
    this.conditionOperatorSelect = page.getByTestId('condition-operator-select');
    this.entitySearchInput = page.getByTestId('entity-search-input');
    this.dropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.footerButtonUpdate = page.getByTestId('footer-button-update');
    this.viewBuilderSave = page.getByTestId('view-builder-save');
    this.resultsContainer = page.getByTestId('browse-v2-results');
    this.menuItemEdit = page.getByTestId('menu-item-edit');
    this.menuItemSetDefault = page.getByTestId('menu-item-set-default');
    this.menuItemRemoveDefault = page.getByTestId('menu-item-remove-default');
    this.menuItemDelete = page.getByTestId('menu-item-delete');
    this.confirmDeleteYes = page.getByRole('button', { name: 'Yes' });
    this.viewsButton = page.getByTestId('views-button');
    this.viewSelectItem = page.getByTestId('view-select-item');
    this.closeIcon = page.getByTestId('views-clear-button');
    this.entityTitle = page.getByTestId('entity-title');
    this.viewsPopover = page.getByTestId('views-popover');
  }

  // ============================================================================
  // PRIVATE HELPER METHODS: Dynamic selectors that depend on runtime parameters
  // These use inline locators because they cannot be defined statically in the
  // constructor (they accept parameters like fieldName, searchValue, viewName)
  // ============================================================================

  private getFieldOption(fieldName: string): Locator {
    return this.page.getByTestId(`option-${fieldName}`);
  }

  private getOperatorOption(operator: string): Locator {
    return this.page.getByTestId(`option-${operator}`);
  }

  private getFilterValueOption(searchValue: string): Locator {
    // Find text containing the search value - the parent label handles the checkbox toggle
    return this.page.getByText(searchValue, { exact: false });
  }

  private getTextElement(text: string): Locator {
    return this.page.getByText(text);
  }

  private async expectTextVisible(text: string, timeout: number = TIMEOUTS.LONG): Promise<void> {
    await expect(this.getTextElement(text)).toBeVisible({ timeout });
  }

  async expectTextNotVisible(text: string, timeout: number = TIMEOUTS.LONG): Promise<void> {
    await expect(this.getTextElement(text)).not.toBeVisible({ timeout });
  }

  private getSelectedViewItem(viewName: string): Locator {
    return this.viewSelectItem.filter({
      hasText: viewName,
    });
  }

  private getViewDropdownTrigger(selectedViewItem: Locator): Locator {
    return selectedViewItem.getByTestId('views-table-dropdown');
  }

  private async waitForPopoverOpen(): Promise<void> {
    await this.viewsPopover.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  private async waitForPopoverClose(): Promise<void> {
    await this.viewsPopover.waitFor({ state: 'hidden', timeout: TIMEOUTS.SHORT });
  }

  // ============================================================================
  // PUBLIC METHODS
  // ============================================================================

  async navigateTo(path: string): Promise<void> {
    await this.page.goto(path);
    await this.page.waitForLoadState('networkidle');
  }

  async skipIntroducePage(): Promise<void> {
    await this.page.evaluate(() => {
      localStorage.setItem('skipIntroPage', 'true');
    });
  }

  async createViewFromSelect(viewName: string): Promise<void> {
    await this.viewsIcon.click();
    await this.viewsTypeSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.addButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.addButton.click();

    await this.viewNameInput.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await this.viewNameInput.fill(viewName);
  }

  async addFilterWithSearch(fieldName: string, operator: string, searchValue: string): Promise<void> {
    // Select condition field
    await this.conditionSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await this.conditionSelect.scrollIntoViewIfNeeded();
    await this.conditionSelect.click();

    const fieldOption = this.getFieldOption(fieldName);
    await fieldOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await fieldOption.click();

    // Select operator
    await this.conditionOperatorSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.conditionOperatorSelect.click();

    const operatorOption = this.getOperatorOption(operator);
    await operatorOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await operatorOption.click();

    // Click the entity search input dropdown to open it
    await this.entitySearchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.entitySearchInput.click();

    // Fill the search input that appears in the dropdown
    await this.dropdownSearchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.dropdownSearchInput.fill(searchValue);

    // Click the checkbox for the value option
    const valueOption = this.getFilterValueOption(searchValue);
    await valueOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await valueOption.click();

    // Update the filter
    await this.footerButtonUpdate.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.footerButtonUpdate.click();
    await this.page.waitForLoadState('networkidle');
  }

  async saveView(): Promise<void> {
    await this.viewBuilderSave.click();
    await this.page.waitForLoadState('networkidle');
  }

  async verifyFilterApplied(expectedText: string): Promise<void> {
    await this.expectTextVisible(expectedText);
  }

  async verifyResultsCount(count: number): Promise<void> {
    await this.expectTextVisible(`of ${count} results`);
  }

  async verifyDatasetVisible(datasetName: string): Promise<void> {
    await expect(this.entityTitle.filter({ hasText: datasetName })).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async verifyViewDeleted(viewName: string): Promise<void> {
    await expect(this.getTextElement(viewName)).not.toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async clearView(): Promise<void> {
    // Hover over the views button to make close icon visible
    await this.viewsButton.hover();

    // Click the close icon
    await this.closeIcon.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.closeIcon.click();
    await this.page.waitForLoadState('networkidle');
  }

  async openViewDropdown(viewName: string): Promise<void> {
    await this.viewsButton.click();
    await this.waitForPopoverOpen();
    await this.page.waitForTimeout(TIMEOUTS.QUICK);

    const selectedViewItem = this.getSelectedViewItem(viewName);
    await selectedViewItem.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });

    await selectedViewItem.hover();
    await this.page.waitForTimeout(TIMEOUTS.QUICK);

    const dropdownTrigger = this.getViewDropdownTrigger(selectedViewItem);
    await dropdownTrigger.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await dropdownTrigger.click();
  }

  async editViewFromSelect(currentName: string, newName: string): Promise<void> {
    await this.openViewDropdown(currentName);
    await this.menuItemEdit.click();

    await this.viewNameInput.waitFor({ state: 'visible' });
    await this.viewNameInput.clear();
    await this.viewNameInput.fill(newName);
    await this.saveView();
    await this.waitForPopoverClose();
  }

  async setViewAsDefault(viewName: string): Promise<void> {
    await this.openViewDropdown(viewName);
    await this.menuItemSetDefault.click();
    await this.page.waitForLoadState('networkidle');
    await this.waitForPopoverClose();
  }

  async removeViewAsDefault(viewName: string): Promise<void> {
    await this.openViewDropdown(viewName);
    await this.menuItemRemoveDefault.click();
    await this.page.waitForLoadState('networkidle');
    await this.waitForPopoverClose();
  }

  async deleteView(viewName: string): Promise<void> {
    await this.openViewDropdown(viewName);
    await this.menuItemDelete.click();

    await this.confirmDeleteYes.click();
    await this.page.waitForLoadState('networkidle');
    await this.waitForPopoverClose();
  }
}
