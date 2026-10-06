import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';
import { URLS } from './views-constants';

export class ManageViewsPage extends BasePage {
  readonly createViewButton: Locator;
  readonly viewNameInput: Locator;
  readonly conditionSelect: Locator;
  readonly conditionOperatorSelect: Locator;
  readonly viewBuilderSave: Locator;
  readonly viewsTableDropdown: Locator;
  readonly menuItemEdit: Locator;
  readonly menuItemSetDefault: Locator;
  readonly menuItemRemoveDefault: Locator;
  readonly menuItemDelete: Locator;
  readonly confirmDeleteYes: Locator;

  // Query Builder Test IDs
  readonly addGroupButton: Locator;
  readonly addConditionButton: Locator;
  readonly andOperatorButton: Locator;
  readonly orOperatorButton: Locator;
  readonly notOperatorButton: Locator;
  readonly entitySearchInput: Locator;
  readonly dropdownSearchInput: Locator;
  readonly footerButtonUpdate: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.createViewButton = page.getByTestId('create-new-view-button');
    this.viewNameInput = page.getByTestId('view-name-input-inner');
    this.conditionSelect = page.getByTestId('condition-select');
    this.conditionOperatorSelect = page.getByTestId('condition-operator-select');
    this.viewBuilderSave = page.getByTestId('view-builder-save');
    this.viewsTableDropdown = page.getByTestId('views-table-dropdown');
    this.menuItemEdit = page.getByTestId('menu-item-edit');
    this.menuItemSetDefault = page.getByTestId('menu-item-set-default');
    this.menuItemRemoveDefault = page.getByTestId('menu-item-remove-default');
    this.menuItemDelete = page.getByTestId('menu-item-delete');
    this.confirmDeleteYes = page.getByRole('button', { name: 'Yes' });

    // Query Builder Test IDs
    this.addGroupButton = page.getByTestId('query-builder-add-group-button');
    this.addConditionButton = page.getByTestId('query-builder-add-condition-button');
    this.andOperatorButton = page.getByTestId('query-builder-all-button');
    this.orOperatorButton = page.getByTestId('query-builder-any-button');
    this.notOperatorButton = page.getByTestId('query-builder-none-button');
    this.entitySearchInput = page.getByTestId('entity-search-input');
    this.dropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.footerButtonUpdate = page.getByTestId('footer-button-update');
  }

  // ============================================================================
  // PRIVATE HELPER METHODS: Dynamic selectors that depend on runtime parameters
  // These use inline locators because they cannot be defined statically in the
  // constructor (they accept parameters like viewName, fieldName, operator)
  // ============================================================================

  private getViewRowByName(viewName: string): Locator {
    return this.page.getByRole('row', {
      name: new RegExp(viewName, 'i'),
    });
  }

  private getViewDropdownButton(viewRow: Locator): Locator {
    return viewRow.getByTestId('views-table-dropdown');
  }

  private getFieldOption(fieldName: string): Locator {
    return this.page.getByTestId(`option-${fieldName}`);
  }

  private getOperatorOption(operator: string): Locator {
    return this.page.getByTestId(`option-${operator}`);
  }

  private getFilterValueOption(searchValue: string): Locator {
    return this.page.getByText(searchValue, { exact: false });
  }

  // ============================================================================
  // PUBLIC METHODS
  // ============================================================================

  async navigate(): Promise<void> {
    await this.page.goto(URLS.SETTINGS_VIEWS);
    await this.page.waitForLoadState('networkidle');
    await expect(this.createViewButton).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async navigateTo(path: string): Promise<void> {
    await this.page.goto(path);
    await this.page.waitForLoadState('networkidle');
  }

  async createView(name: string): Promise<void> {
    await this.createViewButton.click();
    await this.viewNameInput.waitFor({ state: 'visible' });
    await this.viewNameInput.fill(name);
  }

  async addFilterCondition(fieldName: string, operator: string): Promise<void> {
    await this.conditionSelect.click();
    const option = this.getFieldOption(fieldName);
    await option.click();

    await this.conditionOperatorSelect.click();
    const operatorOption = this.getOperatorOption(operator);
    await operatorOption.click();
  }

  async selectProperty(fieldName: string): Promise<void> {
    await this.conditionSelect.click();
    const option = this.getFieldOption(fieldName);
    await option.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await option.click();
  }

  async openOperatorDropdown(): Promise<void> {
    await this.conditionOperatorSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.conditionOperatorSelect.click();
  }

  async expectOperatorOptionVisible(operator: string): Promise<void> {
    await expect(this.getOperatorOption(operator)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async expectOperatorOptionNotVisible(operator: string): Promise<void> {
    await expect(this.getOperatorOption(operator)).toHaveCount(0);
  }

  async expectOperatorDescriptionVisible(description: string): Promise<void> {
    await expect(this.page.getByText(description, { exact: false })).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
  }

  async selectOperator(operator: string): Promise<void> {
    await this.openOperatorDropdown();
    const operatorOption = this.getOperatorOption(operator);
    await operatorOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await operatorOption.click();
  }

  async addFilterWithSearch(fieldName: string, operator: string, searchValue: string): Promise<void> {
    await this.selectProperty(fieldName);
    await this.selectOperator(operator);

    const entitySearchInput = this.page.getByTestId('entity-search-input');
    await entitySearchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await entitySearchInput.click();

    const dropdownSearchInput = this.page.getByTestId('dropdown-search-input');
    await dropdownSearchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await dropdownSearchInput.fill(searchValue);

    const valueOption = this.getFilterValueOption(searchValue);
    await valueOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await valueOption.click();

    const footerButtonUpdate = this.page.getByTestId('footer-button-update');
    await footerButtonUpdate.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await footerButtonUpdate.click();
    await this.page.waitForLoadState('networkidle');
  }

  async expectSelectedOperator(operatorLabel: string): Promise<void> {
    await expect(this.conditionOperatorSelect.getByText(operatorLabel, { exact: false })).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
  }

  async saveView(): Promise<void> {
    await this.viewBuilderSave.click();
    await this.page.waitForLoadState('networkidle');
  }

  async getViewOptionMenu(viewName: string): Promise<Locator> {
    const viewRow = this.getViewRowByName(viewName);
    return this.getViewDropdownButton(viewRow);
  }

  async editView(viewName: string, newName: string): Promise<void> {
    const menuButton = await this.getViewOptionMenu(viewName);
    await menuButton.click();
    await this.menuItemEdit.click();

    await this.viewNameInput.waitFor({ state: 'visible' });
    await this.viewNameInput.clear();
    await this.viewNameInput.fill(newName);
    await this.saveView();
  }

  async setViewAsDefault(viewName: string): Promise<void> {
    const menuButton = await this.getViewOptionMenu(viewName);
    await menuButton.click();
    await this.menuItemSetDefault.click();
    await this.page.waitForLoadState('networkidle');
  }

  async removeViewAsDefault(viewName: string): Promise<void> {
    const menuButton = await this.getViewOptionMenu(viewName);
    await menuButton.click();
    await this.menuItemRemoveDefault.click();
    await this.page.waitForLoadState('networkidle');
  }

  async deleteView(viewName: string): Promise<void> {
    const menuButton = await this.getViewOptionMenu(viewName);
    await menuButton.click();
    await this.menuItemDelete.click();
    await this.confirmDeleteYes.click();
    await this.page.waitForLoadState('networkidle');
    const viewRow = this.getViewRowByName(viewName);
    await expect(viewRow).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async expectViewVisible(viewName: string): Promise<void> {
    const viewRow = this.getViewRowByName(viewName);
    await expect(viewRow).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectViewNotVisible(viewName: string): Promise<void> {
    const viewRow = this.getViewRowByName(viewName);
    await expect(viewRow).not.toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ============================================================================
  // QUERY BUILDER METHODS for nested conditions
  // ============================================================================

  async clickAddGroupButton(): Promise<void> {
    await this.addGroupButton.click();
  }

  async clickAddConditionInNestedGroup(groupIndex: number = 1): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    const addBtn = this.addConditionButton.nth(groupIndex);
    await addBtn.scrollIntoViewIfNeeded();
    await addBtn.click();
  }

  async clickOrOperatorInRootGroup(): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    const rootOrBtn = this.orOperatorButton.first();
    await rootOrBtn.scrollIntoViewIfNeeded();
    await rootOrBtn.click();
  }

  async clickNotOperatorInNestedGroup(groupIndex: number = 1): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    const notBtn = this.notOperatorButton.nth(groupIndex);
    await notBtn.scrollIntoViewIfNeeded();
    await notBtn.click();
  }

  async selectPropertyInNestedCondition(fieldName: string, conditionIndex: number = 1): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    const conditionSelect = this.conditionSelect.nth(conditionIndex);
    await conditionSelect.scrollIntoViewIfNeeded();
    await conditionSelect.click();
    await this.page.getByTestId(`option-${fieldName}`).filter({ visible: true }).click();
  }

  async selectOperatorInNestedCondition(operator: string, conditionIndex: number = 1): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    const operatorSelect = this.conditionOperatorSelect.nth(conditionIndex);
    await operatorSelect.scrollIntoViewIfNeeded();
    await operatorSelect.click();
    await this.page.getByTestId(`option-${operator}`).filter({ visible: true }).click();
  }

  async verifyConditionInGroup(groupIndex: number = 0): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    await expect(this.conditionSelect.nth(groupIndex)).toBeVisible();
  }

  async verifyGroupExists(groupIndex: number = 1): Promise<void> {
    // eslint-disable-next-line playwright/no-nth-methods
    await expect(this.addConditionButton.nth(groupIndex)).toBeVisible();
  }

  async createNestedGroupWithCondition(
    rootProperty: string,
    rootValue: string,
    nestedOperator: string,
    nestedProperty: string,
    nestedOperatorType: string,
  ): Promise<void> {
    await this.addFilterWithSearch(rootProperty, 'equals', rootValue);
    await this.clickAddGroupButton();

    if (nestedOperator === 'not') {
      await this.clickNotOperatorInNestedGroup();
    } else if (nestedOperator === 'or') {
      await this.clickOrOperatorInRootGroup();
    }

    await this.clickAddConditionInNestedGroup();
    await this.selectPropertyInNestedCondition(nestedProperty);
    await this.selectOperatorInNestedCondition(nestedOperatorType);
  }

  async editRootOperator(operator: string): Promise<void> {
    if (operator === 'or') {
      await this.clickOrOperatorInRootGroup();
    }
  }

  async verifyStructureForEdit(): Promise<void> {
    await this.verifyConditionInGroup(0);
    await this.verifyGroupExists(1);
  }

  validateLogicalStructure(
    predicate: { type?: string; operator?: string; operands?: unknown[] },
    expectedOperator: string,
  ): void {
    expect(predicate.type).toBe('logical');
    expect(predicate.operator?.toLowerCase()).toBe(expectedOperator.toLowerCase());
    expect(Array.isArray(predicate.operands)).toBe(true);
  }

  validateRootOperands(
    operands: Array<{ type?: string; property?: string }>,
    {
      expectedCount = 2,
      checkDomain = false,
      checkNestedGroup = false,
    }: {
      expectedCount?: number;
      checkDomain?: boolean;
      checkNestedGroup?: boolean;
    } = {},
  ): void {
    expect(operands.length).toBeGreaterThanOrEqual(expectedCount);

    const allHaveType = operands.every((op) => op.type && ['logical', 'property'].includes(op.type));
    expect(allHaveType).toBe(true);

    if (checkDomain) {
      const domainCondition = operands.find((op) => op.type === 'property' && op.property === 'domains');
      expect(domainCondition).toBeDefined();
    }

    if (checkNestedGroup) {
      const nestedGroup = operands.find((op) => op.type === 'logical');
      expect(nestedGroup).toBeDefined();
    }
  }

  validateNestedGroupCondition(nestedGroup: { operands?: unknown[] } | undefined, propertyName: string): void {
    expect(nestedGroup).toBeDefined();

    const nestedOperands = (nestedGroup?.operands || []) as Array<{ type?: string; property?: string }>;
    expect(nestedOperands.length).toBeGreaterThan(0);

    const condition = nestedOperands.find((op) => op.type === 'property' && op.property === propertyName);
    expect(condition).toBeDefined();
  }

  validateNestedStructure(
    predicate: { type?: string; operator?: string; operands?: unknown[] },
    {
      expectedRootOperator = 'and',
      nestedPropertyName,
    }: {
      expectedRootOperator?: string;
      nestedPropertyName: string;
    },
  ): void {
    this.validateLogicalStructure(predicate, expectedRootOperator);

    const rootOperands = (predicate.operands || []) as Array<{ type?: string; property?: string }>;
    this.validateRootOperands(rootOperands, {
      expectedCount: 2,
      checkDomain: true,
      checkNestedGroup: true,
    });

    const nestedGroup = rootOperands.find((op) => op.type === 'logical');
    this.validateNestedGroupCondition(nestedGroup as { operands?: unknown[] }, nestedPropertyName);
  }
}
