import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { GraphQLHelper } from '../helpers/graphql-helper';

/**
 * Page object for the Manage Structured Properties page (/structured-properties).
 *
 * Covers all CRUD operations on structured properties and helper methods for
 * managing properties across entities and schema fields.
 */
export class StructuredPropertiesPage extends BasePage {
  // ── Top-level page elements ──────────────────────────────────────────────

  readonly createButton: Locator;
  readonly propertiesTable: Locator;
  readonly pageTitle: Locator;

  // ── Form elements in drawer ──────────────────────────────────────────────

  readonly nameInputWrapper: Locator;
  readonly nameInputField: Locator;
  readonly descriptionInputWrapper: Locator;
  readonly descriptionInputField: Locator;
  readonly typeSelector: Locator;
  readonly appliesToSelector: Locator;
  readonly appliesToOptionsList: Locator;
  readonly createUpdateButton: Locator;
  readonly hideSwitch: Locator;
  readonly showInColumnsTableSwitch: Locator;
  readonly showInAssetSummarySwitch: Locator;
  readonly hideWhenEmptyCheckbox: Locator;

  // ── Entity-level property elements ────────────────────────────────────────

  readonly valueInput: Locator;
  readonly addUpdateOnEntityButton: Locator;
  readonly propertyMoreIcon: Locator;
  readonly addPropertyButton: Locator;
  readonly addPropertyDropdown: Locator;

  // ── Modal elements ───────────────────────────────────────────────────────

  readonly confirmButton: Locator;
  readonly dropdownMenu: Locator;
  readonly drawer: Locator;

  private readonly graphqlHelper: GraphQLHelper;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);

    // Top-level elements
    this.createButton = page.getByTestId('structured-props-create-button');
    this.propertiesTable = page.getByTestId('structured-props-table');
    this.pageTitle = page.locator('h1, [role="heading"]');

    // Form elements in drawer
    this.nameInputWrapper = page.getByTestId('structured-props-input-name');
    this.nameInputField = this.nameInputWrapper.locator('input');
    this.descriptionInputWrapper = page.getByTestId('structured-props-input-description');
    this.descriptionInputField = this.descriptionInputWrapper.locator('input');
    this.typeSelector = page.getByTestId('structured-props-select-input-type');
    this.appliesToSelector = page.getByTestId('structured-props-select-input-applies-to');
    this.appliesToOptionsList = page.locator('[data-testid="applies-to-options-list"]');
    this.createUpdateButton = page.getByTestId('structured-props-create-update-button');
    this.hideSwitch = page.getByTestId('structured-props-hide-switch');
    this.showInColumnsTableSwitch = page.getByTestId('structured-props-show-in-columns-table-switch');
    this.showInAssetSummarySwitch = page.getByTestId('structured-props-show-in-asset-summary-switch');
    this.hideWhenEmptyCheckbox = page.getByTestId('structured-props-hide-in-asset-summary-when-empty-checkbox');

    // Entity-level property elements
    this.valueInput = page.getByTestId('structured-property-string-value-input');
    this.addUpdateOnEntityButton = page.getByTestId('add-update-structured-prop-on-entity-button');
    this.propertyMoreIcon = page.getByTestId('structured-prop-entity-more-icon');
    this.addPropertyButton = page.getByTestId('add-structured-prop-button');
    this.addPropertyDropdown = page.getByTestId('add-structured-property-dropdown');

    // Modal elements
    this.confirmButton = page.getByTestId('modal-confirm-button');
    this.dropdownMenu = page.locator('.ant-dropdown-menu');
    this.drawer = page.locator('.ant-drawer-content');
  }

  // ── Navigation ───────────────────────────────────────────────────────────

  async navigate(): Promise<void> {
    this.logger?.step('navigate', { url: '/structured-properties' });
    await this.page.goto('/structured-properties');
    await this.waitForPageLoad();
  }

  // ── Helper to get entity type URN from entity name ──────────────────────

  private getEntityTypeUrn(entityName: string): string {
    const entityMap: { [key: string]: string } = {
      Dataset: 'datahub.dataset',
      Column: 'datahub.schemaField',
      Dashboard: 'datahub.dashboard',
      Chart: 'datahub.chart',
      DataJob: 'datahub.dataJob',
      DataFlow: 'datahub.dataFlow',
      Domain: 'datahub.domain',
      Container: 'datahub.container',
      GlossaryTerm: 'datahub.glossaryTerm',
      GlossaryNode: 'datahub.glossaryNode',
      Mlmodel: 'datahub.mlmodel',
    };
    return entityMap[entityName] || entityName.toLowerCase();
  }

  // ── Create structured property ───────────────────────────────────────────

  async createStructuredProperty(prop: { name: string; entity: string }): Promise<string> {
    this.logger?.step('createStructuredProperty', { prop });

    // Wait for GraphQL response before submitting
    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createStructuredProperty');

    // Click create button
    await this.createButton.click();

    // Fill name using declared selector
    await this.nameInputField.click();
    await this.nameInputField.fill(prop.name);

    // Select type "Text"
    await this.typeSelector.click();
    await this.page.getByText('Text', { exact: true }).click();

    // Select entity type using declared selector
    await this.appliesToSelector.click();
    await this.appliesToOptionsList.waitFor({ state: 'visible' });
    const entityUrn = this.getEntityTypeUrn(prop.entity);
    await this.appliesToOptionsList.locator(`[data-testid*="${entityUrn}"]`).first().click();

    // Submit
    await this.createUpdateButton.click();

    // Wait for GraphQL response and extract URN
    const response = await responsePromise;
    const propertyEntity = (response.data as Record<string, Record<string, string>>).createStructuredProperty;
    if (!propertyEntity || !propertyEntity.urn) {
      throw new Error(`Failed to extract structured property URN from GraphQL response: ${JSON.stringify(response)}`);
    }

    // Wait for creation to complete
    await this.page.waitForLoadState('networkidle');

    return propertyEntity.urn;
  }

  // ── Delete structured property ───────────────────────────────────────────

  async deleteStructuredProperty(prop: { name: string }): Promise<void> {
    this.logger?.step('deleteStructuredProperty', { prop });

    // Find property row in table
    const propRow = this.propertiesTable.locator('tr').filter({ hasText: prop.name });

    // Click more options icon
    const moreIcon = propRow.locator('[data-testid="structured-props-more-options-icon"]');
    await moreIcon.click();

    // Click Delete from dropdown
    await this.dropdownMenu.waitFor({ state: 'visible' });
    await this.page.getByText('Delete', { exact: true }).click();

    // Confirm deletion
    await this.confirmButton.click();

    // Wait for deletion to complete
    await this.page.waitForLoadState('networkidle');
  }

  // ── Update structured property ───────────────────────────────────────────

  async updateStructuredProperty(
    oldName: string,
    updates: { name?: string; description?: string; entity?: string },
  ): Promise<void> {
    this.logger?.step('updateStructuredProperty', { oldName, updates });

    // Find and click property in table
    const propRow = this.propertiesTable.locator('tr').filter({ hasText: oldName });
    const propCell = propRow.locator('td').first();
    await propCell.click();

    // Wait for form to load using declared selector
    await this.nameInputField.waitFor({ state: 'visible' });

    // Update name if provided
    if (updates.name) {
      await this.nameInputField.clear();
      await this.nameInputField.fill(updates.name);
    }

    // Update description if provided
    if (updates.description) {
      await this.descriptionInputField.waitFor({ state: 'visible' });
      await this.descriptionInputField.click();
      await this.descriptionInputField.fill(updates.description);
    }

    // Update entity if provided
    if (updates.entity) {
      await this.appliesToSelector.click();
      await this.appliesToOptionsList.waitFor({ state: 'visible' });
      const entityUrn = this.getEntityTypeUrn(updates.entity);
      await this.appliesToOptionsList.locator(`[data-testid*="${entityUrn}"]`).first().click();
    }

    // Submit
    await this.createUpdateButton.click();

    // Wait for update to complete
    await this.page.waitForLoadState('networkidle');
  }

  // ── Toggle property visibility ───────────────────────────────────────────

  async hideProperty(prop: { name: string }): Promise<void> {
    this.logger?.step('hideProperty', { prop });

    // Find and click property in table
    const propRow = this.propertiesTable.locator('tr').filter({ hasText: prop.name });
    const propCell = propRow.locator('td').first();
    await propCell.click();

    // Wait for the form to be fully loaded
    await this.createUpdateButton.waitFor({ state: 'visible' });

    // Toggle hide switch
    await this.hideSwitch.click();

    // Submit
    await this.createUpdateButton.click();

    // Wait for update
    await this.page.waitForLoadState('networkidle');
  }

  // ── Configure columns table display ──────────────────────────────────────

  async enableShowInColumnsTable(prop: { name: string }): Promise<void> {
    this.logger?.step('enableShowInColumnsTable', { prop });

    // Find and click property in table
    const propRow = this.propertiesTable.locator('tr').filter({ hasText: prop.name });
    const propCell = propRow.locator('td').first();
    await propCell.click();

    // Wait for the form to be fully loaded
    await this.createUpdateButton.waitFor({ state: 'visible' });

    // Toggle columns table switch
    await this.showInColumnsTableSwitch.click();

    // Submit
    await this.createUpdateButton.click();

    // Wait for update
    await this.page.waitForLoadState('networkidle');
  }

  // ── Entity-level property operations ─────────────────────────────────────

  async clickAddPropertyButton(): Promise<void> {
    this.logger?.step('clickAddPropertyButton');
    await this.addPropertyButton.click();
  }

  async selectPropertyFromDropdown(propertyName: string): Promise<void> {
    this.logger?.step('selectPropertyFromDropdown', { propertyName });

    await this.addPropertyDropdown.waitFor({ state: 'visible', timeout: 10000 });

    const propertyOption = this.addPropertyDropdown.locator('li[role="menuitem"]').filter({ hasText: propertyName });
    await propertyOption.waitFor({ state: 'visible', timeout: 45000 });
    await propertyOption.click();
  }

  async fillPropertyValue(value: string): Promise<void> {
    this.logger?.step('fillPropertyValue', { value });
    await this.valueInput.fill(value);
  }

  async submitPropertyValue(): Promise<void> {
    this.logger?.step('submitPropertyValue');
    await this.addUpdateOnEntityButton.click();
  }

  findPropertyRow(value: string): Locator {
    const table = this.page.locator('table');
    const cellWithValue = table.locator('td', { hasText: value }).first();
    return cellWithValue.locator('xpath=ancestor::tr[1]');
  }

  getPropertyMoreIcon(row: Locator): Locator {
    return row.locator('[data-testid="structured-prop-entity-more-icon"]').first();
  }

  async clickPropertyAction(action: 'Edit' | 'Remove'): Promise<void> {
    this.logger?.step('clickPropertyAction', { action });
    const menus = this.page.locator('body .ant-dropdown-menu');
    const menuCount = await menus.count();
    const visibleMenu = menus.nth(menuCount - 1);
    await visibleMenu.locator(`text=${action}`).click();
  }

  async confirmAction(): Promise<void> {
    this.logger?.step('confirmAction');
    await this.confirmButton.click();
  }

  // ── Assertion helpers ────────────────────────────────────────────────────────

  async expectPageContains(text: string): Promise<void> {
    this.logger?.step('expectPageContains', { text });
    await expect(this.page.locator('body')).toContainText(text);
  }

  async expectPageNotContains(text: string): Promise<void> {
    this.logger?.step('expectPageNotContains', { text });
    await expect(this.page.locator('body')).not.toContainText(text);
  }

  async waitForPageLoad(): Promise<void> {
    this.logger?.step('waitForPageLoad');
    await this.page.waitForLoadState('networkidle');
  }

  async waitForDropdownVisible(): Promise<void> {
    this.logger?.step('waitForDropdownVisible');
    const dropdown = this.page.locator('body .ant-dropdown-menu');
    await dropdown.waitFor({ state: 'visible' });
  }

  // ── Field-level property operations ─────────────────────────────────────

  getFieldPropertyButton(propertyName: string): Locator {
    return this.page.getByTestId(`${propertyName}-add-or-edit-button`);
  }

  async waitForFieldPropertyButton(propertyName: string, timeout: number = 30000): Promise<void> {
    this.logger?.step('waitForFieldPropertyButton', { propertyName });
    const button = this.getFieldPropertyButton(propertyName);
    await button.waitFor({ state: 'visible', timeout });
  }

  async fillFieldPropertyValue(value: string): Promise<void> {
    this.logger?.step('fillFieldPropertyValue', { value });
    await this.valueInput.fill(value);
  }

  async clearFieldPropertyValue(): Promise<void> {
    this.logger?.step('clearFieldPropertyValue');
    await this.valueInput.clear();
  }

  async submitFieldProperty(): Promise<void> {
    this.logger?.step('submitFieldProperty');
    await this.addUpdateOnEntityButton.click();
  }

  async clickFieldPropertiesTab(): Promise<void> {
    this.logger?.step('clickFieldPropertiesTab');
    await this.page.getByTestId('Properties-field-drawer-tab-header').click();
  }

  async clickFieldPropertyAction(propertyName: string, action: 'Edit' | 'Remove'): Promise<void> {
    this.logger?.step('clickFieldPropertyAction', { propertyName, action });
    const row = this.drawer.locator('tr').filter({ hasText: propertyName });
    const moreIcon = row.locator('[data-testid="structured-prop-entity-more-icon"]');
    await moreIcon.click();

    await this.dropdownMenu.waitFor({ state: 'visible' });
    await this.dropdownMenu.locator(`text=${action}`).click();
  }

  async expectDrawerContains(text: string): Promise<void> {
    this.logger?.step('expectDrawerContains', { text });
    await expect(this.drawer).toContainText(text);
  }

  async expectDrawerNotContains(text: string): Promise<void> {
    this.logger?.step('expectDrawerNotContains', { text });
    await expect(this.drawer).not.toContainText(text);
  }

  async confirmModalAction(): Promise<void> {
    this.logger?.step('confirmModalAction');
    await this.confirmButton.click();
  }
}
