import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { GraphQLHelper } from '../helpers/graphql-helper';

const DROPDOWN_TIMEOUT = 10000;
const PROPERTY_OPTION_TIMEOUT = 45000;
const FIELD_PROPERTY_TIMEOUT = 30000;

/**
 * Page object for the Manage Structured Properties page (/structured-properties).
 *
 * Covers all CRUD operations on structured properties and helper methods for
 * managing properties across entities and schema fields.
 */
export class StructuredPropertiesPage extends BasePage {
  // ── Top-level page elements ──────────────────────────────────────────────

  readonly createButton: Locator;
  readonly managementPageTable: Locator;
  readonly entityPageTable: Locator;
  readonly pageTitle: Locator;

  // ── Form elements in drawer ──────────────────────────────────────────────

  readonly nameInputField: Locator;
  readonly descriptionInputField: Locator;
  readonly typeSelector: Locator;
  readonly appliesToSelector: Locator;
  readonly appliesToOptionsList: Locator;
  readonly createUpdateButton: Locator;
  readonly hideSwitch: Locator;
  readonly showInColumnsTableSwitch: Locator;
  readonly showInAssetSummarySwitch: Locator;
  readonly hideWhenEmptyCheckbox: Locator;
  readonly fieldPropertiesTabHeader: Locator;

  // ── Entity-level property elements ────────────────────────────────────────

  readonly valueInput: Locator;
  readonly addUpdateOnEntityButton: Locator;
  readonly addPropertyButton: Locator;
  readonly addPropertyDropdown: Locator;

  // ── Modal elements ───────────────────────────────────────────────────────

  readonly confirmButton: Locator;
  readonly managementDrawer: Locator;
  readonly fieldDrawer: Locator;
  readonly pageBody: Locator;

  // ── Dropdown and menu elements ────────────────────────────────────────────

  readonly menuItem: Locator;

  // ── Selectors for table and dropdowns ────────────────────────────────

  private readonly graphqlHelper: GraphQLHelper;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);

    // Top-level elements
    this.createButton = page.getByTestId('structured-props-create-button');
    this.managementPageTable = page.getByTestId('structured-props-table');
    this.entityPageTable = page.getByTestId('entity-properties-table');
    this.pageTitle = page.getByRole('heading', { level: 1 });

    // Form elements in drawer
    this.nameInputField = page.getByTestId('structured-props-input-name').getByRole('textbox');
    this.descriptionInputField = page.getByTestId('structured-props-input-description');
    this.typeSelector = page.getByTestId('structured-props-select-input-type');
    this.appliesToSelector = page.getByTestId('structured-props-select-input-applies-to');
    this.appliesToOptionsList = page.getByTestId('applies-to-options-list');
    this.createUpdateButton = page.getByTestId('structured-props-create-update-button');
    this.hideSwitch = page.getByTestId('structured-props-hide-switch');
    this.showInColumnsTableSwitch = page.getByTestId('structured-props-show-in-columns-table-switch');
    this.showInAssetSummarySwitch = page.getByTestId('structured-props-show-in-asset-summary-switch');
    this.hideWhenEmptyCheckbox = page.getByTestId('structured-props-hide-in-asset-summary-when-empty-checkbox');
    this.fieldPropertiesTabHeader = page.getByTestId('Properties-field-drawer-tab-header');

    // Entity-level property elements
    this.valueInput = page.getByTestId('structured-property-string-value-input');
    this.addUpdateOnEntityButton = page.getByTestId('add-update-structured-prop-on-entity-button');
    this.addPropertyButton = page.getByTestId('add-structured-prop-button');
    this.addPropertyDropdown = page.getByTestId('add-structured-property-dropdown');

    // Modal elements
    this.confirmButton = page.getByTestId('modal-confirm-button');
    this.managementDrawer = page.getByTestId('structured-props-drawer-content');
    this.fieldDrawer = page.getByTestId('schema-field-drawer-content');
    this.pageBody = page.getByRole('document');

    // Dropdown and menu elements
    this.menuItem = page.getByRole('menuitem');
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

  // ── Dynamic locator helpers ──────────────────────────────────────────────

  private getAppliesToOption(entityUrn: string): Locator {
    return this.appliesToOptionsList.getByTestId(new RegExp(entityUrn));
  }

  private findPropertyRow(propertyName: string): Locator {
    return this.entityPageTable.getByRole('row').filter({ hasText: propertyName });
  }

  private findPropertyRowInManagement(propertyName: string): Locator {
    // Find property row in management table (for management page operations)
    return this.managementPageTable.getByRole('row').filter({ hasText: propertyName });
  }

  private getPropertyMoreIconFromRow(row: Locator): Locator {
    return row.getByTestId('structured-props-more-options-icon');
  }

  private getEntityPropertyMoreIcon(row: Locator): Locator {
    return row.getByTestId('structured-prop-entity-more-icon');
  }

  private getDropdownSearchInput(): Locator {
    return this.addPropertyDropdown.getByRole('textbox');
  }

  private getPropertyOptionInDropdown(propertyName: string): Locator {
    return this.addPropertyDropdown.getByRole('menuitem').filter({ hasText: propertyName });
  }

  private getActionMenuItem(action: string): Locator {
    return this.menuItem.filter({ hasText: action });
  }

  private getPropertyNameCell(row: Locator, propertyName: string): Locator {
    // Gets the property name cell by filtering cells in the row by text content
    return row.getByRole('cell').filter({ hasText: propertyName });
  }

  // ── Create structured property ───────────────────────────────────────────

  async createStructuredProperty(prop: { name: string; entity: string }): Promise<string> {
    this.logger?.step('createStructuredProperty', { prop });

    // Intercept the GraphQL response to extract the URN of the newly created property
    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createStructuredProperty');

    // Click create button
    await this.createButton.click();
    await this.nameInputField.click();
    await this.nameInputField.fill(prop.name);

    // Select Text as the property type
    await this.typeSelector.click();
    await this.page.getByText('Text', { exact: true }).click();

    // Select the entity type this property applies to
    await this.appliesToSelector.click();
    await this.appliesToOptionsList.waitFor({ state: 'visible' });
    const entityUrn = this.getEntityTypeUrn(prop.entity);
    await this.getAppliesToOption(entityUrn).click();

    // Submit the form
    await this.createUpdateButton.click();

    // Extract the URN from the GraphQL response
    const response = await responsePromise;
    const propertyEntity = (response.data as Record<string, Record<string, string>>).createStructuredProperty;
    if (!propertyEntity || !propertyEntity.urn) {
      throw new Error(`Failed to extract structured property URN from GraphQL response: ${JSON.stringify(response)}`);
    }

    await this.page.waitForLoadState('networkidle');

    return propertyEntity.urn;
  }

  // ── Delete structured property ───────────────────────────────────────────

  async deleteStructuredProperty(prop: { name: string }): Promise<void> {
    this.logger?.step('deleteStructuredProperty', { prop });

    // Find the property row and open its action menu (management page)
    const propRow = this.findPropertyRowInManagement(prop.name);
    const moreIcon = this.getPropertyMoreIconFromRow(propRow);
    await moreIcon.click();

    // Click Delete from the dropdown menu
    await this.getActionMenuItem('Delete').click();

    // Confirm the deletion
    await this.confirmButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Update structured property ───────────────────────────────────────────

  async updateStructuredProperty(
    oldName: string,
    updates: { name?: string; description?: string; entity?: string },
  ): Promise<void> {
    this.logger?.step('updateStructuredProperty', { oldName, updates });

    // Find and open the property for editing (management page)
    const propRow = this.findPropertyRowInManagement(oldName);
    // Wait for the property row to appear and be visible in the table
    await propRow.waitFor({ state: 'visible' });
    // Click on the property name cell to open the drawer
    const propNameCell = this.getPropertyNameCell(propRow, oldName);
    await propNameCell.click();

    // Wait for the drawer to appear
    await this.managementDrawer.waitFor({ state: 'visible' });
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

    // Update entity type if provided
    if (updates.entity) {
      await this.appliesToSelector.click();
      await this.appliesToOptionsList.waitFor({ state: 'visible' });
      const entityUrn = this.getEntityTypeUrn(updates.entity);
      await this.getAppliesToOption(entityUrn).click();
    }

    // Submit the update
    await this.createUpdateButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Toggle property visibility ───────────────────────────────────────────

  async hideProperty(prop: { name: string }): Promise<void> {
    this.logger?.step('hideProperty', { prop });

    // Find property row (management page)
    const propRow = this.findPropertyRowInManagement(prop.name);
    const propCell = this.getPropertyNameCell(propRow, prop.name);
    await propCell.click();

    // Wait for the drawer to appear
    await this.managementDrawer.waitFor({ state: 'visible' });
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

    // Find and click property in table (management page)
    const propRow = this.findPropertyRowInManagement(prop.name);
    const propCell = this.getPropertyNameCell(propRow, prop.name);
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

    await this.addPropertyDropdown.waitFor({ state: 'visible', timeout: DROPDOWN_TIMEOUT });

    // Type property name in dropdown search to filter results
    const searchInput = this.getDropdownSearchInput();
    await searchInput.fill(propertyName);

    // Find and click the property option from the filtered dropdown menu
    const propertyOption = this.getPropertyOptionInDropdown(propertyName);
    await propertyOption.waitFor({ state: 'visible', timeout: PROPERTY_OPTION_TIMEOUT });
    await propertyOption.click();
  }

  async fillPropertyValue(value: string): Promise<void> {
    this.logger?.step('fillPropertyValue', { value });
    await this.valueInput.fill(value);
  }

  async submitPropertyValue(): Promise<void> {
    this.logger?.step('submitPropertyValue');
    await this.addUpdateOnEntityButton.click();
    // Wait for the value input to disappear, indicating the form has closed
    await this.valueInput.waitFor({ state: 'hidden' });
  }

  async waitForPropertyRow(propertyName: string): Promise<void> {
    this.logger?.step('waitForPropertyRow', { propertyName });
    const row = this.findPropertyRow(propertyName);
    await row.waitFor({ state: 'visible' });
  }

  async waitForPropertyRowToDisappear(propertyName: string): Promise<void> {
    this.logger?.step('waitForPropertyRowToDisappear', { propertyName });
    const row = this.findPropertyRow(propertyName);
    await row.waitFor({ state: 'hidden' });
  }

  async clickPropertyMoreIcon(propertyName: string): Promise<void> {
    this.logger?.step('clickPropertyMoreIcon', { propertyName });
    const row = this.findPropertyRow(propertyName);
    await row.waitFor({ state: 'visible' });
    const moreIcon = this.getEntityPropertyMoreIcon(row);
    await moreIcon.click();
  }

  async clearPropertyValue(): Promise<void> {
    this.logger?.step('clearPropertyValue');
    await this.valueInput.clear();
  }

  async searchPropertyInDropdown(propertyName: string): Promise<void> {
    this.logger?.step('searchPropertyInDropdown', { propertyName });
    const searchInput = this.getDropdownSearchInput();
    await searchInput.fill(propertyName);
  }

  async clickPropertyAction(action: 'Edit' | 'Remove'): Promise<void> {
    this.logger?.step('clickPropertyAction', { action });
    // Click the action from the dropdown menu (e.g., Edit, Remove)
    const actionItem = this.getActionMenuItem(action);
    await actionItem.click();
  }

  async confirmAction(): Promise<void> {
    this.logger?.step('confirmAction');
    await this.confirmButton.click();
  }

  // ── Assertion helpers ────────────────────────────────────────────────────────

  async expectPageContains(text: string): Promise<void> {
    this.logger?.step('expectPageContains', { text });
    // Verify text is present on the page
    await expect(this.pageBody).toContainText(text);
  }

  async expectPageNotContains(text: string): Promise<void> {
    this.logger?.step('expectPageNotContains', { text });
    // Verify text is NOT present on the page
    await expect(this.pageBody).not.toContainText(text);
  }

  async waitForPageLoad(): Promise<void> {
    this.logger?.step('waitForPageLoad');
    await this.page.waitForLoadState('networkidle');
  }

  async waitForDropdownVisible(): Promise<void> {
    this.logger?.step('waitForDropdownVisible');
    await this.menuItem.waitFor({ state: 'visible' });
  }

  // ── Field-level property operations ─────────────────────────────────────

  private getFieldPropertyButton(propertyName: string): Locator {
    return this.page.getByTestId(`${propertyName}-add-or-edit-button`);
  }

  async waitForFieldPropertyButton(propertyName: string, timeout: number = FIELD_PROPERTY_TIMEOUT): Promise<void> {
    this.logger?.step('waitForFieldPropertyButton', { propertyName });
    await this.getFieldPropertyButton(propertyName).waitFor({ state: 'visible', timeout });
  }

  async clickFieldPropertyButton(propertyName: string): Promise<void> {
    this.logger?.step('clickFieldPropertyButton', { propertyName });
    await this.getFieldPropertyButton(propertyName).click();
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
    await this.fieldPropertiesTabHeader.click();
  }

  async clickFieldPropertyAction(propertyName: string, action: 'Edit' | 'Remove'): Promise<void> {
    this.logger?.step('clickFieldPropertyAction', { propertyName, action });
    // Find the property row and open its action menu
    const row = this.fieldDrawer.getByRole('row').filter({ hasText: propertyName });
    await row.waitFor({ state: 'visible' });
    const moreIcon = this.getEntityPropertyMoreIcon(row);
    await moreIcon.waitFor({ state: 'visible' });
    await moreIcon.click();

    // Click the action from the dropdown menu
    const actionItem = this.getActionMenuItem(action);
    await actionItem.waitFor({ state: 'visible' });
    await actionItem.click();
  }

  async expectDrawerContains(text: string): Promise<void> {
    this.logger?.step('expectDrawerContains', { text });
    await expect(this.fieldDrawer).toContainText(text);
  }

  async expectDrawerNotContains(text: string): Promise<void> {
    this.logger?.step('expectDrawerNotContains', { text });
    await expect(this.fieldDrawer).not.toContainText(text);
  }

  async confirmModalAction(): Promise<void> {
    this.logger?.step('confirmModalAction');
    await this.confirmButton.click();
  }
}
