import type { Page } from '@playwright/test';
import { expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

export class SchemaBlamePage extends BasePage {
  // ── UI Element Selectors ──
  readonly schemaBlameButton = this.page.getByTestId('schema-blame-button');
  readonly schemaVersionSelector = this.page.getByTestId('schema-version-selector-dropdown');
  readonly schemaTable = this.page.getByTestId('schema-table');
  readonly schemaTableContainer = this.page.getByTestId('schema-table-container');
  readonly schemaFieldDrawer = this.page.getByTestId('schema-field-drawer-content');
  readonly rawViewButton = this.page.getByTestId('schema-raw-view-button');

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
  }

  // ── Private Selector Factories ──

  private getFieldDescriptionLocator(fieldName: string) {
    return this.page.getByTestId(`schema-field-${fieldName}-description`);
  }

  private getVersionButton(version: string) {
    return this.page.getByTestId(`sem-ver-select-button-${version}`);
  }

  private getFieldRow(fieldName: string) {
    // Scope to columns panel to avoid matching field names in drawer, tooltips, etc.
    const columnsPanel = this.page.getByRole('tabpanel', { name: /columns/i });
    return columnsPanel.getByText(fieldName);
  }

  // ── Public Interaction Methods ──

  async verifyFieldVisible(fieldName: string) {
    this.logger?.step('verify field visible', { fieldName });
    const fieldRow = this.getFieldRow(fieldName);
    await expect(fieldRow).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async verifyFieldNotVisible(fieldName: string) {
    this.logger?.step('verify field not visible', { fieldName });
    const fieldRow = this.getFieldRow(fieldName);
    await expect(fieldRow).not.toBeVisible({ timeout: TIMEOUTS.SHORT });
  }

  async closeModals() {
    this.logger?.step('close modals', {});
    await this.page.keyboard.press('Escape');
  }

  async openVersionSelector() {
    this.logger?.step('open version selector', {});
    await this.schemaVersionSelector.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async selectVersion(version: string) {
    this.logger?.step('select version', { version });
    const versionButton = this.getVersionButton(version);
    // Match version starting with the requested number (e.g., "0.0.0 -", "1.0.0 -")
    // Use negative lookbehind to avoid matching digits within other numbers
    const escapedVersion = version.replace(/\./g, '\\.');
    const versionRegex = new RegExp(`(^|\\s)${escapedVersion}(\\.0)*\\s+-`);
    const versionText = this.page.getByText(versionRegex);
    await versionButton.or(versionText).click({ timeout: TIMEOUTS.LONG });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async clickField(fieldName: string) {
    this.logger?.step('click field', { fieldName });
    const fieldRow = this.getFieldRow(fieldName);
    await fieldRow.click();
  }

  async verifyFieldHasTag(fieldName: string, tagName: string) {
    this.logger?.step('verify field has tag', { fieldName, tagName });
    // Tags appear in either Columns panel (table) or field drawer (detail view)
    // Check both to avoid strict mode violations from duplicate testIds
    const columnsPanel = this.page.getByRole('tabpanel', { name: /columns/i });
    const tagInColumns = columnsPanel.getByText(tagName);
    const tagInDrawer = this.schemaFieldDrawer.getByText(tagName);

    const columnsCount = await tagInColumns.count();
    const drawerCount = await tagInDrawer.count();

    if (columnsCount === 0 && drawerCount === 0) {
      throw new Error(`Tag '${tagName}' not found for field '${fieldName}' in Columns panel or drawer`);
    }

    // Verify tag is visible in whichever context contains it
    if (columnsCount > 0) {
      await expect(columnsPanel).toContainText(tagName, { timeout: TIMEOUTS.MEDIUM });
    } else {
      await expect(this.schemaFieldDrawer).toContainText(tagName, { timeout: TIMEOUTS.MEDIUM });
    }
  }

  async verifyFieldDoesNotHaveTag(fieldName: string, tagName: string) {
    this.logger?.step('verify field does not have tag', { fieldName, tagName });
    // Check both Columns panel and field drawer for tag absence
    const columnsPanel = this.page.getByRole('tabpanel', { name: /columns/i });
    await expect(columnsPanel).not.toContainText(tagName, { timeout: TIMEOUTS.SHORT });
    await expect(this.schemaFieldDrawer).not.toContainText(tagName, { timeout: TIMEOUTS.SHORT });
  }

  async toggleSchemaBlame() {
    this.logger?.step('toggle schema blame', {});
    await this.schemaBlameButton.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifySchemaBlameOpen() {
    this.logger?.step('verify schema blame open', {});
    // Panel shows as "Change History" or "Complete change history"
    const changeHistoryText = this.page.getByText('Change History');
    const completeHistoryText = this.page.getByText('Complete change history');

    const changeHistoryVisible = await changeHistoryText.count().then((count) => count > 0);
    const completeHistoryVisible = await completeHistoryText.count().then((count) => count > 0);

    if (!changeHistoryVisible && !completeHistoryVisible) {
      throw new Error(
        'Schema blame panel not found: neither "Change History" nor "Complete change history" text found',
      );
    }
  }

  async getFieldDescriptionText(fieldName: string): Promise<string | null> {
    this.logger?.step('get field description', { fieldName });
    const description = await this.getFieldDescriptionLocator(fieldName).textContent();
    return description;
  }

  async verifyFieldDescriptionContains(descriptionText: string) {
    this.logger?.step('verify field description contains', { descriptionText });
    // Scope to columns panel to avoid matching text in tooltips or drawer headers
    const columnsPanel = this.page.getByRole('tabpanel', { name: /columns/i });
    await expect(columnsPanel.getByText(descriptionText)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async waitForSchemaLoad() {
    this.logger?.step('wait for schema load', {});
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Get first table (schema table) since other UI elements may contain tables
    // eslint-disable-next-line playwright/no-nth-methods
    await this.page.getByRole('table').first().waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  async navigateToDataset(urn: string) {
    this.logger?.step('navigate to dataset', { urn });
    const datasetPath = `/dataset/${urn}`;
    await this.navigate(datasetPath);
    await this.waitForSchemaLoad();
  }

  async verifyRawViewButtonExists() {
    this.logger?.step('verify raw view button exists', {});
    // Use testId or role-based fallback
    const rawButtonByTestId = this.rawViewButton;
    const rawButtonByRole = this.page.getByRole('button', { name: /raw/i });
    const combinedSelector = rawButtonByTestId.or(rawButtonByRole);

    await expect(combinedSelector).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async toggleRawView() {
    this.logger?.step('toggle raw view', {});
    await this.rawViewButton.click();
  }
}
