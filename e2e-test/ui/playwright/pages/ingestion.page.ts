/**
 * IngestionPage — page object for /ingestion (Sources, Secrets tabs).
 *
 * Covers the ingestion page redesign=false workflow used by v2_ingestion_source,
 * v2_managed_ingestion, and v2_managing_secrets tests.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class IngestionPage extends BasePage {
  // ── Tab / top-level navigation ────────────────────────────────────────────
  readonly sourcesTab: Locator;
  readonly secretsTab: Locator;
  readonly createSourceButton: Locator;
  readonly createSecretButton: Locator;
  readonly searchInput: Locator;

  // ── Snowflake / generic recipe form inputs ────────────────────────────────
  readonly accountIdInput: Locator;
  readonly warehouseInput: Locator;
  readonly usernameInput: Locator;
  readonly passwordInput: Locator;
  readonly authTypeSelect: Locator;
  readonly roleInput: Locator;

  // ── Wizard navigation ─────────────────────────────────────────────────────
  readonly dataSourceSearchInput: Locator;
  readonly recipeYamlButton: Locator;
  readonly recipeNextButton: Locator;
  readonly scheduleNextButton: Locator;
  /** AntD collapse panel that appears after the schedule step. */
  readonly scheduleCollapseItem: Locator;
  readonly sourceNameInput: Locator;
  readonly saveButton: Locator;
  /** AntD modal container; use .not.toBeVisible() to confirm wizard close. */
  readonly wizardModal: Locator;

  // ── Secret modal inputs ───────────────────────────────────────────────────
  readonly secretNameInput: Locator;
  readonly secretValueInput: Locator;
  readonly secretDescriptionInput: Locator;
  readonly secretCreateButton: Locator;
  /** AntD virtual-list for secret/password dropdown options. */
  readonly secretDropdownList: Locator;
  /** First delete icon on the page; used by deleteSecret() after tab switch scopes the view. */
  readonly firstDeleteIcon: Locator;

  // ── Monaco editor ─────────────────────────────────────────────────────────
  /** First (primary) Monaco scrollable element; all editor operations target this. */
  readonly monacoEditor: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    // Tab keys use data-node-key attribute on ant-tabs
    this.sourcesTab = page.locator('[data-node-key="Sources"]');
    this.secretsTab = page.locator('[data-node-key="Secrets"]');
    this.createSourceButton = page.locator('[data-testid="create-ingestion-source-button"]');
    this.createSecretButton = page.locator('[data-testid="create-secret-button"]');
    this.searchInput = page.locator('[data-testid="search-bar-input"]');

    this.accountIdInput = page.locator('#account_id');
    this.warehouseInput = page.locator('#warehouse');
    this.usernameInput = page.locator('#username');
    this.passwordInput = page.locator('#password');
    this.authTypeSelect = page.locator('#authentication_type');
    this.roleInput = page.locator('#role');

    this.dataSourceSearchInput = page.locator('[placeholder="Search data sources..."]');
    this.recipeYamlButton = page.locator('[data-testid="recipe-builder-yaml-button"]');
    this.recipeNextButton = page.locator('[data-testid="recipe-builder-next-button"]');
    this.scheduleNextButton = page.locator('[data-testid="ingestion-schedule-next-button"]');
    this.scheduleCollapseItem = page.locator('.ant-collapse-item'); // no data-testid on this AntD panel
    this.sourceNameInput = page.locator('[data-testid="source-name-input"]');
    this.saveButton = page.locator('[data-testid="ingestion-source-save-button"]');
    this.wizardModal = page.locator('.ant-modal'); // no data-testid on the AntD modal root

    this.secretNameInput = page.locator('[data-testid="secret-modal-name-input"] input');
    this.secretValueInput = page.locator('[data-testid="secret-modal-value-input"] textarea');
    this.secretDescriptionInput = page.locator('[data-testid="secret-modal-description-input"] textarea');
    this.secretCreateButton = page.locator('[data-testid="secret-modal-create-button"]');
    this.secretDropdownList = page.locator('.rc-virtual-list-holder-inner'); // AntD virtual-list, no testid
    this.firstDeleteIcon = page.locator('[data-icon="delete"]').first();

    this.monacoEditor = page.locator('.monaco-scrollable-element').first();
  }

  async navigate(): Promise<void> {
    await this.page.goto('/ingestion');
    await expect(this.page.getByRole('tab', { name: 'Sources' })).toBeVisible({ timeout: 30000 });
  }

  async clickSourcesTab(): Promise<void> {
    await this.sourcesTab.click();
    await this.page.waitForLoadState('networkidle');
  }

  async clickSecretsTab(): Promise<void> {
    await this.secretsTab.click();
    await this.page.waitForLoadState('networkidle');
  }

  async waitForSourcesLoaded(): Promise<void> {
    // Wait until the "Loading ingestion sources..." spinner is gone
    await expect(this.page.getByText('Loading ingestion sources...')).not.toBeVisible({ timeout: 30000 });
  }

  async clickCreateSourceButton(): Promise<void> {
    await this.createSourceButton.click();
  }

  async clickCreateSecretButton(): Promise<void> {
    await this.createSecretButton.click();
  }

  async searchSources(name: string): Promise<void> {
    // Use pressSequentially so the AntD Input onChange fires on each keystroke,
    // matching how the SearchBar's controlled state updates the GraphQL query variable.
    // fill() sets the DOM value but does not reliably trigger the synthetic change event
    // on controlled AntD inputs, causing the filter to stay stale.
    await this.searchInput.click({ clickCount: 3 }); // select-all any existing text
    await this.searchInput.pressSequentially(name);
    await this.page.waitForTimeout(500);
  }

  async refreshSources(): Promise<void> {
    await this.page.getByRole('button', { name: 'Refresh' }).click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Snowflake source creation wizard helpers ────────────────────────────────

  async searchDataSource(query: string): Promise<void> {
    await this.dataSourceSearchInput.fill(query);
    await this.page.waitForTimeout(500);
  }

  async selectDataSource(name: string): Promise<void> {
    await this.page.getByText(name).first().click();
    await this.page.waitForLoadState('networkidle');
  }

  async fillSnowflakeForm(params: {
    accountId: string;
    warehouseId: string;
    username: string;
    password?: string;
    role: string;
  }): Promise<void> {
    await expect(this.accountIdInput).toBeVisible({ timeout: 15000 });
    await this.accountIdInput.fill(params.accountId);
    await this.warehouseInput.fill(params.warehouseId);
    await this.usernameInput.fill(params.username);

    if (params.password !== undefined) {
      await this.selectAuthType('Username & Password');
      await expect(this.passwordInput).toBeVisible({ timeout: 5000 });
      await this.passwordInput.fill(params.password);
      await this.passwordInput.blur();
    }

    await this.roleInput.fill(params.role);
  }

  async clickRecipeYamlButton(): Promise<void> {
    await this.recipeYamlButton.click();
  }

  async clickRecipeNextButton(): Promise<void> {
    await this.recipeNextButton.click();
  }

  async clickScheduleNextButton(): Promise<void> {
    await this.scheduleNextButton.click();
    await expect(this.scheduleCollapseItem).toBeVisible({ timeout: 15000 });
  }

  async fillSourceName(name: string): Promise<void> {
    await this.sourceNameInput.fill(name);
  }

  async clickSaveButton(): Promise<void> {
    await this.saveButton.click();
  }

  async clickNextButton(): Promise<void> {
    await this.page.getByRole('button', { name: 'Next' }).click();
  }

  async expectSnowflakeFormValues(params: {
    accountId: string;
    warehouseId: string;
    username: string;
    password: string;
    role: string;
  }): Promise<void> {
    await expect(this.accountIdInput).toHaveValue(params.accountId, { timeout: 15000 });
    await expect(this.warehouseInput).toHaveValue(params.warehouseId);
    await expect(this.usernameInput).toHaveValue(params.username);
    await expect(
      // XPath needed to reach the AntD form-item wrapper — no data-testid on the container.
      this.authTypeSelect.locator('xpath=ancestor::*[contains(@class,"ant-form-item")][1]'),
    ).toContainText('Username & Password');
    await expect(this.passwordInput).toHaveValue(params.password);
    await expect(this.roleInput).toHaveValue(params.role);
  }

  // ── Verification helpers ──────────────────────────────────────────────────

  async expectSourceVisible(name: string): Promise<void> {
    await expect(this.page.locator('tr').filter({ hasText: name })).toBeVisible({ timeout: 30000 });
  }

  async expectScheduleStepVisible(): Promise<void> {
    await expect(this.page.getByText('Configure an Ingestion Schedule')).toBeVisible({ timeout: 15000 });
  }

  // Polls until ES indexes the new/renamed source (Kafka→MAE→OpenSearch can lag).
  // Clicks Refresh on each iteration to force a fresh GMS query rather than relying
  // on the Apollo cache, and uses a 90-second outer timeout to accommodate indexing
  // delays in slow CI environments.
  async expectSourceEventuallyVisible(name: string): Promise<void> {
    await expect(async () => {
      await this.refreshSources();
      await this.searchSources(name);
      await this.page.waitForTimeout(1000);
      await expect(this.page.locator('tr').filter({ hasText: name })).toBeVisible({ timeout: 3000 });
    }).toPass({ timeout: 90000, intervals: [3000] });
  }

  async expectWizardModalClosed(timeout = 30000): Promise<void> {
    await expect(this.wizardModal).not.toBeVisible({ timeout });
  }

  async fillRoleField(role: string): Promise<void> {
    await this.roleInput.fill(role);
  }

  // ── "Other" data-source type selection ─────────────────────────────────────

  async selectOtherDataSource(): Promise<void> {
    await this.searchDataSource('other');
    const otherOption = this.page.getByText('Other').first();
    await otherOption.scrollIntoViewIfNeeded();
    await otherOption.click();
    // Wait for the YAML editor to appear — confirms the "Other" wizard step loaded
    await expect(this.page.getByText('source-type').first()).toBeVisible({ timeout: 15000 });
  }

  async clickSaveAndRunButton(): Promise<void> {
    // exact: true avoids matching the substring 'Save' inside 'Save & Run'
    const btn = this.page.getByRole('button', { name: 'Save & Run', exact: true });
    await btn.scrollIntoViewIfNeeded();
    await btn.click();
  }

  async expectSourceSucceeded(sourceName: string): Promise<void> {
    await expect(this.page.locator('tr').filter({ hasText: sourceName }).getByText('Succeeded')).toBeVisible({
      timeout: 180000,
    });
  }

  async expectSourceStatusPending(sourceName?: string): Promise<void> {
    const statusLocator = sourceName
      ? this.page.locator('tr').filter({ hasText: sourceName }).locator('[data-testid="ingestion-source-table-status"]')
      : this.page.locator('[data-testid="ingestion-source-table-status"]').first();
    await expect(statusLocator).toContainText('Pending', { timeout: 30000 });
  }

  async openEditForSource(sourceName: string): Promise<void> {
    await this.page.locator('tr').filter({ hasText: sourceName }).getByRole('button', { name: 'EDIT' }).click();
  }

  async deleteSource(sourceName: string): Promise<void> {
    // Search for the source first so it is always visible regardless of pagination.
    await this.searchSources(sourceName);
    await expect(this.page.locator('tr').filter({ hasText: sourceName })).toBeVisible({ timeout: 15000 });
    await this.page.locator('tr').filter({ hasText: sourceName }).locator('[data-icon="delete"]').first().click();
    await expect(this.page.getByText('Confirm Ingestion Source Removal')).toBeVisible();
    await this.page.getByRole('button', { name: 'Yes' }).click();
    await expect(this.page.getByText('Removed ingestion source.')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(sourceName)).not.toBeVisible({ timeout: 15000 });
  }

  // ── Secrets helpers ─────────────────────────────────────────────────────────

  async fillAndSubmitSecretModal(name: string, value: string, description: string): Promise<void> {
    await this.secretNameInput.fill(name);
    await this.secretValueInput.fill(value);
    await this.secretDescriptionInput.fill(description);
    await this.secretCreateButton.click();
  }

  async createSecret(name: string, value: string, description: string): Promise<void> {
    await this.clickCreateSecretButton();
    await this.fillAndSubmitSecretModal(name, value, description);
  }

  async deleteSecret(name: string): Promise<void> {
    await this.firstDeleteIcon.click();
    await expect(this.page.getByText('Confirm Secret Removal')).toBeVisible();
    await this.page.getByRole('button', { name: 'Yes' }).click();
    await expect(this.page.getByText('Removed secret.')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(name)).not.toBeVisible({ timeout: 15000 });
  }

  async selectAuthType(typeName: string): Promise<void> {
    await this.authTypeSelect.click({ force: true });
    // dispatchEvent bypasses Playwright's viewport check; AntD dropdown may render below the fold.
    // The [title] attribute on AntD Select options is the only stable identifier — no data-testid available.
    await this.page.locator(`.ant-select-dropdown [title="${typeName}"]`).dispatchEvent('click');
  }

  async openPasswordDropdown(): Promise<void> {
    await this.passwordInput.clear();
    await this.passwordInput.press('ArrowDown');
  }

  async clickCreateSecretInline(): Promise<void> {
    await this.page.getByText('Create Secret').click();
  }

  async selectSecretForPasswordField(secretName: string): Promise<void> {
    // Type the secret name to filter the dropdown before selecting. This avoids
    // virtual-list scroll issues when many secrets exist from prior test runs.
    await this.passwordInput.clear();
    await this.passwordInput.fill(secretName);
    // Wait for the filtered list to render before clicking.
    await this.secretDropdownList.getByText(secretName, { exact: true }).waitFor({
      state: 'visible',
      timeout: 10000,
    });
    await this.secretDropdownList.getByText(secretName, { exact: true }).click({ force: true });
    await this.passwordInput.blur();
  }

  // ── Monaco editor helpers (used by managed ingestion) ─────────────────────

  async clearMonacoEditor(): Promise<void> {
    const modKey = process.platform === 'darwin' ? 'Meta' : 'Control';
    await this.monacoEditor.click();
    await this.page.keyboard.press(`${modKey}+a`);
    await this.page.keyboard.press('Backspace');
  }

  async typeInMonacoEditor(text: string): Promise<void> {
    await this.monacoEditor.click();
    await this.page.keyboard.type(text);
  }

  // Atomically replaces Monaco editor content: select-all then type in one sequence.
  // More reliable than clearMonacoEditor() + typeInMonacoEditor() under concurrent load
  // because there are no intermediate clicks that can steal focus between clear and type.
  async setMonacoEditorContent(content: string): Promise<void> {
    const modKey = process.platform === 'darwin' ? 'Meta' : 'Control';
    await this.monacoEditor.scrollIntoViewIfNeeded();
    await this.monacoEditor.click();
    await this.page.waitForTimeout(300);
    await this.page.keyboard.press(`${modKey}+a`);
    // Typing with the selection active replaces all content atomically
    await this.page.keyboard.type(content);
  }
}
