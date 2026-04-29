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
  readonly sourcesTab: Locator;
  readonly secretsTab: Locator;
  readonly createSourceButton: Locator;
  readonly createSecretButton: Locator;
  readonly searchInput: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    // Tab keys use data-node-key attribute on ant-tabs
    this.sourcesTab = page.locator('[data-node-key="Sources"]');
    this.secretsTab = page.locator('[data-node-key="Secrets"]');
    this.createSourceButton = page.locator('[data-testid="create-ingestion-source-button"]');
    this.createSecretButton = page.locator('[data-testid="create-secret-button"]');
    this.searchInput = page.locator('[data-testid="search-bar-input"]');
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
    await this.searchInput.fill(name);
    await this.page.waitForTimeout(500);
  }

  // ── Snowflake source creation wizard helpers ────────────────────────────────

  async searchDataSource(query: string): Promise<void> {
    await this.page.locator('[placeholder="Search data sources..."]').fill(query);
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
    await expect(this.page.locator('#account_id')).toBeVisible({ timeout: 15000 });
    await this.page.locator('#account_id').fill(params.accountId);
    await this.page.locator('#warehouse').fill(params.warehouseId);
    await this.page.locator('#username').fill(params.username);

    if (params.password !== undefined) {
      // Select Username & Password auth type to expose password field
      await this.page.locator('#authentication_type').click({ force: true });
      await this.page.locator('.ant-select-dropdown [title="Username & Password"]').click();
      await expect(this.page.locator('#password')).toBeVisible({ timeout: 5000 });
      await this.page.locator('#password').fill(params.password);
      await this.page.locator('#password').blur();
    }

    await this.page.locator('#role').fill(params.role);
  }

  async clickRecipeYamlButton(): Promise<void> {
    await this.page.locator('[data-testid="recipe-builder-yaml-button"]').click();
  }

  async clickRecipeNextButton(): Promise<void> {
    await this.page.locator('[data-testid="recipe-builder-next-button"]').click();
  }

  async clickScheduleNextButton(): Promise<void> {
    await this.page.locator('[data-testid="ingestion-schedule-next-button"]').click();
    await expect(this.page.locator('.ant-collapse-item')).toBeVisible({ timeout: 15000 });
  }

  async fillSourceName(name: string): Promise<void> {
    await this.page.locator('[data-testid="source-name-input"]').fill(name);
  }

  async clickSaveButton(): Promise<void> {
    await this.page.locator('[data-testid="ingestion-source-save-button"]').click();
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
    await expect(this.page.locator('#account_id')).toHaveValue(params.accountId, { timeout: 15000 });
    await expect(this.page.locator('#warehouse')).toHaveValue(params.warehouseId);
    await expect(this.page.locator('#username')).toHaveValue(params.username);
    await expect(
      this.page.locator('#authentication_type').locator('xpath=ancestor::*[contains(@class,"ant-form-item")][1]'),
    ).toContainText('Username & Password');
    await expect(this.page.locator('#password')).toHaveValue(params.password);
    await expect(this.page.locator('#role')).toHaveValue(params.role);
  }

  // ── Verification helpers ──────────────────────────────────────────────────

  async expectSourceVisible(name: string): Promise<void> {
    await expect(this.page.locator('tr').filter({ hasText: name })).toBeVisible({ timeout: 30000 });
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
    await this.page.locator('tr').filter({ hasText: sourceName }).locator('[data-icon="delete"]').first().click();
    await expect(this.page.getByText('Confirm Ingestion Source Removal')).toBeVisible();
    await this.page.getByRole('button', { name: 'Yes' }).click();
    await expect(this.page.getByText('Removed ingestion source.')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(sourceName)).not.toBeVisible({ timeout: 15000 });
  }

  // ── Secrets helpers ─────────────────────────────────────────────────────────

  async fillAndSubmitSecretModal(name: string, value: string, description: string): Promise<void> {
    await this.page.locator('[data-testid="secret-modal-name-input"] input').fill(name);
    await this.page.locator('[data-testid="secret-modal-value-input"] textarea').fill(value);
    await this.page.locator('[data-testid="secret-modal-description-input"] textarea').fill(description);
    await this.page.locator('[data-testid="secret-modal-create-button"]').click();
  }

  async createSecret(name: string, value: string, description: string): Promise<void> {
    await this.clickCreateSecretButton();
    await this.fillAndSubmitSecretModal(name, value, description);
  }

  async deleteSecret(name: string): Promise<void> {
    await this.page.locator('[data-icon="delete"]').first().click();
    await expect(this.page.getByText('Confirm Secret Removal')).toBeVisible();
    await this.page.getByRole('button', { name: 'Yes' }).click();
    await expect(this.page.getByText('Removed secret.')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(name)).not.toBeVisible({ timeout: 15000 });
  }

  async selectAuthType(typeName: string): Promise<void> {
    await this.page.locator('#authentication_type').click({ force: true });
    // dispatchEvent bypasses Playwright's viewport check; AntD dropdown may render below the fold
    await this.page.locator(`.ant-select-dropdown [title="${typeName}"]`).dispatchEvent('click');
  }

  async openPasswordDropdown(): Promise<void> {
    await this.page.locator('#password').clear();
    await this.page.locator('#password').press('ArrowDown');
  }

  async clickCreateSecretInline(): Promise<void> {
    await this.page.getByText('Create Secret').click();
  }

  async selectSecretForPasswordField(secretName: string): Promise<void> {
    await this.openPasswordDropdown();
    await this.page.locator('.rc-virtual-list-holder-inner').getByText(secretName).click({ force: true });
    await this.page.locator('#password').blur();
  }

  // ── Monaco editor helpers (used by managed ingestion) ─────────────────────

  async clearMonacoEditor(): Promise<void> {
    const modKey = process.platform === 'darwin' ? 'Meta' : 'Control';
    await this.page.locator('.monaco-scrollable-element').first().click();
    await this.page.keyboard.press(`${modKey}+a`);
    await this.page.keyboard.press('Backspace');
  }

  async typeInMonacoEditor(text: string): Promise<void> {
    await this.page.locator('.monaco-scrollable-element').first().click();
    await this.page.keyboard.type(text);
  }

  // Atomically replaces Monaco editor content: select-all then type in one sequence.
  // More reliable than clearMonacoEditor() + typeInMonacoEditor() under concurrent load
  // because there are no intermediate clicks that can steal focus between clear and type.
  async setMonacoEditorContent(content: string): Promise<void> {
    const modKey = process.platform === 'darwin' ? 'Meta' : 'Control';
    const monacoEl = this.page.locator('.monaco-scrollable-element').first();
    await monacoEl.scrollIntoViewIfNeeded();
    await monacoEl.click();
    await this.page.waitForTimeout(300);
    await this.page.keyboard.press(`${modKey}+a`);
    // Typing with the selection active replaces all content atomically
    await this.page.keyboard.type(content);
  }
}
