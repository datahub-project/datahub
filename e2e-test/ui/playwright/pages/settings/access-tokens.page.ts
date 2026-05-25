import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import { WAIT_TIMEOUT, TABLE_LOAD_DELAY, TOAST_MESSAGES } from '../../tests/settings-v2/constants';
import type { DataHubLogger } from '../../utils/logger';

export class AccessTokensPage extends BaseSettingsPage {
  private readonly pageContainer: Locator;
  private readonly addTokenButton: Locator;
  private readonly personalTokenMenuItem: Locator;
  private readonly tokenNameInput: Locator;
  private readonly tokenDescInput: Locator;
  private readonly createTokenButton: Locator;
  private readonly tokenValue: Locator;
  private readonly closeModalButton: Locator;
  private readonly confirmButton: Locator;
  private readonly tableBody: Locator;
  private readonly tableRows: Locator;
  private readonly newAccessTokenMessage: Locator;
  private readonly revokeConfirmMessage: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.pageContainer = page.getByTestId('manage-access-tokens-page');
    this.addTokenButton = page.getByTestId('add-token-button');
    this.personalTokenMenuItem = page.getByTestId('menu-item-personal');
    this.tokenNameInput = page.getByTestId('create-access-token-name');
    this.tokenDescInput = page.getByTestId('create-access-token-description');
    this.createTokenButton = page.getByTestId('create-access-token-button');
    this.tokenValue = page.getByTestId('access-token-value');
    this.closeModalButton = page.getByTestId('access-token-modal-close-button');
    this.confirmButton = page.getByTestId('modal-confirm-button');
    this.tableBody = page.locator('tbody');
    this.tableRows = this.tableBody.locator('tr');
    this.newAccessTokenMessage = page.getByText(TOAST_MESSAGES.NEW_ACCESS_TOKEN);
    this.revokeConfirmMessage = page.getByText(TOAST_MESSAGES.ARE_YOU_SURE_REVOKE_TOKEN);
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/tokens');
    await this.page.waitForLoadState('networkidle').catch(() => {});
    await expect(this.pageContainer).toBeVisible();
  }

  async reloadPage(): Promise<void> {
    await this.page.reload();
    await this.page.waitForLoadState('networkidle').catch(() => {});
    await expect(this.pageContainer).toBeVisible({ timeout: 10000 });
  }

  async createPersonalToken(name: string, description: string): Promise<void> {
    await this.addTokenButton.click();
    await this.personalTokenMenuItem.click();
    await this.tokenNameInput.fill(name);
    await this.tokenDescInput.fill(description);
    await this.createTokenButton.click();
    await expect(this.newAccessTokenMessage).toBeVisible();
  }

  async getTokenValue(): Promise<string> {
    await expect(this.tokenValue).toBeVisible();
    return (await this.tokenValue.textContent()) ?? '';
  }

  async closeTokenModal(): Promise<void> {
    await this.closeModalButton.click();
    await expect(this.closeModalButton).toBeHidden();
    await expect(this.tableBody).toBeAttached();
    await this.page.waitForLoadState('networkidle').catch(() => {});
  }

  async revokeToken(tokenName: string): Promise<void> {
    const tokenRow = this.tableRows.filter({ hasText: tokenName });
    const revokeButton = tokenRow.getByTestId('revoke-token-button');
    await revokeButton.click();
    await expect(this.revokeConfirmMessage).toBeVisible();
    await this.confirmButton.click();
  }

  async verifyTokenInList(name: string, description: string): Promise<void> {
    await expect(this.pageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });
    await this.page.waitForLoadState('networkidle').catch(() => {});
    await expect(this.tableBody).toBeVisible({ timeout: WAIT_TIMEOUT });
    await this.page.waitForTimeout(TABLE_LOAD_DELAY);
    await expect(this.page.getByText(name)).toBeVisible({ timeout: WAIT_TIMEOUT });
    await expect(this.page.getByText(description)).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  async verifyTokenRemoved(name: string, description: string): Promise<void> {
    await expect(this.page.getByText(name)).toBeHidden();
    await expect(this.page.getByText(description)).toBeHidden();
  }
}
