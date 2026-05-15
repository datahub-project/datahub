import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
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
  private readonly revokeTokenButton: Locator;
  private readonly confirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.pageContainer = page.locator('[data-testid="manage-access-tokens-page"]');
    this.addTokenButton = page.locator('[data-testid="add-token-button"]');
    this.personalTokenMenuItem = page.locator('[data-testid="menu-item-personal"]');
    this.tokenNameInput = page.locator('[data-testid="create-access-token-name"]');
    this.tokenDescInput = page.locator('[data-testid="create-access-token-description"]');
    this.createTokenButton = page.locator('[data-testid="create-access-token-button"]');
    this.tokenValue = page.locator('[data-testid="access-token-value"]');
    this.closeModalButton = page.locator('[data-testid="access-token-modal-close-button"]');
    this.revokeTokenButton = page.locator('[data-testid="revoke-token-button"]');
    this.confirmButton = page.locator('[data-testid="modal-confirm-button"]');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/tokens');
    await expect(this.pageContainer).toBeVisible({ timeout: 15000 });
  }

  async createPersonalToken(name: string, description: string): Promise<void> {
    await this.addTokenButton.click();
    await this.personalTokenMenuItem.click();
    await this.tokenNameInput.fill(name);
    await this.tokenDescInput.fill(description);
    await this.createTokenButton.click();
    await expect(this.page.getByText('New Access Token')).toBeVisible({ timeout: 15000 });
  }

  async getTokenValue(): Promise<string> {
    await expect(this.tokenValue).toBeVisible();
    return (await this.tokenValue.textContent()) ?? '';
  }

  async closeTokenModal(): Promise<void> {
    await this.closeModalButton.click();
  }

  async revokeToken(tokenName: string): Promise<void> {
    await this.page
      .locator('tr')
      .filter({ hasText: tokenName })
      .locator('[data-testid="revoke-token-button"]')
      .click();
    await expect(this.page.getByText('Are you sure you want to revoke this token?')).toBeVisible({ timeout: 10000 });
    await this.confirmButton.click();
  }

  async verifyTokenInList(name: string, description: string): Promise<void> {
    await expect(this.page.getByText(name)).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(description)).toBeVisible({ timeout: 15000 });
  }

  async verifyTokenRemoved(name: string, description: string): Promise<void> {
    await expect(this.page.getByText(name)).not.toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(description)).not.toBeVisible({ timeout: 15000 });
  }
}
