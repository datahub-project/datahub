import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import type { DataHubLogger } from '../../utils/logger';

export class ServiceAccountsPage extends BaseSettingsPage {
  private readonly createButton: Locator;
  private readonly createModal: Locator;
  private readonly displayNameInput: Locator;
  private readonly descriptionInput: Locator;
  private readonly submitButton: Locator;
  private readonly cancelCreateButton: Locator;
  private readonly deleteModal: Locator;
  private readonly deleteConfirmButton: Locator;
  private readonly deleteCancelButton: Locator;
  private readonly createTokenModal: Locator;
  private readonly tokenNameInput: Locator;
  private readonly tokenDescInput: Locator;
  private readonly createTokenButton: Locator;
  private readonly tokenValue: Locator;
  private readonly closeTokenModalButton: Locator;
  private readonly createTokenMenuItem: Locator;
  private readonly deleteMenuItem: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.createButton = page.locator('[data-testid="create-service-account-button"]');
    this.createModal = page.locator('[data-testid="create-service-account-modal"]');
    this.displayNameInput = page.locator('[data-testid="service-account-display-name-input"]');
    this.descriptionInput = page.locator('[data-testid="service-account-description-input"]');
    this.submitButton = page.locator('[data-testid="create-service-account-submit-button"]');
    this.cancelCreateButton = page.locator('[data-testid="create-service-account-cancel-button"]');
    this.deleteModal = page.locator('[data-testid="delete-service-account-modal"]');
    this.deleteConfirmButton = page.locator('[data-testid="delete-service-account-confirm-button"]');
    this.deleteCancelButton = page.locator('[data-testid="delete-service-account-cancel-button"]');
    this.createTokenModal = page.locator('[data-testid="create-token-modal"]');
    this.tokenNameInput = page.locator('[data-testid="create-access-token-name"]');
    this.tokenDescInput = page.locator('[data-testid="create-access-token-description"]');
    this.createTokenButton = page.locator('[data-testid="create-access-token-button"]');
    this.tokenValue = page.locator('[data-testid="access-token-value"]');
    this.closeTokenModalButton = page.locator('[data-testid="access-token-modal-close-button"]');
    this.createTokenMenuItem = page.locator('[data-testid="menu-item-create-token"]');
    this.deleteMenuItem = page.locator('[data-testid="menu-item-delete"]');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/service-accounts');
    await expect(this.createButton).toBeVisible({ timeout: 15000 });
  }

  private async openAccountMenu(accountName: string): Promise<void> {
    await this.page
      .locator('tr')
      .filter({ hasText: accountName })
      .locator('[data-testid^="service-account-menu-"]')
      .click();
  }

  async createServiceAccount(name: string, description: string): Promise<void> {
    await this.createButton.click();
    // Wait for the modal's submit button to be visible — the modal root div is always
    // in the DOM so checking the root element's visibility does not work.
    await expect(this.submitButton).toBeVisible({ timeout: 10000 });
    await this.displayNameInput.fill(name);
    await this.descriptionInput.fill(description);
    await this.submitButton.click();
    await expect(this.page.getByText('Service account created successfully!')).toBeVisible({ timeout: 15000 });
    await expect(this.page.locator('tbody').getByText(name).first()).toBeVisible({ timeout: 15000 });
  }

  async cancelCreateServiceAccount(name: string): Promise<void> {
    await this.createButton.waitFor({ state: 'visible', timeout: 10000 });
    await this.createButton.click();
    await expect(this.submitButton).toBeVisible({ timeout: 10000 });
    await this.displayNameInput.fill(name);
    await this.cancelCreateButton.click();
    await expect(this.submitButton).not.toBeVisible({ timeout: 10000 });
    await expect(this.page.getByText(name)).not.toBeVisible({ timeout: 10000 });
  }

  async generateToken(accountName: string, tokenName: string, tokenDescription: string): Promise<void> {
    await this.openAccountMenu(accountName);
    await this.createTokenMenuItem.click({ force: true });
    await expect(this.tokenNameInput).toBeVisible({ timeout: 10000 });
    await this.tokenNameInput.fill(tokenName);
    await this.tokenDescInput.fill(tokenDescription);
    await this.createTokenButton.click();
    await expect(this.page.getByText('New Access Token')).toBeVisible({ timeout: 15000 });
    await expect(this.tokenValue).toBeVisible();
    await this.closeTokenModalButton.click();
  }

  async deleteServiceAccount(accountName: string): Promise<void> {
    await this.openAccountMenu(accountName);
    await this.deleteMenuItem.waitFor({ state: 'visible', timeout: 5000 });
    await this.deleteMenuItem.click();
    await expect(this.deleteConfirmButton).toBeVisible({ timeout: 10000 });
    await this.deleteConfirmButton.click();
    await expect(this.page.getByText('Service account deleted')).toBeVisible({ timeout: 15000 });
    await expect(this.page.locator('tbody').getByText(accountName).first()).not.toBeVisible({ timeout: 15000 });
  }

  async cancelDeleteServiceAccount(accountName: string): Promise<void> {
    await this.openAccountMenu(accountName);
    await this.deleteMenuItem.waitFor({ state: 'visible', timeout: 5000 });
    await this.deleteMenuItem.click();
    await expect(this.deleteConfirmButton).toBeVisible({ timeout: 10000 });
    await this.deleteCancelButton.click();
    await expect(this.deleteConfirmButton).not.toBeVisible({ timeout: 10000 });
    await expect(this.page.locator('tbody').getByText(accountName).first()).toBeVisible({ timeout: 15000 });
  }
}
