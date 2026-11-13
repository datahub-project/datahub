import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { TOAST_MESSAGES } from './constants';
import { WAIT_TIMEOUT } from '../../utils/constants';

export class ServiceAccountsPage extends BaseSettingsPage {
  private readonly createButton: Locator;
  private readonly displayNameInput: Locator;
  private readonly descriptionInput: Locator;
  private readonly submitButton: Locator;
  private readonly cancelCreateButton: Locator;
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
  private readonly tableRows: Locator;
  private readonly creationSuccessMessage: Locator;
  private readonly deletionSuccessMessage: Locator;
  private readonly accessTokenModal: Locator;

  constructor(page: Page, options?: PageOptions) {
    super(page, options);
    this.createButton = page.getByTestId('create-service-account-button');
    this.displayNameInput = page.getByTestId('service-account-display-name-input');
    this.descriptionInput = page.getByTestId('service-account-description-input');
    this.submitButton = page.getByTestId('create-service-account-submit-button');
    this.cancelCreateButton = page.getByTestId('create-service-account-cancel-button');
    this.deleteConfirmButton = page.getByTestId('delete-service-account-confirm-button');
    this.deleteCancelButton = page.getByTestId('delete-service-account-cancel-button');
    this.createTokenModal = page.getByTestId('create-token-modal');
    this.tokenNameInput = page.getByTestId('create-access-token-name');
    this.tokenDescInput = page.getByTestId('create-access-token-description');
    this.createTokenButton = page.getByTestId('create-access-token-button');
    this.tokenValue = page.getByTestId('access-token-value');
    this.closeTokenModalButton = page.getByTestId('access-token-modal-close-button');
    this.createTokenMenuItem = page.getByTestId('menu-item-create-token');
    this.deleteMenuItem = page.getByTestId('menu-item-delete');
    this.tableRows = page.getByTestId('service-accounts-table-container').getByRole('row');
    this.creationSuccessMessage = page.getByText(TOAST_MESSAGES.SERVICE_ACCOUNT_CREATED);
    this.deletionSuccessMessage = page.getByText(TOAST_MESSAGES.SERVICE_ACCOUNT_DELETED);
    this.accessTokenModal = page.getByText(TOAST_MESSAGES.NEW_ACCESS_TOKEN);
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/service-accounts');
    await this.page.waitForLoadState('networkidle');
    await expect(this.createButton).toBeVisible();
  }

  // ── Dynamic selectors for service accounts ───────────────────────────────
  // These helpers create locators based on runtime data (account names, etc.)
  private getAccountRow(accountName: string): Locator {
    return this.tableRows.filter({ hasText: accountName });
  }

  private getAccountMenuButton(accountName: string): Locator {
    const row = this.getAccountRow(accountName);
    return row.getByTestId(/service-account-menu-/);
  }

  private async openAccountMenu(accountName: string): Promise<void> {
    const menuButton = this.getAccountMenuButton(accountName);
    await menuButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await menuButton.click();
  }

  async createServiceAccount(name: string, description: string): Promise<string> {
    await this.createButton.click();
    await expect(this.submitButton).toBeVisible();
    await this.displayNameInput.fill(name);
    await this.descriptionInput.fill(description);
    await this.submitButton.click();
    await expect(this.creationSuccessMessage).toBeVisible();
    await this.page.waitForLoadState('networkidle');

    const serviceAccountId = await this.getServiceAccountIdFromRow(name);
    const serviceAccountUrn = `urn:li:serviceAccount:${serviceAccountId}`;
    this.cleanup?.track(serviceAccountUrn);
    return serviceAccountUrn;
  }

  private async getServiceAccountIdFromRow(accountName: string): Promise<string> {
    // Extract the service account ID from the menu button's data-testid attribute.
    // The ID is encoded in the test ID: service-account-menu-{accountId}
    // Fallback to a generated ID if the extraction fails.
    const menuButton = this.getAccountMenuButton(accountName);
    await menuButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    const testId = await menuButton.getAttribute('data-testid');
    if (testId && testId.includes('service-account-menu-')) {
      const id = testId.replace('service-account-menu-', '');
      if (id) return id;
    }
    return accountName.toLowerCase().replace(/\s+/g, '-');
  }

  async cancelCreateServiceAccount(name: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    const initialRowCount = await this.tableRows.count();
    await this.createButton.waitFor({ state: 'visible' });
    await this.createButton.click();
    await expect(this.submitButton).toBeVisible();
    await this.displayNameInput.fill(name);
    await this.cancelCreateButton.click();
    await expect(this.submitButton).toBeHidden();
    const finalRowCount = await this.tableRows.count();
    expect(finalRowCount).toBe(initialRowCount);
  }

  async generateToken(accountName: string, tokenName: string, tokenDescription: string): Promise<void> {
    const accountRow = this.getAccountRow(accountName);
    await expect(accountRow).toBeVisible();
    await this.openAccountMenu(accountName);
    await this.createTokenMenuItem.scrollIntoViewIfNeeded();
    await this.createTokenMenuItem.waitFor({ state: 'visible' });
    await this.createTokenMenuItem.click();
    await expect(this.tokenNameInput).toBeVisible();
    await this.tokenNameInput.fill(tokenName);
    await this.tokenDescInput.fill(tokenDescription);
    await this.createTokenButton.click();
    await expect(this.accessTokenModal).toBeVisible();
    await expect(this.tokenValue).toBeVisible();
    await this.closeTokenModalButton.click();
    await expect(this.createTokenModal).toBeHidden();
  }

  async deleteServiceAccount(accountName: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    const accountRow = this.getAccountRow(accountName);
    await expect(accountRow).toBeVisible({ timeout: WAIT_TIMEOUT });
    await this.openAccountMenu(accountName);
    await this.deleteMenuItem.waitFor({ state: 'visible' });
    await this.deleteMenuItem.click();
    await expect(this.deleteConfirmButton).toBeVisible();
    await this.deleteConfirmButton.click();
    await expect(this.deletionSuccessMessage).toBeVisible();
    await expect(accountRow).toBeHidden();
  }

  async cancelDeleteServiceAccount(accountName: string): Promise<void> {
    const accountRow = this.getAccountRow(accountName);
    await expect(accountRow).toBeVisible();
    await this.openAccountMenu(accountName);
    await this.deleteMenuItem.waitFor({ state: 'visible' });
    await this.deleteMenuItem.click();
    await expect(this.deleteConfirmButton).toBeVisible();
    await this.deleteCancelButton.click();
    await expect(this.deleteConfirmButton).toBeHidden();
    await expect(accountRow).toBeVisible();
  }
}
