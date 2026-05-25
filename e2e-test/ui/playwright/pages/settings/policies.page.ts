import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import { WAIT_TIMEOUT, SHORT_TIMEOUT, TOAST_MESSAGES } from '../../tests/settings-v2/constants';
import type { DataHubLogger } from '../../utils/logger';

export class PoliciesPage extends BaseSettingsPage {
  readonly searchInput: Locator;
  readonly tableBody: Locator;
  private readonly managePage: Locator;
  private readonly addPolicyButton: Locator;
  private readonly policyNameInput: Locator;
  private readonly policyDescInput: Locator;
  private readonly privilegesInput: Locator;
  private readonly usersInput: Locator;
  private readonly groupsInput: Locator;
  private readonly nextButton: Locator;
  private readonly saveButton: Locator;
  private readonly policyFilterBase: Locator;
  private readonly policyFilterAll: Locator;
  private readonly menuItems: Locator;
  private readonly confirmButton: Locator;
  private readonly policyTypeSelector: Locator;
  private readonly metadataTypeButton: Locator;
  private readonly platformTypeButton: Locator;
  private readonly tableRows: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.getByTestId('search-bar-input');
    this.tableBody = page.getByTestId('policies-table-body');
    this.tableRows = this.tableBody.locator('tr');
    this.managePage = page.getByTestId('manage-permissions-page');
    this.addPolicyButton = page.getByTestId('add-policy-button');
    this.policyNameInput = page.getByTestId('policy-name');
    this.policyDescInput = page.getByTestId('policy-description');
    this.privilegesInput = page.getByTestId('privileges');
    this.usersInput = page.getByTestId('users');
    this.groupsInput = page.getByTestId('groups');
    this.nextButton = page.getByTestId('next-button');
    this.saveButton = page.getByTestId('save-button');
    this.policyFilterBase = page.getByTestId('policy-filter-base');
    this.policyFilterAll = page.getByTestId('option-ALL');
    this.menuItems = page.locator('[data-testid^="menu-item-"]');
    this.policyTypeSelector = page.getByTestId('policy-type');
    this.metadataTypeButton = this.policyTypeSelector.locator('[title="Metadata"]');
    this.platformTypeButton = page.getByTestId('platform');
    // Policy deletion uses Ant Design's Modal.confirm, which renders a plain "Yes" button
    this.confirmButton = page.getByRole('dialog').getByRole('button', { name: 'Yes' });
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/permissions/policies');
    await this.skipOnboarding();
    await this.page.keyboard.press('Escape');
    await this.waitForReady();
  }

  async waitForReady(): Promise<void> {
    await expect(this.managePage).toBeVisible();
    await expect(this.searchInput).toBeVisible();
  }

  async searchForPolicy(policyName: string): Promise<void> {
    await this.searchInput.clear();
    await this.searchInput.fill(policyName);
    await this.page.waitForLoadState('networkidle').catch(() => {});
  }

  async openRowMenu(policyName: string): Promise<void> {
    const row = this.tableRows.filter({ has: this.page.getByText(policyName, { exact: true }) });
    const menuButton = row.getByTestId('policy-row-menu-button');
    await menuButton.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await menuButton.click({ force: true });
    await expect(this.menuItems.first()).toBeVisible();
  }

  async clickMenuAction(actionText: string): Promise<void> {
    const item = this.menuItems.getByText(actionText);
    await expect(item).toBeVisible();
    await item.click();
    await this.page.waitForLoadState('networkidle').catch(() => {});
  }

  async openNewPolicyWizard(): Promise<void> {
    await this.addPolicyButton.click();
    await expect(this.policyNameInput).toBeVisible();
    await this.page.waitForLoadState('networkidle').catch(() => {});
  }

  async fillPolicyName(name: string): Promise<void> {
    await this.policyNameInput.click();
    await this.policyNameInput.clear();
    await this.policyNameInput.fill(name);
  }

  async verifyDefaultPolicyType(type: string): Promise<void> {
    await expect(this.policyTypeSelector).toHaveText(type);
  }

  async selectPlatformType(): Promise<void> {
    await this.policyTypeSelector.waitFor({ state: 'visible' });
    const metadata = await this.metadataTypeButton.isVisible().catch(() => false);
    if (metadata) {
      await this.metadataTypeButton.click();
    }
    await this.platformTypeButton.waitFor({ state: 'visible' });
    await this.platformTypeButton.click();
    await this.page.waitForLoadState('networkidle').catch(() => {});
  }

  async fillAndSaveWizard(description: string, policyName: string): Promise<void> {
    await expect(this.policyDescInput).toBeVisible();
    await this.policyDescInput.clear();
    await this.policyDescInput.fill(description);
    await this.nextButton.waitFor({ state: 'visible' });
    await this.nextButton.click();

    await expect(this.privilegesInput).toBeVisible();
    await this.privilegesInput.click();
    await this.page.keyboard.type('All');
    const allPrivilegesOption = this.page.getByTestId('option-all-privileges');
    await allPrivilegesOption.waitFor({ state: 'visible', timeout: 5000 });
    await allPrivilegesOption.click();
    await this.nextButton.waitFor({ state: 'visible' });
    await this.nextButton.click();

    await expect(this.usersInput).toBeVisible();
    await this.usersInput.click();
    await this.page.keyboard.type('All');
    const allUsersOption = this.page.getByTestId('option-all-users');
    await allUsersOption.waitFor({ state: 'visible', timeout: 5000 });
    await allUsersOption.click();
    await this.groupsInput.click();
    await this.page.keyboard.type('All');
    const allGroupsOption = this.page.getByTestId('option-all-groups');
    await allGroupsOption.waitFor({ state: 'visible', timeout: 5000 });
    await allGroupsOption.click();
    await this.saveButton.waitFor({ state: 'visible' });
    await this.saveButton.click();

    await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_SAVED_POLICY);
    await this.searchForPolicy(policyName);
    const policyRow = this.tableRows.filter({ has: this.page.getByText(policyName, { exact: true }) });
    await expect(policyRow).toBeVisible();
  }

  async editPolicy(name: string, newName: string, description: string): Promise<void> {
    await this.navigate();
    await this.searchForPolicy(name);
    await this.openRowMenu(name);
    await this.clickMenuAction('Edit');

    await expect(this.policyNameInput).toBeVisible();
    await this.policyNameInput.clear();
    await this.policyNameInput.fill(newName);

    await expect(this.policyDescInput).toBeVisible();
    await this.policyDescInput.clear();
    await this.policyDescInput.fill(description);

    await this.nextButton.waitFor({ state: 'visible' });
    await this.nextButton.click();
    await this.nextButton.waitFor({ state: 'visible' });
    await this.nextButton.click();
    await this.saveButton.waitFor({ state: 'visible' });
    await this.saveButton.click();

    await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_SAVED_POLICY);
    await this.searchForPolicy(newName);
    const editedPolicyRow = this.tableRows.filter({ has: this.page.getByText(newName, { exact: true }) });
    await editedPolicyRow.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
  }

  async deletePolicy(name: string, deleteDialogTitle: string): Promise<void> {
    await this.navigate();

    await expect(this.policyFilterBase).toBeVisible();
    await this.policyFilterBase.click();
    await expect(this.policyFilterAll).toBeVisible();
    await this.policyFilterAll.click();

    await this.searchForPolicy(name);
    await this.openRowMenu(name);
    await this.clickMenuAction('Deactivate');
    await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_DEACTIVATED_POLICY);

    await this.searchForPolicy(name);
    await this.openRowMenu(name);
    await this.clickMenuAction('Activate');
    await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_ACTIVATED_POLICY);

    await this.openRowMenu(name);
    await this.clickMenuAction('Delete');
    await expect(this.page.getByText(deleteDialogTitle)).toBeVisible();
    await this.confirmButton.click();
    await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_REMOVED_POLICY);
    const deletedPolicyRow = this.tableRows.filter({ has: this.page.getByText(name, { exact: true }) });
    await expect(deletedPolicyRow).toBeHidden();
  }

  async deactivateExistingAllUserPolicies(): Promise<void> {
    await expect(this.tableRows).toBeVisible();
    const allUsersRows = this.tableBody.locator('tr:has-text("All Users")');
    const count = await allUsersRows.count();
    for (let i = 0; i < count; i++) {
      const row = allUsersRows.nth(i);
      const menuButton = row.getByTestId('policy-row-menu-button');
      await menuButton.click({ force: true });
      const deactivateItem = this.menuItems.getByText('Deactivate');
      if ((await deactivateItem.count()) > 0) {
        await deactivateItem.click();
        await expect(this.page.getByText('Successfully deactivated policy.')).toBeVisible();
      }
    }
  }
}
