import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { TOAST_MESSAGES } from './constants';
import { WAIT_TIMEOUT, SHORT_TIMEOUT } from '../../utils/constants';

export class PoliciesPage extends BaseSettingsPage {
  private readonly searchInput: Locator;
  private readonly tableBody: Locator;
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
  private readonly confirmButton: Locator;
  private readonly policyTypeSelector: Locator;
  private readonly metadataTypeButton: Locator;
  private readonly platformTypeButton: Locator;
  private readonly tableRows: Locator;
  private readonly allPrivilegesOption: Locator;
  private readonly allUsersOption: Locator;
  private readonly allGroupsOption: Locator;
  private readonly allUsersRows: Locator;

  constructor(page: Page, options?: PageOptions) {
    super(page, options);
    this.searchInput = page.getByTestId('search-bar-input');
    this.tableBody = page.getByTestId('policies-table-body');
    this.tableRows = this.tableBody.getByRole('row');
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
    this.policyTypeSelector = page.getByTestId('policy-type');
    this.metadataTypeButton = this.policyTypeSelector.getByTitle('Metadata');
    this.platformTypeButton = page.getByTestId('platform');
    this.allPrivilegesOption = page.getByTestId('option-all-privileges');
    this.allUsersOption = page.getByTestId('option-all-users');
    this.allGroupsOption = page.getByTestId('option-all-groups');
    this.allUsersRows = this.tableBody.getByRole('row').filter({ hasText: 'All Users' });
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

  // ── Dynamic selectors for policies ───────────────────────────────────────
  // These helpers create locators based on runtime data (policy names, etc.)
  private getPolicyRow(policyName: string): Locator {
    return this.tableRows.filter({ has: this.page.getByText(policyName, { exact: true }) });
  }

  private getPolicyRowMenuButton(policyName: string): Locator {
    return this.getPolicyRow(policyName).getByTestId('policy-row-menu-button');
  }

  private getMenuItems(): Locator {
    return this.page.getByRole('menuitem');
  }

  private async clickMenuButton(menuButton: Locator): Promise<void> {
    await menuButton.waitFor({ state: 'attached', timeout: SHORT_TIMEOUT });
    await menuButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async searchForPolicy(policyName: string): Promise<void> {
    await this.searchInput.clear();
    await this.searchInput.fill(policyName);
  }

  async openRowMenu(policyName: string): Promise<void> {
    const menuButton = this.getPolicyRowMenuButton(policyName);
    await this.clickMenuButton(menuButton);
  }

  async clickMenuAction(actionText: string): Promise<void> {
    const item = this.getMenuItems().getByText(actionText);
    await expect(item).toBeVisible();
    await item.click();
  }

  async openNewPolicyWizard(): Promise<void> {
    await this.addPolicyButton.click();
    await expect(this.policyNameInput).toBeVisible();
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
    if (await this.metadataTypeButton.isVisible({ timeout: SHORT_TIMEOUT })) {
      await this.metadataTypeButton.click();
    }
    await this.platformTypeButton.waitFor({ state: 'visible' });
    await this.platformTypeButton.click();
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
    await this.allPrivilegesOption.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.allPrivilegesOption.click();
    await this.nextButton.waitFor({ state: 'visible' });
    await this.nextButton.click();

    await expect(this.usersInput).toBeVisible();
    await this.usersInput.click();
    await this.page.keyboard.type('All');
    await this.allUsersOption.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.allUsersOption.click();
    await this.groupsInput.click();
    await this.page.keyboard.type('All');
    await this.allGroupsOption.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.allGroupsOption.click();
    await this.saveButton.waitFor({ state: 'visible' });
    await this.saveButton.click();

    await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_SAVED_POLICY);
    await this.searchForPolicy(policyName);
    await expect(this.getPolicyRow(policyName)).toBeVisible();
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
    await this.getPolicyRow(newName).waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
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
    await expect(this.getPolicyRow(name)).toBeHidden();
  }

  async deactivateExistingAllUserPolicies(): Promise<void> {
    await expect(this.tableRows).toBeVisible();
    const rows = await this.allUsersRows.all();
    for (const row of rows) {
      const menuButton = row.getByTestId('policy-row-menu-button');
      await this.clickMenuButton(menuButton);
      const deactivateItem = this.getMenuItems().getByText('Deactivate');
      if ((await deactivateItem.count()) > 0) {
        await deactivateItem.click();
        await this.waitForToast(TOAST_MESSAGES.SUCCESSFULLY_DEACTIVATED_POLICY);
      }
    }
  }
}
