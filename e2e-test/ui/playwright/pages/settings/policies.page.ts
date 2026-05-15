import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
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

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.locator('[data-testid="search-bar-input"]');
    this.tableBody = page.locator('tbody');
    this.managePage = page.locator('[data-testid="manage-permissions-page"]');
    this.addPolicyButton = page.locator('[data-testid="add-policy-button"]');
    this.policyNameInput = page.locator('[data-testid="policy-name"]');
    this.policyDescInput = page.locator('[data-testid="policy-description"]');
    this.privilegesInput = page.locator('[data-testid="privileges"]');
    this.usersInput = page.locator('[data-testid="users"]');
    this.groupsInput = page.locator('[data-testid="groups"]');
    this.nextButton = page.locator('#nextButton');
    this.saveButton = page.locator('#saveButton');
    this.policyFilterBase = page.locator('[data-testid="policy-filter-base"]');
    this.policyFilterAll = page.locator('[data-testid="option-ALL"]');
    this.menuItems = page.locator('[data-testid^="menu-item-"]');
    // Policy deletion uses Ant Design's Modal.confirm, which renders a plain "Yes" button
    // (not our ConfirmationModal with data-testid="modal-confirm-button").
    this.confirmButton = page.getByRole('dialog').getByRole('button', { name: 'Yes' });
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/permissions/policies');
    await this.waitForReady();
  }

  async waitForReady(): Promise<void> {
    await expect(this.managePage).toBeVisible({ timeout: 15000 });
    await expect(this.searchInput).toBeVisible({ timeout: 15000 });
    // Wait for either table rows or the empty-state message — either means the
    // policies query has completed and the page is interactive.
    await Promise.race([
      expect(this.tableBody.locator('tr:first-child')).toBeVisible({ timeout: 15000 }),
      expect(this.page.getByText('No Policies!')).toBeVisible({ timeout: 15000 }),
    ]);
  }

  async searchForPolicy(policyName: string): Promise<void> {
    await this.searchInput.clear();
    await this.searchInput.fill(policyName);
  }

  async openRowMenu(policyName: string): Promise<void> {
    await this.page
      .locator('tr')
      .filter({ hasText: policyName })
      .locator('td:last-child button')
      .click({ force: true });
    await expect(this.menuItems.first()).toBeVisible();
  }

  async clickMenuAction(actionText: string): Promise<void> {
    const item = this.menuItems.getByText(actionText);
    await item.waitFor({ state: 'visible', timeout: 10000 });
    await item.click();
  }

  async openNewPolicyWizard(): Promise<void> {
    await expect(this.addPolicyButton).toBeVisible();
    await this.addPolicyButton.click();
    await expect(this.policyNameInput).toBeVisible();
  }

  async fillPolicyName(name: string): Promise<void> {
    await this.policyNameInput.click();
    await this.policyNameInput.clear();
    await this.policyNameInput.fill(name);
  }

  async verifyDefaultPolicyType(type: string): Promise<void> {
    await expect(this.page.locator('[data-testid="policy-type"]')).toHaveText(type);
  }

  /** Switches the policy type from Metadata (default) to Platform. */
  async selectPlatformType(): Promise<void> {
    await this.page.locator('[data-testid="policy-type"] [title="Metadata"]').click();
    await this.page.locator('[data-testid="platform"]').click();
  }

  /**
   * Completes wizard steps 1–3 and saves. Call after fillPolicyName (and
   * selectPlatformType if needed) since those are set before the wizard steps.
   */
  async fillAndSaveWizard(description: string, policyName: string): Promise<void> {
    // Step 1: description
    await expect(this.policyDescInput).toBeVisible();
    await this.policyDescInput.click();
    await this.policyDescInput.clear();
    await this.policyDescInput.fill(description);
    await this.nextButton.click();

    // Step 2: privileges — Ant Design Select: click container then type in inner input
    await this.privilegesInput.click();
    await this.privilegesInput.locator('input').fill('All');
    await this.page.locator('.rc-virtual-list').getByText('All Privileges').click({ force: true });
    await this.nextButton.click();

    // Step 3: users & groups — same Ant Design Select pattern
    await this.usersInput.click();
    await this.usersInput.locator('input').fill('All');
    await this.page.locator('.rc-virtual-list').getByText('All Users').click({ force: true });
    await this.groupsInput.click();
    await this.groupsInput.locator('input').fill('All');
    await this.page.locator('.rc-virtual-list').getByText('All Groups').click({ force: true });
    await this.saveButton.click();

    await this.waitForToast('Successfully saved policy.');
    await this.searchForPolicy(policyName);
    await expect(this.tableBody.getByText(policyName)).toBeVisible({ timeout: 15000 });
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

    await this.nextButton.click();
    await this.nextButton.click();
    await this.saveButton.click();

    await this.waitForToast('Successfully saved policy.');
    await expect(this.page.getByText(newName)).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(description)).toBeVisible({ timeout: 15000 });
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
    await this.waitForToast('Successfully deactivated policy.');

    await this.openRowMenu(name);
    await this.clickMenuAction('Activate');
    await this.waitForToast('Successfully activated policy.');

    await this.openRowMenu(name);
    await this.clickMenuAction('Delete');
    await expect(this.page.getByText(deleteDialogTitle)).toBeVisible({ timeout: 15000 });
    await this.confirmButton.click();
    await this.waitForToast('Successfully removed policy.');
    await expect(this.page.getByText(name)).not.toBeVisible({ timeout: 15000 });
  }

  async deactivateExistingAllUserPolicies(): Promise<void> {
    await expect(this.tableBody.locator('tr td')).toBeVisible();
    const rows = this.tableBody.locator('tr');
    const count = await rows.count();
    for (let i = 0; i < count; i++) {
      const row = rows.nth(i);
      const roleText = await row.locator('td').nth(3).textContent();
      if (roleText?.includes('All Users')) {
        await row.locator('td:last-child button').click({ force: true });
        const deactivateItem = this.menuItems.getByText('Deactivate');
        if ((await deactivateItem.count()) > 0) {
          await deactivateItem.click();
          await expect(this.page.getByText('Successfully deactivated policy.')).toBeVisible({ timeout: 15000 });
        }
      }
    }
  }
}
