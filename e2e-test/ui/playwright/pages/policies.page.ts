import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class PoliciesPage extends BasePage {
  readonly searchInput: Locator;
  readonly tableBody: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.getByTestId('search-bar-input');
    this.tableBody = page.getByRole('rowgroup');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/permissions/policies');
    await expect(this.page.getByText('Manage Permissions')).toBeVisible({ timeout: 15000 });
  }

  async searchForPolicy(policyName: string): Promise<void> {
    await expect(this.searchInput).toBeVisible();
    await this.searchInput.clear();
    await this.searchInput.fill(policyName);
    await this.page.waitForTimeout(500);
  }

  async openRowMenu(policyName: string): Promise<void> {
    await this.page.getByRole('row').filter({ hasText: policyName }).getByRole('button').last().click({ force: true });
    await this.page.waitForTimeout(300);
  }

  async clickMenuAction(actionText: string): Promise<void> {
    await this.page.getByRole('menuitem').getByText(actionText).click();
  }

  async deactivateExistingAllUserPolicies(): Promise<void> {
    await expect(this.tableBody.getByRole('cell')).toBeVisible();
    const rows = this.tableBody.getByRole('row');
    const count = await rows.count();
    for (let i = 0; i < count; i++) {
      const row = rows.nth(i);
      const roleText = await row.getByRole('cell').nth(3).textContent();
      if (roleText?.includes('All Users')) {
        await row.getByRole('button').last().click({ force: true });
        await this.page.waitForTimeout(300);
        const deactivateItem = this.page.getByRole('menuitem').getByText('Deactivate');
        if ((await deactivateItem.count()) > 0) {
          await deactivateItem.click();
          await expect(this.page.getByText('Successfully deactivated policy.')).toBeVisible({ timeout: 15000 });
        }
      }
    }
  }
}
