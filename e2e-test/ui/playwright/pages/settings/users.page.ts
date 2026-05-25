import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import type { DataHubLogger } from '../../utils/logger';

export class UsersPage extends BaseSettingsPage {
  private readonly manageContainer: Locator;
  private readonly inviteUsersButton: Locator;
  private readonly inviteLinkText: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.manageContainer = page.locator('[data-testid="manage-users-groups-v2"]');
    this.inviteUsersButton = page.locator('[data-testid="invite-users-button"]');
    this.inviteLinkText = page.getByText(/signup\?invite_token=\w{32}/);
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/users');
    await expect(this.manageContainer).toBeVisible({ timeout: 10000 });
  }

  async getInviteLink(): Promise<string> {
    await this.inviteUsersButton.click();
    await expect(this.inviteLinkText).toBeVisible({ timeout: 15000 });
    return (await this.inviteLinkText.textContent()) ?? '';
  }

  async verifyUsernameVisible(username: string): Promise<void> {
    await expect(this.page.getByText(username)).toBeVisible({ timeout: 15000 });
  }
}
