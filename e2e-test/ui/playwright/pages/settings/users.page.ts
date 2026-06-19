import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { WAIT_TIMEOUT } from '../../utils/constants';

export class UsersPage extends BaseSettingsPage {
  private readonly manageContainer: Locator;
  private readonly inviteUsersButton: Locator;
  private readonly inviteLinkText: Locator;

  constructor(page: Page, options?: PageOptions) {
    super(page, options);
    this.manageContainer = page.getByTestId('manage-users-groups-v2');
    this.inviteUsersButton = page.getByTestId('invite-users-button');
    this.inviteLinkText = page.getByText(/signup\?invite_token=\w{32}/);
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/users');
    await expect(this.manageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  async getInviteLink(): Promise<string> {
    await this.inviteUsersButton.click();
    await expect(this.inviteLinkText).toBeVisible({ timeout: WAIT_TIMEOUT });
    return (await this.inviteLinkText.textContent()) ?? '';
  }

  async verifyUsernameVisible(username: string): Promise<void> {
    await expect(this.page.getByText(username)).toBeVisible({ timeout: WAIT_TIMEOUT });
  }
}
