import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import type { DataHubLogger } from '../../utils/logger';

export class GroupsPage extends BaseSettingsPage {
  private readonly manageContainer: Locator;
  private readonly openCreateGroupButton: Locator;
  private readonly nameInput: Locator;
  private readonly descriptionInput: Locator;
  private readonly groupIdInput: Locator;
  private readonly createGroupButton: Locator;
  private readonly addMemberButton: Locator;
  private readonly addMembersSelect: Locator;
  private readonly addMembersSelectBase: Locator;
  private readonly addMembersSelectDropdown: Locator;
  private readonly addOwnersButton: Locator;
  private readonly addOwnersSelect: Locator;
  private readonly addOwnersSelectBase: Locator;
  private readonly addOwnersSelectDropdown: Locator;
  private readonly advancedButton: Locator;
  private readonly editAboutButton: Locator;
  private readonly editProfileButton: Locator;
  private readonly membersTab: Locator;
  private readonly addMemberConfirmButton: Locator;
  private readonly addOwnerConfirmButton: Locator;
  private readonly deleteGroupConfirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.manageContainer = page.locator('[data-testid="manage-users-groups-v2"]');
    this.openCreateGroupButton = page.locator('[data-testid="create-group-button"]');
    this.nameInput = page.locator('#name');
    this.descriptionInput = page.locator('#description');
    this.groupIdInput = page.locator('#groupId');
    this.createGroupButton = page.locator('#createGroupButton');
    this.addMemberButton = page.locator('[data-testid="add-group-member-button"]');
    this.addMembersSelect = page.locator('[data-testid="add-members-select"]');
    this.addMembersSelectBase = page.locator('[data-testid="add-members-select-base"]');
    this.addMembersSelectDropdown = page.locator('[data-testid="add-members-select-dropdown"]');
    this.addOwnersButton = page.locator('[data-testid="add-owners-sidebar-button"]');
    this.addOwnersSelect = page.locator('[data-testid="add-owners-select"]');
    this.addOwnersSelectBase = page.locator('[data-testid="add-owners-select-base"]');
    this.addOwnersSelectDropdown = page.locator('[data-testid="add-owners-select-dropdown"]');
    this.advancedButton = page.locator('[data-testid="create-group-advanced-button"]');
    this.editAboutButton = page.locator('[data-testid="edit-about-button"]');
    this.editProfileButton = page.locator('[data-testid="edit-group-profile-button"]');
    this.membersTab = page.getByRole('tab', { name: 'Members' });
    this.addMemberConfirmButton = page.locator('[data-testid="modal-add-member-button"]');
    this.addOwnerConfirmButton = page.locator('[data-testid="modal-add-owner-button"]');
    this.deleteGroupConfirmButton = page.locator('[data-testid="delete-group-confirm-button"]');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/groups');
    await expect(this.manageContainer).toBeVisible({ timeout: 10000 });
  }

  async navigateToGroupProfile(groupId: string | number): Promise<void> {
    const urn = `urn:li:corpGroup:${groupId}`;
    const link = this.page.locator(`[href="/group/${urn}"]`);
    await link.waitFor({ state: 'visible', timeout: 10000 });
    await link.click();
    await expect(this.page).toHaveURL(new RegExp(`/group/${urn}`));
  }

  async createGroup(name: string, description: string, groupId?: string): Promise<void> {
    await this.openCreateGroupButton.click();
    await expect(this.page.getByText('Create new group')).toBeVisible({ timeout: 10000 });
    await this.nameInput.fill(name);
    await this.descriptionInput.fill(description);
    if (groupId !== undefined) {
      await this.advancedButton.click();
      await expect(this.groupIdInput).toBeVisible({ timeout: 10000 });
      await this.groupIdInput.fill(groupId);
    }
    await this.createGroupButton.click();
    await expect(this.page.getByText(name)).toBeVisible({ timeout: 15000 });
  }

  async navigateToMembersTab(): Promise<void> {
    await this.membersTab.waitFor({ state: 'visible', timeout: 10000 });
    await this.membersTab.click();
  }

  async addMember(username: string): Promise<void> {
    await this.addMemberButton.waitFor({ state: 'visible', timeout: 10000 });
    await this.addMemberButton.click();
    await expect(this.page.getByRole('dialog')).toBeVisible({ timeout: 10000 });
    await this.addMembersSelect.waitFor({ state: 'visible', timeout: 10000 });
    await this.selectFromDropdown(this.addMembersSelectBase, this.addMembersSelectDropdown, username);
    // Close the dropdown by clicking on the modal title (triggers useClickOutside mousedown
    // outside the select and dropdown refs), then click the Add button.
    await this.page.getByRole('dialog').locator('h1').click({ force: true });
    await this.addMembersSelectDropdown.waitFor({ state: 'detached', timeout: 5000 }).catch(() => {});
    await this.addMemberConfirmButton.waitFor({ state: 'visible', timeout: 5000 });
    await this.addMemberConfirmButton.click();
    await expect(this.page.getByText('Group members added!')).toBeVisible({ timeout: 15000 });
    // The sidebar refetches asynchronously after the mutation — wait for the new member to appear.
    await expect(this.page.locator('[data-testid="sidebar-section-content-Members"]').getByText(username)).toBeVisible({
      timeout: 30000,
    });
  }

  async addOwner(username: string): Promise<void> {
    await this.addOwnersButton.waitFor({ state: 'visible', timeout: 10000 });
    await this.addOwnersButton.click();
    await expect(this.page.getByRole('dialog')).toBeVisible({ timeout: 10000 });
    await this.addOwnersSelect.waitFor({ state: 'visible', timeout: 10000 });
    await this.selectFromDropdown(this.addOwnersSelectBase, this.addOwnersSelectDropdown, username);
    // Close the dropdown by clicking the modal title before clicking Add.
    await this.page.getByRole('dialog').locator('h1').click({ force: true });
    await this.addOwnersSelectDropdown.waitFor({ state: 'detached', timeout: 5000 }).catch(() => {});
    await this.addOwnerConfirmButton.waitFor({ state: 'visible', timeout: 5000 });
    await this.addOwnerConfirmButton.click();
    await expect(this.page.getByText('Owners Added')).toBeVisible({ timeout: 15000 });
  }

  async updateGroupInfo(email: string, slack: string, newName: string): Promise<void> {
    // The edit profile button is display:none by default and only shown on hover of its
    // parent container. Hover over the group name heading (which shares the same container)
    // to trigger the CSS :hover state, then click the now-visible button.
    await this.page.locator('h3').first().hover();
    await this.editProfileButton.waitFor({ state: 'visible', timeout: 5000 });
    await this.editProfileButton.click();
    const dialog = this.page.getByRole('dialog');
    await dialog.locator('#name').clear();
    await dialog.locator('#name').fill(newName);
    await dialog.locator('#email').fill(email);
    await dialog.locator('#slack').fill(slack);
    await dialog.locator('#editGroupButton').click();
  }

  async verifyGroupInfoUpdated(expectedEmail: string, expectedSlack: string): Promise<void> {
    await expect(this.page.getByText('Name Updated')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(expectedEmail)).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(expectedSlack)).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText('Changes saved.')).not.toBeVisible({ timeout: 15000 });
  }

  async editDescription(currentText: string, append: string): Promise<void> {
    await this.editAboutButton.click({ force: true });
    const descEl = this.page.getByText(currentText);
    await expect(descEl).toBeVisible({ timeout: 10000 });
    await descEl.click();
    // Move cursor to end of field before appending text.
    await this.page.keyboard.press('End');
    await this.page.keyboard.type(append);
    await this.page.locator('body').click();
    await this.waitForToast('Changes saved.');
    await expect(this.page.getByText(`${currentText}${append}`)).toBeVisible({ timeout: 15000 });
  }

  async verifyMemberVisible(username: string): Promise<void> {
    await expect(this.page.getByText(username).first()).toBeVisible({ timeout: 10000 });
  }

  async deleteGroup(groupName: string): Promise<void> {
    const menuButton = this.page.locator(`[data-testid="group-menu-${groupName}"]`);
    await menuButton.waitFor({ state: 'visible', timeout: 10000 });
    await menuButton.click();
    await this.page.locator('[data-testid="menu-item-delete"]').click();
    await this.deleteGroupConfirmButton.click();
    await expect(this.page.getByText(`Deleted ${groupName}!`)).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(groupName)).not.toBeVisible({ timeout: 15000 });
  }
}
