import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import { WAIT_TIMEOUT, MODAL_TIMEOUT, UI_SYNC_DELAY, TOAST_MESSAGES } from '../../tests/settings-v2/constants';
import type { DataHubLogger } from '../../utils/logger';

export class GroupsPage extends BaseSettingsPage {
  private readonly manageContainer: Locator;
  private readonly openCreateGroupButton: Locator;
  private readonly groupNameInput: Locator;
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
  private readonly modalDeleteItem: Locator;
  private readonly headingThree: Locator;
  private readonly bodyElement: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.manageContainer = page.getByTestId('manage-users-groups-v2');
    this.openCreateGroupButton = page.getByTestId('create-group-button');
    this.groupNameInput = page.locator('#name');
    this.descriptionInput = page.locator('#description');
    this.groupIdInput = page.locator('#groupId');
    this.createGroupButton = page.locator('#createGroupButton');
    this.addMemberButton = page.getByTestId('add-group-member-button');
    this.addMembersSelect = page.getByTestId('add-members-select');
    this.addMembersSelectBase = page.getByTestId('add-members-select-base');
    this.addMembersSelectDropdown = page.getByTestId('add-members-select-dropdown');
    this.addOwnersButton = page.getByTestId('add-owners-sidebar-button');
    this.addOwnersSelect = page.getByTestId('add-owners-select');
    this.addOwnersSelectBase = page.getByTestId('add-owners-select-base');
    this.addOwnersSelectDropdown = page.getByTestId('add-owners-select-dropdown');
    this.advancedButton = page.getByTestId('create-group-advanced-button');
    this.editAboutButton = page.getByTestId('edit-about-button');
    this.editProfileButton = page.getByTestId('edit-group-profile-button');
    this.membersTab = page.getByRole('tab', { name: 'Members' });
    this.addMemberConfirmButton = page.getByTestId('modal-add-member-button');
    this.addOwnerConfirmButton = page.getByTestId('modal-add-owner-button');
    this.deleteGroupConfirmButton = page.getByTestId('delete-group-confirm-button');
    this.modalDeleteItem = page.getByTestId('menu-item-delete');
    this.headingThree = page.locator('h3');
    this.bodyElement = page.locator('body');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/groups');
    await this.page.waitForLoadState('networkidle').catch(() => {});
    await expect(this.manageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  async navigateToGroupProfile(groupName: string): Promise<void> {
    const groupLink = this.page.getByRole('link').filter({ hasText: groupName });
    await groupLink.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await groupLink.click();
    await this.page.waitForLoadState('networkidle').catch(() => {});
    await expect(this.headingThree.filter({ hasText: groupName })).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  async createGroup(name: string, description: string, groupId?: string): Promise<string> {
    await this.openCreateGroupButton.click();
    await expect(this.page.getByText(TOAST_MESSAGES.CREATE_NEW_GROUP)).toBeVisible();
    await this.groupNameInput.waitFor({ state: 'visible' });
    await this.groupNameInput.fill(name);
    await this.descriptionInput.fill(description);
    if (groupId !== undefined) {
      await this.advancedButton.waitFor({ state: 'visible' });
      await this.advancedButton.click();
      await expect(this.groupIdInput).toBeVisible();
      await this.groupIdInput.fill(groupId);
    }
    await this.createGroupButton.click();
    await expect(this.page.getByRole('dialog')).not.toBeVisible({ timeout: MODAL_TIMEOUT });
    await this.page.waitForLoadState('networkidle').catch(() => {});
    await expect(this.manageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });
    await expect(this.page.getByRole('link').filter({ hasText: name })).toBeVisible({ timeout: WAIT_TIMEOUT });

    const effectiveGroupId = await this.getGroupIdFromProfile(name);
    return `urn:li:corpGroup:${effectiveGroupId}`;
  }

  private async getGroupIdFromProfile(groupName: string): Promise<string> {
    // Navigate to the group profile to get the actual group ID from URL
    const groupLink = this.page.getByRole('link').filter({ hasText: groupName });
    await groupLink.waitFor({ state: 'visible' });
    const href = await groupLink.getAttribute('href');
    // Extract group ID from URL like /group/urn:li:corpGroup:test-group-123
    if (href && href.includes('urn:li:corpGroup:')) {
      const match = href.match(/urn:li:corpGroup:([^/]+)/);
      if (match && match[1]) {
        return match[1];
      }
    }
    // Fallback: generate based on name
    return groupName.toLowerCase().replace(/\s+/g, '-');
  }

  async navigateToMembersTab(): Promise<void> {
    await this.membersTab.waitFor({ state: 'visible' });
    await this.membersTab.click();
  }

  async addMember(username: string): Promise<void> {
    await this.addMemberButton.waitFor({ state: 'visible' });
    await this.addMemberButton.click();
    await expect(this.page.getByRole('dialog')).toBeVisible();
    await this.addMembersSelect.waitFor({ state: 'visible' });
    await this.selectFromDropdown(this.addMembersSelectBase, this.addMembersSelectDropdown, username);
    await this.page.getByRole('dialog').click({ force: true });
    await this.page.waitForTimeout(UI_SYNC_DELAY);
    await this.addMemberConfirmButton.click({ force: true });
    await expect(this.page.getByText(TOAST_MESSAGES.GROUP_MEMBERS_ADDED)).toBeVisible();
    const memberSection = this.page.getByTestId('sidebar-section-content-Members');
    await expect(memberSection.getByText(username)).toBeVisible();
  }

  async addOwner(username: string): Promise<void> {
    await this.addOwnersButton.waitFor({ state: 'visible' });
    await this.addOwnersButton.click();
    await expect(this.page.getByRole('dialog')).toBeVisible();
    await this.addOwnersSelect.waitFor({ state: 'visible' });
    await this.selectFromDropdown(this.addOwnersSelectBase, this.addOwnersSelectDropdown, username);
    await this.page.getByRole('dialog').click({ force: true });
    await this.page.waitForTimeout(UI_SYNC_DELAY);
    await this.addOwnerConfirmButton.click({ force: true });
    await expect(this.page.getByText(TOAST_MESSAGES.OWNERS_ADDED)).toBeVisible();
  }

  async updateGroupInfo(email: string, slack: string, newName: string): Promise<void> {
    const groupHeading = this.headingThree.first();
    await groupHeading.hover();
    await this.editProfileButton.waitFor({ state: 'visible' });
    await this.editProfileButton.click();
    const dialog = this.page.getByRole('dialog');
    await dialog.locator('#name').clear();
    await dialog.locator('#name').fill(newName);
    await dialog.locator('#email').fill(email);
    await dialog.locator('#slack').fill(slack);
    await dialog.locator('#editGroupButton').click();
  }

  async verifyGroupInfoUpdated(expectedEmail: string, expectedSlack: string): Promise<void> {
    await expect(this.page.getByText(TOAST_MESSAGES.NAME_UPDATED)).toBeVisible();
    await expect(this.page.getByText(expectedEmail)).toBeVisible();
    await expect(this.page.getByText(expectedSlack)).toBeVisible();
    await expect(this.page.getByText(TOAST_MESSAGES.CHANGES_SAVED)).toBeHidden();
  }

  async editDescription(currentText: string, append: string): Promise<void> {
    await this.editAboutButton.click({ force: true });
    const descEl = this.page.getByText(currentText);
    await expect(descEl).toBeVisible();
    await descEl.click();
    await this.page.keyboard.press('End');
    await this.page.keyboard.type(append);
    await this.bodyElement.click();
    await this.waitForToast(TOAST_MESSAGES.CHANGES_SAVED);
    await expect(this.page.getByText(`${currentText}${append}`)).toBeVisible();
  }

  async verifyMemberVisible(username: string): Promise<void> {
    const memberSection = this.page.getByTestId('sidebar-section-content-Members');
    await expect(memberSection.getByText(username)).toBeVisible();
  }

  async deleteGroup(groupName: string): Promise<void> {
    const menuButton = this.page.getByTestId(`group-menu-${groupName}`);
    await menuButton.waitFor({ state: 'visible' });
    await menuButton.click();
    await this.modalDeleteItem.click();
    await this.deleteGroupConfirmButton.click();
    await expect(this.page.getByText(`Deleted ${groupName}!`)).toBeVisible();
    await expect(this.page.getByText(groupName)).toBeHidden();
  }
}
