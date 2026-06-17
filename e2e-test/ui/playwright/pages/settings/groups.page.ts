import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { TOAST_MESSAGES } from './constants';
import { WAIT_TIMEOUT, MODAL_TIMEOUT, DEFAULT_TIMEOUT } from '../../utils/constants';

export class GroupsPage extends BaseSettingsPage {
  private readonly manageContainer: Locator;
  private readonly openCreateGroupButton: Locator;
  private readonly groupNameInput: Locator;
  private readonly descriptionInput: Locator;
  private readonly groupIdInput: Locator;
  private readonly createGroupButton: Locator;
  private readonly addMemberButton: Locator;
  private readonly addMembersSelectBase: Locator;
  private readonly addMembersSelectDropdown: Locator;
  private readonly addOwnersButton: Locator;
  private readonly addOwnersSelectBase: Locator;
  private readonly addOwnersSelectDropdown: Locator;
  private readonly advancedButton: Locator;
  private readonly editAboutButton: Locator;
  private readonly editProfileButton: Locator;
  private readonly addMemberConfirmButton: Locator;
  private readonly addOwnerConfirmButton: Locator;
  private readonly deleteGroupConfirmButton: Locator;
  private readonly modalDeleteItem: Locator;
  private readonly membersSection: Locator;
  private readonly groupEditProfileModal: Locator;
  private readonly createGroupModal: Locator;
  private readonly addMembersModal: Locator;
  private readonly addOwnersModal: Locator;
  private readonly aboutTextEdit: Locator;
  private readonly editDialogNameInput: Locator;
  private readonly editDialogEmailInput: Locator;
  private readonly editDialogSlackInput: Locator;
  private readonly editGroupButton: Locator;

  constructor(page: Page, options?: PageOptions) {
    super(page, options);
    this.manageContainer = page.getByTestId('manage-users-groups-v2');
    this.openCreateGroupButton = page.getByTestId('create-group-button');
    this.groupNameInput = page.getByTestId('group-name-input');
    this.descriptionInput = page.getByTestId('group-description-input');
    this.groupIdInput = page.getByTestId('group-id-input');
    this.createGroupButton = page.getByTestId('modal-create-group-button');
    this.addMemberButton = page.getByTestId('add-group-members-sidebar-button');
    this.addMembersSelectBase = page.getByTestId('add-members-select-base');
    this.addMembersSelectDropdown = page.getByTestId('add-members-select-dropdown');
    this.addOwnersButton = page.getByTestId('add-owners-sidebar-button');
    this.addOwnersSelectBase = page.getByTestId('add-owners-select-base');
    this.addOwnersSelectDropdown = page.getByTestId('add-owners-select-dropdown');
    this.advancedButton = page.getByTestId('create-group-advanced-button');
    this.editAboutButton = page.getByTestId('edit-about-button');
    this.editProfileButton = page.getByTestId('edit-group-profile-button');
    this.addMemberConfirmButton = page.getByTestId('modal-add-member-button');
    this.addOwnerConfirmButton = page.getByTestId('modal-add-owner-button');
    this.deleteGroupConfirmButton = page.getByTestId('delete-group-confirm-button');
    this.modalDeleteItem = page.getByTestId('menu-item-delete');
    this.membersSection = page.getByTestId('sidebar-section-content-Members');
    this.groupEditProfileModal = page.getByTestId('group-edit-profile-modal');
    this.createGroupModal = page.getByTestId('create-group-modal');
    this.addMembersModal = page.getByTestId('add-members-modal');
    this.addOwnersModal = page.getByTestId('add-owners-modal');
    this.aboutTextEdit = page.getByTestId('about-text-edit');
    this.editDialogNameInput = page.getByTestId('group-profile-name-input');
    this.editDialogEmailInput = page.getByTestId('group-profile-email-input');
    this.editDialogSlackInput = page.getByTestId('group-profile-slack-input');
    this.editGroupButton = page.getByTestId('group-edit-profile-save-button');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/identities/groups');
    await this.page.waitForLoadState('networkidle');
    await expect(this.manageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  // ── Dynamic selectors for groups ────────────────────────────────────────
  // These helpers create locators based on runtime data (group names, etc.)
  private getGroupLink(groupName: string): Locator {
    return this.page.getByRole('link', { name: new RegExp(groupName, 'i') });
  }

  private getGroupHeading(groupName: string): Locator {
    return this.page.getByTestId('group-profile-name').filter({ hasText: new RegExp(groupName, 'i') });
  }

  private getGroupMenuButton(groupName: string): Locator {
    return this.page.getByTestId(`group-menu-${groupName}`);
  }

  async navigateToGroupProfile(groupName: string): Promise<void> {
    // Ensure we're back on the groups list page
    const groupLink = this.getGroupLink(groupName);
    await groupLink.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await groupLink.click();
    await this.page.waitForLoadState('networkidle');
    await expect(this.getGroupHeading(groupName)).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  async createGroup(name: string, description: string, groupId?: string): Promise<void> {
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
    await this.createGroupModal.waitFor({ state: 'hidden', timeout: MODAL_TIMEOUT });
    await this.page.waitForLoadState('networkidle');
    await expect(this.manageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });
    await this.page.waitForLoadState('networkidle');

    await this.getGroupLink(name).waitFor({ state: 'visible', timeout: DEFAULT_TIMEOUT });
  }

  async addMember(username: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    await this.addMemberButton.click();
    await this.addMembersModal.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });

    await this.selectFromDropdown(this.addMembersSelectBase, this.addMembersSelectDropdown, username);
    await this.addMembersSelectBase.click();
    await this.addMemberConfirmButton.click();
    await expect(this.page.getByText(TOAST_MESSAGES.GROUP_MEMBERS_ADDED)).toBeVisible();
    await expect(this.membersSection.getByText(username)).toBeVisible();
  }

  async addOwner(username: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    await this.addOwnersButton.click();
    await this.addOwnersModal.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });

    await this.selectFromDropdown(this.addOwnersSelectBase, this.addOwnersSelectDropdown, username);
    await this.addOwnersSelectBase.click();
    await this.addOwnerConfirmButton.click();
    await expect(this.page.getByText(TOAST_MESSAGES.OWNERS_ADDED)).toBeVisible();
  }

  async updateGroupInfo(email: string, slack: string, newName: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    await this.editProfileButton.click();
    await this.groupEditProfileModal.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    await this.editDialogNameInput.clear();
    await this.editDialogNameInput.fill(newName);
    await this.editDialogEmailInput.fill(email);
    await this.editDialogSlackInput.fill(slack);
    await this.editGroupButton.click();
  }

  async verifyOwnersAdded(): Promise<void> {
    await expect(this.page.getByText(TOAST_MESSAGES.OWNERS_ADDED)).toBeVisible();
  }

  async verifyGroupInfoUpdated(expectedEmail: string, expectedSlack: string): Promise<void> {
    await expect(this.page.getByText(TOAST_MESSAGES.NAME_UPDATED)).toBeVisible();
    await expect(this.page.getByText(expectedEmail)).toBeVisible();
    await expect(this.page.getByText(expectedSlack)).toBeVisible();
  }

  async editDescription(currentText: string, append: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    await this.editAboutButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await this.editAboutButton.click();

    await this.aboutTextEdit.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await this.aboutTextEdit.focus();
    await this.page.keyboard.press('End');
    await this.page.keyboard.type(append);
    await this.page.mouse.click(0, 0);
    await this.waitForToast(TOAST_MESSAGES.CHANGES_SAVED);
    await expect(this.page.getByText(`${currentText}${append}`)).toBeVisible({ timeout: WAIT_TIMEOUT });
  }

  async verifyMemberVisible(username: string): Promise<void> {
    await expect(this.membersSection.getByText(username)).toBeVisible();
  }

  async deleteGroup(groupName: string): Promise<void> {
    // Ensure we're on the groups list page
    await this.navigate();
    await this.page.waitForLoadState('networkidle');

    // Wait for the group to appear in the list
    const groupLink = this.getGroupLink(groupName);
    await groupLink.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });

    const menuButton = this.getGroupMenuButton(groupName);
    await menuButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await menuButton.click();
    await this.page.waitForLoadState('networkidle');
    await this.modalDeleteItem.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await this.modalDeleteItem.click();
    await this.deleteGroupConfirmButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await this.deleteGroupConfirmButton.click();
    await expect(this.page.getByText(`Deleted ${groupName}!`)).toBeVisible();
    await expect(this.page.getByText(groupName)).toBeHidden();
  }
}
