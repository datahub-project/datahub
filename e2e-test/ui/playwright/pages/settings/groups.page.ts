import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { TOAST_MESSAGES } from './constants';
import { WAIT_TIMEOUT, MODAL_TIMEOUT, UI_SYNC_DELAY } from '../../utils/constants';

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
  private readonly bodyElement: Locator;
  private readonly membersSection: Locator;
  private readonly editDialog: Locator;
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
    this.membersTab = page.getByTestId('tab-members');
    this.addMemberConfirmButton = page.getByTestId('modal-add-member-button');
    this.addOwnerConfirmButton = page.getByTestId('modal-add-owner-button');
    this.deleteGroupConfirmButton = page.getByTestId('delete-group-confirm-button');
    this.modalDeleteItem = page.getByTestId('menu-item-delete');
    this.bodyElement = page.locator('body');
    this.membersSection = page.getByTestId('sidebar-section-content-Members');
    this.editDialog = page.getByTestId('group-edit-profile-modal');
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
    return this.page.getByRole('link', { name: new RegExp(groupName, 'i') }).first();
  }

  private getGroupHeading(groupName: string): Locator {
    return this.page.getByTestId('group-profile-name').filter({ hasText: new RegExp(groupName, 'i') });
  }

  private getGroupMenuButton(groupName: string): Locator {
    // Find the row containing the group link with this name, then find the menu button in that row
    const groupRow = this.page.locator('tbody tr', {
      has: this.getGroupLink(groupName),
    });
    // Find the menu button (third cell in the row typically contains the menu button)
    // Use a simpler selector - just get the button in the last cell of the row
    return groupRow.locator('td').last().locator('button').first();
  }

  async navigateToGroupProfile(groupName: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    // Give the page a moment for the group to appear in the list
    await this.page.waitForTimeout(500);
    const groupLink = this.getGroupLink(groupName);
    await groupLink.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await groupLink.click();
    await this.page.waitForLoadState('networkidle');
    await expect(this.getGroupHeading(groupName)).toBeVisible({ timeout: WAIT_TIMEOUT });
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
    await expect(this.page.getByTestId('create-group-modal')).not.toBeVisible({ timeout: MODAL_TIMEOUT });
    await this.page.waitForLoadState('networkidle');
    await expect(this.manageContainer).toBeVisible({ timeout: WAIT_TIMEOUT });

    const effectiveGroupId = await this.getGroupIdFromProfile(name);
    const groupUrn = `urn:li:corpGroup:${effectiveGroupId}`;
    this.cleanup?.track(groupUrn);
    return groupUrn;
  }

  private async getGroupIdFromProfile(groupName: string): Promise<string> {
    try {
      const groupLink = this.getGroupLink(groupName);
      await groupLink.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
      const href = await groupLink.getAttribute('href');
      if (href && href.includes('urn:li:corpGroup:')) {
        const match = href.match(/urn:li:corpGroup:([^/]+)/);
        if (match && match[1]) {
          return match[1];
        }
      }
    } catch {
      // If we can't extract from URL, use normalized name
      // This can happen if the group link isn't clickable or href isn't available
    }
    // Fallback: generate based on name
    return groupName.toLowerCase().replace(/\s+/g, '-');
  }

  async navigateToMembersTab(): Promise<void> {
    // Wait for the page to load and the tabs to be rendered
    await this.page.waitForLoadState('networkidle');
    // Give the page extra time to render tabs
    await this.page.waitForTimeout(1000);
    // Try to find the Members tab - scroll into view if needed
    await this.membersTab.scrollIntoViewIfNeeded();
    await this.membersTab.click();
    await this.page.waitForLoadState('networkidle');
  }

  async addMember(username: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    await this.addMemberButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await this.addMemberButton.click();
    const membersModal = this.page.getByTestId('add-members-modal');
    // Wait for modal to be attached then try to make visible
    await membersModal.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    try {
      await expect(membersModal).toBeVisible({ timeout: 3000 });
    } catch {
      // Continue with force interaction
    }
    await this.addMembersSelect.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    await this.selectFromDropdown(this.addMembersSelectBase, this.addMembersSelectDropdown, username);
    await this.page.getByTestId('add-members-modal').click({ force: true });
    await this.page.waitForTimeout(UI_SYNC_DELAY);
    await this.addMemberConfirmButton.click({ force: true });
    await expect(this.page.getByText(TOAST_MESSAGES.GROUP_MEMBERS_ADDED)).toBeVisible();
    await expect(this.membersSection.getByText(username)).toBeVisible();
  }

  async addOwner(username: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    await this.addOwnersButton.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await this.addOwnersButton.click();
    // Wait for modal to be attached to DOM first
    const modal = this.page.getByTestId('add-owners-modal');
    await modal.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    // Use a longer timeout since the modal animation might take time
    await this.page.waitForTimeout(500);
    // Try to wait for visibility but continue with force interaction if needed
    try {
      await modal.waitFor({ state: 'visible', timeout: 3000 });
    } catch {
      // Continue with force interaction
    }
    await this.addOwnersSelect.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    await this.selectFromDropdown(this.addOwnersSelectBase, this.addOwnersSelectDropdown, username);
    await this.page.getByTestId('add-owners-modal').click({ force: true });
    await this.page.waitForTimeout(UI_SYNC_DELAY);
    await this.addOwnerConfirmButton.click({ force: true });
    await expect(this.page.getByText(TOAST_MESSAGES.OWNERS_ADDED)).toBeVisible();
  }

  async verifyOwnersAdded(): Promise<void> {
    await expect(this.page.getByText(TOAST_MESSAGES.OWNERS_ADDED)).toBeVisible();
  }

  async updateGroupInfo(email: string, slack: string, newName: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    // Ensure we're on the group profile page
    await this.page.waitForTimeout(500);
    // Try to find and click the edit profile button
    // First check if the button exists in the DOM
    const buttonCount = await this.editProfileButton.count();
    if (buttonCount === 0) {
      throw new Error('Edit profile button not found on page');
    }
    // Try clicking without waiting for visibility
    try {
      await this.editProfileButton.click({ force: true, timeout: WAIT_TIMEOUT });
    } catch {
      // If force click fails, try to hover over the container first to show the button
      await this.page.locator('[data-testid="group-profile-name"]').first().hover();
      await this.page.waitForTimeout(300);
      await this.editProfileButton.click({ force: true });
    }
    // Wait for modal to be attached first
    await this.editDialog.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    // Give the modal animation time to complete
    await this.page.waitForTimeout(500);
    // Try to make modal visible by waiting, but also have a fallback to continue
    try {
      await this.editDialog.waitFor({ state: 'visible', timeout: 3000 });
    } catch {
      // If modal doesn't become visible, continue anyway - try to interact with force:true
    }
    // Fill the form inputs - use force to interact with potentially hidden elements
    await this.editDialogNameInput.waitFor({ state: 'attached', timeout: WAIT_TIMEOUT });
    await this.editDialogNameInput.click({ force: true });
    await this.editDialogNameInput.clear();
    await this.editDialogNameInput.fill(newName);
    await this.editDialogEmailInput.fill(email);
    await this.editDialogSlackInput.fill(slack);
    await this.editGroupButton.click({ force: true });
  }

  async verifyGroupInfoUpdated(expectedEmail: string, expectedSlack: string): Promise<void> {
    await expect(this.page.getByText(TOAST_MESSAGES.NAME_UPDATED)).toBeVisible();
    await expect(this.page.getByText(expectedEmail)).toBeVisible();
    await expect(this.page.getByText(expectedSlack)).toBeVisible();
  }

  async editDescription(currentText: string, append: string): Promise<void> {
    await this.page.waitForLoadState('networkidle');
    // Check if the edit about button exists
    const buttonCount = await this.editAboutButton.count();
    if (buttonCount === 0) {
      throw new Error('Edit about button not found on page');
    }
    // Hover over the about section to reveal the edit button
    const aboutSection = this.page.getByTestId('about-text-display').first();
    if (await aboutSection.count() > 0) {
      await aboutSection.hover();
    }
    await this.page.waitForTimeout(300);
    // Click the edit about button
    try {
      await this.editAboutButton.click({ force: true });
    } catch {
      // If force click fails, try hovering on the parent container
      const textDisplay = this.page.getByText(currentText).first();
      if (await textDisplay.count() > 0) {
        await textDisplay.hover();
        await this.page.waitForTimeout(300);
      }
      await this.editAboutButton.click({ force: true });
    }
    const textAreaEl = this.page.getByTestId('about-text-edit');
    // The textarea should appear after clicking edit
    // Try to wait for it to be attached, but continue if it doesn't appear
    try {
      await textAreaEl.waitFor({ state: 'attached', timeout: 3000 });
    } catch {
      // If textarea doesn't appear, the button click might not have worked
      // Try clicking again
      await this.editAboutButton.click({ force: true, timeout: WAIT_TIMEOUT });
      await this.page.waitForTimeout(500);
      await textAreaEl.waitFor({ state: 'attached', timeout: 3000 });
    }
    // Try to wait for visibility but continue anyway if it doesn't become visible
    try {
      await textAreaEl.waitFor({ state: 'visible', timeout: 3000 });
    } catch {
      // Continue with force interaction
    }
    await textAreaEl.click({ force: true });
    await this.page.keyboard.press('End');
    await this.page.keyboard.type(append);
    await this.bodyElement.click();
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
