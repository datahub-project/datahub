import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class GroupsPage extends BasePage {
  readonly createGroupButton: Locator;
  readonly groupNameInput: Locator;
  readonly groupDescriptionInput: Locator;
  readonly advancedToggle: Locator;
  readonly groupIdInput: Locator;
  readonly submitCreateGroupButton: Locator;
  readonly createdGroupToast: Locator;
  readonly addMemberButton: Locator;
  readonly addMembersSelect: Locator;
  readonly addMembersSelectBase: Locator;
  readonly memberDropdownSearchInput: Locator;
  readonly addMembersSelectDropdown: Locator;
  readonly membersAddedToast: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.createGroupButton = page.getByText('Create Group');
    this.groupNameInput = page.getByLabel('Name');
    this.groupDescriptionInput = page.getByLabel('Description');
    this.advancedToggle = page.getByText('Advanced');
    this.groupIdInput = page.getByLabel('Group Id');
    this.submitCreateGroupButton = page.getByRole('button', { name: 'Create' });
    this.createdGroupToast = page.getByText('Created group!');
    this.addMemberButton = page.getByText('Add Member');
    this.addMembersSelect = page.getByTestId('add-members-select');
    this.addMembersSelectBase = page.getByTestId('add-members-select-base');
    this.memberDropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.addMembersSelectDropdown = page.getByTestId('add-members-select-dropdown');
    this.membersAddedToast = page.getByText('Group members added!');
  }

  async navigateToGroups(): Promise<void> {
    await this.navigate('/settings/identities/groups');
    await this.waitForPageLoad();
  }

  async createGroup(name: string, description: string, groupId: string): Promise<void> {
    await this.createGroupButton.click();
    await expect(this.page.getByText('Create new group')).toBeVisible();
    await this.groupNameInput.fill(name);
    await this.groupDescriptionInput.fill(description);
    await this.advancedToggle.click();
    await expect(this.page.getByText('Group Id')).toBeVisible();
    await this.groupIdInput.fill(groupId);
    await this.submitCreateGroupButton.click();
    await expect(this.createdGroupToast).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(name)).toBeVisible();
  }

  async addMember(groupUrn: string, groupName: string, memberName: string): Promise<void> {
    await this.navigate(`/group/${groupUrn}/assets`);
    await this.page.getByText(groupName).click();
    await expect(this.page.getByText(groupName)).toBeVisible();
    await this.page.getByRole('tab', { name: 'Members' }).click();
    await this.addMemberButton.click();
    await expect(this.addMembersSelect).toBeVisible({ timeout: 10000 });
    await this.addMembersSelectBase.click({ force: true });
    await this.memberDropdownSearchInput.fill(memberName);
    await this.addMembersSelectDropdown.getByText(memberName).click({ force: true });
    await this.page.getByRole('dialog').getByRole('button', { name: 'Add' }).click({ force: true });
    await expect(this.membersAddedToast).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(memberName)).toBeVisible({ timeout: 10000 });
  }
}
