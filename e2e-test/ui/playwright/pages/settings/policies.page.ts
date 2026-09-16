import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { TOAST_MESSAGES } from './constants';
import { WAIT_TIMEOUT, SHORT_TIMEOUT, TIMEOUTS } from '../../utils/constants';

export class PoliciesPage extends BaseSettingsPage {
  private readonly searchInput: Locator;
  private readonly tableBody: Locator;
  private readonly managePage: Locator;
  private readonly addPolicyButton: Locator;
  private readonly policyNameInput: Locator;
  private readonly policyDescInput: Locator;
  readonly privilegesInput: Locator;
  readonly usersInput: Locator;
  readonly groupsInput: Locator;
  readonly nextButton: Locator;
  readonly saveButton: Locator;
  private readonly policyFilterBase: Locator;
  private readonly policyFilterAll: Locator;
  private readonly confirmButton: Locator;
  private readonly policyTypeSelector: Locator;
  private readonly metadataTypeButton: Locator;
  private readonly platformTypeButton: Locator;
  private readonly tableRows: Locator;
  readonly allPrivilegesOption: Locator;
  readonly allUsersOption: Locator;
  readonly allGroupsOption: Locator;
  private readonly allUsersRows: Locator;
  readonly resourceTypeBase: Locator;
  readonly datasetOption: Locator;
  readonly successToastMessage: Locator;
  readonly resourceTypeConditionSelect: Locator;
  readonly resourceConditionSelect: Locator;
  readonly tagConditionSelect: Locator;
  readonly domainConditionSelect: Locator;
  readonly containerConditionSelect: Locator;

  constructor(page: Page, options?: PageOptions) {
    super(page, options);
    this.searchInput = page.getByTestId('search-bar-input');
    this.tableBody = page.getByTestId('policies-table-body');
    this.tableRows = this.tableBody.getByRole('row');
    this.managePage = page.getByTestId('manage-permissions-page');
    this.addPolicyButton = page.getByTestId('add-policy-button');
    this.policyNameInput = page.getByTestId('policy-name');
    this.policyDescInput = page.getByTestId('policy-description');
    this.privilegesInput = page.getByTestId('privileges-base');
    this.usersInput = page.getByTestId('users-base');
    this.groupsInput = page.getByTestId('groups-base');
    this.nextButton = page.getByTestId('next-button');
    this.saveButton = page.getByTestId('save-button');
    this.policyFilterBase = page.getByTestId('policy-filter-base');
    this.policyFilterAll = page.getByTestId('option-ALL');
    this.policyTypeSelector = page.getByTestId('policy-type-base');
    this.metadataTypeButton = page.getByTestId('option-METADATA');
    this.platformTypeButton = page.getByTestId('option-PLATFORM');
    this.allPrivilegesOption = page.getByTestId('privileges-dropdown').getByTestId('option-All');
    this.allUsersOption = page.getByTestId('users-dropdown').getByTestId('option-All');
    this.allGroupsOption = page.getByTestId('groups-dropdown').getByTestId('option-All');
    this.allUsersRows = this.tableBody.getByRole('row').filter({ hasText: 'All Users' });
    this.resourceTypeBase = page.getByTestId('resource-type-base');
    this.datasetOption = page.getByTestId('option-dataset');
    this.successToastMessage = page.getByText('Successfully saved policy.');
    // Condition selects for resource filtering (using actual field type enum values)
    this.resourceTypeConditionSelect = page.getByTestId('condition-TYPE-base');
    this.resourceConditionSelect = page.getByTestId('condition-URN-base');
    this.tagConditionSelect = page.getByTestId('condition-TAG-base');
    this.domainConditionSelect = page.getByTestId('condition-DOMAIN-base');
    this.containerConditionSelect = page.getByTestId('condition-CONTAINER-base');
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
    // The policies search box is debounced, so the table keeps rendering the previous result set
    // for a moment after the last keystroke. Callers open a row menu immediately after searching,
    // and the debounced refetch then unmounts the table rows — silently closing that open menu.
    // Wait for the debounce to fire and its refetch to finish before handing control back.
    // A fixed wait is required here: repeat searches for the same term short-circuit in the UI
    // (no refetch is issued), so there is no network response or DOM change to wait on.
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await this.page.waitForTimeout(TIMEOUTS.OPERATION);
    await this.page.waitForLoadState('networkidle');
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
    await this.policyTypeSelector.click();
    await this.platformTypeButton.waitFor({ state: 'visible' });
    await this.platformTypeButton.click();
  }

  async fillAndSaveWizard(description: string, policyName: string): Promise<void> {
    await expect(this.policyDescInput).toBeVisible();
    await this.policyDescInput.clear();
    await this.policyDescInput.fill(description);
    await this.nextButton.click();
    await this.page.waitForLoadState('networkidle');

    // Wait for the privileges form to load and be in viewport for IntersectionObserver
    await this.page.getByTestId('privileges').scrollIntoViewIfNeeded();
    await this.privilegesInput.waitFor({ state: 'visible', timeout: 10000 });
    await this.privilegesInput.click();
    await this.allPrivilegesOption.click();
    await this.nextButton.click();
    await this.page.waitForLoadState('networkidle');

    await this.usersInput.waitFor({ state: 'visible' });
    await this.usersInput.scrollIntoViewIfNeeded();
    await this.usersInput.click();
    await this.allUsersOption.click();
    await this.usersInput.click();

    await this.groupsInput.waitFor({ state: 'visible' });
    await this.groupsInput.scrollIntoViewIfNeeded();
    await this.groupsInput.click();
    await this.allGroupsOption.click();
    await this.groupsInput.click();

    await this.saveButton.waitFor({ state: 'visible' });
    await this.saveButton.scrollIntoViewIfNeeded();
    await this.saveButton.click();

    await this.toast.expectVisibleThenHidden(TOAST_MESSAGES.SUCCESSFULLY_SAVED_POLICY);
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

    await this.toast.expectVisibleThenHidden(TOAST_MESSAGES.SUCCESSFULLY_SAVED_POLICY);
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
    await this.toast.expectVisibleThenHidden(TOAST_MESSAGES.SUCCESSFULLY_DEACTIVATED_POLICY);

    await this.searchForPolicy(name);
    await this.openRowMenu(name);
    await this.clickMenuAction('Activate');
    await this.toast.expectVisibleThenHidden(TOAST_MESSAGES.SUCCESSFULLY_ACTIVATED_POLICY);

    await this.openRowMenu(name);
    await this.clickMenuAction('Delete');
    await expect(this.page.getByText(deleteDialogTitle)).toBeVisible();
    await this.confirmButton.click();
    await this.toast.expectVisibleThenHidden(TOAST_MESSAGES.SUCCESSFULLY_REMOVED_POLICY);
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
        await this.toast.expectVisibleThenHidden(TOAST_MESSAGES.SUCCESSFULLY_DEACTIVATED_POLICY);
      }
    }
  }

  // ── Condition Select Helpers ────────────────────────────────────────────────
  // Helpers for testing condition selection on resource filters

  async setResourceTypeCondition(condition: string): Promise<void> {
    await this.resourceTypeConditionSelect.scrollIntoViewIfNeeded();
    await this.resourceTypeConditionSelect.waitFor({ state: 'visible', timeout: 10000 });
    await this.resourceTypeConditionSelect.click();
    const dropdown = this.page.getByTestId('condition-TYPE-dropdown');
    await dropdown.waitFor({ state: 'visible', timeout: 10000 });
    // Convert condition label to enum value (e.g., NotEquals -> NOT_EQUALS)
    const conditionValue = condition.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase();
    const option = dropdown.getByTestId(`option-${conditionValue}`);
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.click({ delay: 100 });
    await this.resourceTypeConditionSelect.click();
  }

  async scrollConditionIntoView(fieldType: string): Promise<void> {
    // Scroll the wrapper (without -base) to trigger IntersectionObserver rendering
    await this.page.getByTestId(`condition-${fieldType}`).scrollIntoViewIfNeeded();
  }

  async fillDescriptionAndMoveToPrivilegeForm(description: string): Promise<void> {
    await this.policyDescInput.clear();
    await this.policyDescInput.fill(description);
    await this.nextButton.click();
    await this.page.waitForLoadState('networkidle');
  }
}
