import { Page, Locator, expect } from '@playwright/test';
import { DataHubLogger } from '../utils/logger';

/**
 * Page Object Model for Ownership Management settings page (/settings/ownership).
 *
 * Handles:
 * - Navigation to ownership types management
 * - Creating ownership types
 * - Editing ownership types
 * - Deleting ownership types
 * - Table interactions (finding rows, opening dropdowns)
 */
export class OwnershipManagementPage {
  readonly createOwnershipTypeButton: Locator;
  readonly ownershipTypeNameInput: Locator;
  readonly ownershipTypeDescriptionInput: Locator;
  readonly saveButton: Locator;
  readonly tableBody: Locator;
  readonly editMenuItem: Locator;
  readonly deleteMenuItem: Locator;

  constructor(
    readonly page: Page,
    readonly logger: DataHubLogger,
    readonly logDir: string,
  ) {
    this.createOwnershipTypeButton = page.getByTestId('create-owner-type-v2');
    this.ownershipTypeNameInput = page.getByTestId('ownership-type-name-input');
    this.ownershipTypeDescriptionInput = page.getByTestId('ownership-type-description-input');
    this.saveButton = page.getByTestId('ownership-builder-save');
    this.tableBody = page.getByRole('rowgroup');
    this.editMenuItem = page.getByRole('menuitem', { name: 'Edit' });
    this.deleteMenuItem = page.getByRole('menuitem', { name: 'Delete' });
  }

  /**
   * Navigate to the Ownership Types management page.
   */
  async navigate(): Promise<void> {
    this.logger.info('Navigating to /settings/ownership');
    await this.page.goto('/settings/ownership');
    await expect(this.page.getByText('Manage Ownership')).toBeVisible();
  }

  /**
   * Click the "Create Ownership Type" button to open the modal.
   */
  async openCreateModal(): Promise<void> {
    this.logger.info('Opening create ownership type modal');
    await this.createOwnershipTypeButton.click();
    await expect(this.page.getByRole('heading', { name: 'Create Ownership Type' })).toBeVisible();
  }

  /**
   * Fill in the ownership type name in the modal.
   */
  async setOwnershipTypeName(name: string): Promise<void> {
    this.logger.info(`Setting ownership type name to: ${name}`);
    await this.ownershipTypeNameInput.fill(name);
  }

  /**
   * Fill in the ownership type description in the modal.
   */
  async setOwnershipTypeDescription(description: string): Promise<void> {
    this.logger.info(`Setting ownership type description to: ${description}`);
    await this.ownershipTypeDescriptionInput.clear();
    await this.ownershipTypeDescriptionInput.fill(description);
  }

  /**
   * Click the Save button in the modal to create or update an ownership type.
   * Automatically waits for the modal to close and the item to appear in the list.
   */
  async saveOwnershipType(expectedName: string): Promise<void> {
    this.logger.info(`Saving ownership type: ${expectedName}`);

    await this.saveButton.click();

    // Wait for the modal heading to be completely removed from the DOM
    const heading = this.page.getByRole('heading').filter({ hasText: /Create|Edit.*Ownership Type/ });
    await heading.waitFor({ state: 'hidden' }).catch(() => {
      // Modal might close very quickly
    });

    // Try to find the item in the table. If not found, reload the page to ensure
    // fresh data is loaded from the backend.
    try {
      await expect(this.tableBody.getByText(expectedName)).toBeVisible();
    } catch {
      this.logger.info(`Item not found immediately, reloading page to fetch fresh data`);
      await this.page.reload();
      await expect(this.page.getByText('Manage Ownership')).toBeVisible();
      await expect(this.tableBody.getByText(expectedName)).toBeVisible();
    }

    this.logger.info(`Ownership type saved successfully: ${expectedName}`);
  }

  /**
   * Find a table row by ownership type name.
   */
  private getRowByName(name: string) {
    return this.page.getByRole('row').filter({ has: this.page.getByText(name) });
  }

  /**
   * Open the dropdown menu for a specific ownership type row.
   */
  async openRowDropdown(name: string): Promise<void> {
    this.logger.info(`Opening dropdown for ownership type: ${name}`);
    const row = this.getRowByName(name);
    await row.getByTestId('ownership-table-dropdown').click();
  }

  /**
   * Click the "Edit" option in the dropdown menu.
   */
  async clickEditMenu(): Promise<void> {
    this.logger.info('Clicking Edit from dropdown menu');
    await this.editMenuItem.click();
    await expect(this.page.getByRole('heading', { name: 'Edit Ownership Type' })).toBeVisible();
  }

  /**
   * Click the "Delete" option in the dropdown menu.
   */
  async clickDeleteMenu(): Promise<void> {
    this.logger.info('Clicking Delete from dropdown menu');
    await this.deleteMenuItem.click();
  }

  /**
   * Create an ownership type with the given name and description.
   */
  async createOwnershipType(name: string, description: string): Promise<void> {
    this.logger.info(`Creating ownership type: ${name}`);
    await this.openCreateModal();
    await this.setOwnershipTypeName(name);
    await this.setOwnershipTypeDescription(description);
    await this.saveOwnershipType(name);
  }

  /**
   * Edit an existing ownership type by name.
   */
  async editOwnershipType(originalName: string, newDescription: string): Promise<void> {
    this.logger.info(`Editing ownership type: ${originalName}`);
    await this.openRowDropdown(originalName);
    await this.clickEditMenu();
    await this.setOwnershipTypeDescription(newDescription);
    await this.saveOwnershipType(originalName);
  }

  /**
   * Delete an ownership type by name.
   */
  async deleteOwnershipType(name: string): Promise<void> {
    this.logger.info(`Deleting ownership type: ${name}`);
    await this.openRowDropdown(name);
    await this.clickDeleteMenu();

    // Wait for item to disappear from the list
    await expect(this.page.getByText(name)).toBeHidden();
    this.logger.info(`Ownership type deleted successfully: ${name}`);
  }

  /**
   * Verify that an ownership type is visible in the list.
   */
  async expectOwnershipTypeVisible(name: string): Promise<void> {
    this.logger.info(`Verifying ownership type is visible: ${name}`);
    await expect(this.page.getByText(name)).toBeVisible();
  }

  /**
   * Verify that an ownership type is NOT visible in the list.
   */
  async expectOwnershipTypeNotVisible(name: string): Promise<void> {
    this.logger.info(`Verifying ownership type is not visible: ${name}`);
    await expect(this.page.getByText(name)).toBeHidden();
  }

  /**
   * Verify that an item is visible in the table by text.
   */
  async expectItemVisible(name: string): Promise<void> {
    this.logger.info(`Verifying item is visible in table: ${name}`);
    await expect(this.tableBody.getByText(name)).toBeVisible();
  }

  /**
   * Verify that an item is hidden in the table by text.
   */
  async expectItemHidden(name: string): Promise<void> {
    this.logger.info(`Verifying item is hidden in table: ${name}`);
    await expect(this.tableBody.getByText(name)).toBeHidden();
  }
}
