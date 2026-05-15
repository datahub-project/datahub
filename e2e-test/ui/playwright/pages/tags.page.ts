import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

/**
 * Page object for the Manage Tags page (/tags).
 *
 * Covers all CRUD operations on the tags list page as well as
 * the create / edit modals.
 */
export class TagsPage extends BasePage {
  // ── Top-level page elements ──────────────────────────────────────────────

  readonly pageTitle: Locator;
  readonly createTagButton: Locator;
  readonly searchInput: Locator;
  readonly tagsNotFoundMessage: Locator;

  // ── Create-tag modal elements ────────────────────────────────────────────

  readonly createTagModalContent: Locator;
  readonly createTagNameInput: Locator;
  readonly createTagDescriptionInput: Locator;
  readonly createTagCreateButton: Locator;
  readonly createTagCancelButton: Locator;
  readonly createTagFailedToast: Locator;

  // ── Edit-tag modal elements ──────────────────────────────────────────────

  readonly editTagModalContent: Locator;
  readonly editTagDescriptionInput: Locator;
  readonly editTagSaveButton: Locator;
  readonly actionEditButton: Locator;

  // ── Delete-tag modal elements ────────────────────────────────────────────

  readonly deleteTagConfirmButton: Locator;
  readonly actionDeleteButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.pageTitle = page.getByTestId('page-title');
    this.createTagButton = page.getByTestId('add-tag-button');
    this.searchInput = page.getByTestId('tag-search-input');
    this.tagsNotFoundMessage = page.getByTestId('tags-not-found');

    this.createTagCreateButton = page.getByTestId('create-tag-modal-create-button');
    this.createTagCancelButton = page.getByTestId('create-tag-modal-cancel-button');
    this.createTagFailedToast = page.getByText(/Failed to create tag/);
    this.createTagModalContent = page.locator(
      '.ant-modal-wrap:has([data-testid="create-tag-modal-create-button"]) .ant-modal-content',
    );
    this.createTagNameInput = this.createTagModalContent.getByTestId('tag-name-field').locator('input');
    this.createTagDescriptionInput = this.createTagModalContent.getByTestId('tag-description-field').locator('input');

    this.editTagSaveButton = page.getByTestId('update-tag-button');
    this.editTagModalContent = page.locator(
      '.ant-modal-wrap:has([data-testid="update-tag-button"]) .ant-modal-content',
    );
    this.editTagDescriptionInput = this.editTagModalContent.getByTestId('tag-description-field').locator('input');
    this.actionEditButton = page.getByTestId('action-edit');

    this.deleteTagConfirmButton = page.getByTestId('delete-tag-button');
    this.actionDeleteButton = page.getByTestId('action-delete');
  }

  // ── Dynamic locators ─────────────────────────────────────────────────────

  tagUrn(name: string): string {
    return `urn:li:tag:${name}`;
  }

  getTagActionsButton(urn: string): Locator {
    return this.page.getByTestId(`${urn}-actions`);
  }

  getTagNameCell(urn: string): Locator {
    return this.page.getByTestId(`${urn}-name`);
  }

  getTagDescriptionCell(urn: string): Locator {
    return this.page.getByTestId(`${urn}-description`);
  }

  // ── Navigation ───────────────────────────────────────────────────────────

  async navigate(): Promise<void> {
    this.logger?.step('navigate', { url: '/tags' });
    await this.page.goto('/tags');
    await this.waitForPageLoad();
  }

  // ── Search helpers ───────────────────────────────────────────────────────

  async searchForTag(name: string): Promise<void> {
    this.logger?.step('searchForTag', { name });
    await this.searchInput.fill(name);
    await this.page.waitForLoadState('networkidle');
  }

  async clearSearch(): Promise<void> {
    this.logger?.step('clearSearch');
    await this.searchInput.fill('');
    await this.page.waitForLoadState('networkidle');
  }

  // ── Create tag ───────────────────────────────────────────────────────────

  async createTag(name: string, description: string, expectSuccess = true): Promise<void> {
    this.logger?.step('createTag', { name, description, expectSuccess });
    await expect(this.createTagButton).toBeVisible();
    await this.createTagButton.click();

    await expect(this.createTagModalContent).toBeVisible();

    await this.createTagNameInput.fill(name);
    await this.createTagDescriptionInput.fill(description);

    await this.createTagCreateButton.click();

    if (expectSuccess) {
      await expect(this.createTagModalContent).toBeHidden();
      await expect(this.createTagButton).toBeVisible();
    } else {
      await expect(this.createTagFailedToast).toBeVisible();
      await this.createTagCancelButton.click();
      await expect(this.createTagModalContent).toBeHidden();
    }
  }

  // ── Edit tag ─────────────────────────────────────────────────────────────

  async editTagDescription(name: string, newDescription: string): Promise<void> {
    this.logger?.step('editTagDescription', { name, newDescription });
    await this.searchForTag(name);

    const actionsButton = this.getTagActionsButton(this.tagUrn(name));
    await expect(actionsButton).toBeVisible();
    await actionsButton.click();
    await expect(this.actionEditButton).toBeVisible();
    await this.actionEditButton.click();

    await expect(this.editTagModalContent).toBeVisible();

    await this.editTagDescriptionInput.fill(newDescription);
    await this.editTagSaveButton.click();

    await expect(this.editTagModalContent).toBeHidden();
    await this.clearSearch();
  }

  // ── Delete tag ───────────────────────────────────────────────────────────

  async deleteTag(name: string): Promise<void> {
    this.logger?.step('deleteTag', { name });
    await this.searchForTag(name);

    const actionsButton = this.getTagActionsButton(this.tagUrn(name));
    await expect(actionsButton).toBeVisible();
    await actionsButton.click();
    await expect(this.actionDeleteButton).toBeVisible();
    await this.actionDeleteButton.click();

    await expect(this.deleteTagConfirmButton).toBeVisible();
    await this.deleteTagConfirmButton.click();

    await expect(this.deleteTagConfirmButton).toBeHidden();
    await this.clearSearch();
  }

  // ── Assertions ───────────────────────────────────────────────────────────

  async expectSearchPlaceholder(): Promise<void> {
    this.logger?.step('expectSearchPlaceholder');
    await expect(this.searchInput).toHaveAttribute('placeholder', 'Search tags...');
  }

  async expectPageTitle(): Promise<void> {
    this.logger?.step('expectPageTitle');
    await expect(this.pageTitle).toContainText('Manage Tags');
  }

  async expectTagsNotFound(): Promise<void> {
    this.logger?.step('expectTagsNotFound');
    await expect(this.tagsNotFoundMessage).toContainText('No tags found for your search query');
  }

  async expectTagNameVisible(urn: string, name: string): Promise<void> {
    this.logger?.step('expectTagNameVisible', { urn, name });
    await this.searchForTag(name);
    await expect(this.getTagNameCell(urn)).toContainText(name);
    await this.clearSearch();
  }

  async expectTagInTable(name: string, description: string): Promise<void> {
    this.logger?.step('expectTagInTable', { name, description });
    await this.searchForTag(name);
    await expect(this.getTagNameCell(this.tagUrn(name))).toContainText(name);
    await expect(this.getTagDescriptionCell(this.tagUrn(name))).toContainText(description);
    await this.clearSearch();
  }

  async expectTagNotInTable(name: string): Promise<void> {
    this.logger?.step('expectTagNotInTable', { name });
    await this.searchForTag(name);
    await expect(this.getTagNameCell(this.tagUrn(name))).toBeHidden();
    await this.clearSearch();
  }
}
