/**
 * QueryTab — manages the Queries tab on entity pages.
 *
 * Covers: add, edit, delete queries and verify card details.
 */

import { Locator, Page, expect } from '@playwright/test';
import type { Tab } from './tab.interface';

export class QueryTab implements Tab {
  private readonly page: Page;

  // Selectors
  private readonly tabHeader: Locator;
  private readonly addButton: Locator;
  private readonly sqlEditor: Locator;
  private readonly titleInput: Locator;
  private readonly descriptionEditor: Locator;
  private readonly saveButton: Locator;
  private readonly modalContent: Locator;

  constructor(page: Page) {
    this.page = page;
    this.tabHeader = page.getByTestId('Queries-entity-tab-header');
    this.addButton = page.getByTestId('add-query-button');
    this.sqlEditor = page.locator('[data-mode-id="sql"]');
    this.titleInput = page.getByTestId('query-builder-title-input');
    this.descriptionEditor = page.locator('[contenteditable="true"]').first();
    this.saveButton = page.getByTestId('query-builder-save-button');
    this.modalContent = page.locator('.ant-modal-content');
  }

  private getEditButton(index: number): Locator {
    return this.page.getByTestId(`query-edit-button-${index}`);
  }

  private getMoreButton(index: number): Locator {
    return this.page.getByTestId(`query-more-button-${index}`);
  }

  private getCardContent(index: number): Locator {
    return this.page.getByTestId(`query-content-${index}`);
  }

  async open(): Promise<void> {
    await this.tabHeader.click();
    await this.page.waitForLoadState('networkidle');
  }

  async add(sql: string, title: string, description: string): Promise<void> {
    await this.addButton.click();
    await this.sqlEditor.click();
    await this.sqlEditor.pressSequentially(sql, { delay: 50 });
    await this.titleInput.click();
    await this.titleInput.fill(title);
    await this.descriptionEditor.click();
    await this.descriptionEditor.pressSequentially(description, { delay: 50 });
    await this.saveButton.click();
    await expect(this.page.getByText('Created Query!')).toBeVisible({ timeout: 15000 });
  }

  async edit(index: number, sql: string, title: string, description: string): Promise<void> {
    await this.getEditButton(index).click();
    await expect(this.sqlEditor).toBeVisible({ timeout: 10000 });
    await this.sqlEditor.click();
    await this.sqlEditor.pressSequentially(sql, { delay: 50 });
    await this.titleInput.click();
    await this.titleInput.clear();
    await this.titleInput.fill(title);
    await this.descriptionEditor.click();
    await this.descriptionEditor.pressSequentially(description, { delay: 50 });
    await this.saveButton.click();
    await expect(this.page.getByText('Edited Query!')).toBeVisible({ timeout: 15000 });
  }

  async delete(index: number): Promise<void> {
    await this.getMoreButton(index).click();
    await this.page.getByText('Delete').click();
    await this.page.getByText('Yes').click();
    await expect(this.page.getByText('Deleted Query!')).toBeVisible({ timeout: 15000 });
  }

  async verifyCardDetails(index: number, query: string, title: string, description: string): Promise<void> {
    const card = this.getCardContent(index);
    await card.scrollIntoViewIfNeeded();
    await expect(card).toBeVisible();
    await card.click();
    await expect(this.modalContent.getByText(query)).toBeVisible();
    await expect(this.modalContent.getByText(title)).toBeVisible();
    await expect(this.modalContent.getByText(description)).toBeVisible();
  }
}
