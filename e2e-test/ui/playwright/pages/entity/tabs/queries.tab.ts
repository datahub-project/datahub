/**
 * QueryTab — manages the Queries tab on entity pages.
 *
 * Covers: add, edit, delete queries and verify card details.
 */

import { Page, expect } from '@playwright/test';
import type { Tab } from './tab.interface';

export class QueryTab implements Tab {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async open(): Promise<void> {
    await this.page.locator('[data-testid="Queries-entity-tab-header"]').click();
    await this.page.waitForLoadState('networkidle');
  }

  async add(sql: string, title: string, description: string): Promise<void> {
    await this.page.locator('[data-testid="add-query-button"]').click();
    await this.page.locator('[data-mode-id="sql"]').click();
    await this.page.keyboard.type(sql);
    await this.page.locator('[data-testid="query-builder-title-input"]').click();
    await this.page.locator('[data-testid="query-builder-title-input"]').fill(title);
    await this.page.locator('.ProseMirror').click();
    await this.page.keyboard.type(description);
    await this.page.locator('[data-testid="query-builder-save-button"]').click();
    await expect(this.page.getByText('Created Query!')).toBeVisible({ timeout: 15000 });
  }

  async edit(index: number, sql: string, title: string, description: string): Promise<void> {
    await this.page.locator(`[data-testid="query-edit-button-${index}"]`).click();
    await expect(this.page.locator('[data-mode-id="sql"]')).toBeVisible({ timeout: 10000 });
    await this.page.locator('[data-mode-id="sql"]').click();
    await this.page.keyboard.type(sql);
    await this.page.locator('[data-testid="query-builder-title-input"]').click();
    await this.page.locator('[data-testid="query-builder-title-input"]').clear();
    await this.page.locator('[data-testid="query-builder-title-input"]').fill(title);
    await this.page.locator('.ProseMirror').click();
    await this.page.locator('.ProseMirror').clear();
    await this.page.keyboard.type(description);
    await this.page.locator('[data-testid="query-builder-save-button"]').click();
    await expect(this.page.getByText('Edited Query!')).toBeVisible({ timeout: 15000 });
  }

  async delete(index: number): Promise<void> {
    await this.page.locator(`[data-testid="query-more-button-${index}"]`).click();
    await this.page.getByText('Delete').click();
    await this.page.getByText('Yes').click();
    await expect(this.page.getByText('Deleted Query!')).toBeVisible({ timeout: 15000 });
  }

  async verifyCardDetails(index: number, query: string, title: string, description: string): Promise<void> {
    const card = this.page.locator(`[data-testid="query-content-${index}"]`);
    await card.scrollIntoViewIfNeeded();
    await expect(card).toBeVisible();
    await card.click();
    const modal = this.page.locator('.ant-modal-content');
    await expect(modal.getByText(query)).toBeVisible();
    await expect(modal.getByText(title)).toBeVisible();
    await expect(modal.getByText(description)).toBeVisible();
  }
}
