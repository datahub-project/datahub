/**
 * SchemaTab — manages the Schema tab on DatasetPage only.
 *
 * Opening a field via clickField() opens the FieldDrawer as a side effect.
 * All field-level interactions are on SchemaTab.drawer, not on DatasetPage directly.
 */

import { Page, expect } from '@playwright/test';
import type { Tab } from './tab.interface';
import { FieldDrawer } from './field-drawer';

export class SchemaTab implements Tab {
  private readonly page: Page;
  readonly drawer: FieldDrawer;

  constructor(page: Page) {
    this.page = page;
    this.drawer = new FieldDrawer(page);
  }

  async open(): Promise<void> {
    await this.page.locator('[data-testid="schema-tab"]').click();
    await this.page.waitForLoadState('networkidle');
  }

  async clickField(fieldName: string): Promise<void> {
    // Use the row's id attribute so the click reliably hits the row and opens the FieldDrawer.
    await this.page.locator(`#column-${fieldName}`).click();
  }

  async expectFieldVisible(fieldName: string): Promise<void> {
    await expect(this.page.getByText(fieldName)).toBeVisible();
  }

  async expectFieldNotVisible(fieldName: string): Promise<void> {
    await expect(this.page.getByText(fieldName)).not.toBeVisible();
  }

  async expectFieldTagVisible(fieldName: string, tagName: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="schema-field-${fieldName}-tags"]`).getByText(tagName),
    ).toBeVisible();
  }

  async expectFieldTagNotVisible(fieldName: string, tagName: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="schema-field-${fieldName}-tags"]`).getByText(tagName),
    ).not.toBeVisible();
  }

  async expectBlameDescriptionContains(fieldName: string, text: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="${fieldName}-schema-blame-description"]`),
    ).toContainText(text);
  }

  async clickBlameButton(): Promise<void> {
    await this.page.locator('[data-testid="schema-blame-button"]').click({ force: true });
  }

  /** Select a specific schema version from the version dropdown. */
  async selectVersion(versionPattern: RegExp): Promise<void> {
    await this.page.locator('.ant-select-selection-item').click({ force: true });
    await this.page.locator('.ant-select-dropdown').getByText(versionPattern).click({ force: true });
  }

  /** Click the schema history table icon (file-text icon). */
  async clickHistoryIcon(): Promise<void> {
    await this.page.locator('.anticon-file-text').click();
  }
}
