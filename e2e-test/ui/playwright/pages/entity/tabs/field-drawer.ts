/**
 * FieldDrawer — manages the schema field side panel interactions.
 *
 * Opened as a side-effect of SchemaTab.clickField(). Extracts all field-level
 * interactions that were previously on DatasetPage.
 */

import { Page, Locator, expect } from '@playwright/test';

export class FieldDrawer {
  private readonly page: Page;

  readonly editFieldDescriptionButton: Locator;
  readonly fieldDescriptionEditor: Locator;
  readonly fieldDescriptionUpdateButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.editFieldDescriptionButton = page.locator('[data-testid="edit-field-description"]');
    this.fieldDescriptionEditor = page.locator('[data-testid="description-editor"] .ProseMirror');
    this.fieldDescriptionUpdateButton = page.locator('[data-testid="description-modal-update-button"]');
  }

  async editDescription(text: string): Promise<void> {
    await this.editFieldDescriptionButton.waitFor({ state: 'visible', timeout: 15000 });
    await this.editFieldDescriptionButton.click();
    await expect(this.page.getByText('Update description')).toBeVisible({ timeout: 15000 });
    await this.fieldDescriptionEditor.waitFor({ state: 'attached', timeout: 15000 });
    await this.fieldDescriptionEditor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.press('Delete');
    await this.page.keyboard.type(text);
    await this.fieldDescriptionUpdateButton.click();
    await expect(this.page.getByText('Updated!')).toBeVisible({ timeout: 15000 });
  }

  async clearDescription(): Promise<void> {
    await this.editDescription('');
  }

  async expectDescription(text: string): Promise<void> {
    await expect(this.page.getByText(text)).toBeVisible();
  }

  async addTag(tagName: string): Promise<void> {
    const addTagButton = this.page.locator('[data-testid="add-tags-button"]').first();
    await addTagButton.click();
    await this.page.locator('[data-testid="tag-term-modal-input"]').filter({ hasText: '' }).fill(tagName);
    await this.page.locator('.ant-select-item-option-content').filter({ hasText: tagName }).first().click();
    await this.page.locator('[data-testid="add-tag-term-from-modal-btn"]').click();
  }

  async addTerm(termName: string): Promise<void> {
    const addTermButton = this.page.locator('[data-testid="add-terms-button"]').first();
    await addTermButton.click();
    await this.page.locator('[data-testid="tag-term-modal-input"]').filter({ hasText: '' }).fill(termName);
    await this.page.locator('.ant-select-item-option-content').filter({ hasText: termName }).first().click();
    await this.page.locator('[data-testid="add-tag-term-from-modal-btn"]').click();
  }

  async expectTagVisible(tagName: string): Promise<void> {
    await expect(this.page.getByText(tagName)).toBeVisible();
  }

  async expectTermVisible(termName: string): Promise<void> {
    await expect(this.page.getByText(termName)).toBeVisible();
  }

  // ── Business attributes ────────────────────────────────────────────────────

  private getBusinessAttributeSection(): Locator {
    return this.page.locator('[data-testid="sidebar-section-content-Business Attribute"]');
  }

  async addBusinessAttribute(attributeName: string): Promise<void> {
    const section = this.getBusinessAttributeSection();
    await section.waitFor({ state: 'visible', timeout: 15000 });
    await section.evaluate((el: HTMLElement) => el.scrollIntoView({ block: 'center' }));
    await section.dispatchEvent('mouseover');
    const addButton = section.locator('text=Add Attribute');
    if ((await addButton.count()) === 0) return;
    await addButton.evaluate((el: HTMLElement) => el.click());
    const modalSearchInput = this.page
      .locator('[data-testid="business-attribute-modal-input"]')
      .locator('.ant-select-selection-search-input');
    await modalSearchInput.fill(attributeName);
    await this.page.locator('[data-testid="business-attribute-option"]').first().waitFor({ state: 'visible' });
    await modalSearchInput.press('ArrowDown');
    await modalSearchInput.press('Enter');
    const doneBtn = this.page.locator('[data-testid="add-attribute-from-modal-btn"]');
    await doneBtn.waitFor({ state: 'visible' });
    await expect(doneBtn).toBeEnabled();
    await doneBtn.evaluate((el: HTMLElement) => el.click());
    await expect(doneBtn).toBeHidden();
    await expect(section.getByText(attributeName)).toBeVisible();
  }

  async removeBusinessAttribute(attributeName: string): Promise<void> {
    const section = this.getBusinessAttributeSection();
    const closeIcon = section.locator('[aria-label="close"]').first();
    await closeIcon.evaluate((el: HTMLElement) => {
      el.scrollIntoView({ block: 'center' });
      el.click();
    });
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).waitFor({ state: 'visible' });
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).click({ force: true });
    await this.page.waitForLoadState('networkidle');
    await expect(section.getByText(attributeName)).not.toBeVisible({ timeout: 15000 });
  }

  async expectBusinessAttributeVisible(attributeName: string): Promise<void> {
    await expect(this.getBusinessAttributeSection().getByText(attributeName)).toBeVisible();
  }
}
