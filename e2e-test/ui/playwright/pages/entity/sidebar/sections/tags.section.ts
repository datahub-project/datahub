/**
 * TagsSection — entity sidebar tags section.
 */

import { Page, expect } from '@playwright/test';
import { BaseSidebarSection } from './sidebar-section';

export class TagsSection extends BaseSidebarSection {
  constructor(page: Page) {
    super(page, '#entity-profile-tags');
  }

  async add(tagName: string): Promise<void> {
    await this.container.locator('[data-testid="add-tags-button"]').waitFor({ state: 'visible' });
    await this.container.locator('[data-testid="add-tags-button"]').click();
    await this.page.locator('[data-testid="tag-term-modal-input"]').filter({ hasText: '' }).fill(tagName);
    await this.page.locator('.ant-select-item-option-content').filter({ hasText: tagName }).first().click();
    await this.page.locator('[data-testid="add-tag-term-from-modal-btn"]').click();
  }

  async remove(tagName: string): Promise<void> {
    const tagEl = this.container.getByText(tagName);
    await tagEl.locator('xpath=following-sibling::*[1]').click();
    await this.page.getByText('Yes').click();
  }

  async expectTagVisible(name: string): Promise<void> {
    await expect(this.container.getByText(name)).toBeVisible();
  }

  async expectTagNotVisible(name: string): Promise<void> {
    await expect(this.container.getByText(name)).not.toBeVisible({ timeout: 10000 });
  }
}
