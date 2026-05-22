/**
 * GlossaryTermsSection — entity sidebar glossary terms section.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BaseSidebarSection } from './sidebar-section';

export class GlossaryTermsSection extends BaseSidebarSection {
  private readonly addTermsButton: Locator;
  private readonly termModalInput: Locator;
  private readonly addTermFromModalBtn: Locator;

  constructor(page: Page) {
    super(page, '#entity-profile-glossary-terms');
    this.addTermsButton = this.container.locator('[data-testid="add-terms-button"]');
    this.termModalInput = page.locator('[data-testid="tag-term-modal-input"]').filter({ hasText: '' });
    this.addTermFromModalBtn = page.locator('[data-testid="add-tag-term-from-modal-btn"]');
  }

  async add(termName: string): Promise<void> {
    await this.addTermsButton.click();
    await this.termModalInput.fill(termName);
    await this.page.locator('.ant-select-item-option-content').filter({ hasText: termName }).first().click();
    await this.addTermFromModalBtn.click();
  }

  async remove(termName: string): Promise<void> {
    const termEl = this.container.getByText(termName);
    await termEl.locator('xpath=following-sibling::*[1]').click();
    await this.page.getByText('Yes').click();
  }

  async expectTermVisible(name: string): Promise<void> {
    await expect(this.container.getByText(name)).toBeVisible();
  }
}
