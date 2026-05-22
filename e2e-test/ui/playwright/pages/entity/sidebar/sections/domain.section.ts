/**
 * DomainSection — entity sidebar domain section.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BaseSidebarSection } from './sidebar-section';

export class DomainSection extends BaseSidebarSection {
  private readonly setDomainButton: Locator;
  private readonly setDomainSelect: Locator;
  private readonly submitButton: Locator;
  private readonly unsetDomainButton: Locator;

  constructor(page: Page) {
    super(page, '#entity-profile-domains');
    this.setDomainButton = this.container.locator('[data-testid="set-domain-button"]');
    this.setDomainSelect = page.locator('[data-testid="set-domain-select"]');
    this.submitButton = page.locator('[data-testid="submit-button"]');
    this.unsetDomainButton = this.container.locator('[data-testid="unset-domain-button"]');
  }

  async set(domainName: string): Promise<void> {
    await this.setDomainButton.click();
    await this.setDomainSelect.fill(domainName);
    await this.page.locator('.ant-select-item-option-content').filter({ hasText: domainName }).click();
    await this.submitButton.click();
  }

  async remove(): Promise<void> {
    await this.unsetDomainButton.click();
    await this.page.getByText('Yes').click();
  }

  async expectDomainVisible(name: string): Promise<void> {
    await expect(this.container.getByText(name)).toBeVisible();
  }
}
