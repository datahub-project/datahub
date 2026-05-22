/**
 * BaseSidebarSection — base class for all entity sidebar sections.
 *
 * Provides a shared `container` locator, scrollIntoView(), and expectVisible().
 */

import { Page, Locator, expect } from '@playwright/test';

export abstract class BaseSidebarSection {
  protected readonly page: Page;
  readonly container: Locator;

  constructor(page: Page, containerSelector: string) {
    this.page = page;
    this.container = page.locator(containerSelector);
  }

  async scrollIntoView(): Promise<void> {
    await this.container.scrollIntoViewIfNeeded();
  }

  async expectVisible(): Promise<void> {
    await expect(this.container).toBeVisible();
  }
}
