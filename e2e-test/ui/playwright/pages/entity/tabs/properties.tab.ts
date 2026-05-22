/**
 * PropertiesTab — manages the Properties tab on entity pages.
 *
 * Handles structured properties: add, remove, and verify.
 */

import { Page, Locator, expect } from '@playwright/test';
import type { Tab } from './tab.interface';

export class PropertiesTab implements Tab {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async open(): Promise<void> {
    await this.page.locator('#entity-sidebar-tabs-tab-Properties').click({ force: true });
    await this.page.waitForLoadState('networkidle');
  }

  async addStructuredProperty(propName: string, value: string): Promise<void> {
    await this.page.locator('[data-testid="add-structured-prop-button"]').click();
    await this.page.locator('body .ant-dropdown-menu').waitFor({ state: 'visible' });
    await this.page.locator('body .ant-dropdown-menu').locator(`text=${propName}`).click();
    await this.page.locator('[data-testid="structured-property-string-value-input"]').fill(value);
    await this.page.locator('[data-testid="add-update-structured-prop-on-entity-button"]').click();
    await expect(this.page.getByText('Successfully added structured property!')).toBeVisible({ timeout: 15000 });
  }

  async removeStructuredProperty(propName: string): Promise<void> {
    const row = this.page.locator('td').filter({ hasText: propName }).first();
    await row.locator('[data-testid="structured-prop-entity-more-icon"]').click();
    await this.page.locator('body .ant-dropdown-menu').getByText('Remove').click();
    await this.page.locator('[data-testid="modal-confirm-button"]').click();
  }

  expectPropertyVisible(propName: string, value?: string): Locator {
    if (value) {
      return this.page.locator('[data-testid="structured-prop-entity-more-icon"]').filter({
        hasText: propName,
      });
    }
    return this.page.getByText(propName);
  }
}
