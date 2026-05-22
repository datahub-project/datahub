/**
 * OwnersSection — entity sidebar owners section.
 *
 * Ownership management was on DatasetPage (V1); this section consolidates it
 * for all entity types.
 */

import { Page, expect } from '@playwright/test';
import { BaseSidebarSection } from './sidebar-section';

export class OwnersSection extends BaseSidebarSection {
  constructor(page: Page) {
    // #entity-profile-owners is a common container; add-owners-button is a fallback
    super(page, '#entity-profile-owners, [data-testid="add-owners-button"]');
  }

  async add(name: string, type: string): Promise<void> {
    await this.page.locator('[data-testid="add-owners-button"]').first().click({ force: true });
    await expect(this.page.locator('[data-testid="add-owners-select"]')).toBeVisible({ timeout: 10000 });
    await this.page.locator('[data-testid="add-owners-select-base"]').click({ force: true });
    await this.page.locator('[data-testid="dropdown-search-input"]').fill(name);
    await this.page.locator('[data-testid="add-owners-select-dropdown"]').getByText(name).click({ force: true });
    await expect(this.page.getByText(name)).toBeVisible();
    await this.page.getByRole('dialog').getByText('Technical Owner').click();
    await this.page.getByRole('listbox').locator('..').getByText(type).click();
    await expect(this.page.getByRole('dialog').getByText(type)).toBeVisible();
    await this.page.getByText('Done').click();
    await expect(this.page.getByText('Owners Added')).toBeVisible({ timeout: 15000 });
  }

  // elementId is a dynamic selector (e.g. href targeting a specific user/group URN)
  async remove(owner: string, elementId: string): Promise<void> {
    await this.page.locator(elementId).locator('xpath=following-sibling::*[1]').click();
    await this.page.getByText('Yes').click();
    await expect(this.page.getByText('Owner Removed')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(owner)).not.toBeVisible({ timeout: 10000 });
  }

  async expectOwnerVisible(name: string): Promise<void> {
    await expect(this.container.getByText(name)).toBeVisible();
  }
}
