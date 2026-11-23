/**
 * EntityDeprecationPage — marking an entity deprecated and undoing it from the profile header menu.
 *
 * The header menu, the Mark as Deprecated modal and the deprecation icon are shared components, so
 * one page object serves every entity type. The icon is shown in the profile header and on the
 * entity's search result card.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class EntityDeprecationPage extends BaseEntityPage {
  readonly headerMenuButton: Locator;
  readonly deprecateMenuItem: Locator;
  // Mark as Deprecated modal
  readonly reasonInput: Locator;
  readonly deprecateSubmitButton: Locator;
  /** The deprecation icon in the profile header. */
  readonly headerDeprecationIcon: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.headerMenuButton = page.getByTestId('MoreVertOutlinedIcon');
    // The same menu item reads "Mark as Deprecated" or "Mark as Un-Deprecated" depending on state.
    this.deprecateMenuItem = page.getByTestId('entity-menu-deprecate-button');
    this.reasonInput = page.getByPlaceholder('Add Reason');
    this.deprecateSubmitButton = page.getByTestId('add-deprecation-submit');
    this.headerDeprecationIcon = page.getByTestId('entity-header-test-id').getByTestId('deprecation-icon');
  }

  async openProfile(pathName: string, urn: string, name: string): Promise<void> {
    await this.navigate(`/${pathName}/${encodeURIComponent(urn)}/`);
    await expect(this.headerEntityName).toContainText(name, { timeout: TIMEOUTS.LONG });
    await expect(this.headerMenuButton.first()).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  /** The deprecation icon on the entity's search result card. */
  searchCardDeprecationIcon(urn: string): Locator {
    return this.page.getByTestId(`preview-${urn}`).getByTestId('deprecation-icon');
  }

  async openHeaderMenu(): Promise<void> {
    await this.headerMenuButton.first().click();
    await expect(this.deprecateMenuItem).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async markDeprecated(reason: string): Promise<void> {
    this.logger?.step('mark deprecated', { reason });
    await this.openHeaderMenu();
    await expect(this.deprecateMenuItem).toContainText('Mark as Deprecated');
    await this.deprecateMenuItem.click();
    await this.reasonInput.fill(reason);
    await this.deprecateSubmitButton.click();
    await expect(this.headerDeprecationIcon).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async markUndeprecated(): Promise<void> {
    this.logger?.step('mark undeprecated');
    await this.openHeaderMenu();
    await expect(this.deprecateMenuItem).toContainText(/un-deprecated/i);
    await this.deprecateMenuItem.click();
    await expect(this.headerDeprecationIcon).toBeHidden({ timeout: TIMEOUTS.LONG });
  }

  // ── Assertions ─────────────────────────────────────────────────────────────

  async expectDeprecatedInHeader(): Promise<void> {
    await expect(this.headerDeprecationIcon).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectNotDeprecatedInHeader(): Promise<void> {
    // The menu button only exists once the header has rendered; a hidden assertion passes on zero
    // elements, so wait for it before asserting the icon away.
    await expect(this.headerMenuButton.first()).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.headerDeprecationIcon).toBeHidden({ timeout: TIMEOUTS.LONG });
  }

  /** Hover the header icon and check the popover shows the reason. */
  async expectReasonInPopover(reason: string): Promise<void> {
    await this.headerDeprecationIcon.hover();
    await expect(this.page.getByText(reason, { exact: true })).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /** Search for the entity by name and check its result card shows (or does not show) the icon. */
  async expectSearchCardDeprecation(name: string, urn: string, deprecated: boolean): Promise<void> {
    // Search indexing is asynchronous, so search again until the card reflects the change.
    await expect(async () => {
      await this.navigate(`/search?query=${encodeURIComponent(name)}`);
      const card = this.page.getByTestId(`preview-${urn}`);
      await expect(card).toBeVisible({ timeout: TIMEOUTS.SHORT });
      if (deprecated) {
        await expect(this.searchCardDeprecationIcon(urn)).toBeVisible({ timeout: TIMEOUTS.SHORT });
      } else {
        await expect(this.searchCardDeprecationIcon(urn)).toBeHidden({ timeout: TIMEOUTS.SHORT });
      }
    }).toPass({ timeout: TIMEOUTS.EXTRA_LONG * 2 });
  }
}
