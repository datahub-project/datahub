/**
 * EntityProfilePage — checking that an entity profile renders: header, tabs, and sidebar sections.
 *
 * The profile shell (header, tab strip, sidebar) is shared across entity types; only the set of
 * tabs differs, so one page object serves every type and the spec supplies the tabs to expect.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class EntityProfilePage extends BaseEntityPage {
  /** Uncaught errors the page threw while it rendered. */
  readonly pageErrors: string[] = [];

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    page.on('pageerror', (error) => this.pageErrors.push(error.message));
  }

  /** A profile tab, by its visible name. The sidebar has tabs of its own with some of the same names. */
  tab(name: string): Locator {
    return this.page.getByRole('tab').filter({ has: this.page.getByTestId(`${name}-entity-tab-header`) });
  }

  /** A sidebar section's content, by its title. */
  sidebarSection(title: string): Locator {
    return this.page.getByTestId(`sidebar-section-content-${title}`);
  }

  async openProfile(pathName: string, urn: string, name: string): Promise<void> {
    await this.navigate(`/${pathName}/${encodeURIComponent(urn)}/`);
    await expect(this.headerEntityName).toContainText(name, { timeout: TIMEOUTS.LONG });
  }

  async expectTabs(tabNames: readonly string[]): Promise<void> {
    for (const name of tabNames) {
      await expect(this.tab(name), `tab "${name}"`).toBeVisible({ timeout: TIMEOUTS.LONG });
    }
  }

  /** Open a tab and check it becomes the selected one. */
  async openTab(name: string): Promise<void> {
    this.logger?.step('open tab', { name });
    await this.tab(name).click();
    await expect(this.tab(name)).toHaveAttribute('aria-selected', 'true', { timeout: TIMEOUTS.MEDIUM });
  }

  async expectSidebarSections(titles: readonly string[]): Promise<void> {
    for (const title of titles) {
      await expect(this.sidebarSection(title), `sidebar section "${title}"`).toBeVisible({ timeout: TIMEOUTS.LONG });
    }
  }

  expectNoPageErrors(): void {
    expect(this.pageErrors, 'uncaught page errors while the profile rendered').toEqual([]);
  }
}
