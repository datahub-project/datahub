/**
 * BaseEntityPage — Shared POM for entity pages.
 *
 * Provides navigation, header utilities, and sidebar access.
 * Entity-specific pages (DatasetPage, DocumentPage, etc.) extend this.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

/** Entity type path segments used in URL construction. */
export enum EntityType {
  Dataset = 'dataset',
  Dashboard = 'dashboard',
  Chart = 'chart',
  DataFlow = 'dataFlow',
  DataJob = 'dataJob',
  Domain = 'domain',
  GlossaryTerm = 'glossaryTerm',
  GlossaryNode = 'glossaryNode',
  DataProduct = 'dataProduct',
}

export class BaseEntityPage extends BasePage {
  readonly entityNameLocator: Locator;
  readonly deprecationBadge: Locator;
  readonly threeDotMenu: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.entityNameLocator = page.getByTestId('entity-name');
    this.deprecationBadge = page.getByTestId('deprecation-badge');
    this.threeDotMenu = page.getByTestId('entity-header-dropdown');
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  async navigateTo(type: EntityType, urn: string, path?: string, entityName?: string): Promise<void> {
    const suffix = path ? `/${path}` : '';
    await this.navigate(`/${type}/${encodeURIComponent(urn)}${suffix}`);
    await this.page.waitForLoadState('networkidle');
    if (entityName) {
      await expect(this.page.getByText(entityName, { exact: false })).toBeVisible({ timeout: TIMEOUTS.LONG });
    }
  }

  // ── Entity header ─────────────────────────────────────────────────────────

  async getEntityName(): Promise<string> {
    return (await this.entityNameLocator.textContent()) ?? '';
  }

  async clickMenuOption(option: string): Promise<void> {
    await this.threeDotMenu.click();
    await this.page.getByText(option).click();
  }

  async expectEntityNameVisible(): Promise<void> {
    await expect(this.entityNameLocator).toBeVisible({ timeout: TIMEOUTS.LONG });
  }
}
