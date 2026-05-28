/**
 * ChartPage — page object for the DataHub Chart entity profile.
 */

import { Page, Locator } from '@playwright/test';
import { BaseEntityPage, EntityType } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';

export class ChartPage extends BaseEntityPage {
  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
  }

  async navigateToChart(urn: string): Promise<void> {
    await this.navigateTo(EntityType.Chart, urn);
  }

  /** Returns a Locator for dashboard links in the Chart's dashboard list. */
  getDashboardLinks(): Locator {
    return this.page.locator('[data-testid="dashboard-link"]');
  }
}
