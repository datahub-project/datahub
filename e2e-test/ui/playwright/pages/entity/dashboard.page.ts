/**
 * DashboardPage — page object for the DataHub Dashboard entity profile.
 */

import { Page, Locator } from '@playwright/test';
import { BaseEntityPage, EntityType } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';

export class DashboardPage extends BaseEntityPage {
  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
  }

  async navigateToDashboard(urn: string): Promise<void> {
    await this.navigateTo(EntityType.Dashboard, urn);
  }

  async getChartCount(): Promise<number> {
    const charts = this.page.locator('[data-testid="chart-item"]');
    return charts.count();
  }

  /** Returns a Locator for chart links in the Dashboard's chart list. */
  getDashboardLinks(): Locator {
    return this.page.locator('[data-testid="chart-item"] a');
  }
}
