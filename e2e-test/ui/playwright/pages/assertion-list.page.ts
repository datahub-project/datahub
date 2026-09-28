import { expect, Locator, Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class AssertionListPage extends BasePage {
  readonly searchInput: Locator;
  readonly table: Locator;
  readonly rows: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.getByPlaceholder('Search...');
    this.table = page.getByTestId('assertions-table');
    this.rows = page.getByTestId('assertion-row');
  }

  async navigateToDatasetAssertions(urn: string): Promise<void> {
    await this.navigate(`/dataset/${encodeURIComponent(urn)}/Quality/List`);
    await this.waitForPageLoad();
  }

  async search(value: string): Promise<void> {
    await this.searchInput.fill(value);
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await this.page.waitForTimeout(600);
  }

  async expectAssertionVisible(description: string): Promise<void> {
    await expect(this.table).toBeVisible();
    await expect(this.rows.filter({ hasText: description })).toBeVisible();
  }
}
