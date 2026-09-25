import type { Page, Locator } from '@playwright/test';
import { expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

/**
 * Analytics page object — semantic methods for core workflows.
 *
 * Methods combine related operations (navigation + verification) to reduce redundancy.
 */
export class AnalyticsPage extends BasePage {
  private readonly entityHeader: Locator;
  private readonly landscapeSummarySection: Locator;
  private readonly tabViewsChart: Locator;

  constructor(
    readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);
    this.entityHeader = this.page.getByTestId('entity-header-test-id');
    this.landscapeSummarySection = this.page.getByTestId('analytics-landscape-summary');
    this.tabViewsChart = this.page.getByTestId('analytics-tab-views-by-entity-type-(past-week)');
  }

  // Helper selectors for dynamic elements
  private getTabSelector(tabName: string): Locator {
    return this.page.getByRole('tab', { name: tabName });
  }

  private getLegendItemSelector(tabName: string): Locator {
    return this.page.getByTestId(`analytics-entity-type-${tabName.toLowerCase()}`);
  }

  /**
   * Navigate to chart and wait for load.
   */
  async goToChart(urn: string): Promise<void> {
    this.logger?.step('navigate to chart', { urn });
    await this.navigate(`/chart/${urn}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  /**
   * Navigate to analytics page and wait for load.
   */
  async goToAnalytics(): Promise<void> {
    this.logger?.step('navigate to analytics', {});
    await this.navigate('/analytics');
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  /**
   * Verify chart title is visible in entity header.
   */
  async verifyChartTitle(title: string): Promise<void> {
    this.logger?.step('verify chart title', { title });
    await expect(this.entityHeader.getByText(title)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify Data Landscape Summary section is visible.
   */
  async verifyLandscapeSummaryVisible(): Promise<void> {
    this.logger?.step('verify landscape summary visible', {});
    await expect(this.landscapeSummarySection).toBeVisible({ timeout: TIMEOUTS.SHORT });
  }

  /**
   * Click on entity tab (e.g., Dashboards, Summary) and wait for content to load.
   */
  async clickTab(tabName: string): Promise<void> {
    this.logger?.step('click tab', { tabName });
    const tab = this.getTabSelector(tabName);
    await expect(tab).toBeVisible();
    await tab.click();
    // Wait for tab content to load after click
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  /**
   * Verify Tab Views chart is visible and scroll into viewport if needed.
   */
  async verifyTabViewsChartVisible(): Promise<void> {
    this.logger?.step('verify Tab Views chart visible', {});
    await expect(this.tabViewsChart).toBeVisible({ timeout: TIMEOUTS.LONG });
    await this.tabViewsChart.scrollIntoViewIfNeeded();
  }

  /**
   * Verify tab interaction appears in chart legend (indicates tab click was tracked).
   */
  async verifyTabInteractionTracked(tabName: string): Promise<void> {
    this.logger?.step('verify tab interaction tracked', { tabName });
    const legendItem = this.getLegendItemSelector(tabName);
    await expect(legendItem).toBeVisible({ timeout: TIMEOUTS.SHORT });
  }
}
