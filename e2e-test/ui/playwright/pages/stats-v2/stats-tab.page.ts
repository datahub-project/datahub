import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

/**
 * Stats Tab V2 Page Object
 *
 * Manages the complete statistics tab for datasets, including:
 * - Navigation and tab visibility
 * - Highlight cards (Latest/Last Month stats)
 * - Charts (Row Count, Query, Storage)
 * - Column statistics table
 * - Time range filtering
 *
 * Complex interactions (Change History calendar) delegated to helper.
 *
 * Usage:
 *   const statsTab = new StatsTabPage(page);
 *   await statsTab.navigateToDatasetStats(urn);
 *   await statsTab.verifyLatestStatsCard('rows-card', '1000');
 */
export class StatsTabPage extends BasePage {
  // ── Selectors ──────────────────────────────────────────────────────────────
  readonly statsTab: Locator;
  readonly statsContainer: Locator;

  // Highlight Cards
  readonly latestStatsContainer: Locator;
  readonly lastMonthStatsContainer: Locator;

  // Charts
  readonly rowCountCard: Locator;
  readonly rowCountChart: Locator;
  readonly queryCountCard: Locator;
  readonly queryCountChart: Locator;
  readonly storageSizeCard: Locator;
  readonly storageSizeChart: Locator;
  readonly changeHistoryChart: Locator;

  // Column Stats
  readonly columnStatsContainer: Locator;

  // Time Range Selectors (chart-specific to avoid strict mode violations)
  readonly rowCountTimeRangeSelect: Locator;

  // Chart Empty States
  readonly rowCountChartEmpty: Locator;
  readonly queryCountChartEmpty: Locator;
  readonly storageSizeChartEmpty: Locator;
  readonly changeHistoryChartEmpty: Locator;

  // Changes Card & Schema Drawer
  readonly changesCard: Locator;
  readonly schemaFieldDrawerContent: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    // Navigation
    this.statsTab = page.getByTestId('Stats-entity-tab-header');
    this.statsContainer = page.getByTestId('stats-tab-container');

    // Highlight Cards
    this.latestStatsContainer = page.getByTestId('latest-stats');
    this.lastMonthStatsContainer = page.getByTestId('last-month-stats');

    // Charts
    this.rowCountCard = page.getByTestId('row-count-card');
    this.rowCountChart = page.getByTestId('row-count-chart');
    this.queryCountCard = page.getByTestId('query-count-card');
    this.queryCountChart = page.getByTestId('query-count-chart');
    this.storageSizeCard = page.getByTestId('storage-size-card');
    this.storageSizeChart = page.getByTestId('storage-size-chart');
    this.changeHistoryChart = page.getByTestId('change-history-chart');

    // Column Stats
    this.columnStatsContainer = page.getByTestId('column-stats-container');

    // Time Range Selectors - scoped to specific chart cards
    this.rowCountTimeRangeSelect = this.rowCountCard.getByTestId('timerange-select');

    // Chart Empty States
    this.rowCountChartEmpty = page.getByTestId('row-count-chart-empty');
    this.queryCountChartEmpty = page.getByTestId('query-count-chart-empty');
    this.storageSizeChartEmpty = page.getByTestId('storage-size-chart-empty');
    this.changeHistoryChartEmpty = page.getByTestId('change-history-chart-empty');

    // Changes Card & Schema Drawer
    this.changesCard = page.getByTestId('changes-card');
    this.schemaFieldDrawerContent = page.getByTestId('schema-field-drawer-content');
  }

  // ── Dynamic Selectors (Parameterized) ─────────────────────────────────────

  /**
   * Get a time range dropdown option by name
   */
  private getTimeRangeOption(timeRange: string): Locator {
    return this.page.getByTestId(`option-${timeRange}`);
  }

  /**
   * Get a time range selected value indicator
   */
  private getTimeRangeSelectedValue(timeRange: string): Locator {
    return this.page.getByTestId(`value-${timeRange}`);
  }

  /**
   * Get a column row in the statistics table by column name
   */
  private getColumnRow(columnName: string): Locator {
    return this.columnStatsContainer.getByTestId(`row-${columnName}`);
  }

  /**
   * Get a card from latest stats container
   */
  private getLatestStatsCard(cardTestId: string): Locator {
    return this.latestStatsContainer.getByTestId(cardTestId);
  }

  /**
   * Get a card from last month stats container
   */
  private getLastMonthStatsCard(cardTestId: string): Locator {
    return this.lastMonthStatsContainer.getByTestId(cardTestId);
  }

  /**
   * Get button from a card by testId
   */
  private getCardButton(card: Locator): Locator {
    return card.getByRole('button');
  }

  /**
   * Get button from changes card
   */
  private getChangesCardButton(): Locator {
    return this.changesCard.getByRole('button');
  }

  /**
   * Get cell from column row
   */
  private getCellFromRow(row: Locator, fieldTestId: string): Locator {
    return row.getByTestId(fieldTestId);
  }

  /**
   * Get no-permissions element from chart
   */
  private getNoPermissionsElement(chartTestId: string): Locator {
    return this.page.getByTestId(chartTestId).getByTestId('no-permissions');
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  /**
   * Navigate to a dataset's statistics tab
   */
  async navigateToDatasetStats(datasetUrn: string): Promise<void> {
    this.logger?.step('navigateToDatasetStats', { datasetUrn });
    await this.navigate(`/dataset/${encodeURIComponent(datasetUrn)}/Stats`);
    await this.waitForPageLoad();
  }

  // ── Tab Visibility ────────────────────────────────────────────────────────

  /**
   * Get stats tab element via role (for attribute assertions)
   * Returns the tab element itself rather than testid-based locator
   */
  getStatsTabElement(): Locator {
    return this.page.getByRole('tab', { name: 'Stats' });
  }

  /**
   * Click stats tab to open it
   */
  async clickStatsTab(): Promise<void> {
    this.logger?.step('clickStatsTab');
    await this.statsTab.click({ timeout: TIMEOUTS.SHORT });
    await this.statsContainer.waitFor({
      state: 'visible',
      timeout: TIMEOUTS.MEDIUM,
    });
  }

  // ── Highlight Cards ───────────────────────────────────────────────────────

  /**
   * Verify a stat card in Latest Stats section
   */
  async verifyLatestStatsCard(cardTestId: string, expectedValue: string): Promise<void> {
    this.logger?.step('verifyLatestStatsCard', { cardTestId, expectedValue });
    const card = this.getLatestStatsCard(cardTestId);
    await expect(card).toContainText(expectedValue);
  }

  /**
   * Verify a stat card in Last Month Stats section
   */
  async verifyLastMonthStatsCard(cardTestId: string, expectedValue: string): Promise<void> {
    this.logger?.step('verifyLastMonthStatsCard', { cardTestId, expectedValue });
    const card = this.getLastMonthStatsCard(cardTestId);
    await expect(card).toContainText(expectedValue);
  }

  /**
   * Verify a stat card does NOT contain a specific value (for null/empty checks)
   */
  async verifyLatestStatsCardDoesNotContain(cardTestId: string, unexpectedValue: string): Promise<void> {
    this.logger?.step('verifyLatestStatsCardDoesNotContain', { cardTestId, unexpectedValue });
    const card = this.getLatestStatsCard(cardTestId);
    await expect(card).not.toContainText(unexpectedValue);
  }

  /**
   * Verify latest stats container is visible
   */
  async verifyLatestStatsVisible(): Promise<void> {
    this.logger?.step('verifyLatestStatsVisible');
    await this.latestStatsContainer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify last month stats container is visible
   */
  async verifyLastMonthStatsVisible(): Promise<void> {
    this.logger?.step('verifyLastMonthStatsVisible');
    await this.lastMonthStatsContainer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify view button is visible for a card in latest stats
   */
  async verifyLatestStatsCardButtonVisible(cardTestId: string): Promise<void> {
    this.logger?.step('verifyLatestStatsCardButtonVisible', { cardTestId });
    const card = this.getLatestStatsCard(cardTestId);
    const button = this.getCardButton(card);
    await expect(button).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify view button is visible for a card in last month stats
   */
  async verifyLastMonthStatsCardButtonVisible(cardTestId: string): Promise<void> {
    this.logger?.step('verifyLastMonthStatsCardButtonVisible', { cardTestId });
    const card = this.getLastMonthStatsCard(cardTestId);
    const button = this.getCardButton(card);
    await expect(button).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify changes card is visible
   */
  async verifyChangesCardVisible(): Promise<void> {
    this.logger?.step('verifyChangesCardVisible');
    await expect(this.changesCard).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify changes card contains expected value
   */
  async verifyChangesCardValue(expectedValue: string): Promise<void> {
    this.logger?.step('verifyChangesCardValue', { expectedValue });
    await expect(this.changesCard).toContainText(expectedValue);
  }

  /**
   * Verify changes card button is visible
   */
  async verifyChangesCardButtonVisible(): Promise<void> {
    this.logger?.step('verifyChangesCardButtonVisible');
    const button = this.getChangesCardButton();
    await expect(button).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  // ── Charts - Row Count ────────────────────────────────────────────────────

  /**
   * Verify row count chart is visible with data
   */
  async verifyRowCountChartIsVisible(): Promise<void> {
    this.logger?.step('verifyRowCountChartIsVisible');
    await this.rowCountChart.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify row count chart is empty
   */
  async verifyRowCountChartIsEmpty(): Promise<void> {
    this.logger?.step('verifyRowCountChartIsEmpty');
    await this.rowCountChartEmpty.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── Charts - Query Count ──────────────────────────────────────────────────

  /**
   * Verify query count chart is visible with data
   */
  async verifyQueryCountChartIsVisible(): Promise<void> {
    this.logger?.step('verifyQueryCountChartIsVisible');
    await this.queryCountChart.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify query count chart is empty
   */
  async verifyQueryCountChartIsEmpty(): Promise<void> {
    this.logger?.step('verifyQueryCountChartIsEmpty');
    await this.queryCountChartEmpty.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── Charts - Storage Size ─────────────────────────────────────────────────

  /**
   * Verify storage size chart is visible with data
   */
  async verifyStorageSizeChartIsVisible(): Promise<void> {
    this.logger?.step('verifyStorageSizeChartIsVisible');
    await this.storageSizeChart.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify storage size chart is empty
   */
  async verifyStorageSizeChartIsEmpty(): Promise<void> {
    this.logger?.step('verifyStorageSizeChartIsEmpty');
    await this.storageSizeChartEmpty.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── Charts - Change History ───────────────────────────────────────────────

  /**
   * Verify change history chart is visible with data
   */
  async verifyChangeHistoryChartIsVisible(): Promise<void> {
    this.logger?.step('verifyChangeHistoryChartIsVisible');
    await this.changeHistoryChart.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify change history chart is empty
   */
  async verifyChangeHistoryChartIsEmpty(): Promise<void> {
    this.logger?.step('verifyChangeHistoryChartIsEmpty');
    await this.changeHistoryChartEmpty.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── Time Range Filtering ──────────────────────────────────────────────────

  /**
   * Verify time range selector is visible
   * Checks row count chart selector as representative (all have same visibility)
   */
  async verifyTimeRangeSelectorExists(): Promise<void> {
    this.logger?.step('verifyTimeRangeSelectorExists');
    await this.rowCountTimeRangeSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify time range selector is NOT visible
   * Checks row count chart selector as representative
   */
  async verifyTimeRangeSelectorDoesNotExist(): Promise<void> {
    this.logger?.step('verifyTimeRangeSelectorDoesNotExist');
    await expect(this.rowCountTimeRangeSelect).toBeHidden();
  }

  // ── Time Range Verification ────────────────────────────────────────────────

  /**
   * Verify a specific time range option is available
   * Uses row count chart selector to open dropdown
   */
  async verifyTimeRangeOptionExists(timeRange: string): Promise<void> {
    this.logger?.step('verifyTimeRangeOptionExists', { timeRange });
    // Click dropdown to open it
    await this.rowCountTimeRangeSelect.click();
    // Wait for the option to become visible
    await this.getTimeRangeOption(timeRange).waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify multiple time range options are available
   * Opens dropdown, checks all options exist, then closes dropdown
   */
  async verifyTimeRangeOptionsExist(timeRanges: string[]): Promise<void> {
    this.logger?.step('verifyTimeRangeOptionsExist', { timeRanges });
    // Open dropdown
    await this.rowCountTimeRangeSelect.click();
    // Verify each option is visible
    for (const timeRange of timeRanges) {
      await expect(this.getTimeRangeOption(timeRange)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    }
    // Close dropdown
    await this.rowCountTimeRangeSelect.click();
  }

  /**
   * Verify a specific time range option is NOT available
   */
  async verifyTimeRangeOptionDoesNotExist(timeRange: string): Promise<void> {
    this.logger?.step('verifyTimeRangeOptionDoesNotExist', { timeRange });
    await expect(this.getTimeRangeOption(timeRange)).toBeHidden();
  }

  /**
   * Verify multiple time range options are NOT available
   * Opens dropdown, checks all options are hidden, then closes dropdown
   */
  async verifyTimeRangeOptionsDoNotExist(timeRanges: string[]): Promise<void> {
    this.logger?.step('verifyTimeRangeOptionsDoNotExist', { timeRanges });
    // Open dropdown
    await this.rowCountTimeRangeSelect.click();
    // Verify each option is hidden
    for (const timeRange of timeRanges) {
      await expect(this.getTimeRangeOption(timeRange)).toBeHidden();
    }
    // Close dropdown
    await this.rowCountTimeRangeSelect.click();
  }

  /**
   * Verify a specific time range is selected/highlighted
   */
  async verifyTimeRangeSelected(timeRange: string): Promise<void> {
    this.logger?.step('verifyTimeRangeSelected', { timeRange });
    await expect(this.getTimeRangeSelectedValue(timeRange)).toBeVisible();
  }

  // ── Column Stats Table ────────────────────────────────────────────────────

  /**
   * Click on a column row to open details
   */
  async clickColumnRow(columnName: string): Promise<void> {
    this.logger?.step('clickColumnRow', { columnName });
    await this.getColumnRow(columnName).click({ timeout: TIMEOUTS.SHORT });
    await this.schemaFieldDrawerContent.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify column has specific field values (e.g., type, nullPercentage, uniqueValues)
   * Matches each fieldTestId with expected value within the column row
   */
  async verifyColumnFields(columnName: string, fields: Record<string, string>): Promise<void> {
    this.logger?.step('verifyColumnFields', { columnName, fields });
    const row = this.getColumnRow(columnName);
    await row.evaluate((el) => el.scrollIntoView());

    for (const [fieldTestId, expectedValue] of Object.entries(fields)) {
      const cell = this.getCellFromRow(row, fieldTestId);
      await expect(cell).toContainText(expectedValue);
    }
  }

  /**
   * Verify schema field drawer is open after clicking column
   */
  async verifySchemaFieldDrawerOpen(): Promise<void> {
    this.logger?.step('verifySchemaFieldDrawerOpen');
    const drawerContent = this.schemaFieldDrawerContent;
    await drawerContent.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── No Permission States ──────────────────────────────────────────────────

  /**
   * Verify "no permissions" message is shown for a chart
   */
  async verifyNoPermissionsForChart(chartTestId: string): Promise<void> {
    this.logger?.step('verifyNoPermissionsForChart', { chartTestId });
    const noPermission = this.getNoPermissionsElement(chartTestId);
    await noPermission.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }
}
