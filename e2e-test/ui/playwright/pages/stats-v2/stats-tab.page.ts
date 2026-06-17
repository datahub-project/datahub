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
  readonly changeHistoryCard: Locator;
  readonly changeHistoryChart: Locator;

  // Column Stats
  readonly columnStatsContainer: Locator;
  readonly columnStatsSearchBar: Locator;

  // Time Range Selectors (chart-specific to avoid strict mode violations)
  readonly rowCountTimeRangeSelect: Locator;
  readonly queryCountTimeRangeSelect: Locator;
  readonly storageSizeTimeRangeSelect: Locator;

  // Common
  readonly loadingSpinner: Locator;

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
    this.changeHistoryCard = page.getByTestId('change-history-card');
    this.changeHistoryChart = page.getByTestId('change-history-chart');

    // Column Stats
    this.columnStatsContainer = page.getByTestId('column-stats-container');
    this.columnStatsSearchBar = page.getByTestId('column-stats-search-bar');

    // Time Range Selectors - scoped to specific chart cards
    this.rowCountTimeRangeSelect = this.rowCountCard.getByTestId('timerange-select');
    this.queryCountTimeRangeSelect = this.queryCountCard.getByTestId('timerange-select');
    this.storageSizeTimeRangeSelect = this.storageSizeCard.getByTestId('timerange-select');

    // Common
    this.loadingSpinner = page.getByRole('status');
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
   * Verify stats tab exists and is visible
   */
  async verifyStatsTabIsVisible(): Promise<void> {
    this.logger?.step('verifyStatsTabIsVisible');
    await this.statsTab.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify stats container is loaded and visible
   */
  async verifyStatsContainerLoaded(): Promise<void> {
    this.logger?.step('verifyStatsContainerLoaded');
    await this.statsContainer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
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
    const card = this.latestStatsContainer.getByTestId(cardTestId);
    await expect(card).toContainText(expectedValue);
  }

  /**
   * Verify a stat card in Last Month Stats section
   */
  async verifyLastMonthStatsCard(cardTestId: string, expectedValue: string): Promise<void> {
    this.logger?.step('verifyLastMonthStatsCard', { cardTestId, expectedValue });
    const card = this.lastMonthStatsContainer.getByTestId(cardTestId);
    await expect(card).toContainText(expectedValue);
  }

  /**
   * Get value from a stat card
   */
  async getStatsCardValue(container: Locator, cardTestId: string): Promise<string> {
    const card = container.getByTestId(cardTestId);
    return card.textContent().then((content) => content?.trim() || '');
  }

  /**
   * Verify a stat card does NOT contain a specific value (for null/empty checks)
   */
  async verifyLatestStatsCardDoesNotContain(cardTestId: string, unexpectedValue: string): Promise<void> {
    this.logger?.step('verifyLatestStatsCardDoesNotContain', { cardTestId, unexpectedValue });
    const card = this.latestStatsContainer.getByTestId(cardTestId);
    const text = await card.textContent();
    if (text?.includes(unexpectedValue)) {
      throw new Error(`Card ${cardTestId} should not contain "${unexpectedValue}" but text was: "${text}"`);
    }
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
    const button = this.latestStatsContainer.getByTestId(cardTestId).getByRole('button');
    await expect(button).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify view button is visible for a card in last month stats
   */
  async verifyLastMonthStatsCardButtonVisible(cardTestId: string): Promise<void> {
    this.logger?.step('verifyLastMonthStatsCardButtonVisible', { cardTestId });
    const button = this.lastMonthStatsContainer.getByTestId(cardTestId).getByRole('button');
    await expect(button).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify changes card is visible
   */
  async verifyChangesCardVisible(): Promise<void> {
    this.logger?.step('verifyChangesCardVisible');
    const changesCard = this.page.getByTestId('changes-card');
    await expect(changesCard).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify changes card contains expected value
   */
  async verifyChangesCardValue(expectedValue: string): Promise<void> {
    this.logger?.step('verifyChangesCardValue', { expectedValue });
    const changesCard = this.page.getByTestId('changes-card');
    await expect(changesCard).toContainText(expectedValue);
  }

  /**
   * Verify changes card button is visible
   */
  async verifyChangesCardButtonVisible(): Promise<void> {
    this.logger?.step('verifyChangesCardButtonVisible');
    const button = this.page.getByTestId('changes-card').getByRole('button');
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
    const emptyState = this.page.getByTestId('row-count-chart-empty');
    await emptyState.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
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
    const emptyState = this.page.getByTestId('query-count-chart-empty');
    await emptyState.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
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
    const emptyState = this.page.getByTestId('storage-size-chart-empty');
    await emptyState.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
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
    const emptyState = this.page.getByTestId('change-history-chart-empty');
    await emptyState.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── Time Range Filtering ──────────────────────────────────────────────────

  /**
   * Select a time range from the dropdown (e.g., MONTH, WEEK, ALL)
   * Uses row count chart selector as representative (all charts have same options)
   */
  async selectTimeRange(timeRange: string): Promise<void> {
    this.logger?.step('selectTimeRange', { timeRange });
    await this.rowCountTimeRangeSelect.click({ timeout: TIMEOUTS.MEDIUM });
    await this.page.getByTestId(`option-${timeRange}`).click({ timeout: TIMEOUTS.SHORT });
    // Wait for charts to update
    await this.page.waitForLoadState('networkidle');
  }

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
    const count = await this.rowCountTimeRangeSelect.count();
    if (count > 0) {
      throw new Error('Time range selector should not be visible but was found');
    }
  }

  /**
   * Verify a specific time range option is available
   * Uses row count chart selector to open dropdown
   */
  async verifyTimeRangeOptionExists(timeRange: string): Promise<void> {
    this.logger?.step('verifyTimeRangeOptionExists', { timeRange });
    // Click dropdown to open it
    await this.rowCountTimeRangeSelect.click();
    // Wait for the option to become visible
    const option = this.page.getByTestId(`option-${timeRange}`);
    await option.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify a specific time range option is NOT available
   */
  async verifyTimeRangeOptionDoesNotExist(timeRange: string): Promise<void> {
    this.logger?.step('verifyTimeRangeOptionDoesNotExist', { timeRange });
    const option = this.page.getByTestId(`option-${timeRange}`);
    const count = await option.count();
    if (count > 0) {
      throw new Error(`Time range option "${timeRange}" should not exist but was found`);
    }
  }

  /**
   * Verify a specific time range is selected/highlighted
   */
  async verifyTimeRangeSelected(timeRange: string): Promise<void> {
    this.logger?.step('verifyTimeRangeSelected', { timeRange });
    const selectedValue = this.page.getByTestId(`value-${timeRange}`);
    await expect(selectedValue).toBeVisible();
  }

  // ── Column Stats Table ────────────────────────────────────────────────────

  /**
   * Verify column stats table is visible
   */
  async verifyColumnStatsTableIsVisible(): Promise<void> {
    this.logger?.step('verifyColumnStatsTableIsVisible');
    await this.columnStatsContainer.waitFor({
      state: 'visible',
      timeout: TIMEOUTS.MEDIUM,
    });
  }

  /**
   * Get count of column rows in table
   */
  async getColumnRowCount(): Promise<number> {
    this.logger?.step('getColumnRowCount');
    return this.columnStatsContainer.getByTestId(/^row-/).count();
  }

  /**
   * Verify a column exists in the stats table
   */
  async verifyColumnExists(columnName: string): Promise<void> {
    this.logger?.step('verifyColumnExists', { columnName });
    const row = this.columnStatsContainer.getByTestId(`row-${columnName}`);
    await row.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify a column does NOT exist in the stats table
   */
  async verifyColumnDoesNotExist(columnName: string): Promise<void> {
    this.logger?.step('verifyColumnDoesNotExist', { columnName });
    const row = this.columnStatsContainer.getByTestId(`row-${columnName}`);
    const count = await row.count();
    if (count > 0) {
      throw new Error(`Column "${columnName}" should not exist but was found`);
    }
  }

  /**
   * Click on a column row to open details
   */
  async clickColumnRow(columnName: string): Promise<void> {
    this.logger?.step('clickColumnRow', { columnName });
    const row = this.columnStatsContainer.getByTestId(`row-${columnName}`);
    await row.click({ timeout: TIMEOUTS.SHORT });
    await this.page.getByTestId('schema-field-drawer-content').waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Search/filter columns by name
   */
  async searchColumns(searchTerm: string): Promise<void> {
    this.logger?.step('searchColumns', { searchTerm });
    await this.columnStatsSearchBar.fill(searchTerm);
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Clear column search filter
   */
  async clearColumnSearch(): Promise<void> {
    this.logger?.step('clearColumnSearch');
    await this.columnStatsSearchBar.clear();
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Verify column has specific field values (e.g., type, nullPercentage, uniqueValues)
   * Matches each fieldTestId with expected value within the column row
   */
  async verifyColumnFields(columnName: string, fields: Record<string, string>): Promise<void> {
    this.logger?.step('verifyColumnFields', { columnName, fields });
    const row = this.columnStatsContainer.getByTestId(`row-${columnName}`);
    await row.evaluate((el) => el.scrollIntoView());

    for (const [fieldTestId, expectedValue] of Object.entries(fields)) {
      const cell = row.getByTestId(fieldTestId);
      await expect(cell).toContainText(expectedValue);
    }
  }

  /**
   * Verify schema field drawer is open after clicking column
   */
  async verifySchemaFieldDrawerOpen(): Promise<void> {
    this.logger?.step('verifySchemaFieldDrawerOpen');
    const drawerContent = this.page.getByTestId('schema-field-drawer-content');
    await drawerContent.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // ── No Permission States ──────────────────────────────────────────────────

  /**
   * Verify "no permissions" message is shown for a chart
   */
  async verifyNoPermissionsForChart(chartTestId: string): Promise<void> {
    this.logger?.step('verifyNoPermissionsForChart', { chartTestId });
    const noPermission = this.page.getByTestId(chartTestId).getByTestId('no-permissions');
    await noPermission.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }
}
