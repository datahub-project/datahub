/**
 * LineageV3Page — page object for lineageV3 graph and impact analysis interactions.
 *
 * Wraps lineage-v3 specific selectors and actions for:
 * - View switching (Impact Analysis ↔ Explorer/Graph)
 * - Direction filtering (Upstream/Downstream)
 * - Degree filtering (Direct/Indirect)
 * - Advanced search and filters
 * - Column-level lineage
 * - Manual lineage editing
 * - Time-range filtering
 * - Graph visualization and node expansion
 *
 * Extends LineageV2Page for backward compatibility with V2 selectors.
 */

import { Page, Locator, BrowserContext, expect } from '@playwright/test';
import { LineageV2Page } from './lineage-v2.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

export class LineageV3Page extends LineageV2Page {
  readonly impactAnalysisViewButton: Locator;
  readonly explorerViewButton: Locator;
  readonly advSearchAddFilterButton: Locator;
  readonly lineageEmptyState: Locator;
  readonly impactAnalysisOption: Locator;
  readonly columnLineageOption: Locator;
  readonly downstreamsOption: Locator;
  readonly upstreamsOption: Locator;
  readonly lineageTab: Locator;
  readonly columnShipmentInfo: Locator;
  readonly columnFieldBar: Locator;
  readonly columnLineageSelect: Locator;
  readonly columnLineageCombobox: Locator;
  readonly columnOptionShipmentInfo: Locator;
  readonly columnOptionFieldBar: Locator;
  readonly displayedColumnFieldBar: Locator;
  readonly displayedColumnShipmentInfo: Locator;
  readonly kafkaDatasetText: Locator;
  readonly page: Page;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.page = page;
    this.impactAnalysisViewButton = page.getByTestId('lineage-view-impact-analysis-btn');
    this.explorerViewButton = page.getByTestId('lineage-view-explorer-btn');
    this.advSearchAddFilterButton = page.getByTestId('search-results-advanced-search');
    this.lineageEmptyState = page.getByTestId('lineage-empty-state');
    this.impactAnalysisOption = page.getByText('Impact Analysis');
    this.columnLineageOption = page.getByText('Column Lineage');
    this.downstreamsOption = page.getByText('Downstreams');
    this.upstreamsOption = page.getByText('Upstreams');
    this.lineageTab = page.getByTestId('Lineage-entity-tab-header');
    this.columnShipmentInfo = page.getByTestId('column-shipment_info');
    this.columnFieldBar = page.getByTestId('column-field_bar');
    this.columnLineageSelect = page.getByTestId('column-selector-virtual-list');
    this.columnLineageCombobox = this.columnLineageSelect.getByRole('combobox');
    this.columnOptionShipmentInfo = page.getByTestId('column-option-shipment_info');
    this.columnOptionFieldBar = page.getByTestId('column-option-field_bar');
    this.displayedColumnFieldBar = page.getByTestId('displayed-column-field_bar');
    this.displayedColumnShipmentInfo = page.getByTestId('displayed-column-shipment_info');
    this.kafkaDatasetText = page.getByText('SamplePlaywrightKafkaDataset');
  }

  // ── View Switching ──────────────────────────────────────────────────────────

  /**
   * Switch to Impact Analysis view (table-based view).
   * Waits for results to load.
   * If the impact analysis button doesn't exist (fullsize mode), assumes we're already on impact analysis.
   */
  async switchToImpactAnalysis(): Promise<void> {
    const buttonExists = await this.impactAnalysisViewButton.isVisible().catch(() => false);
    if (buttonExists) {
      await this.impactAnalysisViewButton.click();
    }
    await this.page.waitForLoadState('domcontentloaded');
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  /**
   * Switch to Explorer view (graph visualization).
   * Waits for graph canvas to render.
   * If the explorer button doesn't exist (fullsize mode), assumes we're already on explorer/graph view.
   */
  async switchToExplorer(): Promise<void> {
    const buttonExists = await this.explorerViewButton.isVisible().catch(() => false);
    if (buttonExists) {
      await this.explorerViewButton.click();
    }
    await this.waitForGraphToRender();
  }

  /**
   * Verify the current active view (Impact Analysis or Explorer).
   */
  async getActiveView(): Promise<string> {
    const impactButton = this.page.getByTestId('lineage-view-impact-analysis-btn');
    const isActive = await impactButton.evaluate((el: Element) => el.getAttribute('data-active'));
    return isActive === 'true' ? 'impact-analysis' : 'explorer';
  }

  // ── Direction Filtering ─────────────────────────────────────────────────────

  /**
   * Switch direction in compact view using direction selector buttons.
   */
  async switchToUpstream(): Promise<void> {
    await this.upstreamDirectionOption.click();
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  /**
   * Switch to downstream direction.
   */
  async switchToDownstream(): Promise<void> {
    await this.downstreamDirectionOption.click();
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  /**
   * Switch direction in wide view using the dropdown selector.
   */
  async selectDirectionInWideView(direction: 'upstream' | 'downstream'): Promise<void> {
    await this.lineageTabDirectionSelect.click();
    const option = direction === 'upstream' ? this.lineageTabUpstreamOption : this.lineageTabDownstreamOption;
    await option.click();
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  // ── Degree / Level Filtering ────────────────────────────────────────────────

  /**
   * Toggle the Direct level filter (1-hop entities).
   */
  async toggleDirectFilter(): Promise<void> {
    const filter = this.page.getByTestId('facet-degree-1');
    await filter.click();
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  /**
   * Toggle the Indirect level filter (2+ hops).
   */
  async toggleIndirectFilter(): Promise<void> {
    const filter = this.page.getByTestId('facet-degree-2');
    await filter.click();
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  /**
   * Check if a level filter is currently selected.
   */
  async isLevelFilterActive(level: 'direct' | 'indirect'): Promise<boolean> {
    const testId = level === 'direct' ? 'facet-degree-1' : 'facet-degree-2';
    const filter = this.page.getByTestId(testId);
    return await filter.evaluate((el: Element) => el.getAttribute('data-selected') === 'true');
  }

  // ── Advanced Search and Filtering ───────────────────────────────────────────

  /**
   * Open the advanced filter panel.
   */
  async openAdvancedFilter(): Promise<void> {
    await this.advSearchAddFilterButton.waitFor({
      state: 'visible',
      timeout: TIMEOUTS.MEDIUM,
    });
    await this.advSearchAddFilterButton.click();
  }

  /**
   * Close the advanced filter panel.
   */
  async closeAdvancedFilter(): Promise<void> {
    const button = this.page.getByTestId('adv-search-toggle');
    const isVisible = await button.isVisible().catch(() => false);
    if (isVisible) {
      const isExpanded = await button.evaluate((el: Element) => el.getAttribute('data-expanded'));
      if (isExpanded === 'true') {
        await button.click();
        await this.page.waitForTimeout(TIMEOUTS.QUICK);
      }
    }
  }

  /**
   * Click the Add Filter button to show filter options.
   */
  async clickAddFilter(): Promise<void> {
    const button = this.page.getByTestId('adv-search-add-filter-select');
    await button.click();
    await this.filterByDescriptionOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Add a description filter with the given text.
   */
  async addDescriptionFilter(text: string): Promise<void> {
    await this.openAdvancedFilter();
    await this.clickAddFilter();
    await this.clickFilterByDescription();
    await this.typeFilterText(text);
    await this.confirmFilterText();
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
  }

  /**
   * Search lineage results by entity name.
   * Locates search input in the results section and types the search term.
   */
  async searchResults(query: string): Promise<void> {
    const searchInput = this.page.getByTestId('lineage-search-input');
    await searchInput.clear();
    await searchInput.fill(query);
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
  }

  // ── Column-Level Lineage ────────────────────────────────────────────────────

  /**
   * Navigate to column-level lineage for a specific column.
   */
  async goToColumnLineage(entityType: string, urn: string, columnPath: string): Promise<void> {
    const encodedColumn = encodeURIComponent(columnPath);
    await this.navigate(`/${entityType}/${urn}/Lineage?column=${encodedColumn}`);
    await this.page.waitForLoadState('domcontentloaded');
    await expect(this.columnLineageToggle).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify column-level lineage is active.
   */
  async expectColumnLineageActive(): Promise<void> {
    await expect(this.columnLineageToggle).toHaveAttribute('aria-pressed', 'true', { timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Toggle off column-level lineage to show table-level lineage.
   */
  async toggleToTableLevelLineage(): Promise<void> {
    await this.columnLineageToggle.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS * 2);
  }

  /**
   * Select a different column from the column dropdown.
   */
  async selectColumnFromSelector(columnName: string): Promise<void> {
    await this.page.getByTestId('column-selector').click();
    await this.columnDropdownVirtualList.getByText(columnName, { exact: true }).click();
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS * 2);
  }

  // ── Time-Range Filtering ────────────────────────────────────────────────────

  /**
   * Navigate to lineage with time-range parameters.
   */
  async goToLineageWithTimeRange(
    entityType: string,
    urn: string,
    startTimeMillis: number,
    endTimeMillis: number,
  ): Promise<void> {
    await this.navigate(
      `/${entityType}/${urn}/Lineage?start_time_millis=${startTimeMillis}&end_time_millis=${endTimeMillis}`,
    );
    await this.page.waitForLoadState('domcontentloaded');
  }

  /**
   * Open the time range selector in wide view.
   */
  async openTimeRangeSelector(): Promise<void> {
    const timeSelector = this.page.getByTestId('lineage-time-range-selector');
    await timeSelector.click();
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
  }

  /**
   * Select a date range in the time selector.
   */
  async selectTimeRange(startDate: string, endDate: string): Promise<void> {
    const startInput = this.page.getByTestId('time-range-start-date');
    await startInput.fill(startDate);

    const endInput = this.page.getByTestId('time-range-end-date');
    await endInput.fill(endDate);

    const applyButton = this.page.getByTestId('time-range-apply');
    await applyButton.click();
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS * 2);
  }

  /**
   * Navigate to lineage graph view with time-range parameters.
   * Waits for the graph to render after navigation.
   */
  async goToLineageGraphWithTimeRange(
    entityType: string,
    urn: string,
    startTimeMillis: number,
    endTimeMillis: number,
  ): Promise<void> {
    await this.goToLineageWithTimeRange(entityType, urn, startTimeMillis, endTimeMillis);
    // Switch to explorer/graph view if available (handles both compact and full-width modes)
    const explorerButtonExists = await this.explorerViewButton.isVisible().catch(() => false);
    if (explorerButtonExists) {
      await this.explorerViewButton.click();
    }
    // Wait for graph to render - this ensures ReactFlow nodes are ready
    await this.waitForGraphToRender();
  }

  // ── Manual Lineage Editing ──────────────────────────────────────────────────

  /**
   * Open the manage lineage menu and select edit direction.
   */
  async openEditLineageModal(direction: 'upstream' | 'downstream'): Promise<void> {
    await this.clickLineageEditMenuButton();
    await this.page.waitForTimeout(TIMEOUTS.QUICK);

    const option = direction === 'upstream' ? this.editUpstreamLineageButton : this.editDownstreamLineageButton;
    await option.click();
    await expect(this.page.getByRole('dialog')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Search for an entity in the lineage edit modal and select it.
   */
  async searchAndSelectInEditModal(entityName: string): Promise<void> {
    await this.searchInLineageEditModal(entityName);
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    // Click the search result button - uses semantic selector instead of nth() methods
    await this.page.getByRole('button', { name: entityName }).click();
  }

  /**
   * Save changes in the lineage edit modal.
   */
  async saveLineageChanges(): Promise<void> {
    const saveButton = this.page.getByTestId('lineage-modal-save');
    await saveButton.click();
    await expect(this.page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  // ── Cache and Data Refresh ──────────────────────────────────────────────────

  /**
   * Click the refresh cache button to reload lineage data.
   */
  async refreshCache(): Promise<void> {
    const refreshButton = this.page.getByTestId('lineage-refresh-cache');
    await refreshButton.click();
    await expect(this.page.getByTestId('lineage-loading')).not.toBeVisible({
      timeout: TIMEOUTS.EXTRA_LONG,
    });
  }

  // ── Graph Visualization (V3 specific) ────────────────────────────────────────

  /**
   * Wait for the graph to fully render with at least one node.
   */
  async waitForGraphToRender(): Promise<void> {
    await this.page.waitForFunction(
      () => {
        const nodes = document.querySelectorAll('[data-testid^="rf__node-"]');
        return nodes.length > 0;
      },
      { timeout: TIMEOUTS.EXTRA_LONG },
    );
  }

  /**
   * Check if nodes are visible in the graph.
   */
  async areGraphNodesVisible(): Promise<boolean> {
    return await this.page.evaluate(() => {
      const nodes = document.querySelectorAll('[data-testid^="rf__node-"]');
      return nodes.length > 0;
    });
  }

  /**
   * Check if edges are visible in the graph.
   */
  async areGraphEdgesVisible(): Promise<boolean> {
    return await this.page.evaluate(() => {
      const edges = document.querySelectorAll('[data-testid^="rf__edge-"]');
      return edges.length > 0;
    });
  }

  /**
   * Get Locator for a specific ReactFlow node by URN (for assertion in tests).
   */
  getReactFlowNodeByUrn(urn: string) {
    return this.page.getByTestId(new RegExp(`^rf__node-${urn.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
  }

  /**
   * Get locator for a specific column within a node.
   * Used for verifying column visibility in column-level lineage tests.
   */
  getColumnLocatorInNode(nodeLocator: Locator, columnName: string): Locator {
    return nodeLocator.getByText(columnName, { exact: true });
  }

  /**
   * Get the count of graph nodes visible in the lineage visualization.
   */
  async getGraphNodeCount(): Promise<number> {
    return await this.page.evaluate(() => {
      const nodes = document.querySelectorAll('[data-testid^="rf__node-"]');
      return nodes.length;
    });
  }

  /**
   * Test graph interactivity: pan and zoom.
   */
  async testGraphInteractivity(): Promise<void> {
    await this.page.evaluate(() => {
      const graphContainer = document.querySelector('[class*="react-flow"]');
      if (graphContainer) {
        const wheelEvent = new WheelEvent('wheel', { deltaY: 100 });
        graphContainer.dispatchEvent(wheelEvent);
      }
    });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  // ── Assertions for Empty States and Error Handling ────────────────────────

  /**
   * Verify empty state message is displayed.
   */
  async expectEmptyState(message: string): Promise<void> {
    const emptyState = this.page.getByTestId('lineage-empty-state');
    await expect(emptyState).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await expect(emptyState.getByText(message)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify lineage results are displayed.
   */
  async expectResultsDisplayed(): Promise<void> {
    const resultsContainer = this.page.getByTestId('lineage-results-container');
    await expect(resultsContainer).toBeVisible({ timeout: TIMEOUTS.LONG });
    const hasResults = await this.page.evaluate(() => {
      const items = document.querySelectorAll('[data-testid="lineage-result-item"]');
      return items.length > 0;
    });
    expect(hasResults).toBe(true);
  }

  /**
   * Verify loading state is shown and then disappears.
   */
  async expectLoadingState(): Promise<void> {
    const loadingSpinner = this.page.getByTestId('lineage-loading');
    await expect(loadingSpinner).not.toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  /**
   * Get the count of visible lineage results.
   */
  async getResultsCount(): Promise<number> {
    const resultItems = this.page.getByTestId('lineage-result-item');
    return await resultItems.count();
  }

  // ── Graph Node Selection (V3-specific) ──────────────────────────────────────

  /**
   * Check that a dataset node is visible in the graph by matching its text content.
   * Uses auto-retry via expect() (like Cypress .should("be.visible")).
   * Scopes to graph nodes which display the dataset path.
   */
  async checkDatasetNodeVisible(datasetPath: string, timeout: number = TIMEOUTS.LONG): Promise<void> {
    const escapedPath = datasetPath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const textLocator = this.page.getByText(new RegExp(`^${escapedPath}$`, 'i'));

    const nodeLocator = this.page.getByRole('button', { pressed: false }).filter({ has: textLocator });

    await expect(nodeLocator).toBeVisible({ timeout });
  }

  /**
   * Check that a dataset node is hidden in the graph by its text content.
   * Uses auto-retry via expect() (like Cypress .should("not.be.visible")).
   */
  async checkDatasetNodeHidden(datasetPath: string, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    const escapedPath = datasetPath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const textLocator = this.page.getByText(new RegExp(`^${escapedPath}$`, 'i'));

    const nodeLocator = this.page.getByRole('button', { pressed: false }).filter({ has: textLocator });

    await expect(nodeLocator).not.toBeVisible({ timeout });
  }

  /**
   * Download CSV file with given filename.
   * Returns the path to the downloaded file for further processing.
   */
  async downloadCSV(context: BrowserContext, filename: string): Promise<string> {
    const downloadPromise = context.waitForEvent('download');
    await expect(this.downloadCsvButton).toBeVisible({ timeout: TIMEOUTS.LONG });
    await this.downloadCsvButton.click();

    await expect(this.downloadCsvInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await this.downloadCsvInput.fill(filename);
    await this.csvModalDownloadButton.click();

    await expect(this.page.getByText('Creating CSV')).not.toBeVisible({ timeout: TIMEOUTS.LONG });

    const download = await downloadPromise;
    const downloadPath = await download.path();
    return downloadPath!;
  }

  // ── Impact Analysis and Column Lineage Navigation ────────────────────────

  /**
   * Navigate to impact analysis view and enable column lineage.
   */
  async navigateToImpactAnalysis(): Promise<void> {
    await this.impactAnalysisOption.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  /**
   * Enable column lineage view in impact analysis.
   */
  async enableColumnLineage(): Promise<void> {
    await this.columnLineageOption.click();
  }

  /**
   * Switch to downstreams direction in impact analysis.
   */
  async selectDownstreamsDirection(): Promise<void> {
    await this.downstreamsOption.click();
  }

  /**
   * Switch to upstreams direction in impact analysis.
   */
  async selectUpstreamsDirection(): Promise<void> {
    await this.upstreamsOption.click();
  }

  /**
   * Click the lineage tab in the entity page.
   */
  async clickLineageTab(): Promise<void> {
    await this.lineageTab.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  /**
   * Get search result row by dataset URN.
   */
  getSearchResultRow(urn: string): Locator {
    return this.page.getByTestId(`search-result-row-${urn}`);
  }

  /**
   * Get displayed column element from a search result row.
   */
  getDisplayedColumnInResultRow(resultRow: Locator, columnName: string): Locator {
    return resultRow.getByTestId(`displayed-column-${columnName}`);
  }

  /**
   * Select a column from the column lineage selector dropdown.
   */
  async selectColumnFromLineageSelector(columnName: string): Promise<void> {
    await this.columnLineageCombobox.click({ timeout: TIMEOUTS.MEDIUM });
    const columnOption = this.page.getByTestId(`column-option-${columnName}`);
    await columnOption.click();
  }

  /**
   * Get displayed column by name from a search result row.
   */
  getDisplayedColumnByName(resultRow: Locator, columnName: string): Locator {
    return this.getDisplayedColumnInResultRow(resultRow, columnName);
  }

  /**
   * Navigate to dataset and open lineage tab.
   */
  async navigateToDatasetLineage(urn: string): Promise<void> {
    await this.navigate(`/dataset/${urn}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.clickLineageTab();
  }

  /**
   * Verify column SVG color state (fill or stroke attribute).
   * Used to verify that a column has a visual indicator when selected.
   */
  async verifyColumnSvgColorState(
    nodeLocator: Locator,
    columnName: string,
    attribute: 'fill' | 'stroke',
    shouldNotBeDefault: boolean,
  ): Promise<void> {
    const defaultValue = attribute === 'fill' ? 'white' : 'transparent';
    const columnLocator = nodeLocator.getByText(columnName, { exact: true });
    // eslint-disable-next-line playwright/no-raw-locators -- XPath parent (..) and multi-element CSS selector; no semantic API for parent traversal or SVG elements
    const svgElement = columnLocator.locator('..').locator('svg, circle, path').first();

    if (shouldNotBeDefault) {
      await expect(svgElement).not.toHaveAttribute(attribute, defaultValue);
    } else {
      await expect(svgElement).toHaveAttribute(attribute, defaultValue);
    }
  }

  /**
   * Select column from dropdown in impact analysis view.
   * Overrides v2 to add exact text matching for nested columns.
   */
  async selectColumnFromDropdown(columnName: string): Promise<void> {
    const selectDropdown = this.page.getByText('Select column');
    await selectDropdown.click({ force: true, timeout: TIMEOUTS.MEDIUM });
    await this.page.waitForTimeout(TIMEOUTS.SHORT);
    // eslint-disable-next-line playwright/no-raw-locators -- RC virtual list uses class-based selector
    const virtualList = this.page.locator('.rc-virtual-list');
    await virtualList.getByText(columnName, { exact: true }).click({ timeout: TIMEOUTS.MEDIUM });
  }
}
