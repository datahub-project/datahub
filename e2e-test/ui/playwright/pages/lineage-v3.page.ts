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
  readonly advSearchAddFilterButton: Locator;
  readonly advSearchAddFilterSelect: Locator;
  readonly explorerViewButton: Locator;
  readonly lineageTab: Locator;
  readonly columnShipmentInfo: Locator;
  readonly columnFieldBar: Locator;
  readonly creatingCsvText: Locator;
  readonly selectColumnDropdownButton: Locator;
  readonly columnSelectVirtualList: Locator;
  readonly page: Page;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.page = page;
    this.advSearchAddFilterButton = page.getByTestId('search-results-advanced-search');
    this.advSearchAddFilterSelect = page.getByTestId('adv-search-add-filter-select');
    this.explorerViewButton = page.getByTestId('lineage-view-explorer-btn');
    this.lineageTab = page.getByTestId('Lineage-entity-tab-header');
    this.columnShipmentInfo = page.getByTestId('column-shipment_info');
    this.columnFieldBar = page.getByTestId('column-field_bar');
    this.creatingCsvText = page.getByText('Creating CSV');
    this.selectColumnDropdownButton = page.getByTestId('column-selector-dropdown');
    // eslint-disable-next-line playwright/no-raw-locators -- RC virtual list component uses class-based selector; no semantic API
    this.columnSelectVirtualList = page.locator('.rc-virtual-list');
  }

  // ── Private Helpers ────────────────────────────────────────────────────────
  // Encapsulate inline selectors for dynamic/filtered locators.

  /**
   * Get first occurrence of text (isolates result list from headers).
   */
  private getFirstResultText(text: string): Locator {
    // eslint-disable-next-line playwright/no-nth-methods -- Encapsulated helper; first() isolates result list text from headers/filters
    return this.page.getByText(text).first();
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
   * Click the Add Filter button to show filter options.
   */
  async clickAddFilter(): Promise<void> {
    await this.advSearchAddFilterSelect.click();
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
    await this.page.waitForLoadState(LOAD_STATES.DOMCONTENTLOADED);
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

  // ── Graph Visualization (V3 specific) ────────────────────────────────────────

  /**
   * Wait for the graph to fully render with at least one node.
   */
  async waitForGraphToRender(): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators,playwright/no-nth-methods -- Data attribute prefix match; first() selects first node
    await this.page.locator('[data-testid^="rf__node-"]').first().waitFor({ timeout: TIMEOUTS.EXTRA_LONG });
  }

  /**
   * Check if edges are visible in the graph.
   */
  async areGraphEdgesVisible(): Promise<boolean> {
    // eslint-disable-next-line playwright/no-raw-locators -- Data attribute prefix match for ReactFlow edges
    const count = await this.page.locator('[data-testid^="rf__edge-"]').count();
    return count > 0;
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

    await expect(this.creatingCsvText).not.toBeVisible({ timeout: TIMEOUTS.LONG });

    const download = await downloadPromise;
    const downloadPath = await download.path();
    return downloadPath!;
  }

  // ── Impact Analysis and Column Lineage Navigation ────────────────────────

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
    const columnLocator = nodeLocator.getByTestId(`column-${columnName}`);
    // eslint-disable-next-line playwright/no-nth-methods,playwright/no-raw-locators -- XPath parent traversal and CSS selector for SVG attribute verification; icon is nested within column wrapper
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
    await this.selectColumnDropdownButton.click({ timeout: TIMEOUTS.MEDIUM });
    await this.columnSelectVirtualList.getByText(columnName, { exact: true }).click({ timeout: TIMEOUTS.MEDIUM });
  }

  // ── Result and Text Utilities ──────────────────────────────────────────────

  /**
   * Expect result text visible (first occurrence, supports RegExp).
   */
  async expectResultTextVisible(text: string | RegExp, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    if (typeof text === 'string') {
      await expect(this.getFirstResultText(text)).toBeVisible({ timeout });
    } else {
      // eslint-disable-next-line playwright/no-nth-methods -- Encapsulated regex text selection
      await expect(this.page.getByText(text).first()).toBeVisible({ timeout });
    }
  }

  /**
   * Expect result text hidden (first occurrence).
   */
  async expectResultTextHidden(text: string, timeout: number = TIMEOUTS.SHORT): Promise<void> {
    await expect(this.getFirstResultText(text)).not.toBeVisible({ timeout });
  }

  /**
   * Expect result text not visible (first occurrence).
   */
  async expectResultTextNotVisible(text: string, timeout: number = TIMEOUTS.SHORT): Promise<void> {
    await expect(this.getFirstResultText(text)).not.toBeVisible({ timeout });
  }

  /**
   * Click on a text element (e.g., degree filter button).
   */
  async clickText(text: string): Promise<void> {
    await this.page.getByText(text).click();
  }

  /**
   * Expect text visible with optional exact matching.
   */
  async expectTextVisible(
    text: string | RegExp,
    timeout: number = TIMEOUTS.MEDIUM,
    options?: { exact?: boolean },
  ): Promise<void> {
    await expect(this.page.getByText(text, options)).toBeVisible({ timeout });
  }

  /**
   * Expect text not visible with optional exact matching.
   */
  async expectTextNotVisible(
    text: string | RegExp,
    timeout: number = TIMEOUTS.SHORT,
    options?: { exact?: boolean },
  ): Promise<void> {
    await expect(this.page.getByText(text, options)).not.toBeVisible({ timeout });
  }
}
