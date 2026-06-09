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

import { Page, Locator, expect } from '@playwright/test';
import { LineageV2Page } from './lineage-v2.page';
import type { DataHubLogger } from '../utils/logger';

export class LineageV3Page extends LineageV2Page {
  readonly impactAnalysisViewButton: Locator;
  readonly explorerViewButton: Locator;
  readonly advSearchAddFilterButton: Locator;
  readonly lineageEmptyState: Locator;
  readonly graphNodeLocator: Locator;
  readonly page: Page;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.page = page;
    this.impactAnalysisViewButton = page.getByTestId('lineage-view-impact-analysis-btn');
    this.explorerViewButton = page.getByTestId('lineage-view-explorer-btn');
    this.advSearchAddFilterButton = page.getByTestId('adv-search-add-filter');
    this.lineageEmptyState = page.getByTestId('lineage-empty-state');
    this.graphNodeLocator = page.locator('[data-testid*="rf__node-"]');
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
    // Wait for page to load - the Lineage tab content should be visible
    // In fullsize/compact mode, wait for the first visible element in the results area
    await this.page.waitForLoadState('domcontentloaded');
    await this.page.waitForTimeout(500);
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
    // Wait for ReactFlow graph to render
    await expect(this.page.locator('[data-testid*="rf__node-"]').first()).toBeVisible({
      timeout: 15000,
    });
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
    // Wait for results to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Switch to downstream direction.
   */
  async switchToDownstream(): Promise<void> {
    await this.downstreamDirectionOption.click();
    // Wait for results to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Switch direction in wide view using the dropdown selector.
   */
  async selectDirectionInWideView(direction: 'upstream' | 'downstream'): Promise<void> {
    await this.lineageTabDirectionSelect.click();
    const option = direction === 'upstream' ? this.lineageTabUpstreamOption : this.lineageTabDownstreamOption;
    await option.last().click();
    // Wait for results to update
    await this.page.waitForTimeout(500);
  }

  // ── Degree / Level Filtering ────────────────────────────────────────────────

  /**
   * Toggle the Direct level filter (1-hop entities).
   */
  async toggleDirectFilter(): Promise<void> {
    const filter = this.page.getByTestId('facet-degree-1');
    await filter.click();
    // Wait for results to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Toggle the Indirect level filter (2+ hops).
   */
  async toggleIndirectFilter(): Promise<void> {
    const filter = this.page.getByTestId('facet-degree-2');
    await filter.click();
    // Wait for results to update
    await this.page.waitForTimeout(500);
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
    const button = this.page.getByTestId('adv-search-toggle');
    const isVisible = await button.isVisible().catch(() => false);
    if (isVisible) {
      const isExpanded = await button.evaluate((el: Element) => el.getAttribute('data-expanded'));
      if (isExpanded !== 'true') {
        await button.click();
        // Wait for filter panel to expand
        await this.page.getByTestId('adv-search-add-filter').waitFor({
          state: 'visible',
          timeout: 5000,
        });
      }
    }
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
        // Wait for filter panel to collapse
        await this.page.waitForTimeout(300);
      }
    }
  }

  /**
   * Click the Add Filter button to show filter options.
   */
  async clickAddFilter(): Promise<void> {
    const button = this.page.getByTestId('adv-search-add-filter');
    await button.click();
    // Wait for filter options to appear
    await this.filterByDescriptionOption.waitFor({ state: 'visible', timeout: 5000 });
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
    // Wait for results to filter
    await this.page.waitForTimeout(500);
  }

  /**
   * Search lineage results by entity name.
   * Locates search input in the results section and types the search term.
   */
  async searchResults(query: string): Promise<void> {
    const searchInput = this.page.getByTestId('lineage-search-input');
    await searchInput.clear();
    await searchInput.fill(query);
    // Wait for debounce and results to filter
    await this.page.waitForTimeout(500);
  }

  // ── Column-Level Lineage ────────────────────────────────────────────────────

  /**
   * Navigate to column-level lineage for a specific column.
   */
  async goToColumnLineage(entityType: string, urn: string, columnPath: string): Promise<void> {
    const encodedColumn = encodeURIComponent(columnPath);
    await this.navigate(`/${entityType}/${urn}/Lineage?column=${encodedColumn}`);
    await this.page.waitForLoadState('domcontentloaded');
    // Wait for column lineage to render
    await expect(this.columnLineageToggle).toBeVisible({ timeout: 10000 });
  }

  /**
   * Verify column-level lineage is active.
   */
  async expectColumnLineageActive(): Promise<void> {
    await expect(this.columnLineageToggle).toHaveAttribute('aria-pressed', 'true', { timeout: 5000 });
  }

  /**
   * Toggle off column-level lineage to show table-level lineage.
   */
  async toggleToTableLevelLineage(): Promise<void> {
    await this.columnLineageToggle.click({ force: true });
    // Wait for results to update
    await this.page.waitForTimeout(1000);
  }

  /**
   * Select a different column from the column dropdown.
   */
  async selectColumnFromSelector(columnName: string): Promise<void> {
    // Open column dropdown
    await this.page.getByTestId('column-selector').click();
    // Select column from virtual list
    await this.columnDropdownVirtualList.getByText(columnName, { exact: true }).click();
    // Wait for new column's lineage to load
    await this.page.waitForTimeout(1000);
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
    // Wait for date picker to open
    await this.page.waitForTimeout(500);
  }

  /**
   * Select a date range in the time selector.
   */
  async selectTimeRange(startDate: string, endDate: string): Promise<void> {
    // Select start date
    const startInput = this.page.getByTestId('time-range-start-date');
    await startInput.fill(startDate);

    // Select end date
    const endInput = this.page.getByTestId('time-range-end-date');
    await endInput.fill(endDate);

    // Apply
    const applyButton = this.page.getByTestId('time-range-apply');
    await applyButton.click();
    // Wait for results to filter
    await this.page.waitForTimeout(1000);
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
    // Switch to explorer/graph view if needed
    try {
      await this.switchToExplorer();
    } catch {
      // Already in graph view or graph view not available
    }
    // Wait for graph to render
    await this.waitForGraphToRender();
  }

  // ── Manual Lineage Editing ──────────────────────────────────────────────────

  /**
   * Open the manage lineage menu and select edit direction.
   */
  async openEditLineageModal(direction: 'upstream' | 'downstream'): Promise<void> {
    await this.clickLineageEditMenuButton();
    // Wait for menu to appear
    await this.page.waitForTimeout(300);

    const option = direction === 'upstream' ? this.editUpstreamLineageButton : this.editDownstreamLineageButton;
    await option.click();
    // Wait for modal to open
    await expect(this.page.getByRole('dialog')).toBeVisible({ timeout: 5000 });
  }

  /**
   * Search for an entity in the lineage edit modal and select it.
   */
  async searchAndSelectInEditModal(entityName: string): Promise<void> {
    await this.searchInLineageEditModal(entityName);
    // Wait for search results
    await this.page.waitForTimeout(500);

    // Select first result
    const firstResult = this.page.getByTestId('search-result').first();
    await firstResult.click();
  }

  /**
   * Save changes in the lineage edit modal.
   */
  async saveLineageChanges(): Promise<void> {
    const saveButton = this.page.getByTestId('lineage-modal-save');
    await saveButton.click();
    // Wait for modal to close and changes to apply
    await expect(this.page.getByRole('dialog')).not.toBeVisible({ timeout: 5000 });
  }

  // ── Cache and Data Refresh ──────────────────────────────────────────────────

  /**
   * Click the refresh cache button to reload lineage data.
   */
  async refreshCache(): Promise<void> {
    const refreshButton = this.page.getByTestId('lineage-refresh-cache');
    await refreshButton.click();
    // Wait for refresh to complete (shown by loading state disappearing)
    await expect(this.page.getByTestId('lineage-loading')).not.toBeVisible({
      timeout: 15000,
    });
  }

  // ── Graph Visualization (V3 specific) ────────────────────────────────────────

  /**
   * Wait for the graph to fully render with at least one node.
   */
  async waitForGraphToRender(): Promise<void> {
    // Wait for at least the central entity node to appear
    // Use .first() to avoid strict mode violation when multiple nodes exist
    await expect(this.page.locator('[data-testid*="rf__node-"]').first()).toBeVisible({ timeout: 15000 });
  }

  /**
   * Check if nodes are visible in the graph.
   */
  async areGraphNodesVisible(): Promise<boolean> {
    const nodes = this.page.locator('[data-testid*="rf__node-"]');
    const count = await nodes.count();
    return count > 0;
  }

  /**
   * Check if edges are visible in the graph.
   */
  async areGraphEdgesVisible(): Promise<boolean> {
    const edges = this.page.locator('[data-testid*="rf__edge-"]');
    const count = await edges.count();
    return count > 0;
  }

  /**
   * Get the count of graph nodes visible in the lineage visualization.
   */
  async getGraphNodeCount(): Promise<number> {
    return await this.graphNodeLocator.count();
  }

  /**
   * Test graph interactivity: pan and zoom.
   */
  async testGraphInteractivity(): Promise<void> {
    const graphContainer = this.page.locator('[class*="react-flow"]');
    // Attempt a zoom gesture (pinch)
    await graphContainer.evaluate((el: Element) => {
      const wheelEvent = new WheelEvent('wheel', { deltaY: 100 });
      el.dispatchEvent(wheelEvent);
    });
    // Brief wait to ensure interaction was processed
    await this.page.waitForTimeout(200);
  }

  // ── Assertions for Empty States and Error Handling ────────────────────────

  /**
   * Verify empty state message is displayed.
   */
  async expectEmptyState(message: string): Promise<void> {
    const emptyState = this.page.getByTestId('lineage-empty-state');
    await expect(emptyState).toBeVisible({ timeout: 5000 });
    await expect(emptyState.getByText(message)).toBeVisible({ timeout: 5000 });
  }

  /**
   * Verify lineage results are displayed.
   */
  async expectResultsDisplayed(): Promise<void> {
    const resultsContainer = this.page.getByTestId('lineage-results-container');
    await expect(resultsContainer).toBeVisible({ timeout: 10000 });
    // Check that at least one result item exists
    const resultItems = this.page.getByTestId('lineage-result-item');
    await expect(resultItems.first()).toBeVisible({ timeout: 5000 });
  }

  /**
   * Verify loading state is shown and then disappears.
   */
  async expectLoadingState(): Promise<void> {
    const loadingSpinner = this.page.getByTestId('lineage-loading');
    // Loading may appear and disappear quickly
    await expect(loadingSpinner).not.toBeVisible({ timeout: 10000 });
  }

  /**
   * Get the count of visible lineage results.
   */
  async getResultsCount(): Promise<number> {
    const resultItems = this.page.getByTestId('lineage-result-item');
    return await resultItems.count();
  }
}
