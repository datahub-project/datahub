/**
 * LineageV2Page — page object for lineageV2 graph and impact analysis interactions.
 *
 * Wraps all lineage-specific selectors and actions used by v2_lineage_graph,
 * v2_lineage_column_level, v2_lineage_column_path, v2_download_lineage_results,
 * and v2_impact_analysis Cypress tests.
 *
 * ReactFlow auto-generates node/edge DOM IDs from the node IDs we supply:
 *   - Entity node:   [data-testid="lineage-node-{urn}"]
 *   - RF node:       [data-testid="rf__node-{urn}"]
 *   - Expand one:    [data-testid="expand-one-{urn}-button"]
 *   - Expand all:    [data-testid="expand-all-{urn}-button"]
 *   - Contract:      [data-testid="contract-{urn}-button"]
 *   - Edge:          [data-testid="rf__edge-{upstreamUrn}-:-{downstreamUrn}"]
 *   - Column edge:   [data-testid="rf__edge-{urn1}::{col1}-{urn2}::{col2}"]
 *   - Filter node:   [data-testid="rf__node-lf:{dir}:{urn}"]  dir = u | d
 *   - Column:        within lineage-node, [data-testid="column-{name}"]
 */

import * as path from 'path';
import * as fs from 'fs';
import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class LineageV2Page extends BasePage {
  // ── Static selector properties ───────────────────────────────────────────────
  readonly lineageEditMenuButton: Locator;
  readonly editUpstreamLineageButton: Locator;
  readonly editDownstreamLineageButton: Locator;
  readonly lineageTabKey: Locator;
  readonly sidebarLineageTab: Locator;
  readonly upstreamDirectionOption: Locator;
  readonly downstreamDirectionOption: Locator;
  readonly columnLineageToggle: Locator;
  readonly degree2Filter: Locator;
  readonly degree3PlusFilter: Locator;
  readonly filterByDescriptionOption: Locator;
  readonly filterTextInput: Locator;
  readonly filterTextDoneButton: Locator;
  readonly listItems: Locator;
  readonly downloadCsvButton: Locator;
  readonly downloadCsvInput: Locator;
  readonly csvModalDownloadButton: Locator;
  readonly lineageEditSearchInput: Locator;
  readonly lineageTabDirectionSelect: Locator;
  readonly lineageTabDownstreamOption: Locator;
  readonly lineageTabUpstreamOption: Locator;
  readonly columnDropdownVirtualList: Locator;
  readonly resultTextLink: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.lineageEditMenuButton = page.locator('[data-testid="lineage-edit-menu-button"]').first();
    this.editUpstreamLineageButton = page.locator('[data-testid="edit-upstream-lineage"]');
    this.editDownstreamLineageButton = page.locator('[data-testid="edit-downstream-lineage"]');
    this.lineageTabKey = page.locator('[data-node-key="Lineage"]').first();
    this.sidebarLineageTab = page.locator('#entity-sidebar-tabs-tab-Lineage');
    this.upstreamDirectionOption = page.locator('[data-testid="compact-lineage-tab-direction-select-option-upstream"]');
    this.downstreamDirectionOption = page.locator(
      '[data-testid="compact-lineage-tab-direction-select-option-downstream"]',
    );
    this.columnLineageToggle = page.locator('[data-testid="column-lineage-toggle"]');
    this.degree2Filter = page.locator('[data-testid="facet-degree-2"]');
    this.degree3PlusFilter = page.locator('[data-testid="facet-degree-3+"]');
    this.filterByDescriptionOption = page.locator('[data-testid="adv-search-add-filter-description"]');
    this.filterTextInput = page.locator('[data-testid="edit-text-input"]');
    this.filterTextDoneButton = page.locator('[data-testid="edit-text-done-btn"]');
    this.listItems = page.locator('.ant-list-items');
    this.downloadCsvButton = page.locator('[data-testid="download-csv-button"]');
    this.downloadCsvInput = page.locator('[data-testid="download-as-csv-input"]');
    this.csvModalDownloadButton = page.locator('[data-testid="csv-modal-download-button"]');
    this.lineageEditSearchInput = page.locator('[role="dialog"] [data-testid="search-input"]');
    this.lineageTabDirectionSelect = page.locator('[data-testid="lineage-tab-direction-select"]');
    this.lineageTabDownstreamOption = page.locator('[data-testid="lineage-tab-direction-select-option-downstream"]');
    this.lineageTabUpstreamOption = page.locator('[data-testid="lineage-tab-direction-select-option-upstream"]');
    this.columnDropdownVirtualList = page.locator('.rc-virtual-list');
    this.resultTextLink = page.locator('.ant-list-items [class*="ResultText"]').first();
  }

  // ── Navigation ──────────────────────────────────────────────────────────────

  async goToLineageGraph(entityType: string, urn: string): Promise<void> {
    await this.navigate(`/${entityType}/${urn}/Lineage`);
    await this.page.waitForLoadState('domcontentloaded');
  }

  async goToLineageGraphWithTimeRange(
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

  async goToDataset(urn: string, datasetName: string): Promise<void> {
    await this.navigate(`/dataset/${urn}/`);
    await this.page.waitForLoadState('domcontentloaded');
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 15000 });
  }

  /** Navigate to a dataset entity page and open the Lineage tab. */
  async goToDatasetLineage(urn: string, name: string): Promise<void> {
    await this.goToDataset(urn, name);
    await this.clickLineageTab();
  }

  // ── Node existence checks ───────────────────────────────────────────────────

  getNode(nodeUrn: string): Locator {
    return this.page.locator(`[data-testid="lineage-node-${nodeUrn}"]`);
  }

  /** Get the ReactFlow canvas node element for a given entity URN. */
  getReactFlowNode(urn: string): Locator {
    return this.page.locator(`[data-testid="rf__node-${urn}"]`);
  }

  async checkNodeExists(nodeUrn: string): Promise<void> {
    await expect(this.getNode(nodeUrn)).toBeAttached({ timeout: 10000 });
  }

  async checkNodeNotExists(nodeUrn: string): Promise<void> {
    await expect(this.getNode(nodeUrn)).not.toBeAttached({ timeout: 5000 });
  }

  // ── Edge existence checks ───────────────────────────────────────────────────

  async checkEdgeExists(node1Urn: string, node2Urn: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="rf__edge-${node1Urn}-:-${node2Urn}"]`)).toBeAttached({
      timeout: 10000,
    });
  }

  async checkEdgeBetweenColumnsExists(
    node1Urn: string,
    col1Name: string,
    node2Urn: string,
    col2Name: string,
  ): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="rf__edge-${node1Urn}::${col1Name}-${node2Urn}::${col2Name}"]`),
    ).toBeAttached({ timeout: 10000 });
  }

  async checkEdgeBetweenColumnsNotExists(
    node1Urn: string,
    col1Name: string,
    node2Urn: string,
    col2Name: string,
  ): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="rf__edge-${node1Urn}::${col1Name}-${node2Urn}::${col2Name}"]`),
    ).not.toBeAttached({ timeout: 5000 });
  }

  // ── Expand / contract ───────────────────────────────────────────────────────

  async expandOne(nodeUrn: string): Promise<void> {
    // ReactFlow nodes can be off-screen after auto-fit; dispatch the click event directly
    // on the DOM element to bypass Playwright's viewport check.
    await this.page.locator(`[data-testid="expand-one-${nodeUrn}-button"]`).dispatchEvent('click');
  }

  async expandAll(nodeUrn: string): Promise<void> {
    await this.page.locator(`[data-testid="expand-all-${nodeUrn}-button"]`).dispatchEvent('click');
  }

  async contract(nodeUrn: string): Promise<void> {
    // Contract button may be outside the ReactFlow viewport; use dispatchEvent to bypass checks.
    await this.page.locator(`[data-testid="contract-${nodeUrn}-button"]`).first().dispatchEvent('click');
  }

  // ── Column interactions ─────────────────────────────────────────────────────

  async expandContractColumns(nodeUrn: string): Promise<void> {
    const button = this.page
      .locator(`[data-testid="lineage-node-${nodeUrn}"]`)
      .locator('[data-testid="expand-contract-columns"]');
    await button.scrollIntoViewIfNeeded();
    await button.click();
  }

  async hoverColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${nodeUrn}"]`)
      .locator(`[data-testid="column-${columnName}"]`)
      .hover();
  }

  async unhoverColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${nodeUrn}"]`)
      .locator(`[data-testid="column-${columnName}"]`)
      .dispatchEvent('mouseout');
  }

  async selectColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${nodeUrn}"]`)
      .locator(`[data-testid="column-${columnName}"]`)
      .first()
      .click({ force: true });
  }

  // ── Filtering node helpers ──────────────────────────────────────────────────

  getFilterNode(nodeUrn: string, direction: 'up' | 'down'): Locator {
    const dir = direction === 'up' ? 'u' : 'd';
    return this.page.locator(`[data-testid="rf__node-lf:${dir}:${nodeUrn}"]`);
  }

  async checkFilterNodeExists(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await expect(this.getFilterNode(nodeUrn, direction)).toBeAttached({ timeout: 10000 });
  }

  async showMore(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    // Filter nodes may be positioned off-screen in the ReactFlow canvas; dispatch the event
    // directly to bypass Playwright's viewport enforcement.
    await this.getFilterNode(nodeUrn, direction).locator('[data-testid="show-more"]').dispatchEvent('click');
  }

  async showAll(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(nodeUrn, direction).locator('[data-testid="show-all"]').dispatchEvent('click');
  }

  async showLess(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(nodeUrn, direction).locator('[data-testid="show-less"]').dispatchEvent('click');
  }

  async filterNodes(nodeUrn: string, direction: 'up' | 'down', query: string): Promise<void> {
    const searchInput = this.getFilterNode(nodeUrn, direction).locator('[data-testid="search-input"]');
    await searchInput.clear();
    await searchInput.fill(query);
  }

  async clearFilter(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(nodeUrn, direction).locator('[data-testid="search-input"]').clear();
  }

  async ensureFilterNodeTitleHasText(nodeUrn: string, direction: 'up' | 'down', text: string): Promise<void> {
    await expect(this.getFilterNode(nodeUrn, direction).locator('[data-testid="title"]')).toHaveText(text, {
      timeout: 10000,
    });
  }

  async checkFilterMatches(nodeUrn: string, direction: 'up' | 'down', matchesNumber: string): Promise<void> {
    await expect(this.getFilterNode(nodeUrn, direction).locator('[data-testid="matches"]')).toHaveText(
      `${matchesNumber} matches`,
      { timeout: 5000 },
    );
  }

  async checkFilterCounter(
    nodeUrn: string,
    direction: 'up' | 'down',
    counterSection: string,
    counterType: string,
    value: string,
  ): Promise<void> {
    await expect(
      this.getFilterNode(nodeUrn, direction).locator(`[data-testid="filter-counter-${counterSection}-${counterType}"]`),
    ).toHaveText(value, { timeout: 5000 });
  }

  // ── Manage lineage menu ────────────────────────────────────────────────────

  async openManageLineageMenu(nodeUrn: string): Promise<void> {
    await this.page.locator(`[data-testid="manage-lineage-menu-${nodeUrn}"]`).click();
  }

  async clickLineageEditMenuButton(): Promise<void> {
    await this.lineageEditMenuButton.click();
  }

  async clickEditUpstreamLineage(): Promise<void> {
    await this.editUpstreamLineageButton.click();
  }

  async clickEditDownstreamLineage(): Promise<void> {
    await this.editDownstreamLineageButton.click();
  }

  // ── Lineage tab / entity page ─────────────────────────────────────────────

  async clickLineageTab(): Promise<void> {
    await this.lineageTabKey.click();
    await this.page.waitForLoadState('domcontentloaded');
  }

  /** Click the Lineage tab inside the entity sidebar (used on task/datajob pages). */
  async clickSidebarLineageTab(): Promise<void> {
    await this.sidebarLineageTab.click();
  }

  async clickImpactAnalysis(): Promise<void> {
    await this.page.getByText('Impact Analysis').click();
  }

  async clickUpstreamDirection(): Promise<void> {
    await this.upstreamDirectionOption.click();
  }

  async clickDownstreamDirection(): Promise<void> {
    await this.downstreamDirectionOption.click();
  }

  async clickColumnLineageToggle(): Promise<void> {
    await this.columnLineageToggle.click({ force: true });
  }

  // ── Impact analysis — degree filters ────────────────────────────────────────

  async clickDegree2Filter(): Promise<void> {
    await this.degree2Filter.click();
  }

  async clickDegree3PlusFilter(): Promise<void> {
    await this.degree3PlusFilter.click();
  }

  // ── Impact analysis — search / advanced filters ──────────────────────────────

  async clickAdvancedFilter(): Promise<void> {
    await this.page.getByText('Advanced').click();
  }

  async clickAddFilter(): Promise<void> {
    await this.page.getByText('Add Filter').click();
  }

  async clickFilterByDescription(): Promise<void> {
    await this.filterByDescriptionOption.click();
  }

  async typeFilterText(text: string): Promise<void> {
    await this.filterTextInput.fill(text);
  }

  async confirmFilterText(): Promise<void> {
    await this.filterTextDoneButton.click();
  }

  // ── Download CSV ─────────────────────────────────────────────────────────────

  async downloadCsvFile(filename: string): Promise<void> {
    await expect(this.listItems).toBeVisible({ timeout: 15000 });
    await this.downloadCsvButton.click();
    await this.downloadCsvInput.clear();
    await this.downloadCsvInput.fill(filename);
    await this.csvModalDownloadButton.click();
    // Wait for download to complete — "Creating CSV" disappears
    await expect(this.page.getByText('Creating CSV')).not.toBeVisible({ timeout: 30000 });
  }

  /** Trigger a CSV download and return the file contents as a string. */
  async downloadCsvAndRead(filename: string): Promise<string> {
    const downloadPromise = this.page.waitForEvent('download');
    await this.downloadCsvFile(filename);
    const download = await downloadPromise;
    const downloadPath = path.join('/tmp', filename);
    await download.saveAs(downloadPath);
    return fs.readFileSync(downloadPath, 'utf-8');
  }

  // ── Column path modal ────────────────────────────────────────────────────────

  async verifyColumnPathModal(from: string, to: string): Promise<void> {
    // Ant Design portals modal content to document.body — the [data-testid="entity-paths-modal"]
    // placeholder in the React tree stays display:none. Target the portaled dialog by role instead.
    // Use .first() because the dialog title "Column path from X to Y" also contains the column
    // names as text nodes, causing getByText() to resolve to 2 elements (strict-mode violation).
    const dialog = this.page.getByRole('dialog', { name: /column path/i });
    await expect(dialog.getByText(from).first()).toBeVisible({ timeout: 10000 });
    await expect(dialog.getByText(to).first()).toBeVisible({ timeout: 10000 });
  }

  async closeEntityPathsModal(): Promise<void> {
    await this.page
      .getByRole('dialog', { name: /column path/i })
      .getByRole('button', { name: 'Close' })
      .click();
  }

  // ── Column dropdown and result text ──────────────────────────────────────────

  /** Open the column selector dropdown and pick a column by name. */
  async selectColumnFromDropdown(columnName: string): Promise<void> {
    await this.page.getByText('Select column').click({ force: true });
    await this.page.waitForTimeout(1000);
    await this.columnDropdownVirtualList.getByText(columnName, { exact: true }).click();
  }

  /**
   * Click the ResultText link that opens the column path modal.
   * Uses toPass to retry through the MatchesContainer CSS height transition (300ms)
   * that can swallow a click before the animation settles.
   */
  async clickResultTextAndOpenModal(): Promise<void> {
    await expect(this.resultTextLink).toBeVisible({ timeout: 5000 });
    await expect(async () => {
      await this.resultTextLink.click();
      await expect(this.page.getByRole('dialog', { name: /column path/i })).toBeVisible({ timeout: 2000 });
    }).toPass({ timeout: 12000 });
  }

  // ── Edit lineage modal ────────────────────────────────────────────────────────

  async searchInLineageEditModal(text: string): Promise<void> {
    // fill() triggers React's onChange → sets local searchQuery state in SearchBar.
    // Pressing Enter then fires onPressEnter → handleSearch(searchQuery) → onSearch/onQueryChange
    // in SearchSelect, which updates the GraphQL search query. pressSequentially does NOT work
    // here because Ant Design's AutoComplete.onSearch is not triggered by synthetic key events
    // on the inner Input element — only fill() + Enter produces the correct event chain.
    await this.lineageEditSearchInput.clear();
    await this.lineageEditSearchInput.fill(text);
    await this.lineageEditSearchInput.press('Enter');
  }

  async getSetUpstreamsButton(): Promise<Locator> {
    return this.page.getByText('Set Upstreams');
  }

  async getSetDownstreamsButton(): Promise<Locator> {
    return this.page.getByText('Set Downstreams');
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  async waitForText(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).toBeVisible({ timeout: 15000 });
  }

  async assertTextNotPresent(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).not.toBeVisible({ timeout: 5000 });
  }

  /**
   * Click the "Downstreams" option in the impact-analysis direction selector.
   * Ant Design Select renders the selected option's label both in the trigger and the popup,
   * so we use .last() to target the popup copy.
   */
  async clickDownstreamOption(): Promise<void> {
    await this.lineageTabDirectionSelect.click();
    await this.lineageTabDownstreamOption.last().click();
  }

  async clickUpstreamOption(): Promise<void> {
    await this.lineageTabDirectionSelect.click();
    await this.lineageTabUpstreamOption.last().click();
  }
}
