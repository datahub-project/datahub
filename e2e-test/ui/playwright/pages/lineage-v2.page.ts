/**
 * LineageV2Page — page object for lineageV2 graph and impact analysis interactions.
 *
 * Wraps all lineage-specific selectors and actions used by v2_lineage_graph,
 * v2_lineage_column_level, v2_lineage_column_path, v2_download_lineage_results,
 * and v2_impact_analysis Cypress tests.
 *
 * ReactFlow auto-generates node/edge DOM IDs from the node IDs we supply:
 *   - Entity node:   [data-testid="lineage-node-{urn}"]
 *   - Expand one:    [data-testid="expand-one-{urn}-button"]
 *   - Expand all:    [data-testid="expand-all-{urn}-button"]
 *   - Contract:      [data-testid="contract-{urn}-button"]
 *   - Edge:          [data-testid="rf__edge-{upstreamUrn}-:-{downstreamUrn}"]
 *   - Column edge:   [data-testid="rf__edge-{urn1}::{col1}-{urn2}::{col2}"]
 *   - Filter node:   [data-testid="rf__node-lf:{dir}:{urn}"]  dir = u | d
 *   - Column:        within lineage-node, [data-testid="column-{name}"]
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class LineageV2Page extends BasePage {
  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
  }

  // ── Navigation ──────────────────────────────────────────────────────────────

  /** Navigate to the lineage graph view for any entity. */
  async goToLineageGraph(entityType: string, urn: string): Promise<void> {
    await this.navigate(`/${entityType}/${urn}/Lineage`);
    await this.page.waitForLoadState('domcontentloaded');
  }

  /** Navigate to the lineage graph view with time-range filter. */
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

  /** Navigate to a dataset entity page and click through to lineage. */
  async goToDataset(urn: string, datasetName: string): Promise<void> {
    await this.navigate(`/dataset/${urn}/`);
    await this.page.waitForLoadState('domcontentloaded');
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 15000 });
  }

  // ── Node existence checks ───────────────────────────────────────────────────

  getNode(nodeUrn: string): Locator {
    return this.page.locator(`[data-testid="lineage-node-${nodeUrn}"]`);
  }

  async checkNodeExists(nodeUrn: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="lineage-node-${nodeUrn}"]`)).toBeAttached({ timeout: 10000 });
  }

  async checkNodeNotExists(nodeUrn: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="lineage-node-${nodeUrn}"]`)).not.toBeAttached({ timeout: 5000 });
  }

  // ── Edge existence checks ───────────────────────────────────────────────────

  async checkEdgeExists(node1Urn: string, node2Urn: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="rf__edge-${node1Urn}-:-${node2Urn}"]`),
    ).toBeAttached({ timeout: 10000 });
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
    await this.page
      .locator(`[data-testid="expand-one-${nodeUrn}-button"]`)
      .dispatchEvent('click');
  }

  async expandAll(nodeUrn: string): Promise<void> {
    // ReactFlow nodes can be off-screen after auto-fit; dispatch the click event directly
    // on the DOM element to bypass Playwright's viewport check.
    await this.page
      .locator(`[data-testid="expand-all-${nodeUrn}-button"]`)
      .dispatchEvent('click');
  }

  async contract(nodeUrn: string): Promise<void> {
    // Contract button may be outside the ReactFlow viewport; use dispatchEvent to bypass checks.
    await this.page.locator(`[data-testid="contract-${nodeUrn}-button"]`).first().dispatchEvent('click');
  }

  // ── Column interactions ─────────────────────────────────────────────────────

  async expandContractColumns(nodeUrn: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${nodeUrn}"]`)
      .locator('[data-testid="expand-contract-columns"]')
      .click({ force: true });
  }

  async hoverColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${nodeUrn}"]`)
      .locator(`[data-testid="column-${columnName}"]`)
      .hover();
  }

  async unhoverColumn(nodeUrn: string, columnName: string): Promise<void> {
    // Move mouse away from the column to remove hover state
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

  /** Get the ReactFlow filter node element for upstream (u) or downstream (d). */
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
    const searchInput = this.getFilterNode(nodeUrn, direction).locator('[data-testid="search-input"]');
    await searchInput.clear();
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
      this.getFilterNode(nodeUrn, direction).locator(
        `[data-testid="filter-counter-${counterSection}-${counterType}"]`,
      ),
    ).toHaveText(value, { timeout: 5000 });
  }

  // ── Manage lineage menu ────────────────────────────────────────────────────

  async openManageLineageMenu(nodeUrn: string): Promise<void> {
    await this.page.locator(`[data-testid="manage-lineage-menu-${nodeUrn}"]`).click();
  }

  async clickEditUpstreamLineage(): Promise<void> {
    await this.page.locator('[data-testid="edit-upstream-lineage"]').click();
  }

  async clickEditDownstreamLineage(): Promise<void> {
    await this.page.locator('[data-testid="edit-downstream-lineage"]').click();
  }

  // ── Lineage tab / entity page ─────────────────────────────────────────────

  async clickLineageTab(): Promise<void> {
    await this.page.locator('[data-node-key="Lineage"]').first().click();
    await this.page.waitForLoadState('domcontentloaded');
  }

  async clickImpactAnalysis(): Promise<void> {
    await this.page.getByText('Impact Analysis').click();
  }

  async clickUpstreamDirection(): Promise<void> {
    await this.page.locator('[data-testid="compact-lineage-tab-direction-select-option-upstream"]').click();
  }

  async clickDownstreamDirection(): Promise<void> {
    await this.page.locator('[data-testid="compact-lineage-tab-direction-select-option-downstream"]').click();
  }

  async clickColumnLineageToggle(): Promise<void> {
    await this.page.locator('[data-testid="column-lineage-toggle"]').click({ force: true });
  }

  // ── Impact analysis — degree filters ────────────────────────────────────────

  async clickDegree2Filter(): Promise<void> {
    await this.page.locator('[data-testid="facet-degree-2"]').click();
  }

  async clickDegree3PlusFilter(): Promise<void> {
    await this.page.locator('[data-testid="facet-degree-3+"]').click();
  }

  // ── Impact analysis — search / advanced filters ──────────────────────────────

  async clickAdvancedFilter(): Promise<void> {
    await this.page.getByText('Advanced').click();
  }

  async clickAddFilter(): Promise<void> {
    await this.page.getByText('Add Filter').click();
  }

  async clickFilterByDescription(): Promise<void> {
    await this.page.locator('[data-testid="adv-search-add-filter-description"]').click();
  }

  async typeFilterText(text: string): Promise<void> {
    await this.page.locator('[data-testid="edit-text-input"]').fill(text);
  }

  async confirmFilterText(): Promise<void> {
    await this.page.locator('[data-testid="edit-text-done-btn"]').click();
  }

  // ── Download CSV ─────────────────────────────────────────────────────────────

  async downloadCsvFile(filename: string): Promise<void> {
    await expect(this.page.locator('.ant-list-items')).toBeVisible({ timeout: 15000 });
    await this.page.locator('[data-testid="download-csv-button"]').click();
    await this.page.locator('[data-testid="download-as-csv-input"]').clear();
    await this.page.locator('[data-testid="download-as-csv-input"]').fill(filename);
    await this.page.locator('[data-testid="csv-modal-download-button"]').click();
    // Wait for download to complete — "Creating CSV" disappears
    await expect(this.page.getByText('Creating CSV')).not.toBeVisible({ timeout: 30000 });
  }

  // ── Column path modal ────────────────────────────────────────────────────────

  async verifyColumnPathModal(from: string, to: string): Promise<void> {
    // Ant Design portals modal content to document.body — the [data-testid="entity-paths-modal"]
    // placeholder in the React tree stays display:none. Target the portaled dialog by role instead.
    const dialog = this.page.getByRole('dialog', { name: /column path/i });
    await expect(dialog.getByText(from)).toBeVisible({ timeout: 10000 });
    await expect(dialog.getByText(to)).toBeVisible({ timeout: 10000 });
  }

  async closeEntityPathsModal(): Promise<void> {
    await this.page.getByRole('dialog', { name: /column path/i }).getByRole('button', { name: 'Close' }).click();
  }

  // ── Edit lineage modal ────────────────────────────────────────────────────────

  /** Type in the search input inside the lineage edit dialog. */
  async searchInLineageEditModal(text: string): Promise<void> {
    await this.page.locator('[role="dialog"] [data-testid="search-input"]').fill(text);
  }

  async getSetUpstreamsButton(): Promise<Locator> {
    return this.page.getByText('Set Upstreams');
  }

  async getSetDownstreamsButton(): Promise<Locator> {
    return this.page.getByText('Set Downstreams');
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  /** Wait for a text to be visible anywhere on the page. */
  async waitForText(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).toBeVisible({ timeout: 15000 });
  }

  /** Assert that a text is NOT present anywhere on the page. */
  async assertTextNotPresent(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).not.toBeVisible({ timeout: 5000 });
  }

  async clickColumnLineageOption(): Promise<void> {
    await this.page.locator('[data-testid="column-lineage-toggle"]').click({ force: true });
  }

  /**
   * Click the "Downstreams" option in the impact-analysis direction selector.
   * The selector opens an Ant Design dropdown — we target the option by its data-testid.
   */
  async clickDownstreamOption(): Promise<void> {
    // Open the direction select dropdown first, then click the downstream option in the popup.
    // Ant Design Select renders the selected option's label both in the trigger and the popup,
    // so we use .last() to target the popup copy.
    await this.page.locator('[data-testid="lineage-tab-direction-select"]').click();
    await this.page.locator('[data-testid="lineage-tab-direction-select-option-downstream"]').last().click();
  }

  /**
   * Click the "Upstreams" option in the impact-analysis direction selector.
   */
  async clickUpstreamOption(): Promise<void> {
    // Open the direction select dropdown first, then click the upstream option in the popup.
    await this.page.locator('[data-testid="lineage-tab-direction-select"]').click();
    await this.page.locator('[data-testid="lineage-tab-direction-select-option-upstream"]').last().click();
  }
}
