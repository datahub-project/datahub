/**
 * LineageTab — manages lineage tab interactions on entity pages.
 *
 * Extracted from LineageV2Page. Tests that previously imported LineageV2Page
 * should use `page.lineage` on DatasetPage instead.
 *
 * Navigation helpers (goToLineageGraph, goToDataset) are absorbed by
 * BaseEntityPage.navigateTo() and DatasetPage.navigateToDataset().
 */

import * as path from 'path';
import * as fs from 'fs';
import { Page, Locator, expect } from '@playwright/test';
import type { Tab } from './tab.interface';

export class LineageTab implements Tab {
  private readonly page: Page;

  readonly lineageEditMenuButton: Locator;
  readonly editUpstreamLineageButton: Locator;
  readonly editDownstreamLineageButton: Locator;
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

  constructor(page: Page) {
    this.page = page;
    this.lineageEditMenuButton = page.locator('[data-testid="lineage-edit-menu-button"]').first();
    this.editUpstreamLineageButton = page.locator('[data-testid="edit-upstream-lineage"]');
    this.editDownstreamLineageButton = page.locator('[data-testid="edit-downstream-lineage"]');
    this.upstreamDirectionOption = page.locator(
      '[data-testid="compact-lineage-tab-direction-select-option-upstream"]',
    );
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
    this.lineageTabDownstreamOption = page.locator(
      '[data-testid="lineage-tab-direction-select-option-downstream"]',
    );
    this.lineageTabUpstreamOption = page.locator(
      '[data-testid="lineage-tab-direction-select-option-upstream"]',
    );
    this.columnDropdownVirtualList = page.locator('.rc-virtual-list');
    this.resultTextLink = page.locator('.ant-list-items [class*="ResultText"]').first();
  }

  async open(): Promise<void> {
    await this.page.locator('[data-node-key="Lineage"]').first().click();
    await this.page.waitForLoadState('domcontentloaded');
  }

  async openSidebarLineageTab(): Promise<void> {
    await this.page.locator('#entity-sidebar-tabs-tab-Lineage').click();
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

  // ── Node existence checks ──────────────────────────────────────────────────

  getNode(urn: string): Locator {
    return this.page.locator(`[data-testid="lineage-node-${urn}"]`);
  }

  async checkNodeExists(urn: string): Promise<void> {
    await expect(this.getNode(urn)).toBeAttached({ timeout: 10000 });
  }

  async checkNodeNotExists(urn: string): Promise<void> {
    await expect(this.getNode(urn)).not.toBeAttached({ timeout: 5000 });
  }

  // ── Edge existence checks ──────────────────────────────────────────────────

  async checkEdgeExists(urn1: string, urn2: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="rf__edge-${urn1}-:-${urn2}"]`),
    ).toBeAttached({ timeout: 10000 });
  }

  async checkEdgeBetweenColumnsExists(urn1: string, col1: string, urn2: string, col2: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="rf__edge-${urn1}::${col1}-${urn2}::${col2}"]`),
    ).toBeAttached({ timeout: 10000 });
  }

  async checkEdgeBetweenColumnsNotExists(urn1: string, col1: string, urn2: string, col2: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="rf__edge-${urn1}::${col1}-${urn2}::${col2}"]`),
    ).not.toBeAttached({ timeout: 5000 });
  }

  getReactFlowNode(urn: string): Locator {
    return this.page.locator(`[data-testid="rf__node-${urn}"]`);
  }

  async openManageLineageMenu(urn: string): Promise<void> {
    await this.page.locator(`[data-testid="manage-lineage-menu-${urn}"]`).click();
  }

  async checkFilterNodeExists(urn: string, direction: 'up' | 'down'): Promise<void> {
    await expect(this.getFilterNode(urn, direction)).toBeAttached({ timeout: 10000 });
  }

  async ensureFilterNodeTitleHasText(urn: string, direction: 'up' | 'down', text: string): Promise<void> {
    await expect(this.getFilterNode(urn, direction).locator('[data-testid="title"]')).toHaveText(text, {
      timeout: 10000,
    });
  }

  async checkFilterMatches(urn: string, direction: 'up' | 'down', matchesNumber: string): Promise<void> {
    await expect(this.getFilterNode(urn, direction).locator('[data-testid="matches"]')).toHaveText(
      `${matchesNumber} matches`,
      { timeout: 5000 },
    );
  }

  async checkFilterCounter(
    urn: string,
    direction: 'up' | 'down',
    counterSection: string,
    counterType: string,
    value: string,
  ): Promise<void> {
    await expect(
      this.getFilterNode(urn, direction).locator(
        `[data-testid="filter-counter-${counterSection}-${counterType}"]`,
      ),
    ).toHaveText(value, { timeout: 5000 });
  }

  // ── Expand / contract ──────────────────────────────────────────────────────

  async expandOne(urn: string): Promise<void> {
    await this.page.locator(`[data-testid="expand-one-${urn}-button"]`).dispatchEvent('click');
  }

  async expandAll(urn: string): Promise<void> {
    await this.page.locator(`[data-testid="expand-all-${urn}-button"]`).dispatchEvent('click');
  }

  async contract(urn: string): Promise<void> {
    await this.page.locator(`[data-testid="contract-${urn}-button"]`).first().dispatchEvent('click');
  }

  // ── Column interactions ────────────────────────────────────────────────────

  async expandContractColumns(urn: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${urn}"]`)
      .locator('[data-testid="expand-contract-columns"]')
      .click({ force: true });
  }

  async hoverColumn(urn: string, col: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${urn}"]`)
      .locator(`[data-testid="column-${col}"]`)
      .hover();
  }

  async unhoverColumn(urn: string, col: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${urn}"]`)
      .locator(`[data-testid="column-${col}"]`)
      .dispatchEvent('mouseout');
  }

  async selectColumn(urn: string, col: string): Promise<void> {
    await this.page
      .locator(`[data-testid="lineage-node-${urn}"]`)
      .locator(`[data-testid="column-${col}"]`)
      .first()
      .click({ force: true });
  }

  async selectColumnFromDropdown(col: string): Promise<void> {
    await this.page.getByText('Select column').click({ force: true });
    await this.page.waitForTimeout(1000);
    await this.columnDropdownVirtualList.getByText(col, { exact: true }).click();
  }

  // ── Filter nodes ────────────────────────────────────────────────────────────

  getFilterNode(urn: string, direction: 'up' | 'down'): Locator {
    const dir = direction === 'up' ? 'u' : 'd';
    return this.page.locator(`[data-testid="rf__node-lf:${dir}:${urn}"]`);
  }

  async filterNodes(urn: string, direction: 'up' | 'down', query: string): Promise<void> {
    const searchInput = this.getFilterNode(urn, direction).locator('[data-testid="search-input"]');
    await searchInput.clear();
    await searchInput.fill(query);
  }

  async clearFilter(urn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(urn, direction).locator('[data-testid="search-input"]').clear();
  }

  async showMore(urn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(urn, direction).locator('[data-testid="show-more"]').dispatchEvent('click');
  }

  async showAll(urn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(urn, direction).locator('[data-testid="show-all"]').dispatchEvent('click');
  }

  async showLess(urn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(urn, direction).locator('[data-testid="show-less"]').dispatchEvent('click');
  }

  // ── Impact analysis filters ────────────────────────────────────────────────

  async clickDegree2Filter(): Promise<void> {
    await this.degree2Filter.click();
  }

  async clickDegree3PlusFilter(): Promise<void> {
    await this.degree3PlusFilter.click();
  }

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

  // ── Edit lineage modal ────────────────────────────────────────────────────

  async clickLineageEditMenuButton(): Promise<void> {
    await this.lineageEditMenuButton.click();
  }

  async clickEditUpstreamLineage(): Promise<void> {
    await this.editUpstreamLineageButton.click();
  }

  async clickEditDownstreamLineage(): Promise<void> {
    await this.editDownstreamLineageButton.click();
  }

  async searchInLineageEditModal(text: string): Promise<void> {
    await this.lineageEditSearchInput.clear();
    await this.lineageEditSearchInput.fill(text);
    await this.lineageEditSearchInput.press('Enter');
  }

  // ── Column path modal ─────────────────────────────────────────────────────

  async clickResultTextAndOpenModal(): Promise<void> {
    await expect(this.resultTextLink).toBeVisible({ timeout: 5000 });
    await expect(async () => {
      await this.resultTextLink.click();
      await expect(this.page.getByRole('dialog', { name: /column path/i })).toBeVisible({ timeout: 2000 });
    }).toPass({ timeout: 12000 });
  }

  async verifyColumnPathModal(from: string, to: string): Promise<void> {
    const dialog = this.page.getByRole('dialog', { name: /column path/i });
    await expect(dialog.getByText(from).first()).toBeVisible({ timeout: 10000 });
    await expect(dialog.getByText(to).first()).toBeVisible({ timeout: 10000 });
  }

  async closeColumnPathModal(): Promise<void> {
    await this.page
      .getByRole('dialog', { name: /column path/i })
      .getByRole('button', { name: 'Close' })
      .click();
  }

  // ── Download CSV ──────────────────────────────────────────────────────────

  async downloadCsvAndRead(filename: string): Promise<string> {
    await expect(this.listItems).toBeVisible({ timeout: 15000 });
    const downloadPromise = this.page.waitForEvent('download');
    await this.downloadCsvButton.click();
    await this.downloadCsvInput.clear();
    await this.downloadCsvInput.fill(filename);
    await this.csvModalDownloadButton.click();
    await expect(this.page.getByText('Creating CSV')).not.toBeVisible({ timeout: 30000 });
    const download = await downloadPromise;
    const downloadPath = path.join('/tmp', filename);
    await download.saveAs(downloadPath);
    return fs.readFileSync(downloadPath, 'utf-8');
  }

  // ── Direction select ──────────────────────────────────────────────────────

  async clickDownstreamOption(): Promise<void> {
    await this.lineageTabDirectionSelect.click();
    await this.lineageTabDownstreamOption.last().click();
  }

  async clickUpstreamOption(): Promise<void> {
    await this.lineageTabDirectionSelect.click();
    await this.lineageTabUpstreamOption.last().click();
  }
}
