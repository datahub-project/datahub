/**
 * LineageBasePage — page object for lineage graph and impact analysis interactions.
 *
 * Wraps all lineage-specific selectors and actions shared across the lineage graph
 * tests. Extended by LineageV3Page.
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
 *   - Column search: within lineage-node, [data-testid="column-search"]
 *   - Column pages:  within lineage-node, [data-testid="column-pagination"]
 *   - Filters panel: [data-testid="lineage-filters-panel"], toggles
 *                    [data-testid="lineage-filter-{name}"]
 *   - Node sidebar:  [data-testid="lineage-sidebar"]
 */

import * as path from 'path';
import * as fs from 'fs';
import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

/**
 * How long the graph viewport must hold still to count as settled: longer than the graph's own
 * deferred fitView (a 1s timer once entity data loads, then a 1s pan/zoom animation — see
 * useFitView in LineageDisplay.tsx), so a pending fit that has not visibly started yet is waited
 * out rather than declared settled.
 */
const VIEWPORT_SETTLE_MS = 2200;

/** Toggles in the graph's filters panel, keyed by the suffix of their test id. */
export type LineageFilterToggle =
  | 'hide-transformations'
  | 'hide-process-instances'
  | 'show-ghost-entities'
  | 'output-ports-only';

export class LineageBasePage extends BasePage {
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
  readonly lineageFiltersPanel: Locator;
  readonly lineageSidebar: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.lineageEditMenuButton = page.getByTestId('lineage-edit-menu-button').first();
    this.editUpstreamLineageButton = page.getByTestId('edit-upstream-lineage');
    this.editDownstreamLineageButton = page.getByTestId('edit-downstream-lineage');
    // eslint-disable-next-line playwright/no-raw-locators -- AntD data-node-key attribute; getByRole('tab') may match by visible text but not by node key
    this.lineageTabKey = page.locator('[data-node-key="Lineage"]').first();
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated HTML id on AntD sidebar tab; no data-testid available
    this.sidebarLineageTab = page.locator('#entity-sidebar-tabs-tab-Lineage');
    this.upstreamDirectionOption = page.getByTestId('compact-lineage-tab-direction-select-option-upstream');
    this.downstreamDirectionOption = page.getByTestId('compact-lineage-tab-direction-select-option-downstream');
    this.columnLineageToggle = page.getByTestId('column-lineage-toggle');
    this.degree2Filter = page.getByTestId('facet-degree-2');
    this.degree3PlusFilter = page.getByTestId('facet-degree-3+');
    this.filterByDescriptionOption = page.getByTestId('adv-search-add-filter-description');
    this.filterTextInput = page.getByTestId('edit-text-input');
    this.filterTextDoneButton = page.getByTestId('edit-text-done-btn');
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design list container class; no data-testid or ARIA role on this wrapper
    this.listItems = page.locator('.ant-list-items');
    this.downloadCsvButton = page.getByTestId('download-csv-button');
    this.downloadCsvInput = page.getByTestId('download-as-csv-input');
    this.csvModalDownloadButton = page.getByTestId('csv-modal-download-button');
    this.lineageEditSearchInput = page.getByRole('dialog').getByTestId('search-input');
    this.lineageTabDirectionSelect = page.getByTestId('lineage-tab-direction-select');
    this.lineageTabDownstreamOption = page.getByTestId('lineage-tab-direction-select-option-downstream');
    this.lineageTabUpstreamOption = page.getByTestId('lineage-tab-direction-select-option-upstream');
    // eslint-disable-next-line playwright/no-raw-locators -- rc-virtual-list internal class; no data-testid or ARIA role
    this.columnDropdownVirtualList = page.locator('.rc-virtual-list');
    // eslint-disable-next-line playwright/no-raw-locators -- CSS class substring match for generated class name; no data-testid on ResultText
    this.resultTextLink = page.locator('.ant-list-items [class*="ResultText"]').first();
    this.lineageFiltersPanel = page.getByTestId('lineage-filters-panel');
    this.lineageSidebar = page.getByTestId('lineage-sidebar');
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

  /** Open a schema field's own lineage graph, e.g. from a column's "view lineage" action. */
  async goToSchemaFieldLineage(fieldUrn: string): Promise<void> {
    await this.navigate(`/schemaField/${fieldUrn}/Lineage`);
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
    return this.page.getByTestId(`lineage-node-${nodeUrn}`);
  }

  /** Get the ReactFlow canvas node element for a given entity URN. */
  getReactFlowNode(urn: string): Locator {
    return this.page.getByTestId(`rf__node-${urn}`);
  }

  async checkNodeExists(nodeUrn: string): Promise<void> {
    await expect(this.getNode(nodeUrn)).toBeAttached({ timeout: 10000 });
  }

  async checkNodeNotExists(nodeUrn: string): Promise<void> {
    await expect(this.getNode(nodeUrn)).not.toBeAttached({ timeout: 5000 });
  }

  // ── Edge existence checks ───────────────────────────────────────────────────

  async checkEdgeExists(node1Urn: string, node2Urn: string): Promise<void> {
    await expect(this.page.getByTestId(`rf__edge-${node1Urn}-:-${node2Urn}`)).toBeAttached({
      timeout: 10000,
    });
  }

  /** Column-level lineage edge between two rendered columns, e.g. drawn on column hover/select. */
  getColumnEdge(node1Urn: string, col1Name: string, node2Urn: string, col2Name: string): Locator {
    return this.page.getByTestId(`rf__edge-${node1Urn}::${col1Name}-${node2Urn}::${col2Name}`);
  }

  /**
   * The upstream segment of a column edge routed through a displayed operation node — a query or
   * data job: `column -> operation`. The operation side of the edge id uses the operation node's
   * urn as its "field" (see addEdge in useColumnHighlighting + parseColumnRef truncation of the
   * fine-grained operation ref).
   */
  getColumnToOperationEdge(nodeUrn: string, colName: string, operationUrn: string): Locator {
    return this.page.getByTestId(`rf__edge-${nodeUrn}::${colName}-${operationUrn}::${operationUrn}`);
  }

  /** The downstream segment of a column edge routed through a displayed operation node: `operation -> column`. */
  getOperationToColumnEdge(operationUrn: string, nodeUrn: string, colName: string): Locator {
    return this.page.getByTestId(`rf__edge-${operationUrn}::${operationUrn}-${nodeUrn}::${colName}`);
  }

  /** Assert a rendered edge is drawn with the lineage arrowhead marker (i.e. it's a real arrow). */
  async checkEdgeHasArrowMarker(edge: Locator): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- ReactFlow edge path has no test id of its own
    await expect(edge.locator('path.react-flow__edge-path')).toHaveAttribute('marker-end', /lineage-arrow/);
  }

  async checkEdgeBetweenColumnsExists(
    node1Urn: string,
    col1Name: string,
    node2Urn: string,
    col2Name: string,
  ): Promise<void> {
    await expect(this.getColumnEdge(node1Urn, col1Name, node2Urn, col2Name)).toBeAttached({
      timeout: 10000,
    });
  }

  async checkEdgeBetweenColumnsNotExists(
    node1Urn: string,
    col1Name: string,
    node2Urn: string,
    col2Name: string,
  ): Promise<void> {
    await expect(this.getColumnEdge(node1Urn, col1Name, node2Urn, col2Name)).not.toBeAttached({
      timeout: 5000,
    });
  }

  // ── Expand / contract ───────────────────────────────────────────────────────

  async expandOne(nodeUrn: string): Promise<void> {
    // ReactFlow nodes can be off-screen after auto-fit; dispatch the click event directly
    // on the DOM element to bypass Playwright's viewport check.
    await this.page.getByTestId(`expand-one-${nodeUrn}-button`).dispatchEvent('click');
  }

  async expandAll(nodeUrn: string): Promise<void> {
    await this.page.getByTestId(`expand-all-${nodeUrn}-button`).dispatchEvent('click');
  }

  async contract(nodeUrn: string): Promise<void> {
    // Contract button may be outside the ReactFlow viewport; use dispatchEvent to bypass checks.
    await this.page.getByTestId(`contract-${nodeUrn}-button`).first().dispatchEvent('click');
  }

  /**
   * Wait for the graph viewport to stop moving before hover-driven interactions.
   *
   * The graph pans/zooms itself well after it looks ready: a deferred fitView fires up to a
   * second after all entity data loads and animates for another second. If that lands after a
   * column has been hovered, the column slides out from under the stationary cursor, the browser
   * recomputes hover and fires mouseleave, and hover-driven UI (column highlights, the column
   * lineage controls) unmounts mid-assertion.
   */
  async waitForViewportToSettle(): Promise<void> {
    await this.page.waitForFunction(
      (settleMs) => {
        const viewport = document.querySelector<HTMLElement>('.react-flow__viewport');
        const transform = viewport?.style.transform ?? '';
        const holder = window as { __viewportSettle?: { transform: string; since: number } };
        if (holder.__viewportSettle?.transform !== transform) {
          holder.__viewportSettle = { transform, since: Date.now() };
          return false;
        }
        return Date.now() - holder.__viewportSettle.since >= settleMs;
      },
      VIEWPORT_SETTLE_MS,
      { polling: 200, timeout: 15000 },
    );
  }

  // ── Column interactions ─────────────────────────────────────────────────────

  async expandContractColumns(nodeUrn: string): Promise<void> {
    const button = this.page.getByTestId(`lineage-node-${nodeUrn}`).getByTestId('expand-contract-columns');
    await button.scrollIntoViewIfNeeded();
    await button.click();
  }

  async hoverColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page.getByTestId(`lineage-node-${nodeUrn}`).getByTestId(`column-${columnName}`).hover();
  }

  async unhoverColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page
      .getByTestId(`lineage-node-${nodeUrn}`)
      .getByTestId(`column-${columnName}`)
      .dispatchEvent('mouseout');
  }

  async selectColumn(nodeUrn: string, columnName: string): Promise<void> {
    await this.page
      .getByTestId(`lineage-node-${nodeUrn}`)
      .getByTestId(`column-${columnName}`)
      .first()
      .click({ force: true });
  }

  // ── Filtering node helpers ──────────────────────────────────────────────────

  getFilterNode(nodeUrn: string, direction: 'up' | 'down'): Locator {
    const dir = direction === 'up' ? 'u' : 'd';
    return this.page.getByTestId(`rf__node-lf:${dir}:${nodeUrn}`);
  }

  async checkFilterNodeExists(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await expect(this.getFilterNode(nodeUrn, direction)).toBeAttached({ timeout: 10000 });
  }

  async showMore(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    // Filter nodes may be positioned off-screen in the ReactFlow canvas; dispatch the event
    // directly to bypass Playwright's viewport enforcement.
    await this.getFilterNode(nodeUrn, direction).getByTestId('show-more').dispatchEvent('click');
  }

  async showAll(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(nodeUrn, direction).getByTestId('show-all').dispatchEvent('click');
  }

  async showLess(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(nodeUrn, direction).getByTestId('show-less').dispatchEvent('click');
  }

  async filterNodes(nodeUrn: string, direction: 'up' | 'down', query: string): Promise<void> {
    const searchInput = this.getFilterNode(nodeUrn, direction).getByTestId('search-input');
    await searchInput.clear();
    await searchInput.fill(query);
  }

  async clearFilter(nodeUrn: string, direction: 'up' | 'down'): Promise<void> {
    await this.getFilterNode(nodeUrn, direction).getByTestId('search-input').clear();
  }

  async ensureFilterNodeTitleHasText(nodeUrn: string, direction: 'up' | 'down', text: string): Promise<void> {
    await expect(this.getFilterNode(nodeUrn, direction).getByTestId('title')).toHaveText(text, {
      timeout: 10000,
    });
  }

  async checkFilterMatches(nodeUrn: string, direction: 'up' | 'down', matchesNumber: string): Promise<void> {
    const label = matchesNumber === '1' ? '1 match' : `${matchesNumber} matches`;
    await expect(this.getFilterNode(nodeUrn, direction).getByTestId('matches')).toHaveText(label, {
      timeout: 5000,
    });
  }

  async checkFilterCounter(
    nodeUrn: string,
    direction: 'up' | 'down',
    counterSection: string,
    counterType: string,
    value: string,
  ): Promise<void> {
    await expect(
      this.getFilterNode(nodeUrn, direction).getByTestId(`filter-counter-${counterSection}-${counterType}`),
    ).toHaveText(value, { timeout: 5000 });
  }

  // ── Manage lineage menu ────────────────────────────────────────────────────

  async openManageLineageMenu(nodeUrn: string): Promise<void> {
    await this.page.getByTestId(`manage-lineage-menu-${nodeUrn}`).click();
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
    // eslint-disable-next-line playwright/no-wait-for-timeout
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

  // ── Filters panel ───────────────────────────────────────────────────────────

  /** Open the graph's filters panel, if it is not already open. */
  async openFilterPanel(): Promise<void> {
    if (!(await this.lineageFiltersPanel.isVisible())) {
      await this.page.getByTestId('lineage-filters-button').click();
    }
    await expect(this.lineageFiltersPanel).toBeVisible({ timeout: 10000 });
  }

  getFilterToggle(filter: LineageFilterToggle): Locator {
    return this.lineageFiltersPanel.getByTestId(`lineage-filter-${filter}`);
  }

  /**
   * Set a filter toggle, opening the panel first. The toggle's `<input>` is transparent and sits
   * under the slider that paints it, so dispatch the click rather than fighting actionability.
   */
  async setFilter(filter: LineageFilterToggle, enabled: boolean): Promise<void> {
    await this.openFilterPanel();
    const toggle = this.getFilterToggle(filter);
    if ((await toggle.isChecked()) !== enabled) {
      await toggle.dispatchEvent('click');
    }
    await expect(toggle).toBeChecked({ checked: enabled });
  }

  // ── Node selection and sidebar ──────────────────────────────────────────────

  /** Click a node to select it, which opens the lineage sidebar for that entity. */
  async clickNode(nodeUrn: string): Promise<void> {
    await this.getReactFlowNode(nodeUrn).dispatchEvent('click');
  }

  /** Assert the lineage sidebar is open and showing the given entity. */
  async checkSidebarShows(entityName: string): Promise<void> {
    await expect(this.lineageSidebar).toBeVisible({ timeout: 15000 });
    await expect(this.lineageSidebar.getByText(entityName).first()).toBeVisible({ timeout: 15000 });
  }

  // ── Column search and pagination within a node ──────────────────────────────

  getColumnSearchInput(nodeUrn: string): Locator {
    return this.page
      .getByTestId(`lineage-node-${nodeUrn}`)
      .getByTestId('column-search')
      .getByTestId('search-bar-input');
  }

  async searchColumns(nodeUrn: string, query: string): Promise<void> {
    const input = this.getColumnSearchInput(nodeUrn);
    await input.click();
    await input.fill(query);
  }

  async clearColumnSearch(nodeUrn: string): Promise<void> {
    await this.getColumnSearchInput(nodeUrn).fill('');
  }

  getColumnPagination(nodeUrn: string): Locator {
    return this.page.getByTestId(`lineage-node-${nodeUrn}`).getByTestId('column-pagination');
  }

  async goToColumnPage(nodeUrn: string, pageNumber: number): Promise<void> {
    // antd renders each page button as li[title="<n>"]; title is the exact page number.
    // eslint-disable-next-line playwright/no-raw-locators -- antd pagination item keyed by title attribute
    await this.getColumnPagination(nodeUrn).locator(`li[title="${pageNumber}"]`).click();
  }

  /** The columns currently rendered on a node, in the order the node draws them. */
  async getShownColumnNames(nodeUrn: string): Promise<string[]> {
    const list = this.page.getByTestId(`lineage-node-${nodeUrn}`).getByTestId('columns-list');
    // Each column's test id carries its own name, so match by prefix; the hover/selection
    // readouts a column renders beside itself share that prefix and are not columns.
    // eslint-disable-next-line playwright/no-raw-locators -- prefix match on generated per-column test ids
    const testIds = await list
      .locator('[data-testid^="column-"]:not([data-testid^="column-lineage-control-"])')
      .evaluateAll((els) => els.map((el) => el.getAttribute('data-testid') ?? ''));
    return testIds.map((id) => id.slice('column-'.length));
  }

  // ── Edge styling ────────────────────────────────────────────────────────────

  /**
   * Manually added edges are drawn dashed (`stroke-dasharray`), every other edge solid. The dash
   * pattern itself is a style choice; assert only that the edge is dashed or is not.
   */
  async checkEdgeIsManual(node1Urn: string, node2Urn: string, isManual: boolean): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- ReactFlow edge path has no test id of its own
    const path = this.page.getByTestId(`rf__edge-${node1Urn}-:-${node2Urn}`).locator('path.react-flow__edge-path');
    if (isManual) {
      await expect(path).not.toHaveCSS('stroke-dasharray', 'none');
    } else {
      await expect(path).toHaveCSS('stroke-dasharray', 'none');
    }
  }
}
