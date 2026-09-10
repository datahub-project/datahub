/**
 * MetricsPage — page object for the Metrics shell (/metrics, /metric/:urn, /semanticModel/:urn).
 *
 * Covers landing content, sidebar browse/search/grouping, and profile module helpers.
 * Reuse SearchPage / LineageV3Page for global search and lineage graph assertions.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

export class MetricsPage extends BasePage {
  readonly navMetricsItem: Locator;
  readonly metricsPage: Locator;
  readonly mainContent: Locator;
  readonly sidebar: Locator;
  readonly sidebarHome: Locator;
  readonly sidebarSearchInput: Locator;
  readonly sidebarDisplay: Locator;
  readonly sidebarTree: Locator;
  readonly modelsSection: Locator;
  readonly modelsExpandAll: Locator;
  readonly countModels: Locator;
  readonly countMetrics: Locator;
  readonly countPlatforms: Locator;
  readonly latestUpdate: Locator;
  readonly recentModels: Locator;
  readonly recentMetrics: Locator;
  readonly entityHeader: Locator;
  readonly entityName: Locator;
  readonly summaryTab: Locator;
  readonly definitionTab: Locator;
  readonly lineageTab: Locator;
  readonly propertiesTab: Locator;
  readonly datasetsModule: Locator;
  readonly metricsModule: Locator;
  readonly relationshipsModule: Locator;
  readonly dimensionsModule: Locator;
  readonly sqlModule: Locator;
  readonly relatedMetricsModule: Locator;
  readonly definitionCode: Locator;
  readonly definitionCopy: Locator;
  readonly sidebarSearchResults: Locator;
  readonly clearSearch: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.navMetricsItem = page.getByTestId('nav-menu-item-metrics');
    this.metricsPage = page.getByTestId('metrics-page');
    this.mainContent = page.getByTestId('metrics-main-content');
    this.sidebar = page.getByTestId('metrics-sidebar');
    this.sidebarHome = page.getByTestId('metrics-sidebar-home');
    this.sidebarSearchInput = page.getByTestId('metrics-sidebar-search-input');
    this.sidebarDisplay = page.getByTestId('metrics-sidebar-display');
    this.sidebarTree = page.getByTestId('metrics-sidebar-tree');
    this.modelsSection = page.getByTestId('metrics-sidebar-models-section');
    this.modelsExpandAll = page.getByTestId('metrics-sidebar-models-section-expand-all');
    this.countModels = page.getByTestId('metrics-count-models');
    this.countMetrics = page.getByTestId('metrics-count-metrics');
    this.countPlatforms = page.getByTestId('metrics-count-platforms');
    this.latestUpdate = page.getByTestId('metrics-latest-update');
    this.recentModels = page.getByTestId('metrics-recent-models');
    this.recentMetrics = page.getByTestId('metrics-recent-metrics');
    this.entityHeader = page.getByTestId('entity-header-test-id');
    // Profile header + right summary drawer both render entity-name-display.
    this.entityName = this.entityHeader.getByTestId('entity-name-display');
    this.summaryTab = page.getByTestId('Summary-entity-tab-header');
    this.definitionTab = page.getByTestId('Definition-entity-tab-header');
    this.lineageTab = page.getByTestId('Lineage-entity-tab-header');
    this.propertiesTab = page.getByTestId('Properties-entity-tab-header');
    this.datasetsModule = page.getByTestId('semantic-model-datasets-module');
    this.metricsModule = page.getByTestId('semantic-model-metrics-module');
    this.relationshipsModule = page.getByTestId('semantic-model-relationships-module');
    this.dimensionsModule = page.getByTestId('semantic-model-dimensions-module');
    this.sqlModule = page.getByTestId('sql-module');
    this.relatedMetricsModule = page.getByTestId('related-metrics-module');
    this.definitionCode = page.getByTestId('definition-code-block');
    this.definitionCopy = page.getByTestId('definition-copy-button');
    this.sidebarSearchResults = page.getByTestId('metrics-sidebar-search-results');
    this.clearSearch = page.getByTestId('metrics-sidebar-clear-search');
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  async navigateToHome(): Promise<void> {
    this.logger?.step('navigate to metrics home');
    await this.page.addInitScript(() => {
      localStorage.setItem('skipOnboardingTour', 'true');
    });
    await this.navigate('/metrics');
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.sidebar).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async navigateToSemanticModel(urn: string): Promise<void> {
    this.logger?.step('navigate to semantic model', { urn });
    await this.navigate(`/semanticModel/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.entityHeader).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async navigateToMetric(urn: string): Promise<void> {
    this.logger?.step('navigate to metric', { urn });
    await this.navigate(`/metric/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.entityHeader).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async openMetricsFromNav(): Promise<void> {
    this.logger?.step('open metrics from navbar');
    await this.navMetricsItem.click();
    await expect(this.metricsPage).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Locators ──────────────────────────────────────────────────────────────

  recentModel(urn: string): Locator {
    return this.page.getByTestId(`recent-model-${urn}`);
  }

  recentMetric(urn: string): Locator {
    return this.page.getByTestId(`recent-metric-${urn}`);
  }

  sidebarModel(urn: string): Locator {
    return this.page.getByTestId(`metrics-sidebar-model-${urn}`);
  }

  sidebarMetric(urn: string): Locator {
    return this.page.getByTestId(`metrics-sidebar-metric-${urn}`);
  }

  sidebarSearchMetric(urn: string): Locator {
    return this.page.getByTestId(`metrics-sidebar-search-metric-${urn}`);
  }

  groupingOption(value: string): Locator {
    return this.page.getByTestId(`metrics-sidebar-display-grouping-option-${value}`);
  }

  sortingOption(value: string): Locator {
    return this.page.getByTestId(`metrics-sidebar-display-sorting-option-${value}`);
  }

  platformGroups(): Locator {
    return this.page.getByTestId('metrics-sidebar-platform-groups');
  }

  domainGroups(): Locator {
    return this.page.getByTestId('metrics-sidebar-domain-groups');
  }

  // ── Home assertions ───────────────────────────────────────────────────────

  async expectHomeVisible(): Promise<void> {
    await expect(this.metricsPage).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.mainContent).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectSummaryCounts(models: number, metrics: number, platforms: number): Promise<void> {
    // Use >= so dirty local instances with extra metrics data still pass; CI with
    // only seeded fixtures will equal the expected inventory.
    await expect(this.countModels).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.countMetrics).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.countPlatforms).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect
      .poll(async () => Number.parseInt((await this.countModels.innerText()).replace(/\D/g, ''), 10), {
        timeout: TIMEOUTS.LONG,
      })
      .toBeGreaterThanOrEqual(models);
    await expect
      .poll(async () => Number.parseInt((await this.countMetrics.innerText()).replace(/\D/g, ''), 10), {
        timeout: TIMEOUTS.LONG,
      })
      .toBeGreaterThanOrEqual(metrics);
    await expect
      .poll(async () => Number.parseInt((await this.countPlatforms.innerText()).replace(/\D/g, ''), 10), {
        timeout: TIMEOUTS.LONG,
      })
      .toBeGreaterThanOrEqual(platforms);
    await expect(this.latestUpdate).toBeVisible();
  }

  async expectRecentListsVisible(): Promise<void> {
    await expect(this.recentModels).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.recentMetrics).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Sidebar actions ───────────────────────────────────────────────────────

  async expandSidebarModel(urn: string): Promise<void> {
    this.logger?.step('expand sidebar semantic model', { urn });
    const row = this.sidebarModel(urn);
    await expect(row).toBeVisible({ timeout: TIMEOUTS.LONG });
    await row.scrollIntoViewIfNeeded();
    const expand = row.getByRole('button', { name: /expand/i });
    if (await expand.isVisible().catch(() => false)) {
      await expand.click();
    }
  }

  async expandSidebarMetric(urn: string): Promise<void> {
    this.logger?.step('expand sidebar metric', { urn });
    const row = this.sidebarMetric(urn);
    await expect(row).toBeVisible({ timeout: TIMEOUTS.LONG });
    const expand = row.getByRole('button', { name: /expand/i });
    if (await expand.isVisible().catch(() => false)) {
      await expand.click();
    }
  }

  async clickSidebarModel(urn: string): Promise<void> {
    await this.sidebarModel(urn).click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async clickSidebarMetric(urn: string): Promise<void> {
    await this.sidebarMetric(urn).click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async searchSidebar(query: string): Promise<void> {
    this.logger?.step('sidebar search', { query });
    await this.sidebarSearchInput.fill(query);
    await expect(this.sidebarSearchResults).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async clearSidebarSearch(): Promise<void> {
    this.logger?.step('clear sidebar search');
    await this.clearSearch.click();
    await expect(this.sidebarTree).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async openDisplayMenu(): Promise<void> {
    await this.sidebarDisplay.click();
    await expect(this.page.getByTestId('metrics-sidebar-display-panel')).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
  }

  async setGroupBy(value: 'semantic_model' | 'platform' | 'domain'): Promise<void> {
    this.logger?.step('set sidebar group by', { value });
    await this.openDisplayMenu();
    await this.groupingOption(value).click();
    // Close panel
    await this.page.keyboard.press('Escape');
  }

  async setSort(value: string): Promise<void> {
    this.logger?.step('set sidebar sort', { value });
    await this.openDisplayMenu();
    await this.sortingOption(value).click();
    await this.page.keyboard.press('Escape');
  }

  async clickSidebarHome(): Promise<void> {
    await this.sidebarHome.click();
    await expect(this.metricsPage).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Profile helpers ───────────────────────────────────────────────────────

  async expectEntityNamed(name: string): Promise<void> {
    await expect(this.entityName).toContainText(name, { timeout: TIMEOUTS.LONG });
  }

  async openSummaryTab(): Promise<void> {
    await this.summaryTab.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async openDefinitionTab(): Promise<void> {
    await this.definitionTab.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async openLineageTab(): Promise<void> {
    await this.lineageTab.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async openPropertiesTab(): Promise<void> {
    await this.propertiesTab.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async expectSemanticModelModulesVisible(): Promise<void> {
    await expect(this.datasetsModule).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.metricsModule).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.dimensionsModule).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.relationshipsModule).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectMetricModulesVisible(): Promise<void> {
    await expect(this.sqlModule).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.relatedMetricsModule).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectModuleContains(module: Locator, text: string): Promise<void> {
    await expect(module.getByText(text, { exact: false }).first()).toBeVisible({
      timeout: TIMEOUTS.LONG,
    });
  }

  /** Click a module row by visible label (EntityItem / Link). */
  async clickModuleItem(module: Locator, text: string): Promise<void> {
    const link = module.getByRole('link', { name: new RegExp(text, 'i') }).first();
    if (await link.count()) {
      await expect(link).toBeVisible({ timeout: TIMEOUTS.LONG });
      await link.scrollIntoViewIfNeeded();
      await link.click();
      return;
    }
    const item = module.getByTestId('entity-item').filter({ hasText: text }).first();
    await expect(item).toBeVisible({ timeout: TIMEOUTS.LONG });
    await item.scrollIntoViewIfNeeded();
    await item.click();
  }
}
