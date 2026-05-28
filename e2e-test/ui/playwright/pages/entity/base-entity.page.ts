import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { EntitySidebar } from './sidebar/entity-sidebar';
import { DocumentationTab } from './tabs/documentation.tab';
import { PropertiesTab } from './tabs/properties.tab';
import { SummaryTab } from './tabs/summary.tab';
import { QueryTab } from './tabs/queries.tab';
import { LineageTab } from './tabs/lineage.tab';

/** Entity type path segments used in URL construction. */
export enum EntityType {
  Dataset = 'dataset',
  Dashboard = 'dashboard',
  Chart = 'chart',
  DataFlow = 'dataFlow',
  DataJob = 'dataJob',
  Domain = 'domain',
  GlossaryTerm = 'glossaryTerm',
  GlossaryNode = 'glossaryNode',
  DataProduct = 'dataProduct',
}

export class BaseEntityPage extends BasePage {
  readonly entityNameLocator: Locator;
  readonly deprecationBadge: Locator;
  readonly threeDotMenu: Locator;
  readonly sidebar: EntitySidebar;
  readonly documentation: DocumentationTab;
  readonly properties: PropertiesTab;
  readonly summary: SummaryTab;
  readonly queries: QueryTab;
  readonly lineage: LineageTab;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.entityNameLocator = page.locator('[data-testid="entity-name"]');
    this.deprecationBadge = page.locator('[data-testid="deprecation-badge"]');
    this.threeDotMenu = page.locator('[data-testid="entity-header-dropdown"]');
    this.sidebar = new EntitySidebar(page);
    this.documentation = new DocumentationTab(page);
    this.properties = new PropertiesTab(page);
    this.summary = new SummaryTab(page);
    this.queries = new QueryTab(page);
    this.lineage = new LineageTab(page);
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  async navigateTo(type: EntityType, urn: string, path?: string, entityName?: string): Promise<void> {
    const suffix = path ? `/${path}` : '';
    await this.navigate(`/${type}/${encodeURIComponent(urn)}${suffix}`);
    await this.page.waitForLoadState('networkidle');
    if (entityName) {
      await expect(this.page.getByText(entityName).first()).toBeVisible({ timeout: 15000 });
    }
  }

  // ── Entity header ─────────────────────────────────────────────────────────

  async getEntityName(): Promise<string> {
    return (await this.entityNameLocator.textContent()) ?? '';
  }

  async clickMenuOption(option: string): Promise<void> {
    await this.threeDotMenu.click();
    await this.page.getByText(option).click();
  }

  async expectEntityNameVisible(): Promise<void> {
    await expect(this.entityNameLocator).toBeVisible({ timeout: 15000 });
  }
}
