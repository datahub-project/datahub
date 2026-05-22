/**
 * DatasetPage — page object for the DataHub Dataset entity profile.
 *
 * Extends BaseEntityPage with dataset-specific navigation and the SchemaTab.
 */

import { Page, Locator, expect } from '@playwright/test';
import { BaseEntityPage, EntityType } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { SchemaTab } from './tabs/schema.tab';
import { ENTITY_PAGE } from '../../selectors/entity-page.selectors';

export class DatasetPage extends BaseEntityPage {
  readonly schema: SchemaTab;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.schema = new SchemaTab(page);
  }

  async navigateToDataset(urn: string, datasetName?: string): Promise<void> {
    await this.navigateTo(EntityType.Dataset, urn, undefined, datasetName);
  }

  async navigateToDatasetSchema(urn: string): Promise<void> {
    await this.navigateTo(EntityType.Dataset, urn, 'Schema');
    await expect(this.page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });
  }

  async navigateToDatasetDocumentationTab(urn: string, datasetName: string): Promise<void> {
    await this.navigateTo(EntityType.Dataset, urn, undefined, datasetName);
    await this.documentation.open();
  }

  // ── Health badge ──────────────────────────────────────────────────────────

  healthIconForUrn(urn: string): Locator {
    return this.page.locator(ENTITY_PAGE.healthIcon(urn)).first();
  }

  async hoverHealthIcon(urn: string): Promise<void> {
    const icon = this.healthIconForUrn(urn);
    await expect(icon).toBeVisible({ timeout: 15000 });
    await icon.hover({ force: true });
  }

  async expectHealthPopoverVisible(): Promise<void> {
    await expect(this.page.locator(ENTITY_PAGE.assertionsDetails)).toBeVisible({ timeout: 10000 });
  }

  async expectHealthLinksPresent(): Promise<void> {
    const links = this.page.locator(ENTITY_PAGE.assertionsDetails).locator('a');
    await expect(links.first()).toBeVisible({ timeout: 5000 });
    expect(await links.count()).toBeGreaterThanOrEqual(1);
  }

  // ── Lineage navigation helpers ────────────────────────────────────────────

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
}
