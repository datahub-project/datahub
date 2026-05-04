/**
 * DatasetHealthPage — page object for dataset health badge and popover.
 *
 * Covers the health icon hover → assertions/incidents popover workflow
 * used by dataset_health.js.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class DatasetHealthPage extends BasePage {
  // Assertions/incidents popover that appears on health icon hover
  readonly healthPopover: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.healthPopover = page.locator('[data-testid="assertions-details"]');
  }

  // Health icon testid includes the full dataset URN — dynamic, returned as a locator
  healthIconForUrn(urn: string): Locator {
    return this.page.locator(`[data-testid="${urn}-health-icon"]`).first();
  }

  async navigateToDataset(urn: string, datasetName: string): Promise<void> {
    // Gate navigation on the first GMS GraphQL response to avoid asserting before
    // the entity fetch completes (prevents a "Not Found" race condition).
    await Promise.all([
      this.page.waitForResponse(
        (resp) => resp.url().includes('/api/v2/graphql') && resp.request().method() === 'POST',
        { timeout: 30000 },
      ),
      this.page.goto(`/dataset/${encodeURIComponent(urn)}/`),
    ]);
    await this.page.waitForLoadState('networkidle', { timeout: 30000 });
    // Use .first() to avoid strict-mode violations when the name appears in both
    // the breadcrumb and the page heading.
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 15000 });
  }

  async hoverHealthIcon(urn: string): Promise<void> {
    const icon = this.healthIconForUrn(urn);
    await expect(icon).toBeVisible({ timeout: 15000 });
    await icon.hover({ force: true });
  }

  async expectHealthPopoverVisible(): Promise<void> {
    await expect(this.healthPopover).toBeVisible({ timeout: 10000 });
  }

  async expectHealthLinksPresent(): Promise<void> {
    // Due to search index timing in CI, at least one link must be shown.
    const links = this.healthPopover.locator('a');
    await expect(links.first()).toBeVisible({ timeout: 5000 });
    expect(await links.count()).toBeGreaterThanOrEqual(1);
  }
}
