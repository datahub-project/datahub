import { Page, Locator } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS } from '../utils/constants';

const PAGINATION_TEXT = /\d+ - \d+ of \d+/;

/**
 * Container entity page object.
 * Represents the container details page showing child entities/datasets.
 *
 * Usage:
 *   const containerPage = new ContainerPage(page);
 *   await containerPage.navigateToContainer('urn:li:container:test');
 *   await containerPage.verifyMultipleEntitiesVisible(['entity1', 'entity2']);
 */
export class ContainerPage extends BasePage {
  readonly pageTitle: Locator;
  readonly entityHeader: Locator;
  readonly childEntitiesList: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.pageTitle = page.getByTestId('entity-name-display');
    this.entityHeader = page.getByTestId('entity-header-test-id');
    this.childEntitiesList = page.getByTestId('embedded-list-search-results');
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  async navigateToContainer(urn: string): Promise<void> {
    this.logger?.step('navigateToContainer', { urn });
    await this.navigate(`/container/${urn}`);
    await this.waitForPageLoad();
  }

  // ── Assertions ──────────────────────────────────────────────────────────

  /**
   * Verify that the container's own name is visible in the page header.
   */
  async verifyContainerNameVisible(containerName: string): Promise<void> {
    this.logger?.step('verifyContainerNameVisible', { containerName });
    await this.entityHeader.getByText(containerName).waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify that a specific entity name is visible in the container's child entities list.
   */
  async verifyEntityNameVisible(entityName: string): Promise<void> {
    this.logger?.step('verifyEntityNameVisible', { entityName });
    await this.childEntitiesList.getByText(entityName).waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  /**
   * Verify pagination information is displayed correctly.
   */
  async verifyPaginationInfo(): Promise<void> {
    this.logger?.step('verifyPaginationInfo');
    await this.page.getByText(PAGINATION_TEXT).waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
  }

  /**
   * Verify multiple entity names are visible in the list.
   */
  async verifyMultipleEntitiesVisible(entityNames: readonly string[]): Promise<void> {
    this.logger?.step('verifyMultipleEntitiesVisible', { count: entityNames.length });
    for (const entityName of entityNames) {
      await this.verifyEntityNameVisible(entityName);
    }
  }
}
