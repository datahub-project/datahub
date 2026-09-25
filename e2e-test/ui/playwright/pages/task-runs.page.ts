/**
 * TaskRunsPage — Page Object Model for runs views
 * Migrated from Cypress
 *
 * All selectors are parameterized methods defined in constructor.
 * To add new selectors: add data-testid to frontend, define method in constructor, create POM method.
 */

import { expect, Locator } from '@playwright/test';
import type { Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class TaskRunsPage extends BasePage {
  readonly getRunNameCell: (name: string) => Locator;
  readonly getRunStatusCell: (name: string) => Locator;
  readonly getInputDatasetLink: (urn: string) => Locator;
  readonly getOutputDatasetLink: (urn: string) => Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);
    this.getRunNameCell = (name: string) => page.getByTestId(`run-name-${name}`);
    this.getRunStatusCell = (name: string) => page.getByTestId(`run-status-${name}`);
    // Column-scoped selectors ensure assertions match the correct dataset in inputs/outputs columns
    this.getInputDatasetLink = (urn: string) =>
      page.getByTestId('run-inputs-cell').getByTestId(`compact-entity-link-${urn}`);
    this.getOutputDatasetLink = (urn: string) =>
      page.getByTestId('run-outputs-cell').getByTestId(`compact-entity-link-${urn}`);
  }

  async navigateToDatasetRuns(urn: string): Promise<void> {
    this.logger?.step('navigate to dataset runs', { urn });
    await this.navigate(`/dataset/${encodeURIComponent(urn)}/Runs`);
    await this.waitForPageLoad();
  }

  async navigateToTaskRuns(urn: string): Promise<void> {
    this.logger?.step('navigate to task runs', { urn });
    await this.navigate(`/tasks/${encodeURIComponent(urn)}/Runs`);
    await this.waitForPageLoad();
  }

  async verifyRunId(name: string, expected: string): Promise<void> {
    this.logger?.step('verify run id', { name, expected });
    await expect(this.getRunNameCell(name)).toContainText(expected);
  }

  async verifyRunStatus(name: string, expected: string): Promise<void> {
    this.logger?.step('verify run status', { name, expected });
    await expect(this.getRunStatusCell(name)).toContainText(expected);
  }

  /** Assert input dataset link is visible with correct name (scoped to inputs column) */
  async assertInputDatasetLinkVisible(urn: string, name: string): Promise<void> {
    this.logger?.step('assert input dataset link visible', { urn, name });
    const link = this.getInputDatasetLink(urn);
    await expect(link).toBeVisible();
    await expect(link).toContainText(name);
  }

  /** Assert output dataset link is visible with correct name (scoped to outputs column) */
  async assertOutputDatasetLinkVisible(urn: string, name: string): Promise<void> {
    this.logger?.step('assert output dataset link visible', { urn, name });
    const link = this.getOutputDatasetLink(urn);
    await expect(link).toBeVisible();
    await expect(link).toContainText(name);
  }
}
