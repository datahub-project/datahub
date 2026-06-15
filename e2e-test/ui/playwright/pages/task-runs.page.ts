/**
 * TaskRunsPage — Page Object Model for runs views (Runs tab on Dataset/Task pages)
 *
 * Selector Strategy:
 * All selectors are defined in the constructor as Locator objects or methods.
 * This consolidates element definitions and simplifies test maintenance.
 * Selector sources:
 * - runIdCell, runStatusCell: data-testid added to RunsTab.tsx and OperationsTab.tsx
 * - getDatasetLink: dynamic selector for input/output dataset links (CompactEntityNameList)
 *
 * To add new selectors:
 * 1. Add data-testid to the frontend component (RunsTab.tsx or OperationsTab.tsx)
 * 2. Define selector in constructor: `this.newSelector = page.getByTestId('...')`
 * 3. Create a method if the selector takes parameters
 * 4. Reference only from test methods via this.selectorName
 *
 * Usage:
 * - await this.navigateToDatasetRuns(urn) - navigate to dataset runs view
 * - await this.verifyRunId('expected_id') - verify run ID cell contains text
 * - await this.assertDatasetLinkVisible(urn, name) - verify dataset link visible and named correctly
 */

import { expect, Locator } from '@playwright/test';
import type { Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class TaskRunsPage extends BasePage {
  readonly runIdCell: Locator;
  readonly runStatusCell: Locator;
  readonly getDatasetLink: (urn: string) => Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);
    // Selector for run ID cell - contains timestamp of the run
    this.runIdCell = page.getByTestId('run-id-cell');
    // Selector for run status cell - contains status badge (e.g., "Failed", "Success")
    this.runStatusCell = page.getByTestId('run-status-cell');
    // Dynamic selector for dataset links in inputs/outputs - parameterized by dataset URN
    this.getDatasetLink = (urn: string) => page.getByTestId(`compact-entity-link-${urn}`);
  }

  async navigateToDatasetRuns(urn: string): Promise<void> {
    this.logger?.step('navigate to dataset runs', { urn });
    await this.navigate(`/dataset/${urn}/Runs`);
    await this.waitForPageLoad();
  }

  async navigateToTaskRuns(urn: string): Promise<void> {
    this.logger?.step('navigate to task runs', { urn });
    await this.navigate(`/tasks/${urn}/Runs`);
    await this.waitForPageLoad();
  }

  async verifyRunId(expected: string): Promise<void> {
    this.logger?.step('verify run id', { expected });
    await expect(this.runIdCell).toContainText(expected);
  }

  async verifyRunStatus(expected: string): Promise<void> {
    this.logger?.step('verify run status', { expected });
    await expect(this.runStatusCell).toContainText(expected);
  }

  async assertDatasetLinkVisible(urn: string, name: string): Promise<void> {
    this.logger?.step('assert dataset link visible', { urn, name });
    const link = this.getDatasetLink(urn);
    await expect(link).toBeVisible();
    await expect(link).toContainText(name);
  }
}
