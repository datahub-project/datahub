/**
 * ML Entities Page Object — Shared POM for ML entity pages.
 *
 * Covers:
 * - MLModel (SageMaker, MLFlow)
 * - MLModelGroup (SageMaker, MLFlow)
 * - Container (Experiment)
 * - DataProcessInstance (Training Run)
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { LOAD_STATES } from '../utils/constants';

export class MLEntitiesPage extends BasePage {
  // ── Metrics & Hyperparameters Tables ──────────────────────────────────────
  readonly metricsTable: Locator;
  readonly hyperparametersTable: Locator;

  // ── Entity Header ──────────────────────────────────────────────────────────
  readonly entityHeader: Locator;

  // ── Selector Factories (parameterized locator generators) ────────────────
  readonly getTabLocator: (tabName: string) => Locator;
  readonly getMetricLocator: (metricName: string) => Locator;
  readonly getHyperparameterLocator: (paramName: string) => Locator;
  readonly getGroupNameLocator: (groupName: string) => Locator;
  readonly getTitleLocator: (title: string) => Locator;
  readonly getTrainingRunLinkLocator: (runName: string) => Locator;
  readonly getModelGroupLinkLocator: (groupName: string) => Locator;
  readonly getModelLinkLocator: (modelName: string) => Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.metricsTable = page.getByTestId('mlmodel-training-metrics-table');
    this.hyperparametersTable = page.getByTestId('mlmodel-hyperparams-table');
    this.entityHeader = page.getByTestId('entity-header-test-id');

    this.getTabLocator = (tabName: string): Locator => {
      const tabId = `${tabName}-entity-tab-header`;
      return page.getByTestId(tabId);
    };

    this.getMetricLocator = (metricName: string): Locator => {
      return this.metricsTable.getByTestId(`mlmodel-metric-row-${metricName}`);
    };

    this.getHyperparameterLocator = (paramName: string): Locator => {
      return this.hyperparametersTable.getByTestId(`mlmodel-hyperparam-row-${paramName}`);
    };

    this.getGroupNameLocator = (groupName: string): Locator => {
      return page.getByTestId(`model-group-name-${groupName}`);
    };

    this.getTitleLocator = (title: string): Locator => {
      return this.entityHeader.getByText(title, { exact: false });
    };

    this.getTrainingRunLinkLocator = (runName: string): Locator => {
      return page.getByRole('link', { name: runName });
    };

    this.getModelGroupLinkLocator = (groupName: string): Locator => {
      return page.getByRole('link', { name: groupName });
    };

    this.getModelLinkLocator = (modelName: string): Locator => {
      return page.getByRole('link', { name: modelName });
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // ─ Navigation Methods ────────────────────────────────────────────────────
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Navigate to an MLModel page by URN
   */
  async navigateToMLModel(urn: string): Promise<void> {
    const encodedUrn = this.encodeUrnForUrl(urn);
    await this.navigate(`/mlModels/${encodedUrn}/Summary?is_lineage_mode=false`);
    await this.waitForPageLoad();
  }

  /**
   * Navigate to an MLModelGroup page by URN
   */
  async navigateToMLModelGroup(urn: string): Promise<void> {
    const encodedUrn = this.encodeUrnForUrl(urn);
    await this.navigate(`/mlModelGroup/${encodedUrn}`);
    await this.waitForPageLoad();
  }

  /**
   * Navigate to a Container (Experiment) page by URN
   */
  async navigateToContainer(urn: string): Promise<void> {
    const encodedUrn = this.encodeUrnForUrl(urn);
    await this.navigate(`/container/${encodedUrn}/Summary?is_lineage_mode=false`);
    await this.waitForPageLoad();
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // ─ Tab Navigation Methods ───────────────────────────────────────────────
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Click on a tab by its testid (e.g., "Features-entity-tab-header")
   */
  async clickTab(
    tabName: 'Features' | 'Properties' | 'Group' | 'Models' | 'Summary' | 'Documents' | 'Incidents',
  ): Promise<void> {
    const tab = this.getTabLocator(tabName);
    await tab.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // ─ Content Verification Methods ──────────────────────────────────────────
  // ═══════════════════════════════════════════════════════════════════════════

  async expectTextVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text, { exact: false })).toBeVisible();
  }

  async expectMetricVisible(metricName: string): Promise<void> {
    const metric = this.getMetricLocator(metricName);
    await expect(metric).toBeVisible();
  }

  async expectHyperparameterVisible(paramName: string): Promise<void> {
    const param = this.getHyperparameterLocator(paramName);
    await expect(param).toBeVisible();
  }

  async expectFeatureVisible(featureName: string): Promise<void> {
    await expect(this.page.getByText(featureName, { exact: false })).toBeVisible();
  }

  async expectModelGroupNameVisible(groupName: string): Promise<void> {
    const group = this.getGroupNameLocator(groupName);
    await expect(group).toBeVisible();
  }

  async expectContainerTitleVisible(title: string): Promise<void> {
    const titleLocator = this.getTitleLocator(title);
    await expect(titleLocator).toBeVisible();
  }

  async clickTrainingRunLink(runName: string): Promise<void> {
    const link = this.getTrainingRunLinkLocator(runName);
    await link.click();
    await this.waitForPageLoad();
  }

  async clickModelGroupLink(groupName: string): Promise<void> {
    const link = this.getModelGroupLinkLocator(groupName);
    await link.click();
    await this.waitForPageLoad();
  }

  async clickModelLink(modelName: string): Promise<void> {
    const link = this.getModelLinkLocator(modelName);
    await link.click();
    await this.waitForPageLoad();
  }

  async waitForMetricsTable(): Promise<void> {
    await this.metricsTable.waitFor({ state: 'visible' });
  }

  async waitForHyperparametersTable(): Promise<void> {
    await this.hyperparametersTable.waitFor({ state: 'visible' });
  }

  /**
   * Encode URN for use in URL path
   */
  private encodeUrnForUrl(urn: string): string {
    return encodeURIComponent(urn);
  }
}
