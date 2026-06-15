/**
 * MLFlow MLModel Tests — Playwright E2E tests for MLFlow ML models.
 *
 * Equivalent to Cypress: smoke-test/tests/cypress/cypress/e2e/ml/model_mlflow.js
 *
 * Tests cover:
 * - MLModel (MLFlow) with description, metrics, hyperparameters, and groups
 * - MLModel to MLModelGroup navigation
 * - MLModelGroup with models list and description
 *
 * Test Data (globally seeded):
 * - MLFlow model: sample_ml_model
 * - MLFlow model group: sample_ml_model_group
 *
 * All tests assume authenticated context via loginFixture.
 */

import { test, expect } from '../../fixtures/base-test';
import { MLEntitiesPage } from '../../pages/ml-entities.page';

// Test Data Constants
const TEST_DATA = {
  MLFLOW_MODEL_URN: 'urn:li:mlModel:(urn:li:dataPlatform:mlflow,sample_ml_model,PROD)',
  MLFLOW_GROUP_URN: 'urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,sample_ml_model_group,PROD)',
} as const;

const MLFLOW_STRINGS = {
  MODEL_URL_PATTERN: 'mlModels',
  GROUP_URL_PATTERN: 'mlModelGroup',
  SUMMARY_TAB: 'Summary',
  PROPERTIES_TAB: 'Properties',
  MODELS_TAB: 'Models',
  GROUP_TAB: 'Group',
  CREATOR_URN: 'urn:li:corpuser:datahub',
  TEAM_PROPERTY: 'data_science',
  MODEL_NAME: 'SAMPLE ML MODEL',
  MODEL_DESCRIPTION: 'A sample ML model',
  GROUP_NAME: 'SAMPLE ML MODEL GROUP',
  GROUP_DESCRIPTION: 'A sample ML model group',
  TRAINING_RUN: 'Simple Training Run',
  METRIC_NAME: 'val_loss',
  HYPERPARAM_NAME: 'max_depth',
} as const;

test.describe('MLFlow Models', () => {
  let mlEntitiesPage: MLEntitiesPage;

  // ═══════════════════════════════════════════════════════════════════════════
  // MLFlow Model Group Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('MLFlow MLModelGroup with Navigation', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit mlflow model groups', async () => {
      await mlEntitiesPage.navigateToMLModelGroup(TEST_DATA.MLFLOW_GROUP_URN);

      const currentUrl = await mlEntitiesPage.getCurrentUrl();
      expect(currentUrl).toContain(MLFLOW_STRINGS.GROUP_URL_PATTERN);

      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.CREATOR_URN);
      await mlEntitiesPage.clickTab(MLFLOW_STRINGS.PROPERTIES_TAB);
      await mlEntitiesPage.waitForPageLoad();
      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.TEAM_PROPERTY);
      await mlEntitiesPage.clickTab(MLFLOW_STRINGS.MODELS_TAB);
      await mlEntitiesPage.waitForPageLoad();
      await mlEntitiesPage.clickModelLink(MLFLOW_STRINGS.MODEL_NAME);
      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.MODEL_DESCRIPTION);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // MLFlow Model Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('MLFlow MLModel with Groups', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit mlflow model', async () => {
      await mlEntitiesPage.navigateToMLModel(TEST_DATA.MLFLOW_MODEL_URN);

      const currentUrl = await mlEntitiesPage.getCurrentUrl();
      expect(currentUrl).toContain(MLFLOW_STRINGS.MODEL_URL_PATTERN);
      expect(currentUrl).toContain(MLFLOW_STRINGS.SUMMARY_TAB);

      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.TRAINING_RUN);
      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.MODEL_DESCRIPTION);
      await mlEntitiesPage.waitForMetricsTable();
      await mlEntitiesPage.expectMetricVisible(MLFLOW_STRINGS.METRIC_NAME);
      await mlEntitiesPage.waitForHyperparametersTable();
      await mlEntitiesPage.expectHyperparameterVisible(MLFLOW_STRINGS.HYPERPARAM_NAME);
      await mlEntitiesPage.clickTab(MLFLOW_STRINGS.PROPERTIES_TAB);
      await mlEntitiesPage.waitForPageLoad();
      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.TEAM_PROPERTY);
      await mlEntitiesPage.clickTab(MLFLOW_STRINGS.GROUP_TAB);
      await mlEntitiesPage.waitForPageLoad();
      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.GROUP_DESCRIPTION);
      await mlEntitiesPage.clickModelGroupLink(MLFLOW_STRINGS.GROUP_NAME);
      await mlEntitiesPage.expectTextVisible(MLFLOW_STRINGS.GROUP_DESCRIPTION);
    });
  });
});
