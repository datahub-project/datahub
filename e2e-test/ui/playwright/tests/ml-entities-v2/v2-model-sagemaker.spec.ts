/**
 * SageMaker MLModel Tests — Playwright E2E tests for SageMaker ML models.
 *
 * Equivalent to Cypress: smoke-test/tests/cypress/cypress/e2e/ml/model_sagemaker.js
 *
 * Tests cover:
 * - MLModel (SageMaker) with description, metrics, hyperparameters, and features
 * - MLModel to MLModelGroup navigation
 * - MLModelGroup with models list and description
 *
 * Test Data (globally seeded):
 * - SageMaker model: playwright-model
 * - SageMaker model group: playwright-model-package-group
 *
 * All tests assume authenticated context via loginFixture.
 */

import { test, expect } from '../../fixtures/base-test';
import { MLEntitiesPage } from '../../pages/ml-entities.page';

// Test Data Constants
const TEST_DATA = {
  SAGEMAKER_MODEL_URN: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,playwright-model,PROD)',
  SAGEMAKER_GROUP_URN: 'urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,playwright-model-package-group,PROD)',
} as const;

const SAGEMAKER_STRINGS = {
  MODEL_URL_PATTERN: 'mlModels',
  GROUP_URL_PATTERN: 'mlModelGroup',
  SUMMARY_TAB: 'Summary',
  GROUP_TAB: 'Group',
  FEATURES_TAB: 'Features',
  MODEL_DESCRIPTION: 'ml model description',
  METRIC_NAME: 'another-metric',
  HYPERPARAM_NAME: 'parameter-1',
  FEATURE_NAME: 'some-playwright-feature-1',
  MODEL_NAME: 'playwright-model',
  GROUP_NAME: 'playwright-model-package-group',
  GROUP_DESCRIPTION: 'Just a model package group.',
} as const;

test.describe('SageMaker Models', () => {
  let mlEntitiesPage: MLEntitiesPage;

  // ═══════════════════════════════════════════════════════════════════════════
  // MLModel Summary Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('SageMaker MLModel Summary and Groups', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit sagemaker models and groups', async () => {
      // Navigate to SageMaker model page - Summary tab
      await mlEntitiesPage.navigateToMLModel(TEST_DATA.SAGEMAKER_MODEL_URN);

      // Verify model page loaded with correct URL
      const currentUrl = await mlEntitiesPage.getCurrentUrl();
      expect(currentUrl).toContain(SAGEMAKER_STRINGS.MODEL_URL_PATTERN);
      expect(currentUrl).toContain(SAGEMAKER_STRINGS.SUMMARY_TAB);

      await mlEntitiesPage.expectTextVisible(SAGEMAKER_STRINGS.MODEL_DESCRIPTION);

      // Verify metrics table and its content
      await mlEntitiesPage.waitForMetricsTable();
      await mlEntitiesPage.expectMetricVisible(SAGEMAKER_STRINGS.METRIC_NAME);
      await mlEntitiesPage.waitForHyperparametersTable();
      await mlEntitiesPage.expectHyperparameterVisible(SAGEMAKER_STRINGS.HYPERPARAM_NAME);

      // Navigate to Features tab
      await mlEntitiesPage.clickTab(SAGEMAKER_STRINGS.FEATURES_TAB);
      await mlEntitiesPage.waitForPageLoad();
      await mlEntitiesPage.expectFeatureVisible(SAGEMAKER_STRINGS.FEATURE_NAME);

      // Navigate to Group tab
      await mlEntitiesPage.clickTab(SAGEMAKER_STRINGS.GROUP_TAB);
      await mlEntitiesPage.waitForPageLoad();

      const groupUrl = await mlEntitiesPage.getCurrentUrl();
      expect(groupUrl).toContain(SAGEMAKER_STRINGS.GROUP_TAB);

      await mlEntitiesPage.expectModelGroupNameVisible(SAGEMAKER_STRINGS.GROUP_NAME);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // MLModelGroup Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('SageMaker MLModelGroup', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit sagemaker model group with description', async () => {
      // Navigate directly to model group page
      await mlEntitiesPage.navigateToMLModelGroup(TEST_DATA.SAGEMAKER_GROUP_URN);

      const currentUrl = await mlEntitiesPage.getCurrentUrl();
      expect(currentUrl).toContain(SAGEMAKER_STRINGS.GROUP_URL_PATTERN);

      // Verify the specific model is visible in the group (using exact match for model link)
      await mlEntitiesPage.clickModelLink(SAGEMAKER_STRINGS.MODEL_NAME);
      await mlEntitiesPage.navigateToMLModelGroup(TEST_DATA.SAGEMAKER_GROUP_URN);
      await mlEntitiesPage.expectTextVisible(SAGEMAKER_STRINGS.GROUP_DESCRIPTION);
    });
  });
});
