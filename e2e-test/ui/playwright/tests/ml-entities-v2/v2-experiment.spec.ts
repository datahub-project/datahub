/**
 * Experiment Container Tests — Playwright E2E tests for ML experiments.
 *
 * Equivalent to Cypress: smoke-test/tests/cypress/cypress/e2e/ml/experiment.js
 *
 * Tests cover:
 * - Container (Experiment) with associated training runs
 * - DataProcessInstance (Training Run) with metrics, parameters, and parent experiment
 *
 * Test Data (globally seeded):
 * - Container: airline_forecast_experiment
 * - Training Run: simple_training_run
 *
 * All tests assume authenticated context via loginFixture.
 */

import { test } from '../../fixtures/base-test';
import { MLEntitiesPage } from '../../pages/ml-entities.page';

// Test Data Constants
const TEST_DATA = {
  CONTAINER_URN: 'urn:li:container:airline_forecast_experiment',
} as const;

const EXPERIMENT_STRINGS = {
  CONTAINER_TITLE: 'Airline Forecast Experiment',
  CONTAINER_DESCRIPTION: 'Experiment to forecast airline passenger numbers',
  TRAINING_RUN_NAME: 'Simple Training Run',
  RUN_STATUS: 'Failure',
  RUN_DURATION: '1 sec',
  RUN_ID: 'simple_training_run',
  RUN_OUTPUT: 's3://my-bucket/output',
  METRIC_ACCURACY: 'accuracy',
  HYPERPARAM_LEARNING_RATE: 'learning_rate',
  CREATOR_URN: 'urn:li:corpuser:datahub',
} as const;

test.describe('Experiment Container and Training Runs', () => {
  let mlEntitiesPage: MLEntitiesPage;

  // ═══════════════════════════════════════════════════════════════════════════
  // Container (Experiment) Summary Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('Container (Experiment) Summary', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit experiment and see training run', async ({ logger }) => {
      logger.step('Navigate to experiment container');
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      logger.step('Verify experiment title and description');
      await mlEntitiesPage.expectContainerTitleVisible(EXPERIMENT_STRINGS.CONTAINER_TITLE);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.CONTAINER_DESCRIPTION);
      logger.step('Verify training run link is visible');
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.TRAINING_RUN_NAME);
    });

    test('can navigate to training run and return to experiment', async ({ logger }) => {
      logger.step('Navigate to experiment container');
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      logger.step('Verify experiment title visible before navigation');
      await mlEntitiesPage.expectContainerTitleVisible(EXPERIMENT_STRINGS.CONTAINER_TITLE);
      logger.step('Click training run link to navigate away');
      await mlEntitiesPage.clickTrainingRunLink(EXPERIMENT_STRINGS.TRAINING_RUN_NAME);
      logger.step('Verify training run page loaded');
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_ID);
      logger.step('Navigate back to experiment');
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      logger.step('Verify returned to experiment page');
      await mlEntitiesPage.expectContainerTitleVisible(EXPERIMENT_STRINGS.CONTAINER_TITLE);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // Training Run (DataProcessInstance) Details Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('Training Run Details', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit container and see training run details', async ({ logger }) => {
      logger.step('Navigate to training run');
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      logger.step('Verify experiment metadata');
      await mlEntitiesPage.expectContainerTitleVisible(EXPERIMENT_STRINGS.CONTAINER_TITLE);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.CONTAINER_DESCRIPTION);
      logger.step('Click training run link');
      await mlEntitiesPage.clickTrainingRunLink(EXPERIMENT_STRINGS.TRAINING_RUN_NAME);
      logger.step('Verify training run details');
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_STATUS);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_DURATION);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_ID);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_OUTPUT);
      logger.step('Verify creator information');
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.CREATOR_URN);
    });

    test('can view and verify training run metrics', async ({ logger }) => {
      logger.step('Navigate to training run');
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      logger.step('Click training run link');
      await mlEntitiesPage.clickTrainingRunLink(EXPERIMENT_STRINGS.TRAINING_RUN_NAME);
      logger.step('Wait for metrics table to load');
      await mlEntitiesPage.waitForMetricsTable();
      logger.step('Verify metric values are visible');
      await mlEntitiesPage.expectMetricVisible(EXPERIMENT_STRINGS.METRIC_ACCURACY);
      logger.step('Wait for hyperparameters table to load');
      await mlEntitiesPage.waitForHyperparametersTable();
      logger.step('Verify hyperparameter values are visible');
      await mlEntitiesPage.expectHyperparameterVisible(EXPERIMENT_STRINGS.HYPERPARAM_LEARNING_RATE);
    });
  });
});
