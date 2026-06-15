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

    test('can visit experiment and see training run', async () => {
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      await mlEntitiesPage.expectContainerTitleVisible(EXPERIMENT_STRINGS.CONTAINER_TITLE);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.CONTAINER_DESCRIPTION);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.TRAINING_RUN_NAME);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // Training Run (DataProcessInstance) Details Tests
  // ═══════════════════════════════════════════════════════════════════════════

  test.describe('Training Run Details', () => {
    test.beforeEach(async ({ page, logger, logDir }) => {
      mlEntitiesPage = new MLEntitiesPage(page, logger, logDir);
    });

    test('can visit container and see training run details', async () => {
      await mlEntitiesPage.navigateToContainer(TEST_DATA.CONTAINER_URN);
      await mlEntitiesPage.expectContainerTitleVisible(EXPERIMENT_STRINGS.CONTAINER_TITLE);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.CONTAINER_DESCRIPTION);
      await mlEntitiesPage.clickTrainingRunLink(EXPERIMENT_STRINGS.TRAINING_RUN_NAME);
      await mlEntitiesPage.waitForPageLoad();
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_STATUS);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_DURATION);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_ID);
      await mlEntitiesPage.expectTextVisible(EXPERIMENT_STRINGS.RUN_OUTPUT);
    });
  });
});
