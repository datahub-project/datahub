/**
 * Task Runs V2 tests — Migrated from Cypress task-runs-v2
 *
 * Coverage:
 * 1. Navigate to dataset runs view and verify run metadata (timestamp, status, datasets)
 * 2. Navigate to task runs view and verify complete run metadata with all relationships
 * 3. Verify input dataset link is visible and contains correct name
 * 4. Verify both output dataset links are visible with correct names
 *
 * Test Data (from global test-data/data.json):
 * - SamplePlaywrightHiveDataset: output dataset with configured runs
 * - dataProcessInstance: manual__2022-03-30T11:35:08.970522+00:00 (run record)
 *   - Input: fct_playwright_users_created_no_tag
 *   - Outputs: SamplePlaywrightHiveDataset, playwright_logging_events
 *   - Status: Failed (with COMPLETE state + FAILURE result type)
 *   - Parent Task: playwright_task_123 in playwright_dag_abc
 *
 * Page Objects:
 * - TaskRunsPage: Handles navigation and assertions for runs views
 *   Selectors in POM: getRunNameCell(name), getRunStatusCell(name), getDatasetLink(urn) — all dynamic/parameterized
 */

import { test } from '../../fixtures/base-test';
import { TaskRunsPage } from '../../pages/task-runs.page';

const SAMPLE_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)';
const SAMPLE_TASK_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,playwright_dag_abc,PROD),playwright_task_123)';

const RUN_TIMESTAMP = 'manual__2022-03-30T11:35:08.970522+00:00';
const RUN_STATUS = 'Failed';
const INPUT_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_playwright_users_created_no_tag,PROD)';
const INPUT_DATASET_NAME = 'fct_playwright_users_created_no_tag';
const OUTPUT_DATASET_URN_1 = 'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)';
const OUTPUT_DATASET_NAME_1 = 'SamplePlaywrightHiveDataset';
const OUTPUT_DATASET_URN_2 = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_logging_events,PROD)';
const OUTPUT_DATASET_NAME_2 = 'playwright_logging_events';

test.describe('task runs', () => {
  let taskRunsPage: TaskRunsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    taskRunsPage = new TaskRunsPage(page, logger, logDir);
  });

  test('can visit dataset with runs aspect and verify the task run is present', async () => {
    await taskRunsPage.navigateToDatasetRuns(SAMPLE_DATASET_URN);

    await taskRunsPage.verifyRunId(RUN_TIMESTAMP, RUN_TIMESTAMP);
    await taskRunsPage.verifyRunStatus(RUN_TIMESTAMP, RUN_STATUS);
    await taskRunsPage.assertDatasetLinkVisible(INPUT_DATASET_URN, INPUT_DATASET_NAME);
    await taskRunsPage.assertDatasetLinkVisible(OUTPUT_DATASET_URN_1, OUTPUT_DATASET_NAME_1);
    await taskRunsPage.assertDatasetLinkVisible(OUTPUT_DATASET_URN_2, OUTPUT_DATASET_NAME_2);
  });

  test('can visit task with runs aspect and verify the task run is present', async () => {
    await taskRunsPage.navigateToTaskRuns(SAMPLE_TASK_URN);

    await taskRunsPage.verifyRunId(RUN_TIMESTAMP, RUN_TIMESTAMP);
    await taskRunsPage.verifyRunStatus(RUN_TIMESTAMP, RUN_STATUS);
    await taskRunsPage.assertDatasetLinkVisible(INPUT_DATASET_URN, INPUT_DATASET_NAME);
    await taskRunsPage.assertDatasetLinkVisible(OUTPUT_DATASET_URN_1, OUTPUT_DATASET_NAME_1);
    await taskRunsPage.assertDatasetLinkVisible(OUTPUT_DATASET_URN_2, OUTPUT_DATASET_NAME_2);
  });
});
