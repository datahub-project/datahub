/**
 * Autocomplete Search Bar tests — migrated from Cypress
 *
 * Coverage:
 * 1. Typing in search bar triggers autocomplete dropdown
 * 2. Results appear grouped by entity type with section headers
 * 3. Multiple entity types searchable (datasets, dashboards, dataflows)
 * 4. Clearing input and searching again works
 * 5. Clicking a result navigates to entity profile with correct URN
 *
 * Test Data (fixtures/data.json):
 * - SamplePlaywrightHdfsDataset (DATASET / hdfs)
 * - SamplePlaywrightDashboard (DASHBOARD / looker)
 * - SamplePlaywrightDataflow (DATA_JOB / airflow)
 */

import { test } from '../../fixtures/base-test';
import { AutocompletePage } from '../../pages/autocomplete.page';

test.use({ featureName: 'autocomplete-v2' });

const SAMPLE_DATASET_NAME = 'SamplePlaywrightHdfsDataset';
const SAMPLE_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const SAMPLE_DASHBOARD_NAME = 'SamplePlaywrightDashboard';
const SAMPLE_DASHBOARD_URN = 'urn:li:dashboard:(looker,sample-dashboard)';
const SAMPLE_CHART_NAME = 'SamplePlaywrightChart';
const SAMPLE_CHART_URN = 'urn:li:chart:(looker,sample-chart)';

test.describe('Autocomplete Search Bar', () => {
  let autocompletePage: AutocompletePage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    autocompletePage = new AutocompletePage(page, logger, logDir);
    await autocompletePage.navigateToHome();
  });

  test('should see auto-complete results after typing in a search query', async () => {
    // Test 1: Search for dataset — verify autocomplete appears with results
    await autocompletePage.searchAndVerifyResult(SAMPLE_DATASET_NAME, SAMPLE_DATASET_URN);
    await autocompletePage.clearSearchInput();

    // Test 2: Search for dashboard — verify autocomplete with different entity
    await autocompletePage.searchAndVerifyResult(SAMPLE_DASHBOARD_NAME, SAMPLE_DASHBOARD_URN);
    await autocompletePage.clearSearchInput();

    // Test 3: Search for chart — verify autocomplete works for multiple entity types
    await autocompletePage.searchAndVerifyResult(SAMPLE_CHART_NAME, SAMPLE_CHART_URN);
  });

  test('should send you to the entity profile after clicking on an auto-complete option', async () => {
    // Search for dataset, verify autocomplete, click result, and verify navigation
    await autocompletePage.searchAndNavigateToEntity(SAMPLE_DATASET_NAME, SAMPLE_DATASET_URN);
  });
});
