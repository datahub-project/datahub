/**
 * Application Sidebar Integration Tests — Migrated from Cypress applications.js
 *
 * Tests the application sidebar section on dataset entity pages:
 * - View application details and linked assets
 * - Feature flag control (showSidebarSectionWhenEmpty) for sidebar visibility
 * - Add/remove application associations to datasets
 * - Sidebar shows when filled (regardless of flag)
 * - Sidebar visibility when empty depends on feature flag state
 *
 * Test Data:
 * - Global Playwright seeded data:
 *   • APP: urn:li:application:d63587c6-cacc-4590-851c-4f51ca429b51 (Playwright Accounts Application)
 *   • Dataset with app: playwright_logging_events (hive)
 * - Feature-specific fixtures (from tests/application/fixtures/data.json):
 *   • Dataset without app: playwright_project.jaffle_shop.customers (bigquery)
 *   • Seeded via test.use({ featureName: 'application' })
 *
 * Page Object:
 * - ApplicationsPage: All application and sidebar interactions
 */

import { test } from '../../fixtures/base-test';
import { Page } from '@playwright/test';
import { ApplicationsPage } from '../../pages/applications.page';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';

const APP_URN = 'urn:li:application:d63587c6-cacc-4590-851c-4f51ca429b51';
const DATASET_WITH_APP_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_logging_events,PROD)';
const DATASET_WITH_APP_NAME = 'playwright_logging_events';
const DATASET_WITHOUT_APP_URN =
  'urn:li:dataset:(urn:li:dataPlatform:bigquery,playwright_project.jaffle_shop.customers,PROD)';
const DATASET_WITHOUT_APP_NAME = 'customers';
const TEST_APP_NAME = 'Playwright Accounts Application';

test.use({ featureName: 'application' });

test.describe('Application Sidebar Integration', () => {
  let applicationsPage: ApplicationsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    applicationsPage = new ApplicationsPage(page, logger, logDir);
    logger?.info('Initialized ApplicationsPage');
  });

  async function setApplicationFeatureFlag(page: Page, showSidebarSectionWhenEmpty: boolean): Promise<void> {
    await page.route('**/api/v2/graphql', async (route) => {
      const request = route.request();
      const postData = request.postDataJSON();

      if (postData?.operationName === 'appConfig') {
        const response = await route.fetch();
        const json = await response.json();

        if (json.data?.appConfig?.visualConfig?.application) {
          json.data.appConfig.visualConfig.application.showSidebarSectionWhenEmpty = showSidebarSectionWhenEmpty;
        }

        await route.fulfill({ response, json });
      } else {
        await route.continue();
      }
    });
  }

  test('should view application details and linked assets', async ({ logger }) => {
    logger?.info('Test: View application details and linked assets');
    await applicationsPage.navigateToApplication(APP_URN, 'Assets');
    logger?.info(`Verifying asset appears: ${DATASET_WITH_APP_NAME}`);
    await applicationsPage.verifyAssetInList(DATASET_WITH_APP_NAME);
    logger?.info('Verifying asset count: 1 - 1 of 1');
    await applicationsPage.verifyAssetCount('1 - 1 of 1');
    logger?.info('✓ Test passed');
  });

  test('should show application sidebar section always when filled', async ({ page, logger }) => {
    logger?.info('Test: Show sidebar when filled (flag OFF)');
    logger?.info('Setting feature flag to false');
    await setApplicationFeatureFlag(page, false);
    logger?.info(`Navigating to dataset: ${DATASET_WITH_APP_NAME}`);
    await applicationsPage.navigateToDataset(DATASET_WITH_APP_URN);
    logger?.info('Verifying application section is visible');
    await applicationsPage.verifyApplicationSectionVisible();
    logger?.info(`Verifying application visible: ${TEST_APP_NAME}`);
    await applicationsPage.verifyApplicationVisible(TEST_APP_NAME);
    logger?.info('✓ Test passed');
  });

  test('should show application sidebar section when empty if feature flag is on', async ({ page, logger }) => {
    logger?.info('Test: Show sidebar when empty (flag ON)');
    logger?.info('Setting feature flag to true');
    await setApplicationFeatureFlag(page, true);
    logger?.info(`Navigating to dataset without app: ${DATASET_WITHOUT_APP_NAME}`);
    await applicationsPage.navigateToDataset(DATASET_WITHOUT_APP_URN);
    logger?.info('Verifying "No application yet" text');
    await applicationsPage.verifyNoApplicationYetText();
    logger?.info('✓ Test passed');
  });

  test('should hide application sidebar section when empty if feature flag is off', async ({ page, logger }) => {
    logger?.info('Test: Hide sidebar when empty (flag OFF)');
    logger?.info('Setting feature flag to false');
    await setApplicationFeatureFlag(page, false);
    logger?.info(`Navigating to dataset without app: ${DATASET_WITHOUT_APP_NAME}`);
    await applicationsPage.navigateToDataset(DATASET_WITHOUT_APP_URN);
    logger?.info('Verifying "No application yet" text is NOT visible');
    await applicationsPage.verifyNoApplicationYetTextNotVisible();
    logger?.info('✓ Test passed');
  });

  test('should add and remove application from dataset', async ({ page, logger }) => {
    logger?.info('Test: Add and remove application from dataset');

    // Set feature flag BEFORE navigation so appConfig query is properly intercepted
    logger?.info('Setting feature flag to true');
    await setApplicationFeatureFlag(page, true);

    logger?.info(`Navigating to dataset: ${DATASET_WITHOUT_APP_NAME}`);
    await applicationsPage.navigateToDataset(DATASET_WITHOUT_APP_URN);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await page.waitForTimeout(TIMEOUTS.OPERATION);

    logger?.info(`Adding application: ${TEST_APP_NAME}`);
    await applicationsPage.addApplicationToDataset(TEST_APP_NAME);
    logger?.info('Verifying application is visible');
    await applicationsPage.verifyApplicationVisible(TEST_APP_NAME);

    logger?.info('Removing application');
    await applicationsPage.removeApplication();
    logger?.info('Verifying application is no longer visible');
    await applicationsPage.verifyApplicationNotVisible(TEST_APP_NAME);
    logger?.info('✓ Test passed');
  });
});
