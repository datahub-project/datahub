/**
 * Application Management Page Tests — Migrated from Cypress manage_applications.js
 *
 * Tests the /applications management page:
 * - Search functionality with no results verification
 * - Create application with name and description
 * - Delete application workflow with confirmation
 * - Page title and UI element visibility
 *
 * Test Data:
 * - Uses dynamic application names with withRandomSuffix() for isolation
 * - No fixture seeding required — tests create/delete on-demand
 *
 * Page Object:
 * - ApplicationsPage: All application management page interactions
 */

import { test } from '../../fixtures/base-test';
import { ApplicationsPage } from '../../pages/applications.page';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';
import { withRandomSuffix } from '../../utils/random';

const NEW_APP_NAME = withRandomSuffix('test-new');
const NEW_APP_DESCRIPTION = 'test new description';

test.describe('Application Management Page', () => {
  let applicationsPage: ApplicationsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    applicationsPage = new ApplicationsPage(page, logger, logDir);
    logger?.info('Initialized ApplicationsPage');
  });

  test('should verify search input placeholder and page title', async ({ logger }) => {
    logger?.info('Test: Verify search input placeholder and page title');
    await applicationsPage.navigateToApplicationsPage();
    await applicationsPage.verifyPageTitle();
    await applicationsPage.verifySearchInputPlaceholder();
    logger?.info('✓ Test passed');
  });

  test('should verify search returns no results for non-existent application', async ({ logger }) => {
    logger?.info('Test: Verify search returns no results');
    await applicationsPage.navigateToApplicationsPage();
    await applicationsPage.verifyPageTitle();
    await applicationsPage.verifyPageLoaded();
    logger?.info('Searching for non-existent application: testtestnomatch');
    await applicationsPage.searchApplication('testtestnomatch');
    await applicationsPage.verifyNoSearchResults();
    logger?.info('✓ Test passed');
  });

  test('should create and delete an application', async ({ page, logger }) => {
    logger?.info('Test: Create and delete application');
    await applicationsPage.navigateToApplicationsPage();
    await applicationsPage.verifyPageTitle();
    await applicationsPage.verifyPageLoaded();
    logger?.info(`Creating application: ${NEW_APP_NAME}`);
    await applicationsPage.clickCreateApplicationButton();
    await applicationsPage.enterApplicationName(NEW_APP_NAME);
    await applicationsPage.enterApplicationDescription(NEW_APP_DESCRIPTION);
    await applicationsPage.clickCreateButton();

    // Wait for async create operation to complete before checking table
    logger?.info('Waiting for refetch after create');
    await page.waitForTimeout(TIMEOUTS.SHORT);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await applicationsPage.verifyApplicationInTable(NEW_APP_NAME);
    await applicationsPage.verifyPageLoaded();
    logger?.info(`Deleting application: ${NEW_APP_NAME}`);
    await applicationsPage.deleteApplication(NEW_APP_NAME);

    // Wait for async delete operation to complete before checking table
    logger?.info('Waiting for refetch after delete');
    await page.waitForTimeout(TIMEOUTS.SHORT);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await applicationsPage.verifyApplicationNotInTable(NEW_APP_NAME);
    logger?.info('✓ Test passed');
  });
});
