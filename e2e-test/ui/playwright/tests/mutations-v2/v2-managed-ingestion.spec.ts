/**
 * Managed ingestion run tests — migrated from Cypress e2e/mutationsV2/v2_managed_ingestion.js
 *
 * Creates a demo-data ingestion source using the YAML editor ("Other" type),
 * runs it, waits for it to succeed, then cleans up.
 *
 * The showIngestionPageRedesign feature flag is forced to false.
 */

import { test } from '../../fixtures/base-test';
import { IngestionPage } from '../../pages/ingestion.page';

test.describe('run managed ingestion', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ showIngestionPageRedesign: false });
  });

  test('create run managed ingestion source', async ({ page, logger, logDir }) => {
    // Long timeout because we wait for the ingestion run to complete
    test.setTimeout(240000);

    const number = Math.floor(Math.random() * 100000);
    const testName = `cypress test source ${number}`;

    const ingestionPage = new IngestionPage(page, logger, logDir);

    logger.step('navigate to ingestion page');
    await ingestionPage.navigate();
    await ingestionPage.waitForSourcesLoaded();

    logger.step('create "Other" type ingestion source with demo-data recipe');
    await ingestionPage.clickCreateSourceButton();
    await ingestionPage.selectOtherDataSource();

    // Set the recipe — atomic select-all+type is more reliable under concurrent load
    await ingestionPage.setMonacoEditorContent('source:\n    type: demo-data\nconfig: {}');

    await ingestionPage.clickNextButton();
    await ingestionPage.clickNextButton();

    await ingestionPage.fillSourceName(testName);

    logger.step('save and run ingestion source');
    await ingestionPage.clickSaveAndRunButton();
    await ingestionPage.expectWizardModalClosed(30000);
    // Use the retry+refresh loop rather than a bare toBeVisible: the ES index
    // may not reflect the newly created source within the 30 s window.
    await ingestionPage.expectSourceEventuallyVisible(testName);

    // Wait for the ingestion run to succeed (up to 3 minutes)
    logger.step('wait for ingestion to succeed');
    await ingestionPage.expectSourceSucceeded(testName);

    // Delete the source
    logger.step('delete ingestion source');
    await ingestionPage.deleteSource(testName);
  });
});
