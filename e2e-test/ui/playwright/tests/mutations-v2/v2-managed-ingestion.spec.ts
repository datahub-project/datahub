/**
 * Managed ingestion run tests — migrated from Cypress e2e/mutationsV2/v2_managed_ingestion.js
 *
 * Creates a demo-data ingestion source using the YAML editor ("Other" type),
 * runs it, waits for it to succeed, then cleans up.
 *
 * The showIngestionPageRedesign feature flag is forced to false.
 */

import { test, expect } from '../../fixtures/base-test';
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
    await ingestionPage.searchDataSource('other');
    // "Other" option may need scrolling into view
    const otherOption = page.getByText('Other').first();
    await otherOption.scrollIntoViewIfNeeded();
    await otherOption.click();
    await expect(page.getByText('source-type').first()).toBeVisible({ timeout: 15000 });

    // Set the recipe — atomic select-all+type is more reliable under concurrent load
    await ingestionPage.setMonacoEditorContent('source:\n    type: demo-data\nconfig: {}');

    await ingestionPage.clickNextButton();
    await ingestionPage.clickNextButton();

    await ingestionPage.fillSourceName(testName);

    logger.step('save and run ingestion source');
    // Use exact role match to avoid matching 'Save' which is also a substring of 'Save & Run'
    const saveAndRun = page.getByRole('button', { name: 'Save & Run', exact: true });
    await saveAndRun.scrollIntoViewIfNeeded();
    await saveAndRun.click();
    // Wait for the wizard modal to close — confirms the save (and run trigger) completed
    await expect(page.locator('.ant-modal')).not.toBeVisible({ timeout: 30000 });
    // The source row should now be in the list
    await expect(page.locator('tr').filter({ hasText: testName })).toBeVisible({ timeout: 30000 });

    // Wait for the ingestion run to succeed (up to 3 minutes)
    logger.step('wait for ingestion to succeed');
    await expect(page.locator('tr').filter({ hasText: testName }).getByText('Succeeded')).toBeVisible({
      timeout: 180000,
    });

    // Delete the source
    logger.step('delete ingestion source');
    await page
      .locator('tr')
      .filter({ hasText: testName })
      .locator(`[data-testid="delete-ingestion-source-${testName}"]`)
      .click();
    await page.locator(`[data-testid="confirm-delete-ingestion-source"]`).click();
    await expect(page.getByText(testName)).not.toBeVisible({ timeout: 15000 });
  });
});
