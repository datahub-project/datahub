/**
 * Ingestion source creation flow tests — migrated from Cypress e2e/mutationsV2/v2_ingestion_source.js
 *
 * Tests the full Snowflake ingestion source wizard: create, verify saved values,
 * edit the name, and delete.
 *
 * The showIngestionPageRedesign feature flag is forced to false so the classic
 * ingestion wizard is used throughout.
 */

import { test, expect } from '../../fixtures/base-test';
import { IngestionPage } from '../../pages/ingestion.page';

test.describe('ingestion source creation flow', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ showIngestionPageRedesign: false });
  });

  test('create a ingestion source using ui, verify ingestion source details saved correctly, remove ingestion source', async ({
    page,
    logger,
    logDir,
  }) => {
    const number = Math.floor(Math.random() * 100000);
    const accountId = `account${number}`;
    const warehouseId = `warehouse${number}`;
    const username = `user${number}`;
    const password = `password${number}`;
    const role = `role${number}`;
    const ingestionSourceName = `ingestion source ${number}`;

    const ingestionPage = new IngestionPage(page, logger, logDir);

    logger.step('navigate to ingestion page');
    await ingestionPage.navigate();

    await ingestionPage.clickSourcesTab();
    await ingestionPage.waitForSourcesLoaded();

    logger.step('create new Snowflake ingestion source');
    await ingestionPage.clickCreateSourceButton();
    await ingestionPage.searchDataSource('snowflake');
    await ingestionPage.selectDataSource('Snowflake');

    await ingestionPage.fillSnowflakeForm({ accountId, warehouseId, username, password, role });

    // Verify YAML recipe is generated correctly
    logger.step('verify yaml recipe');
    await ingestionPage.clickRecipeYamlButton();
    await expect(page.getByText('account_id')).toBeVisible();
    await expect(page.getByText(accountId)).toBeVisible();
    await expect(page.getByText(warehouseId)).toBeVisible();
    await expect(page.getByText(username)).toBeVisible();
    await expect(page.getByText(password)).toBeVisible();
    await expect(page.getByText(role)).toBeVisible();

    // Complete the wizard
    logger.step('finish creating source');
    await ingestionPage.clickRecipeNextButton();
    await ingestionPage.expectScheduleStepVisible();
    await ingestionPage.clickScheduleNextButton();
    await ingestionPage.fillSourceName(ingestionSourceName);
    await ingestionPage.clickSaveButton();
    await expect(page.getByText('Successfully created ingestion source!')).toBeVisible({ timeout: 30000 });
    await ingestionPage.expectSourceEventuallyVisible(ingestionSourceName);
    await ingestionPage.expectSourceStatusPending(ingestionSourceName);

    // Verify values are saved correctly by reopening the wizard
    logger.step('verify saved ingestion source details');
    await ingestionPage.openEditForSource(ingestionSourceName);
    await ingestionPage.expectSnowflakeFormValues({ accountId, warehouseId, username, password, role });

    // Advance through wizard to name step and rename the source
    await ingestionPage.clickNextButton();
    await ingestionPage.expectScheduleStepVisible();
    await ingestionPage.clickScheduleNextButton();
    await ingestionPage.fillSourceName(`${ingestionSourceName} EDITED`);
    await ingestionPage.clickSaveButton();
    await expect(page.getByText('Successfully updated ingestion source!')).toBeVisible({ timeout: 15000 });
    await ingestionPage.expectSourceEventuallyVisible(`${ingestionSourceName} EDITED`);

    // Delete the ingestion source
    logger.step('remove ingestion source');
    await ingestionPage.deleteSource(`${ingestionSourceName} EDITED`);
  });
});
