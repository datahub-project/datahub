/**
 * Managing secrets for ingestion creation tests
 * Migrated from Cypress e2e/mutationsV2/v2_managing_secrets.js
 *
 * Tests:
 *   1. Create a secret
 *   2. Create an ingestion source that uses the secret for the password field
 *   3. Remove the secret, verify it no longer appears in the password dropdown
 *   4. Create a new secret inline during ingestion source creation
 *   5. Cleanup all created resources
 *
 * showIngestionPageRedesign is forced to false throughout.
 */

import { test, expect } from '../../fixtures/base-test';
import { IngestionPage } from '../../pages/ingestion.page';

test.describe('managing secrets for ingestion creation', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ showIngestionPageRedesign: false });
  });

  test('create a secret, create ingestion source using a secret, remove a secret', async ({ page, logger, logDir }) => {
    // This test has 7 multi-step operations (create/delete secrets + sources) — needs > 30s default.
    test.setTimeout(5 * 60 * 1000);
    const number = Math.floor(Math.random() * 100000);
    const accountId = `account${number}`;
    const warehouseId = `warehouse${number}`;
    const username = `user${number}`;
    const role = `role${number}`;
    const ingestionSourceName = `ingestion source ${number}`;
    const secretName = `secretname${number}`;
    const secretValue = `secretvalue${number}`;
    const secretDescription = `secretdescription${number}`;

    const ingestionPage = new IngestionPage(page, logger, logDir);

    // ── Step 1: Create a secret ────────────────────────────────────────────
    logger.step('navigate to ingestion page and create a secret');
    await ingestionPage.navigate();
    await ingestionPage.clickSecretsTab();
    await ingestionPage.createSecret(secretName, secretValue, secretDescription);
    await expect(page.getByText('Successfully created Secret!')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText(secretName)).toBeVisible();
    await expect(page.getByText(secretDescription)).toBeVisible();
    await page.waitForTimeout(5000); // allow ES to index the secret

    // ── Step 2: Create ingestion source using the secret ──────────────────
    logger.step('create ingestion source using the secret');
    await ingestionPage.navigate();
    await ingestionPage.clickSourcesTab();
    await page.locator('#ingestion-create-source').click();
    await ingestionPage.searchDataSource('snowflake');
    await ingestionPage.selectDataSource('Snowflake');

    await ingestionPage.fillSnowflakeForm({ accountId, warehouseId, username, role });
    // Auth type must be selected separately since we're using a secret, not a plain password
    await page.locator('#authentication_type').click({ force: true });
    // dispatchEvent bypasses Playwright's viewport check; AntD dropdown may render below the fold
    await page.locator('.ant-select-dropdown [title="Username & Password"]').dispatchEvent('click');
    await ingestionPage.selectSecretForPasswordField(secretName);

    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.getByText('Configure an Ingestion Schedule')).toBeVisible();
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.locator('.ant-collapse-item')).toBeVisible({ timeout: 15000 });
    await ingestionPage.fillSourceName(ingestionSourceName);
    await ingestionPage.clickSaveButton();
    await expect(page.getByText('Successfully created ingestion source!')).toBeVisible({ timeout: 30000 });
    await page.waitForTimeout(5000);

    await ingestionPage.searchSources(ingestionSourceName);
    await ingestionPage.expectSourceVisible(ingestionSourceName);
    await ingestionPage.expectSourceStatusPending(ingestionSourceName);

    // ── Step 3: Remove the secret ─────────────────────────────────────────
    logger.step('remove the secret');
    await ingestionPage.clickSecretsTab();
    await expect(page.getByText(secretName)).toBeVisible({ timeout: 15000 });
    await ingestionPage.deleteSecret(secretName);
    await expect(page.getByText(secretDescription)).not.toBeVisible({ timeout: 10000 });

    // ── Step 4: Remove ingestion source ───────────────────────────────────
    logger.step('remove ingestion source');
    await ingestionPage.navigate();
    await ingestionPage.clickSourcesTab();
    await ingestionPage.deleteSource(ingestionSourceName);

    // ── Step 5: Verify secret is absent during new source creation ────────
    logger.step('verify deleted secret is not in password dropdown');
    await ingestionPage.clickCreateSourceButton();
    await ingestionPage.searchDataSource('snowflake');
    await ingestionPage.selectDataSource('Snowflake');

    await ingestionPage.fillSnowflakeForm({ accountId, warehouseId, username, role });
    await page.locator('#authentication_type').click({ force: true });
    await page.locator('.ant-select-dropdown [title="Username & Password"]').dispatchEvent('click');
    await page.locator('#password').clear();
    await page.locator('#password').press('ArrowDown');
    await expect(page.getByText(secretName)).not.toBeVisible({ timeout: 5000 });

    // ── Step 6: Create secret inline and verify it can be used ───────────
    logger.step('create secret inline during source creation');
    await page.getByText('Create Secret').click();
    await ingestionPage.fillAndSubmitSecretModal(secretName, secretValue, secretDescription);
    await expect(page.getByText('Created secret!')).toBeVisible({ timeout: 15000 });

    await page.locator('#role').fill(role);
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.getByText('Configure an Ingestion Schedule')).toBeVisible();
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.locator('.ant-collapse-item')).toBeVisible({ timeout: 15000 });
    await ingestionPage.fillSourceName(ingestionSourceName);
    await ingestionPage.clickSaveButton();
    await expect(page.getByText('Successfully created ingestion source!')).toBeVisible({ timeout: 30000 });
    await page.waitForTimeout(5000);

    await ingestionPage.searchSources(ingestionSourceName);
    await ingestionPage.expectSourceVisible(ingestionSourceName);
    await ingestionPage.expectSourceStatusPending(ingestionSourceName);

    // ── Step 7: Final cleanup — ingestion source + secret ────────────────
    logger.step('final cleanup: remove ingestion source and secret');
    await ingestionPage.navigate();
    await ingestionPage.deleteSource(ingestionSourceName);

    await ingestionPage.clickSecretsTab();
    await expect(page.getByText(secretName)).toBeVisible({ timeout: 15000 });
    await ingestionPage.deleteSecret(secretName);
    await expect(page.getByText(secretDescription)).not.toBeVisible({ timeout: 10000 });
  });
});
