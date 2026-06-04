import { SnowflakeFormDetails } from '@pages/ingestion/base/sources/SnowflakeSource';
import { test } from '../../fixtures/base-test';
import { IngestionV3Page } from '../../pages/ingestion/v3/ingestion-v3.page';
import { generateRandomString } from '../../utils/random';

test.use({ featureName: 'ingestion-v3' });

test.describe('secrets tab in manage data sources', () => {
  let ingestionPage: IngestionV3Page;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    ingestionPage = new IngestionV3Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      showIngestionPageRedesign: true,
      ingestionOnboardingRedesignV1: true,
      showNavBarRedesign: true,
    });

    await apiMock.suppressOnboardingModals();

    await ingestionPage.goto();
    await ingestionPage.secretsTab.open();
  });

  test('create and delete a secret', async () => {
    const suffix = generateRandomString();
    const secretName = `playwright_secret_${suffix}`;
    const secretValue = `secret-value-${suffix}`;
    const secretDescription = `playwright test secret description ${suffix}`;

    const secretUrn = await ingestionPage.secretsTab.createSecret(secretName, secretValue, secretDescription);
    await ingestionPage.secretsTab.expectSecretVisible(secretUrn);
    await ingestionPage.secretsTab.expectSecretDescriptionVisible(secretDescription);

    await ingestionPage.secretsTab.deleteSecret(secretUrn);
    await ingestionPage.secretsTab.expectSecretNotVisible(secretUrn);
    await ingestionPage.secretsTab.expectSecretDescriptionNotVisible(secretDescription);
  });

  test('create ingestion source using a secret', async ({ cleanup }) => {
    test.slow();

    const suffix = generateRandomString();
    const secretName = `playwright_ingestion_secret_${suffix}`;
    const secretValue = `secret-value-${suffix}`;
    const sourceName = `ingestion source ${suffix}`;
    const sourceDetails: SnowflakeFormDetails = {
      accountId: `account_${suffix}`,
      warehouseId: `warehouse_${suffix}`,
      username: `user_${suffix}`,
      role: `role_${suffix}`,
      authenticationType: 'userNameAndPassword',
      passwordSecret: secretName,
    };

    // Create the secret first via UI (MCP-seeded secrets do not appear in listSecrets).
    // Navigate to Sources via goto (full page reload) to reset the Apollo cache, so the
    // newly-created secret appears in the RecipeForm's secrets list when the modal opens.
    const secretUrn = await ingestionPage.secretsTab.createSecret(secretName, secretValue);

    await ingestionPage.sourcesTab.navigate();
    const sourceUrn = await ingestionPage.sourcesTab.createIngestionSource(sourceName, {
      sourceType: 'Snowflake',
      fillForm: async () => {
        await ingestionPage.sourcesTab.snowflakeSource.fillForm(sourceDetails);
      },
    });
    cleanup.track(sourceUrn, secretUrn);
    await ingestionPage.sourcesTab.expectSourceVisible(sourceName);
    await ingestionPage.sourcesTab.expectSourceStatusPending(sourceName);
  });

  test('deleted secret is absent from password dropdown', async () => {
    const suffix = generateRandomString();
    const secretName = `playwright_deleted_secret_${suffix}`;
    const secretValue = `secret-value-${suffix}`;

    const secretUrn = await ingestionPage.secretsTab.createSecret(secretName, secretValue);
    await ingestionPage.secretsTab.deleteSecret(secretUrn);

    await ingestionPage.sourcesTab.open();
    await ingestionPage.sourcesTab.openCreateSourceModal();
    await ingestionPage.sourcesTab.selectSourceType('Snowflake');
    await ingestionPage.sourcesTab.snowflakeSource.fillAuthenticationType({
      authenticationType: 'userNameAndPassword',
    });
    await ingestionPage.sourcesTab.snowflakeSource.expectSecretAbsentInPasswordDropdown(secretName);
    await ingestionPage.sourcesTab.cancelCreateSourceModal();
  });

  test('create secret inline during source creation', async ({ cleanup }) => {
    test.slow();

    const suffix = generateRandomString();
    const sourceName = `ingestion source inline ${suffix}`;
    const secretName = `playwright_inline_secret_${suffix}`;
    const secretValue = `secret-value-${suffix}`;
    const sourceDetails: SnowflakeFormDetails = {
      accountId: `account_${suffix}`,
      warehouseId: `warehouse_${suffix}`,
      username: `user_${suffix}`,
      role: `role_${suffix}`,
      authenticationType: 'userNameAndPassword',
    };

    let secretUrn = '';
    await ingestionPage.sourcesTab.open();
    const sourceUrn = await ingestionPage.sourcesTab.createIngestionSource(sourceName, {
      sourceType: 'Snowflake',
      fillForm: async () => {
        await ingestionPage.sourcesTab.snowflakeSource.fillForm(sourceDetails);
        secretUrn = await ingestionPage.sourcesTab.createSecretInlineForPassword(secretName, secretValue);
      },
    });
    cleanup.track(sourceUrn, secretUrn);
    await ingestionPage.sourcesTab.expectSourceVisible(sourceName);
    await ingestionPage.sourcesTab.expectSourceStatusPending(sourceName);
  });
});
