import { SnowflakeFormDetails } from '@pages/ingestion/base/sources/SnowflakeSource';
import { test } from '../../fixtures/base-test';
import { IngestionV3Page } from '../../pages/ingestion/v3/ingestion-v3.page';
import { generateRandomString, withRandomSuffix } from '../../utils/random';

// Pre-seeded source names from tests/ingestion-v3/fixtures/data.json
const EXECUTE_SOURCE = 'playwright v3 execute source';
const EXECUTE_SOURCE_URN = 'urn:li:dataHubIngestionSource:playwright-v3-execute-source';
const RUN_DETAILS_SOURCE = 'playwright v3 run details source';
const SEARCH_SOURCE_1 = 'playwright v3 sources search apple';
const SEARCH_SOURCE_2 = 'playwright v3 sources search banana';

test.use({ featureName: 'ingestion-v3' });

test.describe('ingestion sources', () => {
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
  });

  test('create, edit, and delete an ingestion source', async () => {
    const suffix = generateRandomString();
    const sourceName = `ingestion source ${suffix}`;
    const sourceDetails: SnowflakeFormDetails = {
      accountId: `account${suffix}`,
      warehouseId: `warehouse${suffix}`,
      username: `user${suffix}`,
      password: `password${suffix}`,
      role: `role${suffix}`,
      authenticationType: 'userNameAndPassword',
    };
    const updatedSourceName = `updated ingestion source ${suffix}`;
    const updatedSourceDetails: SnowflakeFormDetails = { ...sourceDetails, password: `updated${suffix}` };

    const sourceUrn = await ingestionPage.sourcesTab.createIngestionSource(sourceName, {
      sourceType: 'Snowflake',
      fillForm: async () => {
        await ingestionPage.sourcesTab.snowflakeSource.fillForm(sourceDetails);
        await ingestionPage.sourcesTab.snowflakeSource.expectYamlRecipe(
          [
            sourceDetails.accountId,
            sourceDetails.warehouseId,
            sourceDetails.username,
            sourceDetails.password,
            sourceDetails.role,
          ].filter((value): value is string => !!value),
        );
      },
      schedule: { enabled: true, hour: '01' },
    });
    await ingestionPage.sourcesTab.expectSourceVisible(sourceName);
    await ingestionPage.sourcesTab.expectSourceStatusPending(sourceName);
    await ingestionPage.sourcesTab.expectSchedule(sourceName, '01:00 am');

    await ingestionPage.sourcesTab.updateIngestionSource(sourceUrn, {
      sourceName: updatedSourceName,
      displayName: sourceName,
      verifyForm: async () => {
        await ingestionPage.sourcesTab.snowflakeSource.expectFormValues(sourceDetails);
      },
      fillForm: async () => {
        await ingestionPage.sourcesTab.snowflakeSource.fillForm(updatedSourceDetails);
      },
      schedule: { enabled: true, hour: '06' },
    });
    await ingestionPage.sourcesTab.expectSourceVisible(updatedSourceName);
    await ingestionPage.sourcesTab.expectSchedule(updatedSourceName, '06:00 am');

    await ingestionPage.sourcesTab.deleteIngestionSource(sourceUrn, updatedSourceName);
    await ingestionPage.sourcesTab.expectSourceNotVisible(updatedSourceName);
  });

  test('create and run an ingestion source', async ({ cleanup }) => {
    test.slow();

    const sourceName = withRandomSuffix('create and run source');

    const sourceUrn = await ingestionPage.sourcesTab.createIngestionSource(sourceName, {
      sourceType: 'Custom',
      fillForm: async () => {
        await ingestionPage.sourcesTab.customSource.fillForm();
      },
      shouldRun: true,
    });
    cleanup.track(sourceUrn);
  });

  test('execute an ingestion source', async () => {
    test.slow();

    await ingestionPage.sourcesTab.runIngestionSource(EXECUTE_SOURCE_URN, EXECUTE_SOURCE);
    await ingestionPage.sourcesTab.expectSourceStatusContains(EXECUTE_SOURCE, 'Success');
  });

  test('search for an ingestion source', async () => {
    await ingestionPage.sourcesTab.search(SEARCH_SOURCE_1);

    await ingestionPage.sourcesTab.expectSourceVisible(SEARCH_SOURCE_1);
    await ingestionPage.sourcesTab.expectSourceNotVisible(SEARCH_SOURCE_2);

    await ingestionPage.sourcesTab.clearSearch();
  });

  test('view run details page', async () => {
    // Source and its SUCCESS execution are pre-seeded via fixtures/data.json.
    await ingestionPage.sourcesTab.search(RUN_DETAILS_SOURCE);
    await ingestionPage.sourcesTab.expectSourceVisible(RUN_DETAILS_SOURCE);

    // Click the status badge in the Last Run column to navigate to run details
    await ingestionPage.sourcesTab.clickRunDetails(RUN_DETAILS_SOURCE);

    await ingestionPage.sourcesTab.expectRunDetailsPageVisible();
    await ingestionPage.sourcesTab.expectRunSuccessVisible();

    await ingestionPage.sourcesTab.navigateBackToIngestion();
  });
});
