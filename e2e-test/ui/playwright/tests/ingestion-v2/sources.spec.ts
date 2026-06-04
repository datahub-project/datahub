import { SnowflakeFormDetails } from '@pages/ingestion/base/sources/SnowflakeSource';
import { test, expect } from '../../fixtures/base-test';
import { IngestionV2Page } from '../../pages/ingestion/v2/ingestion-v2.page';
import { generateRandomString, withRandomSuffix } from '../../utils/random';

// Pre-seeded source names from tests/ingestion-v2/fixtures/data.json
const EXECUTE_SOURCE = 'playwright execute source';
const EXECUTE_SOURCE_URN = 'urn:li:dataHubIngestionSource:playwright-execute-source';
const SEARCH_SOURCE_ONE = 'playwright search source one';
const SEARCH_SOURCE_TWO = 'playwright search source two';
const FILTER_SOURCE = 'playwright filter source';

test.use({ featureName: 'ingestion-v2' });

test.describe('ingestion sources', () => {
  let ingestionPage: IngestionV2Page;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    ingestionPage = new IngestionV2Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      showIngestionPageRedesign: true,
      ingestionOnboardingRedesignV1: false,
      showNavBarRedesign: true,
    });

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

    await ingestionPage.sourcesTab.expectSourceStatusContains(sourceName, 'Success');
  });

  test('execute an ingestion source', async () => {
    test.slow();

    await ingestionPage.sourcesTab.runIngestionSource(EXECUTE_SOURCE_URN, EXECUTE_SOURCE);

    await ingestionPage.sourcesTab.expectSourceStatusContains(EXECUTE_SOURCE, 'Success');
  });

  test('search for an ingestion source', async () => {
    await expect(ingestionPage.sourcesTab.sourcesSearchInput).toBeVisible();
    await ingestionPage.sourcesTab.search(SEARCH_SOURCE_ONE);

    await ingestionPage.sourcesTab.expectSourceVisible(SEARCH_SOURCE_ONE);
    await ingestionPage.sourcesTab.expectSourceNotVisible(SEARCH_SOURCE_TWO);

    await ingestionPage.sourcesTab.clearSearch();
  });

  test('filter ingestion sources by UI', async () => {
    await ingestionPage.sourcesTab.search(FILTER_SOURCE);
    await ingestionPage.sourcesTab.expectSourceVisible(FILTER_SOURCE);

    // Filter to UI sources only
    await ingestionPage.sourcesTab.selectTypeFilter('UI');

    // After filtering to UI only, CLI pill should not appear
    await ingestionPage.sourcesTab.expectCliPillNotVisible();

    // The pre-seeded UI source should still be visible
    await expect(ingestionPage.sourcesTab.getSourceCell(FILTER_SOURCE)).toBeVisible();
  });
});
