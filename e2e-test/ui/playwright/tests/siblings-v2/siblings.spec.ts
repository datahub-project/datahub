import { test, expect } from '../../fixtures/base-test';
import { SiblingsPage } from '../../pages/siblings.page';

test.use({ featureName: 'siblings-v2' });

// Test data constants
const DBT_CUSTOMERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,playwright_project.jaffle_shop.customers,PROD)';
const BIGQUERY_CUSTOMERS_URN =
  'urn:li:dataset:(urn:li:dataPlatform:bigquery,playwright_project.jaffle_shop.customers,PROD)';
const DBT_RAW_ORDERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,playwright_project.jaffle_shop.raw_orders,PROD)';
const DBT_STG_ORDERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,playwright_project.jaffle_shop.stg_orders,PROD)';
const PLAYWRIGHT_TERM = 'PlaywrightSiblingsTerm';

// Dataset and platform constants
const DBT_PLATFORM = 'dbt';
const BIGQUERY_PLATFORM = 'bigquery';
const PLATFORMS = [DBT_PLATFORM, BIGQUERY_PLATFORM];
const STG_ORDERS_DATASET = 'stg_orders';
const SEARCH_QUERY_STG = 'stg';
const DATASET_ENTITY_TYPE = 'dataset';

test.describe('siblings', () => {
  let siblingsPage: SiblingsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    siblingsPage = new SiblingsPage(page, logger, logDir);
  });

  test('will merge metadata to non-primary sibling', async () => {
    await siblingsPage.navigateToDataset(BIGQUERY_CUSTOMERS_URN);
    await siblingsPage.verifyPlatformLogosVisible(PLATFORMS);
    await expect(siblingsPage.entityHeader).toBeVisible();
  });

  test('will merge metadata to primary sibling', async () => {
    await siblingsPage.navigateToDataset(DBT_CUSTOMERS_URN);
    await siblingsPage.verifyPlatformLogosVisible(PLATFORMS);
    await expect(siblingsPage.entityHeader).toBeVisible();
  });

  test('can view individual nodes', async () => {
    await siblingsPage.navigateToDataset(DBT_CUSTOMERS_URN);
    await siblingsPage.navigateToSiblingByUrn(BIGQUERY_CUSTOMERS_URN);
    await siblingsPage.verifyIndividualPlatformHeader(BIGQUERY_PLATFORM);
    await siblingsPage.verifyPlatformLogoNotVisible(DBT_PLATFORM);
  });

  test('can mutate at individual node or combined node level', async () => {
    await siblingsPage.navigateToDataset(DBT_CUSTOMERS_URN);
    await siblingsPage.navigateToSiblingByUrn(BIGQUERY_CUSTOMERS_URN);
    await siblingsPage.addGlossaryTerm(PLAYWRIGHT_TERM);
    await siblingsPage.verifyTermAddedToast();
    await siblingsPage.navigateToDataset(DBT_CUSTOMERS_URN);
    await siblingsPage.verifyGlossaryTermVisible(PLAYWRIGHT_TERM);
    await siblingsPage.removeGlossaryTerm(PLAYWRIGHT_TERM);
  });

  test('will combine results in search', async () => {
    await siblingsPage.navigateToSearchResults(SEARCH_QUERY_STG);
    await siblingsPage.verifySingleSearchResult(STG_ORDERS_DATASET, PLATFORMS);
  });

  test('separates siblings in lineage', async () => {
    await siblingsPage.navigateToLineageGraph(DATASET_ENTITY_TYPE, DBT_STG_ORDERS_URN);
    await siblingsPage.verifyLineageNodesVisible([DBT_STG_ORDERS_URN, DBT_RAW_ORDERS_URN]);
  });
});
