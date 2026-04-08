/**
 * Business Attribute CRUD, inheritance, and field operation tests.
 *
 * Uses base-test — authenticated context is handled by loginFixture, no login
 * step needed. Page objects are constructed in beforeEach with logger so all
 * interactions appear in the structured log.
 *
 * GraphQLHelper is constructed directly from page — no graphqlHelper fixture.
 */

import { test } from '../../fixtures/base-test';
import { BusinessAttributePage } from '../../pages/business-attribute-page';
import { DatasetPage } from '../../pages/dataset-page';
import { GraphQLHelper } from '../../helpers/graphql-helper';

test.describe('Business Attributes', () => {
  const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,Playwright_logging_events,PROD)';
  const FIELD_NAME = 'event_name';
  const TEST_ATTRIBUTE = 'PlaywrightBusinessAttribute';
  const TEST_ATTRIBUTE_2 = 'PlaywrightAttribute';
  const TEST_ATTRIBUTE_3 = 'PlaywrightTestAttribute';

  let businessAttributePage: BusinessAttributePage;
  let datasetPage: DatasetPage;
  let graphqlHelper: GraphQLHelper;

  test.beforeEach(async ({ page, logger, logDir }) => {
    businessAttributePage = new BusinessAttributePage(page, logger, logDir);
    datasetPage = new DatasetPage(page, logger, logDir);
    graphqlHelper = new GraphQLHelper(page);
  });

  test.describe('Business Attribute CRUD Operations', () => {
    test('should create, view, and delete a business attribute', async () => {
      const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
      test.skip(!enabled, 'Business Attribute feature is not enabled');

      await businessAttributePage.navigateToBusinessAttributes();
      await businessAttributePage.clickCreateButton();
      await businessAttributePage.createAttribute(TEST_ATTRIBUTE);

      await businessAttributePage.navigateToBusinessAttributes();
      await businessAttributePage.expectAttributeVisible(TEST_ATTRIBUTE);

      await datasetPage.navigateToDataset(DATASET_URN);
      await datasetPage.closeAnyOpenModal();
      await datasetPage.addBusinessAttributeToField(FIELD_NAME, TEST_ATTRIBUTE);
      await datasetPage.removeBusinessAttributeFromField(FIELD_NAME, TEST_ATTRIBUTE);

      await businessAttributePage.navigateToBusinessAttributes();
      await businessAttributePage.selectAttribute(TEST_ATTRIBUTE);
      await businessAttributePage.deleteAttribute();

      await businessAttributePage.navigateToBusinessAttributes();
      await businessAttributePage.expectAttributeNotVisible(TEST_ATTRIBUTE);
    });
  });

  test.describe('Business Attribute Inheritance', () => {
    test('should inherit tags and terms from business attribute to dataset field', async () => {
      const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
      test.skip(!enabled, 'Business Attribute feature is not enabled');

      await datasetPage.navigateToDataset(DATASET_URN);
      await datasetPage.closeAnyOpenModal();
      await datasetPage.clickSchemaField(FIELD_NAME);

      await businessAttributePage.expectTextVisible('Business Attribute');
      await businessAttributePage.clickAddAttributeButton(FIELD_NAME);
      await businessAttributePage.selectAttributeInModal(TEST_ATTRIBUTE_2);

      await businessAttributePage.expectTextVisible(TEST_ATTRIBUTE_2);
      await businessAttributePage.expectTextVisible('PlaywrightTerm');
      await businessAttributePage.expectTextVisible('Playwright');
    });
  });

  test.describe('Business Attribute Related Entities', () => {
    test('should view related entities for a business attribute', async () => {
      const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
      test.skip(!enabled, 'Business Attribute feature is not enabled');

      await businessAttributePage.navigateToBusinessAttributes();
      await businessAttributePage.selectAttribute(TEST_ATTRIBUTE_2);
      await businessAttributePage.clickRelatedEntitiesTab();

      await businessAttributePage.expectNoRelatedEntities();
      await businessAttributePage.expectRelatedEntitiesCount(/of [0-9]+/);
    });

    test('should search related entities by query', async ({ page }) => {
      const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
      test.skip(!enabled, 'Business Attribute feature is not enabled');

      await page.goto(
        '/business-attribute/urn:li:businessAttribute:37c81832-06e0-40b1-a682-858e1dd0d449/Related%20Entities',
      );
      await page.waitForLoadState('networkidle');

      await businessAttributePage.searchRelatedEntities('event_n');
      await businessAttributePage.expectNoRelatedEntities();
      await businessAttributePage.expectRelatedEntitiesCount(/of 1/);
      await businessAttributePage.expectTextVisible('event_name');
    });
  });

  test.describe('Business Attribute Field Operations', () => {
    test('should remove business attribute from dataset field', async () => {
      const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
      test.skip(!enabled, 'Business Attribute feature is not enabled');

      await datasetPage.navigateToDataset(DATASET_URN);
      await datasetPage.closeAnyOpenModal();
      await datasetPage.clickSchemaField(FIELD_NAME);

      const businessAttributeSection = businessAttributePage.getBusinessAttributeSection(FIELD_NAME);
      await businessAttributePage.removeAttributeFromSection(businessAttributeSection, TEST_ATTRIBUTE_2);
    });
  });

  test.describe('Business Attribute Data Type', () => {
    test('should update the data type of a business attribute', async () => {
      const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
      test.skip(!enabled, 'Business Attribute feature is not enabled');

      await businessAttributePage.navigateToBusinessAttributes();
      await businessAttributePage.selectAttribute(TEST_ATTRIBUTE_3);
      await businessAttributePage.updateDataType('STRING');
      await businessAttributePage.expectTextVisible('STRING');
    });
  });
});
