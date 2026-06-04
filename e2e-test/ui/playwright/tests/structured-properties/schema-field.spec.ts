import { test } from '../../fixtures/base-test';
import { StructuredPropertiesPage } from '../../pages/structured-properties.page';
import { DatasetPage } from '../../pages/dataset.page';
import { withRandomSuffix } from '../../utils/random';
import { TEST_DATA, TOAST_MESSAGES, TIMEOUTS } from './constants';

/**
 * Schema Field Structured Properties tests
 * Tests for applying structured properties to dataset schema fields
 */

test.use({ featureName: 'structured-properties' });

test.describe('Schema Field Structured Properties', () => {
  let structuredPropertiesPage: StructuredPropertiesPage;
  let datasetPage: DatasetPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    structuredPropertiesPage = new StructuredPropertiesPage(page, logger, logDir);
    datasetPage = new DatasetPage(page, logger, logDir);

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await structuredPropertiesPage.navigate();
  });

  test('Verify adding a structured property to a schema field', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('field-add');
    const fieldProperty = { name: propertyName, entity: 'Column' };

    const propertyUrn = await structuredPropertiesPage.createStructuredProperty(fieldProperty);

    await structuredPropertiesPage.enableShowInColumnsTable(fieldProperty);

    // Wait for property setting to propagate to backend before navigating to dataset
    await new Promise((resolve) => setTimeout(resolve, TIMEOUTS.SETTING_PROPAGATION));

    await datasetPage.navigateToDataset(TEST_DATA.SCHEMA_FIELD_ADD_DATASET);
    await datasetPage.clickSchemaFieldByName(TEST_DATA.SCHEMA_FIELD_ADD);

    await datasetPage.waitForFieldDrawer(TIMEOUTS.DRAWER_VISIBLE);

    await structuredPropertiesPage.waitForFieldPropertyButton(propertyName, TIMEOUTS.FIELD_BUTTON_VISIBLE);
    await structuredPropertiesPage.clickFieldPropertyButton(propertyName);

    await structuredPropertiesPage.fillFieldPropertyValue(TEST_DATA.PROPERTY_VALUE);
    await structuredPropertiesPage.submitFieldProperty();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_ADDED);
    await structuredPropertiesPage.expectDrawerContains(TEST_DATA.PROPERTY_VALUE);

    cleanup.track(propertyUrn);
  });

  test('Verify updating a structured property on a schema field', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('field-update');
    const fieldProperty = { name: propertyName, entity: 'Column' };

    const propertyUrn = await structuredPropertiesPage.createStructuredProperty(fieldProperty);

    await structuredPropertiesPage.enableShowInColumnsTable(fieldProperty);

    // Wait for property setting to propagate to backend before navigating to dataset
    await new Promise((resolve) => setTimeout(resolve, TIMEOUTS.SETTING_PROPAGATION));

    await datasetPage.navigateToDataset(TEST_DATA.SCHEMA_FIELD_UPDATE_DATASET);
    await datasetPage.clickSchemaFieldByName(TEST_DATA.SCHEMA_FIELD_UPDATE);

    await datasetPage.waitForFieldDrawer(TIMEOUTS.DRAWER_VISIBLE);

    await structuredPropertiesPage.waitForFieldPropertyButton(propertyName, TIMEOUTS.FIELD_BUTTON_VISIBLE);
    await structuredPropertiesPage.clickFieldPropertyButton(propertyName);

    await structuredPropertiesPage.fillFieldPropertyValue(TEST_DATA.PROPERTY_VALUE);
    await structuredPropertiesPage.submitFieldProperty();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_ADDED);

    await structuredPropertiesPage.waitForFieldPropertyButton(propertyName, TIMEOUTS.DRAWER_VISIBLE);
    await structuredPropertiesPage.clickFieldPropertyButton(propertyName);

    await structuredPropertiesPage.clearFieldPropertyValue();
    await structuredPropertiesPage.fillFieldPropertyValue(TEST_DATA.PROPERTY_VALUE_UPDATED);
    await structuredPropertiesPage.submitFieldProperty();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_UPDATED);
    await structuredPropertiesPage.expectDrawerContains(TEST_DATA.PROPERTY_VALUE_UPDATED);

    cleanup.track(propertyUrn);
  });

  test('Verify removing a structured property from a schema field', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('field-remove');
    const fieldProperty = { name: propertyName, entity: 'Column' };

    const propertyUrn = await structuredPropertiesPage.createStructuredProperty(fieldProperty);

    await structuredPropertiesPage.enableShowInColumnsTable(fieldProperty);

    // Wait for property setting to propagate to backend before navigating to dataset
    await new Promise((resolve) => setTimeout(resolve, TIMEOUTS.SETTING_PROPAGATION));

    await datasetPage.navigateToDataset(TEST_DATA.SCHEMA_FIELD_REMOVE_DATASET);
    await datasetPage.clickSchemaFieldByName(TEST_DATA.SCHEMA_FIELD_REMOVE);

    await datasetPage.waitForFieldDrawer(TIMEOUTS.DRAWER_VISIBLE);

    await structuredPropertiesPage.waitForPageLoad();

    await structuredPropertiesPage.waitForFieldPropertyButton(propertyName, TIMEOUTS.FIELD_BUTTON_VISIBLE);
    await structuredPropertiesPage.clickFieldPropertyButton(propertyName);

    await structuredPropertiesPage.fillFieldPropertyValue(TEST_DATA.PROPERTY_VALUE);
    await structuredPropertiesPage.submitFieldProperty();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_ADDED);

    // Wait for the drawer to refresh with the new property before accessing the properties tab
    await structuredPropertiesPage.waitForPageLoad();

    await structuredPropertiesPage.clickFieldPropertiesTab();

    await structuredPropertiesPage.clickFieldPropertyAction(propertyName, 'Remove');

    await structuredPropertiesPage.confirmModalAction();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_REMOVED);
    // Check that the specific property is gone by verifying its name is not in the drawer
    await structuredPropertiesPage.expectDrawerNotContains(propertyName);

    cleanup.track(propertyUrn);
  });
});
