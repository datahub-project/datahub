import { test } from '../../fixtures/base-test';
import { StructuredPropertiesPage } from '../../pages/structured-properties.page';
import { DatasetPage } from '../../pages/dataset.page';
import { TEST_DATA, TOAST_MESSAGES, TIMEOUTS } from './constants';
import { withRandomSuffix } from '../../utils/random';

/**
 * Entity-level Structured Properties tests
 * Tests for creating, editing, deleting, and applying structured properties to entities
 */

test.use({ featureName: 'structured-properties' });

test.describe('Entity-level Structured Properties', () => {
  let structuredPropertiesPage: StructuredPropertiesPage;
  let datasetPage: DatasetPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    structuredPropertiesPage = new StructuredPropertiesPage(page, logger, logDir);
    datasetPage = new DatasetPage(page, logger, logDir);

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await structuredPropertiesPage.navigate();
  });

  test('Verify adding a structured property to an entity', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-add');
    const propertyUrn = await structuredPropertiesPage.createStructuredProperty({
      name: propertyName,
      entity: 'Dataset',
    });

    await datasetPage.navigateToDataset(TEST_DATA.ENTITY_LEVEL_DATASET_ADD);
    await datasetPage.viewProperties();
    await structuredPropertiesPage.clickAddPropertyButton();

    await structuredPropertiesPage.selectPropertyFromDropdown(propertyName);
    await structuredPropertiesPage.fillPropertyValue(TEST_DATA.PROPERTY_VALUE);
    await structuredPropertiesPage.submitPropertyValue();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_ADDED);
    await structuredPropertiesPage.expectPageContains(TEST_DATA.PROPERTY_VALUE);

    cleanup.track(propertyUrn);
  });

  test('Verify removing a structured property from an entity', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-remove');
    const propertyUrn = await structuredPropertiesPage.createStructuredProperty({
      name: propertyName,
      entity: 'Dataset',
    });

    await datasetPage.navigateToDataset(TEST_DATA.ENTITY_LEVEL_DATASET_REMOVE);
    await datasetPage.viewProperties();
    await structuredPropertiesPage.clickAddPropertyButton();

    await structuredPropertiesPage.selectPropertyFromDropdown(propertyName);
    await structuredPropertiesPage.fillPropertyValue(TEST_DATA.PROPERTY_VALUE);
    await structuredPropertiesPage.submitPropertyValue();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_ADDED);

    await structuredPropertiesPage.waitForPropertyRow(propertyName);
    await structuredPropertiesPage.clickPropertyMoreIcon(propertyName);

    await structuredPropertiesPage.clickPropertyAction('Remove');
    await structuredPropertiesPage.confirmAction();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_REMOVED);
    await structuredPropertiesPage.waitForPropertyRowToDisappear(propertyName);

    cleanup.track(propertyUrn);
  });

  test('Verify editing a structured property on an entity', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-edit');
    const propertyUrn = await structuredPropertiesPage.createStructuredProperty({
      name: propertyName,
      entity: 'Dataset',
    });

    await datasetPage.navigateToDataset(TEST_DATA.ENTITY_LEVEL_DATASET_EDIT);
    await datasetPage.viewProperties();
    await structuredPropertiesPage.clickAddPropertyButton();

    await structuredPropertiesPage.selectPropertyFromDropdown(propertyName);
    await structuredPropertiesPage.fillPropertyValue(TEST_DATA.PROPERTY_VALUE);
    await structuredPropertiesPage.submitPropertyValue();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_ADDED);

    await structuredPropertiesPage.waitForPropertyRow(propertyName);
    await structuredPropertiesPage.clickPropertyMoreIcon(propertyName);

    await structuredPropertiesPage.clickPropertyAction('Edit');
    await structuredPropertiesPage.clearPropertyValue();
    await structuredPropertiesPage.fillPropertyValue(TEST_DATA.PROPERTY_VALUE_UPDATED);
    await structuredPropertiesPage.submitPropertyValue();

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_UPDATED);
    await structuredPropertiesPage.expectPageContains(TEST_DATA.PROPERTY_VALUE_UPDATED);

    cleanup.track(propertyUrn);
  });

  test('Verify the absence of hidden structured property', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-hidden');
    const propertyUrn = await structuredPropertiesPage.createStructuredProperty({
      name: propertyName,
      entity: 'Dataset',
    });

    await structuredPropertiesPage.hideProperty({ name: propertyName });
    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_UPDATED);

    await structuredPropertiesPage.waitForPageLoad();

    await datasetPage.navigateToDataset(TEST_DATA.ENTITY_LEVEL_DATASET_HIDDEN);
    await datasetPage.viewProperties();
    await structuredPropertiesPage.clickAddPropertyButton();

    // Wait for the property dropdown to be visible
    await structuredPropertiesPage.addPropertyDropdown.waitFor({
      state: 'visible',
      timeout: TIMEOUTS.DROPDOWN_VISIBLE,
    });

    // Search for the hidden property to verify it doesn't appear
    await structuredPropertiesPage.searchPropertyInDropdown(propertyName);

    // Verify the property doesn't appear in the filtered results
    await structuredPropertiesPage.expectPageNotContains(propertyName);

    cleanup.track(propertyUrn);
  });
});
