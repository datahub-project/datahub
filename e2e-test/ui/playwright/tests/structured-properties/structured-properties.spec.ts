import { test } from '../../fixtures/base-test';
import { StructuredPropertiesPage } from '../../pages/structured-properties.page';
import { TOAST_MESSAGES } from './constants';
import { withRandomSuffix } from '../../utils/random';

/**
 * Structured Properties CRUD tests
 * Tests for creating, updating, and deleting structured properties
 */

test.describe('Structured Properties CRUD', () => {
  let structuredPropertiesPage: StructuredPropertiesPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    structuredPropertiesPage = new StructuredPropertiesPage(page, logger, logDir);
    await structuredPropertiesPage.navigate();
  });

  test('Verify creating a new structured property', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-create');
    const structuredProperty = { name: propertyName, entity: 'Dataset' };

    const propertyUrn = await structuredPropertiesPage.createStructuredProperty(structuredProperty);

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_CREATED);
    await structuredPropertiesPage.expectPageContains(propertyName);

    cleanup.track(propertyUrn);
  });

  test('Verify updating an existing structured property', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-update');
    const updatedPropertyName = withRandomSuffix('prop-updated');
    const propertyDescription = 'Test property description';

    const structuredProperty = { name: propertyName, entity: 'Dataset' };
    const propertyUrn = await structuredPropertiesPage.createStructuredProperty(structuredProperty);

    // Wait for the properties table to refresh after creation
    await structuredPropertiesPage.waitForPageLoad();

    await structuredPropertiesPage.updateStructuredProperty(propertyName, {
      name: updatedPropertyName,
      description: propertyDescription,
      entity: 'Dashboard',
    });

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_UPDATED);
    await structuredPropertiesPage.expectPageContains(updatedPropertyName);
    await structuredPropertiesPage.expectPageContains(propertyDescription);
    await structuredPropertiesPage.expectPageContains('Dashboard');

    cleanup.track(propertyUrn);
  });

  test('Verify deleting a structured property', async ({ cleanup }) => {
    const propertyName = withRandomSuffix('prop-delete');
    const structuredProperty = { name: propertyName, entity: 'Dataset' };

    const propertyUrn = await structuredPropertiesPage.createStructuredProperty(structuredProperty);

    await structuredPropertiesPage.deleteStructuredProperty({ name: propertyName });

    await structuredPropertiesPage.expectPageContains(TOAST_MESSAGES.PROPERTY_DELETED);

    cleanup.track(propertyUrn);
  });
});
