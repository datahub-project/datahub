/**
 * Business Attribute Navigation tests.
 *
 * Uses base-test — the authenticated context is provided by loginFixture so
 * no beforeEach login is needed. Page objects are instantiated directly with
 * the auto-injected logger and logDir.
 *
 * Feature flag check uses GraphQLHelper constructed from page directly —
 * no graphqlHelper fixture or test-context.ts required.
 */

import { test } from '../../fixtures/base-test';
import { BusinessAttributePage } from '../../pages/business-attribute-page';
import { GraphQLHelper } from '../../helpers/graphql-helper';

test.describe('Business Attribute Navigation', () => {
  test('should navigate to business attributes page', async ({ page, logger, logDir }) => {
    const businessAttributePage = new BusinessAttributePage(page, logger, logDir);
    const graphqlHelper = new GraphQLHelper(page);

    const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
    test.skip(!enabled, 'Business Attribute feature is not enabled');

    await businessAttributePage.navigateToBusinessAttributes();
    await businessAttributePage.expectOnBusinessAttributePage();
    await businessAttributePage.expectPageTitleVisible(10000);
  });

  test('should display business attribute page header', async ({ page, logger, logDir }) => {
    const businessAttributePage = new BusinessAttributePage(page, logger, logDir);
    const graphqlHelper = new GraphQLHelper(page);

    const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
    test.skip(!enabled, 'Business Attribute feature is not enabled');

    await businessAttributePage.navigateToBusinessAttributes();
    await businessAttributePage.expectPageTitleVisible();
  });
});
