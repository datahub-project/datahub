import { test } from '../../fixtures/test-context';

test.describe('Business Attribute Navigation', () => {
  test('should navigate to business attributes page', async ({
    businessAttributePage,
    graphqlHelper,
  }) => {
    const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
    test.skip(!enabled, 'Business Attribute feature is not enabled');

    await businessAttributePage.navigateToBusinessAttributes();

    await businessAttributePage.expectOnBusinessAttributePage();

    await businessAttributePage.expectPageTitleVisible(10000);
  });

  test('should display business attribute page header', async ({
    businessAttributePage,
    graphqlHelper,
  }) => {
    const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
    test.skip(!enabled, 'Business Attribute feature is not enabled');

    await businessAttributePage.navigateToBusinessAttributes();

    await businessAttributePage.expectPageTitleVisible();
  });
});
