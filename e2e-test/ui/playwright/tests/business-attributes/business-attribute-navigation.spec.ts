import { baseTest as test } from '@fixtures/base-test';
import { BusinessAttributePage } from '@pages/business-attribute-page';

test.describe('Business Attribute Navigation', () => {
  test.use(user: shared_admin);
  test.beforeEach(async({ page })) {
    page.goto('/business-attributes')
  }
  test('should navigate to business attributes page', async ({
    page, logger, testInfo
  }) => {
    const businessAttributePage = BusinessAttributePage(page, logger, testInfo)
    const enabled = await businessAttributePage.checkBusinessAttributeFeature(graphqlHelper);
    expect(businessAttributePage)
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
