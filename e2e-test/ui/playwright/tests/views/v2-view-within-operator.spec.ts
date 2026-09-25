import { expect, test } from '../../fixtures/base-test';
import { ManageViewsPage } from '../../pages/views/manage-views.page';
import { withRandomSuffix } from '../../utils/random';

test.use({ featureName: 'views' });

const WITHIN_DESCRIPTION = 'Matches this value or any nested children';
const SEEDED_DOMAIN_NAME = 'PlaywrightViewsWithinDomain';

test.describe('View Within Operator', () => {
  let manageViewsPage: ManageViewsPage;
  const viewName = withRandomSuffix('WithinView');

  test.beforeEach(async ({ page, logger, logDir }) => {
    manageViewsPage = new ManageViewsPage(page, logger, logDir);
    await manageViewsPage.navigate();
  });

  test('shows Within for hierarchical fields, persists DESCENDANTS_INCL, and reloads correctly', async ({ page }) => {
    test.setTimeout(120_000);

    await manageViewsPage.createView(viewName);
    await expect(manageViewsPage.conditionSelect).toBeVisible();

    // Domain supports Within + help text
    await manageViewsPage.selectProperty('domains');
    await manageViewsPage.openOperatorDropdown();
    await manageViewsPage.expectOperatorOptionVisible('within');
    await manageViewsPage.expectOperatorDescriptionVisible(WITHIN_DESCRIPTION);
    // Select Within to close the dropdown without Escape (Escape closes the modal)
    await page.getByTestId('option-within').click();

    // Container and Parent Document also expose Within
    for (const field of ['container', 'parentDocument'] as const) {
      await manageViewsPage.selectProperty(field);
      await manageViewsPage.openOperatorDropdown();
      await manageViewsPage.expectOperatorOptionVisible('within');
      await page.getByTestId('option-within').click();
    }

    // Non-hierarchical URN field does not expose Within
    await manageViewsPage.selectProperty('tags');
    await manageViewsPage.openOperatorDropdown();
    await manageViewsPage.expectOperatorOptionNotVisible('within');
    // Close by selecting Equals
    await page.getByTestId('option-equals').click();

    // Build Domain + Within filter, then save and assert createView payload
    await manageViewsPage.addFilterWithSearch('domains', 'within', SEEDED_DOMAIN_NAME);

    const createViewRequestPromise = page.waitForRequest((request) => {
      if (!request.url().includes('/api/v2/graphql')) return false;
      const postData = request.postDataJSON() as { operationName?: string } | null;
      return postData?.operationName === 'createView';
    });

    await manageViewsPage.saveView();
    const createViewRequest = await createViewRequestPromise;
    const requestBody = createViewRequest.postDataJSON() as {
      variables?: {
        input?: {
          definition?: {
            filter?: {
              filters?: Array<{ field?: string; condition?: string }>;
            };
          };
        };
      };
    };

    const filters = requestBody.variables?.input?.definition?.filter?.filters ?? [];
    const domainFilter = filters.find((f) => f.field === 'domains');
    expect(domainFilter?.condition).toBe('DESCENDANTS_INCL');

    await manageViewsPage.expectViewVisible(viewName);

    // Re-open and confirm Within is restored
    const menuButton = await manageViewsPage.getViewOptionMenu(viewName);
    await menuButton.click();
    await manageViewsPage.menuItemEdit.click();
    await manageViewsPage.expectSelectedOperator('Within');

    await manageViewsPage.saveView();
    await manageViewsPage.deleteView(viewName);
    await manageViewsPage.expectViewNotVisible(viewName);
  });
});
