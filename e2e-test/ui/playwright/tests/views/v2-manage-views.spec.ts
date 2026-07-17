import { test } from '../../fixtures/base-test';
import { ManageViewsPage } from '../../pages/views/manage-views.page';
import { withRandomSuffix } from '../../utils/random';

test.describe('Manage Views', () => {
  let manageViewsPage: ManageViewsPage;
  const viewName = withRandomSuffix('View');
  const editedViewName = `${viewName} - Edited`;

  test.beforeEach(async ({ page, logger, logDir }) => {
    manageViewsPage = new ManageViewsPage(page, logger, logDir);
    await manageViewsPage.navigate();
  });

  test('go to views settings page, create, edit, make default, delete a view', async () => {
    // Create view
    await manageViewsPage.createView(viewName);
    await manageViewsPage.addFilterCondition('hasDescription', 'is_true');
    await manageViewsPage.saveView();

    // Verify creation
    await manageViewsPage.expectViewVisible(viewName);

    // Edit view
    await manageViewsPage.editView(viewName, editedViewName);
    await manageViewsPage.expectViewVisible(editedViewName);

    // Set as default
    await manageViewsPage.setViewAsDefault(editedViewName);
    await manageViewsPage.expectViewVisible(editedViewName);

    // Remove default
    await manageViewsPage.removeViewAsDefault(editedViewName);
    await manageViewsPage.expectViewVisible(editedViewName);

    // Delete view
    await manageViewsPage.deleteView(editedViewName);
    await manageViewsPage.expectViewNotVisible(editedViewName);
  });
});
