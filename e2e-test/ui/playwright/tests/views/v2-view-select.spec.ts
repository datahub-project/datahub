import { test, expect } from '../../fixtures/base-test';
import { ViewSelectPage } from '../../pages/views/view-select.page';
import { TIMEOUTS } from '../../utils/constants';
import { withRandomSuffix } from '../../utils/random';
import { TEST_DATA, TEXT_PATTERNS, URLS } from '../../pages/views/views-constants';

test.use({ featureName: 'views' });

test.describe('View Select', () => {
  let viewSelectPage: ViewSelectPage;
  const viewName = withRandomSuffix('View');

  test.beforeEach(async ({ page, logger, logDir }) => {
    viewSelectPage = new ViewSelectPage(page, logger, logDir);
    await viewSelectPage.navigateTo(URLS.SEARCH);
    await viewSelectPage.skipIntroducePage();
  });

  test('click view select, create view, clear view, make defaults, clear view', async () => {
    // Ensure browse-v2 is present
    await expect(viewSelectPage.viewsIcon).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Create view from select
    await viewSelectPage.createViewFromSelect(viewName);
    await viewSelectPage.addFilterWithSearch(TEST_DATA.FILTER_FIELD, TEST_DATA.FILTER_OPERATOR, TEST_DATA.FILTER_VALUE);
    await viewSelectPage.saveView();

    // Verify filter applied - check for specific datasets
    await viewSelectPage.verifyResultsCount(TEST_DATA.EXPECTED_RESULT_COUNT);
    await viewSelectPage.verifyDatasetVisible(TEST_DATA.EXPECTED_DATASET);
    await expect(viewSelectPage.resultsContainer).toBeVisible();

    // Clear view (remove filter)
    await viewSelectPage.clearView();
    await viewSelectPage.expectTextNotVisible(
      TEXT_PATTERNS.RESULTS_COUNT(TEST_DATA.EXPECTED_RESULT_COUNT),
      TIMEOUTS.SHORT,
    );

    // Edit view
    const editedViewName = `${viewName} - Edited`;
    await viewSelectPage.editViewFromSelect(viewName, editedViewName);
    await viewSelectPage.verifyDatasetVisible(TEST_DATA.EXPECTED_DATASET);
    await viewSelectPage.verifyResultsCount(TEST_DATA.EXPECTED_RESULT_COUNT);

    // Set as default (while view is still active with filter)
    await viewSelectPage.setViewAsDefault(editedViewName);
    await viewSelectPage.verifyDatasetVisible(TEST_DATA.EXPECTED_DATASET);
    await viewSelectPage.verifyResultsCount(TEST_DATA.EXPECTED_RESULT_COUNT);

    // Clear view again (remove filter)
    await viewSelectPage.clearView();
    await viewSelectPage.expectTextNotVisible(
      TEXT_PATTERNS.RESULTS_COUNT(TEST_DATA.EXPECTED_RESULT_COUNT),
      TIMEOUTS.SHORT,
    );

    // Remove default
    await viewSelectPage.removeViewAsDefault(editedViewName);

    // Delete view
    await viewSelectPage.deleteView(editedViewName);

    // Navigate to views settings to confirm deletion
    await viewSelectPage.navigateTo(URLS.SETTINGS_VIEWS);
    await viewSelectPage.verifyViewDeleted(editedViewName);
  });
});
