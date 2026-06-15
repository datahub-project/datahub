import { test } from '../../fixtures/base-test';
import { IngestionV3Page } from '../../pages/ingestion/v3/ingestion-v3.page';

// Pre-seeded from tests/ingestion-v3/fixtures/data.json
const RUN_HISTORY_SOURCE = 'playwright v3 run history source';
const RUN_HISTORY_EXEC_URN = 'urn:li:dataHubExecutionRequest:playwright-v3-exec-run-history';
const FILTER_SOURCE_1 = 'playwright v3 run history filter 1';
const FILTER_EXEC_1_URN = 'urn:li:dataHubExecutionRequest:playwright-v3-exec-filter-1';
const FILTER_EXEC_2_URN = 'urn:li:dataHubExecutionRequest:playwright-v3-exec-filter-2';

test.use({ featureName: 'ingestion-v3' });

test.describe('run history tab in manage data sources', () => {
  let ingestionPage: IngestionV3Page;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    ingestionPage = new IngestionV3Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      showIngestionPageRedesign: true,
      ingestionOnboardingRedesignV1: true,
      showNavBarRedesign: true,
    });

    await apiMock.suppressOnboardingModals();

    await ingestionPage.goto();
  });

  test('navigate to run history tab from last run column of sources tab', async () => {
    await ingestionPage.sourcesTab.search(RUN_HISTORY_SOURCE);
    await ingestionPage.sourcesTab.expectSourceVisible(RUN_HISTORY_SOURCE);

    await ingestionPage.sourcesTab.clickLastRunCell(RUN_HISTORY_SOURCE);

    await ingestionPage.runHistoryTab.expectTabActive();
  });

  test('navigate to run history tab from View run history option in dropdown of sources', async () => {
    await ingestionPage.sourcesTab.search(RUN_HISTORY_SOURCE);
    await ingestionPage.sourcesTab.expectSourceVisible(RUN_HISTORY_SOURCE);

    await ingestionPage.sourcesTab.openMoreOptions(RUN_HISTORY_SOURCE);
    await ingestionPage.sourcesTab.clickDropdownItem('View Run History');

    await ingestionPage.runHistoryTab.expectTabActive();
  });

  test('view past executions in run history tab', async () => {
    await ingestionPage.runHistoryTab.open();
    await ingestionPage.runHistoryTab.filterBySource(RUN_HISTORY_SOURCE);

    await ingestionPage.runHistoryTab.expectExecutionRowVisible(RUN_HISTORY_EXEC_URN);
  });

  test('navigate to sources tab from source name in run history tab', async () => {
    await ingestionPage.runHistoryTab.open();
    await ingestionPage.runHistoryTab.filterBySource(RUN_HISTORY_SOURCE);

    await ingestionPage.runHistoryTab.clickExecutionSourceNameLink(RUN_HISTORY_EXEC_URN);

    await ingestionPage.sourcesTab.expectTabActive();
  });

  test('filter execution requests by source name', async () => {
    await ingestionPage.runHistoryTab.open();
    await ingestionPage.runHistoryTab.filterBySource(FILTER_SOURCE_1);

    await ingestionPage.runHistoryTab.expectExecutionRowVisible(FILTER_EXEC_1_URN);
    await ingestionPage.runHistoryTab.expectExecutionRowHidden(FILTER_EXEC_2_URN);
  });
});
