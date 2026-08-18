import { test, expect } from '../../fixtures/base-test';
import { IngestionV2Page } from '../../pages/ingestion/v2/ingestion-v2.page';

test.use({ featureName: 'ingestion-v2' });

test.describe('connector support badges and capability popover', () => {
  let ingestionPage: IngestionV2Page;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    ingestionPage = new IngestionV2Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      showIngestionPageRedesign: true,
      ingestionOnboardingRedesignV1: false,
      showNavBarRedesign: true,
    });

    await ingestionPage.goto();
  });

  test('shows a support badge on connector cards and a capability popover on hover', async () => {
    const { sourcesTab } = ingestionPage;
    await sourcesTab.openCreateSourceModal();

    // Snowflake is a Certified connector in the bundled connector registry, so its
    // card renders the "Certified" support badge.
    const snowflakeCard = sourcesTab.getSourceTypeOption('Snowflake');
    await snowflakeCard.scrollIntoViewIfNeeded();
    await expect(snowflakeCard).toContainText('Certified');

    // Hovering the card reveals the connector-detail popover with the capabilities section.
    await snowflakeCard.hover();
    await expect(sourcesTab.connectorDetailPopover).toBeVisible();
    await expect(sourcesTab.connectorDetailPopover).toContainText('Capabilities');
  });
});
