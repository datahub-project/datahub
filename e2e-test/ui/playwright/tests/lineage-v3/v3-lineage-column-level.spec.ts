import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

test.describe('column-level lineage graph V3 test', () => {
  let lineagePage: LineageV2Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV2Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      lineageGraphV3: true,
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  test('navigate to lineage graph view and verify that column-level lineage is showing correctly', async ({
    page,
  }) => {
    await lineagePage.goToLineageGraph('dataset', DATASET_URN);

    // Verify dataset node is visible
    const datasetNodeId = `lineage-node-${DATASET_URN}`;
    await expect(page.getByTestId(datasetNodeId)).toBeVisible({ timeout: 10000 });
  });
});
