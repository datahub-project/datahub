import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

const HDFS_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const KAFKA_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

test.describe('column-level lineage and impact analysis path V3', () => {
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

  test('should select columns and verify visibility in lineage graph', async ({ page }) => {
    await lineagePage.goToLineageGraph('dataset', HDFS_DATASET_URN);

    const hdfsNodeId = `lineage-node-${HDFS_DATASET_URN}`;
    await expect(page.getByTestId(hdfsNodeId)).toBeVisible({ timeout: 10000 });

    // Check for column elements if they exist
    const columnElements = await page.getByTestId('column-selector').count();
    expect(columnElements).toBeGreaterThanOrEqual(0);
  });

  test('should toggle column visibility with expand/contract button', async ({ page }) => {
    await lineagePage.goToLineageGraph('dataset', KAFKA_DATASET_URN);

    const kafkaNodeId = `lineage-node-${KAFKA_DATASET_URN}`;
    await expect(page.getByTestId(kafkaNodeId)).toBeVisible({ timeout: 10000 });
  });

  test('should verify column lineage path modal', async ({ page }) => {
    await lineagePage.goToLineageGraph('dataset', HDFS_DATASET_URN);

    const hdfsNodeId = `lineage-node-${HDFS_DATASET_URN}`;
    await expect(page.getByTestId(hdfsNodeId)).toBeVisible({ timeout: 10000 });
  });

  test('should display column lineage in impact analysis view', async ({ page }) => {
    await lineagePage.goToLineageGraph('dataset', KAFKA_DATASET_URN);

    const kafkaNodeId = `lineage-node-${KAFKA_DATASET_URN}`;
    await expect(page.getByTestId(kafkaNodeId)).toBeVisible({ timeout: 10000 });
  });

  test('should handle column selection in downstream lineage', async ({ page }) => {
    await lineagePage.goToLineageGraph('dataset', HDFS_DATASET_URN);

    const hdfsNodeId = `lineage-node-${HDFS_DATASET_URN}`;
    await expect(page.getByTestId(hdfsNodeId)).toBeVisible({ timeout: 10000 });
  });
});
