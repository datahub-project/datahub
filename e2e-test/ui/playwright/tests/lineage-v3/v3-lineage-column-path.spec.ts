/**
 * Column-level lineage path tests V3 — migrated from Cypress v3_lineage_column_path.js
 *
 * Tests column-level lineage in the V3 graph view and in the impact analysis view,
 * including the column path modal that shows the lineage path between two columns.
 *
 * Verifies:
 * - Column visibility in graph nodes
 * - SVG color state (fill/stroke) when columns are selected
 * - Column path modal content
 * - Upstream and downstream direction toggling
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV3Page } from '../../pages/lineage-v3.page';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';

test.use({ featureName: 'lineage-v3' });

const DATASET_ENTITY_TYPE = 'dataset';
const HDFS_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const KAFKA_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';
const KAFKA_DATASET_NAME = 'SamplePlaywrightKafkaDataset';

const COLUMN_NAMES = {
  SHIPMENT_INFO: 'shipment_info',
  FIELD_BAR: 'field_bar',
} as const;

test.describe('column-level lineage and impact analysis path V3', () => {
  let lineagePage: LineageV3Page;

  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    lineagePage = new LineageV3Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      lineageGraphV3: true,
      themeV2Enabled: true,
      themeV2Default: true,
      showNavBarRedesign: true,
    });
  });

  test('verify column-level lineage path at lineage graph and impact analysis', async ({ page }) => {
    test.setTimeout(120000);

    // ── Graph View: Verify column selection and SVG color states ──────────────

    await lineagePage.goToLineageGraph(DATASET_ENTITY_TYPE, HDFS_DATASET_URN);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Expand columns on HDFS dataset (downstream node)
    await lineagePage.expandContractColumns(HDFS_DATASET_URN);
    await expect(lineagePage.columnShipmentInfo).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Expand columns on Kafka dataset (upstream node)
    await lineagePage.expandContractColumns(KAFKA_DATASET_URN);
    await expect(lineagePage.columnFieldBar).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Click field_bar in upstream Kafka node and verify SVG color state
    const kafkaNode = lineagePage.getReactFlowNodeByUrn(KAFKA_DATASET_URN);
    await kafkaNode.getByText(COLUMN_NAMES.FIELD_BAR).click();
    await lineagePage.verifyColumnSvgColorState(kafkaNode, COLUMN_NAMES.FIELD_BAR, 'fill', true);

    // Verify shipment_info shows connection indicator (stroke not transparent)
    const hdfsNode = lineagePage.getReactFlowNodeByUrn(HDFS_DATASET_URN);
    await lineagePage.verifyColumnSvgColorState(hdfsNode, COLUMN_NAMES.SHIPMENT_INFO, 'stroke', true);

    // Click shipment_info in downstream HDFS node and verify SVG color state
    await hdfsNode.getByText(COLUMN_NAMES.SHIPMENT_INFO, { exact: true }).click();
    await lineagePage.verifyColumnSvgColorState(hdfsNode, COLUMN_NAMES.SHIPMENT_INFO, 'fill', true);
    await lineagePage.verifyColumnSvgColorState(kafkaNode, COLUMN_NAMES.FIELD_BAR, 'stroke', true);

    // ── Impact Analysis Upstream: Column path modal verification ───────────────

    await lineagePage.navigateToDatasetLineage(HDFS_DATASET_URN);
    await lineagePage.clickLineageTab();
    await lineagePage.clickImpactAnalysis();
    await lineagePage.clickColumnLineageToggle();
    await lineagePage.clickDownstreamOption();

    // Switch to upstream
    await lineagePage.clickUpstreamOption();
    await lineagePage.expectTextVisible(KAFKA_DATASET_NAME, TIMEOUTS.MEDIUM);
    await lineagePage.expectTextNotVisible(COLUMN_NAMES.FIELD_BAR, TIMEOUTS.SHORT);

    // Select shipment_info and verify field_bar appears
    await lineagePage.selectColumnFromDropdown(COLUMN_NAMES.SHIPMENT_INFO);
    await lineagePage.expectTextVisible(COLUMN_NAMES.FIELD_BAR, TIMEOUTS.MEDIUM);

    // Open and verify column path modal
    await lineagePage.clickResultTextAndOpenModal();
    await lineagePage.verifyColumnPathModal(COLUMN_NAMES.SHIPMENT_INFO, COLUMN_NAMES.FIELD_BAR);
    await lineagePage.closeEntityPathsModal();

    // ── Impact Analysis Downstream: Column path modal verification ─────────────

    await lineagePage.navigateToDatasetLineage(KAFKA_DATASET_URN);
    await lineagePage.clickLineageTab();
    await lineagePage.clickImpactAnalysis();
    await lineagePage.clickColumnLineageToggle();

    // Verify shipment_info is not visible before selection
    await lineagePage.expectTextNotVisible(COLUMN_NAMES.SHIPMENT_INFO, TIMEOUTS.SHORT, { exact: true });

    // Select field_bar and verify shipment_info appears in results
    await lineagePage.selectColumnFromDropdown(COLUMN_NAMES.FIELD_BAR);
    const hdfsResultRow = lineagePage.getSearchResultRow(HDFS_DATASET_URN);
    const displayedColumn = lineagePage.getDisplayedColumnByName(hdfsResultRow, COLUMN_NAMES.SHIPMENT_INFO);
    await expect(displayedColumn).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

    // Open and verify column path modal
    await lineagePage.clickResultTextAndOpenModal();
    await lineagePage.verifyColumnPathModal(COLUMN_NAMES.SHIPMENT_INFO, COLUMN_NAMES.FIELD_BAR);
    await lineagePage.closeEntityPathsModal();
  });
});
