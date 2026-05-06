/**
 * Column-level lineage path tests V2
 * Migrated from Cypress e2e/lineageV2/v2_lineage_column_path.js
 *
 * Tests column-level lineage in the V2 graph view and in the impact analysis view,
 * including the column path modal that shows the lineage path between two columns.
 *
 * Uses apiMock.setFeatureFlags to disable lineageGraphV3 so the V2 graph renders.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

const DATASET_ENTITY_TYPE = 'dataset';
// HDFS dataset — the downstream entity in the lineage graph
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
// Kafka dataset — the upstream entity in the lineage graph (feeds data into HDFS)
const UPSTREAM_URN = 'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

test.describe('column-Level lineage and impact analysis path test', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  test('verify column-level lineage path at lineage graph and impact analysis', async ({ page, logger, logDir }) => {
    // Graph navigation + two impact-analysis modal flows takes ~70s locally; allow extra for CI.
    test.setTimeout(120000);

    const lp = new LineageV2Page(page, logger, logDir);

    // Navigate to the HDFS dataset lineage graph
    await lp.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await expect(page.getByText('SamplePlaywrightHdfs').first()).toBeVisible({ timeout: 15000 });

    // Expand columns on HDFS dataset (downstream node)
    await lp.expandContractColumns(DATASET_URN);
    await expect(page.getByText('shipment_info', { exact: true }).first()).toBeVisible({ timeout: 10000 });

    // Expand columns on Kafka dataset (upstream node)
    await lp.expandContractColumns(UPSTREAM_URN);
    await expect(lp.getReactFlowNode(UPSTREAM_URN).getByText('field_bar')).toBeVisible({ timeout: 10000 });

    // Click field_bar in the upstream Kafka node
    await lp.getReactFlowNode(UPSTREAM_URN).getByText('field_bar').click();

    // Verify the selected field_bar indicator is active (fill not white)
    await expect(
      lp.getReactFlowNode(UPSTREAM_URN).getByText('field_bar').locator('..').locator('svg, circle, path').first(),
    ).not.toHaveAttribute('fill', 'white');

    // Verify shipment_info has a connection indicator (stroke not transparent).
    // Use exact: true to avoid matching sub-fields like shipment_info.date, shipment_info.target, etc.
    await expect(
      lp
        .getReactFlowNode(DATASET_URN)
        .getByText('shipment_info', { exact: true })
        .locator('..')
        .locator('svg, circle, path')
        .first(),
    ).not.toHaveAttribute('stroke', 'transparent');

    // Click shipment_info in downstream node
    await lp.getReactFlowNode(DATASET_URN).getByText('shipment_info', { exact: true }).click();
    await expect(
      lp
        .getReactFlowNode(DATASET_URN)
        .getByText('shipment_info', { exact: true })
        .locator('..')
        .locator('svg, circle, path')
        .first(),
    ).not.toHaveAttribute('fill', 'white');
    await expect(
      lp
        .getReactFlowNode(UPSTREAM_URN)
        .getByText('field_bar', { exact: true })
        .locator('..')
        .locator('svg, circle, path')
        .first(),
    ).not.toHaveAttribute('stroke', 'transparent');

    // ── Impact analysis upstream column path modal ──────────────────────────

    await lp.goToDataset(DATASET_URN, 'SamplePlaywrightHdfsDataset');
    await lp.clickLineageTab();
    await lp.clickImpactAnalysis();
    await lp.clickColumnLineageToggle();
    await lp.clickDownstreamOption();

    // Switch to upstream
    await lp.clickUpstreamOption();
    await expect(page.getByText('SamplePlaywrightKafkaDataset').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('field_bar')).toBeHidden();

    await lp.selectColumnFromDropdown('shipment_info');
    await expect(page.getByText('field_bar').first()).toBeVisible({ timeout: 10000 });

    await lp.clickResultTextAndOpenModal();
    await lp.verifyColumnPathModal('shipment_info', 'field_bar');
    await lp.closeEntityPathsModal();

    // ── Impact analysis downstream column path modal ───────────────────────

    await lp.goToDataset(UPSTREAM_URN, 'SamplePlaywrightKafkaDataset');
    await lp.clickLineageTab();
    await lp.clickImpactAnalysis();
    await lp.clickColumnLineageToggle();

    // Use exact: true to avoid matching sub-field names that contain "shipment_info" as prefix.
    await expect(page.getByText('shipment_info', { exact: true })).toBeHidden();
    await lp.selectColumnFromDropdown('field_bar');
    await expect(page.getByText('shipment_info', { exact: true }).first()).toBeVisible({ timeout: 10000 });

    await lp.clickResultTextAndOpenModal();
    await lp.verifyColumnPathModal('shipment_info', 'field_bar');
    await lp.closeEntityPathsModal();
  });
});
