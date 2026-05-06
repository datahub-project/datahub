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
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';
const DOWNSTREAM_DATASET_URN =
  'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)';

const upstreamNodeSelector =
  '[data-testid="rf__node-urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)"]';
const downstreamNodeSelector =
  '[data-testid="rf__node-urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)"]';

test.describe('column-Level lineage and impact analysis path test', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  test('verify column-level lineage path at lineage graph and impact analysis', async ({
    page,
    logger,
    logDir,
  }) => {
    const lp = new LineageV2Page(page, logger, logDir);

    // Navigate to the HDFS dataset lineage graph
    await lp.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await expect(page.getByText('SamplePlaywrightHdfs').first()).toBeVisible({ timeout: 15000 });

    // Expand columns on HDFS dataset
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)');
    await expect(page.getByText('shipment_info', { exact: true }).first()).toBeVisible({ timeout: 10000 });

    // Expand columns on Kafka dataset
    await lp.expandContractColumns(
      'urn:li:dataset:(urn:li:dataPlatform:kafka,SamplePlaywrightKafkaDataset,PROD)',
    );

    // Click field_bar in the upstream Kafka node
    await page.locator(upstreamNodeSelector).getByText('field_bar').click();

    // Verify the selected field_bar indicator is active (fill not white)
    await expect(
      page.locator(upstreamNodeSelector).getByText('field_bar').locator('..').locator('svg, circle, path').first(),
    ).not.toHaveAttribute('fill', 'white');

    // Verify shipment_info has a connection indicator (stroke not transparent).
    // Use exact: true to avoid matching sub-fields like shipment_info.date, shipment_info.target, etc.
    await expect(
      page
        .locator(downstreamNodeSelector)
        .getByText('shipment_info', { exact: true })
        .locator('..')
        .locator('svg, circle, path')
        .first(),
    ).not.toHaveAttribute('stroke', 'transparent');

    // Click shipment_info in downstream node
    await page.locator(downstreamNodeSelector).getByText('shipment_info', { exact: true }).click();
    await expect(
      page
        .locator(downstreamNodeSelector)
        .getByText('shipment_info', { exact: true })
        .locator('..')
        .locator('svg, circle, path')
        .first(),
    ).not.toHaveAttribute('fill', 'white');
    await expect(
      page.locator(upstreamNodeSelector).getByText('field_bar', { exact: true }).locator('..').locator('svg, circle, path').first(),
    ).not.toHaveAttribute('stroke', 'transparent');

    // ── Impact analysis upstream column path modal ──────────────────────────

    await lp.goToDataset(DATASET_URN, 'SamplePlaywrightHdfsDataset');
    await lp.clickLineageTab();
    await lp.clickImpactAnalysis();
    await lp.clickColumnLineageOption();
    await lp.clickDownstreamOption();

    // Switch to upstream
    await lp.clickUpstreamOption();
    await expect(page.getByText('SamplePlaywrightKafkaDataset').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('field_bar')).not.toBeVisible();

    // Select a column from the dropdown.
    // Use exact: true to avoid matching sub-fields like shipment_info.date in the dropdown list.
    await page.getByText('Select column').click({ force: true });
    await page.waitForTimeout(1000);
    await page.locator('.rc-virtual-list').getByText('shipment_info', { exact: true }).click();
    await expect(page.getByText('field_bar').first()).toBeVisible({ timeout: 10000 });

    // The impact analysis auto-expands the column paths section for all dataset results when
    // column lineage is active. MatchesContainer uses a CSS `transition: height 0.3s ease`, so
    // the click can land during animation before React's synthetic event system is ready.
    // Use toPass to retry the click+modal-appearance pair until the animation has settled.
    await expect(page.locator('.ant-list-items').locator('[class*="ResultText"]').first()).toBeVisible({
      timeout: 5000,
    });
    await expect(async () => {
      await page.locator('.ant-list-items').locator('[class*="ResultText"]').first().click();
      await expect(page.getByRole('dialog', { name: /column path/i })).toBeVisible({ timeout: 2000 });
    }).toPass({ timeout: 12000 });
    await lp.verifyColumnPathModal('shipment_info', 'field_bar');
    await lp.closeEntityPathsModal();

    // ── Impact analysis downstream column path modal ───────────────────────

    await lp.goToDataset(DOWNSTREAM_DATASET_URN, 'SamplePlaywrightKafkaDataset');
    await lp.clickLineageTab();
    await lp.clickImpactAnalysis();
    await lp.clickColumnLineageOption();

    // Use exact: true to avoid matching sub-field names that contain "shipment_info" as prefix.
    await expect(page.getByText('shipment_info', { exact: true })).not.toBeVisible();
    await page.getByText('Select column').click({ force: true });
    await page.waitForTimeout(1000);
    await page.locator('.rc-virtual-list').getByText('field_bar', { exact: true }).click();
    await expect(page.getByText('shipment_info', { exact: true }).first()).toBeVisible({ timeout: 10000 });

    // Same retry pattern for downstream: CSS transition can swallow the first click.
    await expect(page.locator('.ant-list-items').locator('[class*="ResultText"]').first()).toBeVisible({
      timeout: 5000,
    });
    await expect(async () => {
      await page.locator('.ant-list-items').locator('[class*="ResultText"]').first().click();
      await expect(page.getByRole('dialog', { name: /column path/i })).toBeVisible({ timeout: 2000 });
    }).toPass({ timeout: 12000 });
    await lp.verifyColumnPathModal('shipment_info', 'field_bar');
    await lp.closeEntityPathsModal();
  });
});
