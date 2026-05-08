/**
 * Column-level lineage graph V2 tests
 * Migrated from Cypress e2e/lineageV2/v2_lineage_column_level.js
 *
 * Tests the expand/contract columns toggle on lineage nodes to show/hide field-level
 * lineage within the lineageV2 graph view.
 *
 * Uses apiMock.setFeatureFlags to disable lineageGraphV3 so the V2 graph renders.
 */

import { test, expect } from '../../fixtures/base-test';
import { LineageV2Page } from '../../pages/lineage-v2.page';

const DATASET_ENTITY_TYPE = 'dataset';
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)';

test.describe('column-level lineage graph test', () => {
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({ lineageGraphV3: false });
  });

  test('navigate to lineage graph view and verify that column-level lineage is showing correctly', async ({
    page,
    logger,
    logDir,
  }) => {
    const lp = new LineageV2Page(page, logger, logDir);
    await lp.goToLineageGraph(DATASET_ENTITY_TYPE, DATASET_URN);
    await page.waitForTimeout(2000);

    // Columns are NOT shown by default
    await expect(page.getByText('SamplePlaywrightHdfs').first()).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('SamplePlaywrightHive').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('playwright_logging').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info', { exact: true })).toBeHidden();
    await expect(page.getByText('field_foo', { exact: true })).toBeHidden();
    await expect(page.getByText('field_bar', { exact: true })).toBeHidden();
    await expect(page.getByText('event_name', { exact: true })).toBeHidden();
    await expect(page.getByText('event_data', { exact: true })).toBeHidden();
    await expect(page.getByText('timestamp', { exact: true })).toBeHidden();
    await expect(page.getByText('browser', { exact: true })).toBeHidden();

    // Expand HDFS node columns
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)');
    await expect(page.getByText('shipment_info').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info.date').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info.target').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info.destination').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info.geo_info').first()).toBeVisible({ timeout: 10000 });

    // Expand Hive node columns
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)');
    await expect(page.getByText('field_foo').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('field_bar').first()).toBeVisible({ timeout: 10000 });

    // Expand logging events node columns
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hive,playwright_logging_events,PROD)');
    await expect(page.getByText('event_name').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('event_data').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('timestamp').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('browser').first()).toBeVisible({ timeout: 10000 });

    // Contract Hive node — its columns disappear
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)');
    await expect(page.getByText('field_foo', { exact: true })).toBeHidden();
    await expect(page.getByText('field_bar', { exact: true })).toBeHidden();

    // Contract HDFS and logging too, then re-expand just Hive
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)');
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hive,playwright_logging_events,PROD)');
    await lp.expandContractColumns('urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)');

    // Only Hive columns should now be visible
    await expect(page.getByText('field_foo').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('field_bar').first()).toBeVisible({ timeout: 10000 });
    await expect(page.getByText('shipment_info', { exact: true })).toBeHidden();
    await expect(page.getByText('event_name', { exact: true })).toBeHidden();
    await expect(page.getByText('event_data', { exact: true })).toBeHidden();
    await expect(page.getByText('timestamp', { exact: true })).toBeHidden();
    await expect(page.getByText('browser', { exact: true })).toBeHidden();
  });
});
