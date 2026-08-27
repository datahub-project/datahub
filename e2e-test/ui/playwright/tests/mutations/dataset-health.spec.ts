/**
 * Dataset health tests — migrated from Cypress e2e/mutations/dataset_health.js
 *
 * Verifies that the health badge and popover appear correctly for a pre-seeded
 * dataset that has failing assertions and active incidents.
 *
 * Prerequisites: The Hive dataset `playwright_health_test` must exist with at
 * least one failing assertion or active incident in the test environment.
 */

import { test } from '../../fixtures/base-test';
import { DatasetHealthPage } from '../../pages/dataset-health.page';

test.use({ featureName: 'mutations' });

const URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_health_test,PROD)';
const DATASET_NAME = 'playwright_health_test';

test.describe('dataset health test', () => {
  test('go to dataset with failing assertions and active incidents and verify health of dataset', async ({
    page,
    logger,
    logDir,
  }) => {
    const healthPage = new DatasetHealthPage(page, logger, logDir);

    logger.step('navigate to dataset', { urn: URN });
    await healthPage.navigateToDataset(URN, DATASET_NAME);

    logger.step('hover health icon to open popover');
    await healthPage.hoverHealthIcon(URN);
    await healthPage.expectHealthPopoverVisible();
    await healthPage.expectHealthLinksPresent();
  });
});
