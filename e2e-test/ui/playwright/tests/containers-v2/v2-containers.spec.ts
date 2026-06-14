/**
 * Container V2 Tests
 *
 * Tests container entity display and child entity listing functionality.
 *
 * Uses fixture data from tests/containers-v2/fixtures/data.json:
 *   - 5 test-specific datasets: containers_v2_table_1 through table_5
 *   - All linked to container: urn:li:container:containers_v2_test
 *
 */

import { test } from '../../fixtures/base-test';
import { ContainerPage } from '../../pages/container.page';

const TEST_DATA = {
  CONTAINER_URN: 'urn:li:container:containers_v2_test',
  CONTAINER_NAME: 'Playwright Containers V2 Test',
  EXPECTED_ENTITIES: [
    'containers_v2_table_1',
    'containers_v2_table_2',
    'containers_v2_table_3',
    'containers_v2_table_4',
    'containers_v2_table_5',
  ],
} as const;

test.use({ featureName: 'containers-v2' });

test.describe('containers', () => {
  let containerPage: ContainerPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    containerPage = new ContainerPage(page, logger, logDir);
  });

  test('can see elements inside the container', async ({ logger }) => {
    logger.info('Navigating to container page', { urn: TEST_DATA.CONTAINER_URN });
    await containerPage.navigateToContainer(TEST_DATA.CONTAINER_URN);

    logger.info('Verifying container name is visible', { containerName: TEST_DATA.CONTAINER_NAME });
    await containerPage.verifyContainerNameVisible(TEST_DATA.CONTAINER_NAME);

    logger.info('Verifying all child entities are visible', { count: TEST_DATA.EXPECTED_ENTITIES.length });
    await containerPage.verifyMultipleEntitiesVisible(TEST_DATA.EXPECTED_ENTITIES);

    logger.info('Verifying pagination info displays correctly');
    await containerPage.verifyPaginationInfo();
  });
});
