/**
 * Manage Tags — assign / unassign tests
 */

import { test } from '../../fixtures/base-test';
import { DatasetPage } from '../../pages/dataset.page';
import { SearchPage } from '../../pages/search.page';

test.use({ featureName: 'manage-tags' });

const HIVE_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightHiveDataset,PROD)';

const TAG_NAME = 'playwright_assign_unassign_tag';
const TAG_URN = `urn:li:tag:${TAG_NAME}`;

test.describe('tags - assign/unassign', () => {
  test('should allow to assign/unassign tags on a dataset', async ({ page, logger, logDir }) => {
    const datasetPage = new DatasetPage(page, logger, logDir);
    const searchPage = new SearchPage(page, logger, logDir);

    // ── Assign the tag to the dataset ────────────────────────────────────
    await datasetPage.navigateToDataset(HIVE_DATASET_URN);
    await datasetPage.assignTag(TAG_NAME);
    await datasetPage.expectTagAssigned(TAG_NAME);

    // ── Verify the tag filter includes the dataset ────────────────────────
    await searchPage.searchByTag(TAG_URN);
    await searchPage.expectEntityInSearchResults(HIVE_DATASET_URN);

    // ── Unassign the tag from the dataset ────────────────────────────────
    await datasetPage.navigateToDataset(HIVE_DATASET_URN);
    await datasetPage.unassignTag(TAG_NAME);
    await datasetPage.expectTagNotAssigned(TAG_NAME);

    // ── Verify the tag filter no longer includes the dataset ─────────────
    await searchPage.searchByTag(TAG_URN);
    await searchPage.expectEntityNotInSearchResults(HIVE_DATASET_URN);
  });
});
