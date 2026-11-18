/**
 * Bulk Actions and Download Tests
 *
 * Covers the select mode and download on an entity's Assets tab (a domain here):
 * - Select every asset in the list, add a tag to all of them at once, and see it on each asset
 * - Download the list as a CSV and check the file holds the listed assets
 *
 * Seeds (tests/bulk-actions/fixtures/data.json): the domain pw-bulk-domain holding two datasets, and the
 * tag PW Bulk Tag. The tag is removed from both datasets before and after the tagging test.
 */

import { test, expect } from '../../fixtures/base-test';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { DatasetPage } from '../../pages/dataset.page';
import { AssetsTabPage } from '../../pages/entity/assets-tab.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';

test.use({ featureName: 'bulk-actions' });

const DOMAIN_URN = 'urn:li:domain:pw-bulk-domain';
const DATASET_URNS = [
  'urn:li:dataset:(urn:li:dataPlatform:hive,pw_bulk_dataset_a,PROD)',
  'urn:li:dataset:(urn:li:dataPlatform:hive,pw_bulk_dataset_b,PROD)',
];
const DATASET_NAMES = ['pw_bulk_dataset_a', 'pw_bulk_dataset_b'];
const TAG_URN = 'urn:li:tag:pw_bulk_tag_key';
const TAG_NAME = 'PW Bulk Tag';

const BATCH_REMOVE_TAGS_MUTATION = `
  mutation batchRemoveTags($input: BatchRemoveTagsInput!) {
    batchRemoveTags(input: $input)
  }
`;

test.describe('bulk actions and download', () => {
  let assetsTab: AssetsTabPage;

  test.beforeEach(async ({ page, logger }) => {
    assetsTab = new AssetsTabPage(page, logger);
    await assetsTab.open('domain', DOMAIN_URN);
    // The select-mode control renders before the Assets search answers, so wait for the rows themselves.
    for (const urn of DATASET_URNS) {
      await assetsTab.expectAssetListed(urn);
    }
  });

  test(
    'adds a tag to every selected asset at once',
    { tag: [TestTag.P0, DomainTag.Catalog] },
    async ({ page, logger }) => {
      const graphql = new GraphQLHelper(page);
      const datasetPage = new DatasetPage(page, logger);
      const removeTag = async () => {
        const response = await graphql.executeQuery(BATCH_REMOVE_TAGS_MUTATION, {
          input: { tagUrns: [TAG_URN], resources: DATASET_URNS.map((resourceUrn) => ({ resourceUrn })) },
        });
        // GraphQL reports a failed mutation in the body of a 200 response; a tag left over from an earlier
        // run would otherwise let the sidebar check pass without the bulk action having run.
        expect(response.errors, `batchRemoveTags failed: ${JSON.stringify(response.errors)}`).toBeUndefined();
        expect((response.data as { batchRemoveTags?: boolean } | undefined)?.batchRemoveTags).toBe(true);
      };
      await removeTag();
      try {
        await assetsTab.enterSelectMode();
        await assetsTab.selectAll();
        await assetsTab.expectSelectedCount(DATASET_URNS.length);
        await assetsTab.addTagToSelection(TAG_NAME);

        for (const urn of DATASET_URNS) {
          // The sidebar reads the tag from the entity, but reload in case the page was served before the
          // batch landed.
          await expect(async () => {
            await datasetPage.navigateToDataset(urn);
            await expect(datasetPage.getTagChip(TAG_NAME)).toBeVisible({ timeout: TIMEOUTS.SHORT });
          }).toPass({ timeout: TIMEOUTS.LONG });
        }
      } finally {
        await removeTag();
      }
    },
  );

  test('downloads the asset list as a CSV', { tag: [DomainTag.Catalog] }, async () => {
    const csv = await assetsTab.downloadCsv('pw-bulk-assets.csv');
    for (const name of DATASET_NAMES) {
      expect(csv).toContain(name);
    }
  });
});
