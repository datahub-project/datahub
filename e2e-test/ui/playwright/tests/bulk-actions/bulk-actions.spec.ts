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

import { readFile } from 'node:fs/promises';
import { test, expect } from '../../fixtures/base-test';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';

test.use({ featureName: 'bulk-actions' });

const DOMAIN_ASSETS_URL = `/domain/${encodeURIComponent('urn:li:domain:pw-bulk-domain')}/Assets`;
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
  test.beforeEach(async ({ page }) => {
    await page.goto(DOMAIN_ASSETS_URL);
    await page.waitForLoadState('networkidle');
    await expect(page.getByTestId('search-results-edit-button')).toBeVisible({ timeout: TIMEOUTS.LONG });
  });

  test('adds a tag to every selected asset at once', { tag: [TestTag.P0, DomainTag.Catalog] }, async ({ page }) => {
    const graphql = new GraphQLHelper(page);
    const removeTag = () =>
      graphql.executeQuery(BATCH_REMOVE_TAGS_MUTATION, {
        input: { tagUrns: [TAG_URN], resources: DATASET_URNS.map((resourceUrn) => ({ resourceUrn })) },
      });
    await removeTag();
    try {
      await page.getByTestId('search-results-edit-button').click();
      await page.getByTestId('checkbox-base').first().click();
      await expect(page.getByText('2 selected')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });

      // The bulk bar has no test ids; it renders after the left navigation, which also has a "Tags" item.
      await page.getByText('Tags', { exact: true }).last().click();
      await page.getByTestId('menu-item-Add tags').click();

      await page.getByText('Search for tag...').click();
      await page.getByPlaceholder('Search...', { exact: true }).fill(TAG_NAME);
      await page.getByText(TAG_NAME, { exact: true }).click();
      await page.getByRole('dialog').getByRole('button', { name: 'Add', exact: true }).click();

      for (const urn of DATASET_URNS) {
        await expect(async () => {
          await page.goto(`/dataset/${encodeURIComponent(urn)}`);
          await expect(page.getByTestId('sidebar-section-content-Tags').getByText(TAG_NAME)).toBeVisible({
            timeout: TIMEOUTS.SHORT,
          });
        }).toPass({ timeout: TIMEOUTS.LONG });
      }
    } finally {
      await removeTag();
    }
  });

  test('downloads the asset list as a CSV', { tag: [DomainTag.Catalog] }, async ({ page }) => {
    await page.getByTestId('download-csv-button').click();
    await page.getByTestId('download-as-csv-input').fill('pw-bulk-assets.csv');

    const downloadPromise = page.waitForEvent('download');
    await page.getByTestId('csv-modal-download-button').click();
    const download = await downloadPromise;

    expect(download.suggestedFilename()).toBe('pw-bulk-assets.csv');
    const filePath = await download.path();
    const csv = await readFile(filePath, 'utf-8');
    for (const name of DATASET_NAMES) {
      expect(csv).toContain(name);
    }
  });
});
