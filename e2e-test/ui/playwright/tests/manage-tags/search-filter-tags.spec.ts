/**
 * Manage Tags — search / filter tests
 */

import { test } from '../../fixtures/base-test';
import { TagsPage } from '../../pages/tags.page';

test.use({ featureName: 'manage-tags' });

const TAG_NAME = 'playwright_search_filter_tag';
const TAG_URN = `urn:li:tag:${TAG_NAME}`;

test.describe('tags - search / filter', () => {
  let tagsPage: TagsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    tagsPage = new TagsPage(page, logger, logDir);
    await tagsPage.navigate();
  });

  test('should have search bar with correct placeholder', async () => {
    await tagsPage.expectSearchPlaceholder();
  });

  test('should show empty state when searching for a non-existent tag', async () => {
    await tagsPage.expectPageTitle();
    await tagsPage.expectTagNameVisible(TAG_URN, TAG_NAME);

    await tagsPage.searchForTag('invalidvalue_that_does_not_exist');

    await tagsPage.expectTagsNotFound();
  });

  test('should show page title and filter tags by name', async () => {
    await tagsPage.expectPageTitle();
    await tagsPage.expectTagNameVisible(TAG_URN, TAG_NAME);

    await tagsPage.searchForTag(TAG_NAME);

    await tagsPage.expectTagNameVisible(TAG_URN, TAG_NAME);
  });
});
