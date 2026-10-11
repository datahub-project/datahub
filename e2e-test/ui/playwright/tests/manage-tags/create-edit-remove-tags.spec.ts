/**
 * Manage Tags — create / edit / remove tests
 */

import { test } from '../../fixtures/base-test';
import { TagsPage } from '../../pages/tags.page';
import { withRandomSuffix } from '../../utils/random';

const TAG_NAME = withRandomSuffix('playwright_tag_create_edit_remove');

const TAG_CREATE_EDIT_REMOVE = {
  name: TAG_NAME,
  description: `${TAG_NAME} description`,
  editedDescription: `${TAG_NAME} description edited`,
};

test.describe('tags - create/edit/remove', () => {
  test('should allow to create/edit/remove tags on tags page', async ({ page, logger, logDir }) => {
    const tagsPage = new TagsPage(page, logger, logDir);

    // ── Create ──────────────────────────────────────────────────────────────
    await tagsPage.navigate();

    await tagsPage.createTag(TAG_CREATE_EDIT_REMOVE.name, TAG_CREATE_EDIT_REMOVE.description);

    await tagsPage.expectTagInTable(TAG_CREATE_EDIT_REMOVE.name, TAG_CREATE_EDIT_REMOVE.description);

    // ── Duplicate should fail ───────────────────────────────────────────────
    await tagsPage.createTag(TAG_CREATE_EDIT_REMOVE.name, TAG_CREATE_EDIT_REMOVE.description, false);

    // ── Edit ────────────────────────────────────────────────────────────────
    await tagsPage.editTagDescription(TAG_CREATE_EDIT_REMOVE.name, TAG_CREATE_EDIT_REMOVE.editedDescription);

    await tagsPage.expectTagInTable(TAG_CREATE_EDIT_REMOVE.name, TAG_CREATE_EDIT_REMOVE.editedDescription);

    // ── Delete ──────────────────────────────────────────────────────────────
    await tagsPage.deleteTag(TAG_CREATE_EDIT_REMOVE.name);

    await tagsPage.expectTagNotInTable(TAG_CREATE_EDIT_REMOVE.name);
  });
});
