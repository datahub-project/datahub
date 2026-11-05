/**
 * Add and remove a tag across core entity types.
 *
 * One test per entity type on its own seeded entity, against a real GMS. Each test assigns the
 * matrix tag from the sidebar, checks the pill renders with the tag's display name (the tag's key
 * differs from its name, so a URN-tail fallback cannot pass), checks it survives a reload, then
 * removes it and checks it is gone, again across a reload.
 *
 * The raw-identifier guard runs while the Add Tags options are on screen and after the pill
 * renders, which is where tag identifiers have leaked before.
 */

import { test } from '../../fixtures/base-test';
import { expectNoRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { EntityTagsPage } from '../../pages/entity/entity-tags.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';
import { CORE_ENTITIES, MATRIX_TAG, clearTags } from './entity-matrix';

test.use({ featureName: 'catalog-matrix' });

test.describe('add and remove a tag across core entity types', () => {
  for (const entity of CORE_ENTITIES) {
    test(
      `${entity.key}: add a tag from the sidebar, then remove it`,
      { tag: [TestTag.P0, DomainTag.Catalog] },
      async ({ page, gmsRequest, logger, logDir }) => {
        // Two mutations, each followed by a reload.
        test.setTimeout(TIMEOUTS.TEST_LONG);

        // Start with no tags so an earlier failed run cannot leak into this one.
        await clearTags(gmsRequest, entity);

        const tagsPage = new EntityTagsPage(page, logger, logDir);
        await tagsPage.openProfile(entity.pathName, entity.urn, entity.name);
        await tagsPage.expectTagNotAssigned(MATRIX_TAG.name);

        await tagsPage.openAddTagsModal();
        await tagsPage.searchTag(MATRIX_TAG.name);
        await expectNoRawIdentifiers(page, { label: `${entity.key} add tags modal` });
        await tagsPage.selectTagAndConfirm(MATRIX_TAG.name);

        await tagsPage.expectTagAssigned(MATRIX_TAG.name);
        await expectNoRawIdentifiers(page, { label: `${entity.key} profile with tag` });
        await tagsPage.reload();
        await tagsPage.expectTagAssigned(MATRIX_TAG.name);

        await tagsPage.removeTag(MATRIX_TAG.name);
        await tagsPage.expectTagNotAssigned(MATRIX_TAG.name);
        await tagsPage.reload();
        await tagsPage.expectTagNotAssigned(MATRIX_TAG.name);
      },
    );
  }
});
