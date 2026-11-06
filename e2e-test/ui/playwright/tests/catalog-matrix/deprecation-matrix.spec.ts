/**
 * Deprecate and undeprecate across core entity types.
 *
 * One test per entity type on its own seeded entity, against a real GMS. Each test marks the
 * entity deprecated from the header menu with a reason, checks the deprecation icon in the profile
 * header (and its popover reason) and on the entity's search result card, checks it survives a
 * reload, then undoes it and checks the icon is gone from both places.
 *
 * The raw-identifier guard runs on the profile once the deprecation is shown.
 */

import { test } from '../../fixtures/base-test';
import { expectNoRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { EntityDeprecationPage } from '../../pages/entity/entity-deprecation.page';
import { TIMEOUTS } from '../../utils/constants';
import { withRandomSuffix } from '../../utils/random';
import { DomainTag, TestTag } from '../../utils/test-tags';
import { CORE_ENTITIES, clearDeprecation } from './entity-matrix';

test.use({ featureName: 'catalog-matrix' });

// The data job's search result card has no `preview-<urn>` test id, unlike the other core types, so there
// is nothing to scope the deprecation icon to on its card. Its header icon is still checked.
const TYPES_WITHOUT_SEARCH_CARD_TEST_ID: ReadonlySet<string> = new Set(['dataJob']);

test.describe('deprecate and undeprecate across core entity types', () => {
  for (const entity of CORE_ENTITIES) {
    test(
      `${entity.key}: mark deprecated from the header menu, then undo it`,
      { tag: [TestTag.P0, DomainTag.Catalog] },
      async ({ page, gmsRequest, logger, logDir }) => {
        // Two mutations, each followed by a reload and a search.
        test.setTimeout(TIMEOUTS.TEST_LONG);
        const reason = withRandomSuffix('Replaced by v2');
        const checkSearchCard = !TYPES_WITHOUT_SEARCH_CARD_TEST_ID.has(entity.key);

        // Start not deprecated so an earlier failed run cannot leak into this one.
        await clearDeprecation(gmsRequest, entity);

        const deprecationPage = new EntityDeprecationPage(page, logger, logDir);
        await deprecationPage.openProfile(entity.pathName, entity.urn, entity.name);
        await deprecationPage.expectNotDeprecatedInHeader();

        await deprecationPage.markDeprecated(reason);
        await deprecationPage.expectReasonInPopover(reason);
        await expectNoRawIdentifiers(page, { label: `${entity.key} profile deprecated` });
        await deprecationPage.reload();
        await deprecationPage.expectDeprecatedInHeader();
        if (checkSearchCard) await deprecationPage.expectSearchCardDeprecation(entity.name, entity.urn, true);

        await deprecationPage.openProfile(entity.pathName, entity.urn, entity.name);
        await deprecationPage.markUndeprecated();
        await deprecationPage.reload();
        await deprecationPage.expectNotDeprecatedInHeader();
        if (checkSearchCard) await deprecationPage.expectSearchCardDeprecation(entity.name, entity.urn, false);
      },
    );
  }
});
