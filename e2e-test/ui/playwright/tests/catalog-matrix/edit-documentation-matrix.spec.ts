/**
 * Edit documentation across core entity types.
 *
 * One test per entity type, each on its own seeded entity, so the tests run in parallel without
 * racing. Each test edits the description twice against a real GMS: once from the sidebar
 * Documentation pencil and once from the tab the profile renders documentation on (the Summary
 * tab's modal or the Documentation tab's inline editor), checking the sidebar after each save and
 * after a reload.
 *
 * The surface the pencil must open is derived from the live feature flags with the same rule the
 * app uses, so the test fails when the pencil routes to a tab the profile does not render. That is
 * the shape of the escalation this guards against: on charts and dashboards the pencil pointed at
 * a Documentation tab the Summary flag had removed, the profile fell back to Summary, and no editor
 * opened.
 */

import { test } from '../../fixtures/base-test';
import { expectNoRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { EntityDescriptionPage } from '../../pages/entity/entity-description.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';
import {
  CORE_ENTITIES,
  fetchDocumentationFlags,
  resolveDocumentationSurface,
  writeEditableDescription,
} from './entity-matrix';

test.use({ featureName: 'catalog-matrix' });

test.describe('edit documentation across core entity types', () => {
  for (const entity of CORE_ENTITIES) {
    test(
      `${entity.key}: edit from the sidebar pencil and from the tab`,
      { tag: [TestTag.P0, DomainTag.Catalog] },
      async ({ page, gmsRequest, logger, logDir }) => {
        // Two edit, save and re-render cycles plus a reload.
        test.setTimeout(TIMEOUTS.TEST_LONG);

        const flags = await fetchDocumentationFlags(page.request);
        const surface = resolveDocumentationSurface(entity, flags);
        logger.info('documentation surface', { entity: entity.key, surface });

        // Start from the seeded text so an earlier failed run cannot leak into this one.
        await writeEditableDescription(gmsRequest, entity, entity.seedDescription);

        const descriptionPage = new EntityDescriptionPage(page, logger, logDir);
        await descriptionPage.openProfile(entity.pathName, entity.urn, entity.name);
        await descriptionPage.expectSidebarDescription(entity.seedDescription);
        await expectNoRawIdentifiers(page, { label: `${entity.key} profile` });

        const stamp = Date.now();
        const fromSidebar = `Edited from the sidebar pencil ${stamp}`;
        await descriptionPage.editDescriptionFromSidebar(fromSidebar, surface);
        await descriptionPage.expectSidebarDescription(fromSidebar);
        await descriptionPage.reload();
        await descriptionPage.expectSidebarDescription(fromSidebar);

        const fromTab = `Edited from the ${surface} tab ${stamp}`;
        await descriptionPage.editDescriptionFromTab(fromTab, surface);
        await descriptionPage.expectSurfaceDescription(fromTab, surface);
        await descriptionPage.expectSidebarDescription(fromTab);
        await expectNoRawIdentifiers(page, { label: `${entity.key} ${surface} tab` });
      },
    );
  }
});
