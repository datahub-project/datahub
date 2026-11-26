/**
 * Profile page renders across core entity types.
 *
 * One test per entity type on its own seeded entity, against a real GMS. Each test opens the
 * profile and checks the header shows the entity's display name, the tabs that type always has are
 * present and each one opens, the shared sidebar sections render, no raw URN, GUID or URL escape is
 * shown as text on any tab, and the page threw no uncaught errors.
 *
 * Only tabs that are unconditional for the type are listed. Others come and go with feature flags
 * (such as the Summary tab), the edition (such as Quality and Governance) or the entity's data
 * (such as Runs), so asserting them would make the test depend on the environment.
 */

import { test } from '../../fixtures/base-test';
import { expectNoRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { EntityProfilePage } from '../../pages/entity/entity-profile.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';
import { CORE_ENTITIES, type CoreEntityKey } from './entity-matrix';

test.use({ featureName: 'catalog-matrix' });

const ALWAYS_PRESENT_TABS: Record<CoreEntityKey, readonly string[]> = {
  dataset: ['Columns', 'Lineage', 'Properties'],
  chart: ['Lineage', 'Properties'],
  dashboard: ['Lineage', 'Properties'],
  dataFlow: ['Lineage', 'Tasks', 'Properties'],
  dataJob: ['Lineage', 'Properties'],
  container: ['Contents', 'Properties'],
};

const SIDEBAR_SECTIONS = ['Owners', 'Domain', 'Tags', 'Terms'] as const;

test.describe('profile page renders across core entity types', () => {
  for (const entity of CORE_ENTITIES) {
    test(
      `${entity.key}: header, tabs and sidebar render`,
      { tag: [TestTag.P0, DomainTag.Catalog] },
      async ({ page, logger, logDir }) => {
        // Opens every tab once.
        test.setTimeout(TIMEOUTS.TEST_LONG);
        const profilePage = new EntityProfilePage(page, logger, logDir);

        await profilePage.openProfile(entity.pathName, entity.urn, entity.name);
        await profilePage.expectTabs(ALWAYS_PRESENT_TABS[entity.key]);
        await profilePage.expectSidebarSections(SIDEBAR_SECTIONS);
        await expectNoRawIdentifiers(page, { label: `${entity.key} profile` });

        for (const tabName of ALWAYS_PRESENT_TABS[entity.key]) {
          await profilePage.openTab(tabName);
          await expectNoRawIdentifiers(page, { label: `${entity.key} ${tabName} tab` });
        }

        profilePage.expectNoPageErrors();
      },
    );
  }
});
