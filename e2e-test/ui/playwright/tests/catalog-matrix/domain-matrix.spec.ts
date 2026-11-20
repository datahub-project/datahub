/**
 * Set and remove the domain across core entity types.
 *
 * One test per entity type on its own seeded entity, against a real GMS. Each test sets the
 * matrix domain from the sidebar, checks the pill renders with the domain's display name (the
 * domain's key differs from its name, so a URN-tail fallback cannot pass), checks it survives a
 * reload, then removes it and checks it is gone, again across a reload.
 *
 * The raw-identifier guard runs while the Set Domain options are on screen and after the pill
 * renders.
 */

import { test } from '../../fixtures/base-test';
import { expectNoRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { EntityDomainPage } from '../../pages/entity/entity-domain.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';
import { CORE_ENTITIES, MATRIX_DOMAIN, clearDomains } from './entity-matrix';

test.use({ featureName: 'catalog-matrix' });

test.describe('set and remove the domain across core entity types', () => {
  for (const entity of CORE_ENTITIES) {
    test(
      `${entity.key}: set a domain from the sidebar, then remove it`,
      { tag: [TestTag.P0, DomainTag.Catalog] },
      async ({ page, gmsRequest, logger, logDir }) => {
        // Two mutations, each followed by a reload.
        test.setTimeout(TIMEOUTS.TEST_LONG);

        // Start with no domain so an earlier failed run cannot leak into this one.
        await clearDomains(gmsRequest, entity);

        const domainPage = new EntityDomainPage(page, logger, logDir);
        await domainPage.openProfile(entity.pathName, entity.urn, entity.name);
        await domainPage.expectDomainNotSet(MATRIX_DOMAIN.name);

        await domainPage.openSetDomainModal();
        await domainPage.searchDomain(MATRIX_DOMAIN.name);
        await expectNoRawIdentifiers(page, { label: `${entity.key} set domain modal` });
        await domainPage.selectDomainAndSave(MATRIX_DOMAIN.name);

        await domainPage.expectDomainSet(MATRIX_DOMAIN.name);
        await expectNoRawIdentifiers(page, { label: `${entity.key} profile with domain` });
        await domainPage.reload();
        await domainPage.expectDomainSet(MATRIX_DOMAIN.name);

        await domainPage.removeDomain(MATRIX_DOMAIN.name);
        await domainPage.expectDomainNotSet(MATRIX_DOMAIN.name);
        await domainPage.reload();
        await domainPage.expectDomainNotSet(MATRIX_DOMAIN.name);
      },
    );
  }
});
