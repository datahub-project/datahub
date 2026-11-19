/**
 * Add and remove a glossary term across core entity types.
 *
 * One test per entity type on its own seeded entity, against a real GMS. Each test assigns the
 * matrix term from the sidebar, checks the pill renders with the term's display name (the term's
 * key differs from its name, so a URN-tail fallback cannot pass), checks it survives a reload,
 * then removes it and checks it is gone, again across a reload.
 *
 * The raw-identifier guard runs while the Add Terms options are on screen and after the pill
 * renders.
 */

import { test } from '../../fixtures/base-test';
import { expectNoRawIdentifiers } from '../../helpers/raw-identifier-helper';
import { EntityGlossaryTermsPage } from '../../pages/entity/entity-glossary-terms.page';
import { TIMEOUTS } from '../../utils/constants';
import { DomainTag, TestTag } from '../../utils/test-tags';
import { CORE_ENTITIES, MATRIX_TERM, clearGlossaryTerms } from './entity-matrix';

test.use({ featureName: 'catalog-matrix' });

test.describe('add and remove a glossary term across core entity types', () => {
  for (const entity of CORE_ENTITIES) {
    test(
      `${entity.key}: add a glossary term from the sidebar, then remove it`,
      { tag: [TestTag.P0, DomainTag.Catalog] },
      async ({ page, gmsRequest, logger, logDir }) => {
        // Two mutations, each followed by a reload.
        test.setTimeout(TIMEOUTS.TEST_LONG);

        // Start with no terms so an earlier failed run cannot leak into this one.
        await clearGlossaryTerms(gmsRequest, entity);

        const termsPage = new EntityGlossaryTermsPage(page, logger, logDir);
        await termsPage.openProfile(entity.pathName, entity.urn, entity.name);
        await termsPage.expectTermNotAssigned(MATRIX_TERM.name);

        await termsPage.openAddTermsModal();
        await termsPage.searchTerm(MATRIX_TERM.name);
        await expectNoRawIdentifiers(page, { label: `${entity.key} add terms modal` });
        await termsPage.selectTermAndConfirm(MATRIX_TERM.name);

        await termsPage.expectTermAssigned(MATRIX_TERM.name);
        await expectNoRawIdentifiers(page, { label: `${entity.key} profile with term` });
        await termsPage.reload();
        await termsPage.expectTermAssigned(MATRIX_TERM.name);

        await termsPage.removeTerm(MATRIX_TERM.name);
        await termsPage.expectTermNotAssigned(MATRIX_TERM.name);
        await termsPage.reload();
        await termsPage.expectTermNotAssigned(MATRIX_TERM.name);
      },
    );
  }
});
