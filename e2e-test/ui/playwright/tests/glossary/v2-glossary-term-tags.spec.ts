/**
 * Glossary term profile — assign / unassign tags from the sidebar Tags section.
 *
 * Prerequisites (seeded via tests/glossary/fixtures/data.json):
 *   - PlaywrightGlossaryTermTests.PlaywrightTaggableTerm
 *   - PlaywrightGlossaryTermProfileTag
 */

import { test } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';

test.use({ featureName: 'glossary' });

const TAGGABLE_TERM_URN = 'urn:li:glossaryTerm:PlaywrightGlossaryTermTests.PlaywrightTaggableTerm';
const TAG_NAME = 'PlaywrightGlossaryTermProfileTag';

test.describe('glossary term profile tags', () => {
  // Mutations on a shared seeded term — keep serial to avoid chip-state races.
  test.describe.configure({ mode: 'serial' });

  let glossaryPage: GlossaryPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    glossaryPage = new GlossaryPage(page, logger, logDir);
    await page.addInitScript(() => {
      localStorage.setItem('navBarState', '{"state":"COLLAPSED"}');
    });
  });

  test('should allow assigning and unassigning a tag on a glossary term', async () => {
    test.setTimeout(120000);

    await glossaryPage.navigateToGlossaryTermByUrn(TAGGABLE_TERM_URN);

    // Start from a clean sidebar (prior failed runs may have left the tag).
    if ((await glossaryPage.getTagChip(TAG_NAME).count()) > 0) {
      await glossaryPage.unassignTag(TAG_NAME);
      await glossaryPage.expectTagNotAssigned(TAG_NAME);
    }

    await glossaryPage.assignTag(TAG_NAME);
    await glossaryPage.expectTagAssigned(TAG_NAME);

    // Reload to confirm the tag persisted via GraphQL, not just client state.
    await glossaryPage.navigateToGlossaryTermByUrn(TAGGABLE_TERM_URN);
    await glossaryPage.expectTagAssigned(TAG_NAME);

    await glossaryPage.unassignTag(TAG_NAME);
    await glossaryPage.expectTagNotAssigned(TAG_NAME);

    await glossaryPage.navigateToGlossaryTermByUrn(TAGGABLE_TERM_URN);
    await glossaryPage.expectTagNotAssigned(TAG_NAME);
  });
});
