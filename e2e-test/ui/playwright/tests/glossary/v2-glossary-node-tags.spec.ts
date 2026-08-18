/**
 * Glossary node (term group) profile — assign / unassign tags from the sidebar Tags section.
 *
 * Prerequisites (seeded via tests/glossary/fixtures/data.json):
 *   - PlaywrightGlossaryTermTests.PlaywrightTaggableTermGroup
 *   - PlaywrightGlossaryNodeProfileTag
 */

import { test } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';

test.use({ featureName: 'glossary' });

const TAGGABLE_NODE_URN = 'urn:li:glossaryNode:PlaywrightGlossaryTermTests.PlaywrightTaggableTermGroup';
const TAG_NAME = 'PlaywrightGlossaryNodeProfileTag';

test.describe('glossary node profile tags', () => {
  // Mutations on a shared seeded node — keep serial to avoid chip-state races.
  test.describe.configure({ mode: 'serial' });

  let glossaryPage: GlossaryPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    glossaryPage = new GlossaryPage(page, logger, logDir);
    await page.addInitScript(() => {
      localStorage.setItem('navBarState', '{"state":"COLLAPSED"}');
    });
  });

  test('should allow assigning and unassigning a tag on a glossary node', async () => {
    test.setTimeout(120000);

    await glossaryPage.navigateToGlossaryNodeByUrn(TAGGABLE_NODE_URN);

    // Start from a clean sidebar (prior failed runs may have left the tag).
    if ((await glossaryPage.getTagChip(TAG_NAME).count()) > 0) {
      await glossaryPage.unassignTag(TAG_NAME);
      await glossaryPage.expectTagNotAssigned(TAG_NAME);
    }

    await glossaryPage.assignTag(TAG_NAME);
    await glossaryPage.expectTagAssigned(TAG_NAME);

    // Reload to confirm the tag persisted via GraphQL, not just client state.
    await glossaryPage.navigateToGlossaryNodeByUrn(TAGGABLE_NODE_URN);
    await glossaryPage.expectTagAssigned(TAG_NAME);

    await glossaryPage.unassignTag(TAG_NAME);
    await glossaryPage.expectTagNotAssigned(TAG_NAME);
  });
});
