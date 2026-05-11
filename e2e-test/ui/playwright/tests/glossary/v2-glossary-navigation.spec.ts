/**
 * Glossary Sidebar Navigation tests — migrated from Cypress e2e/glossaryV2/v2_glossary_navigation.js
 *
 * Tests the move and sidebar navigation behaviour for glossary entities.
 */

import { test } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';
import { withRandomSuffix } from '../../utils/random';

// Pre-seeded via fixtures/data.json — scoped to the Properties-tab-persistence test.
const PROPS_TEST_TERM1_URN = 'urn:li:glossaryTerm:PlaywrightNavPropsTest.PlaywrightNavTerm1';
const PROPS_TEST_TERM2_URN = 'urn:li:glossaryTerm:PlaywrightNavPropsTest.PlaywrightNavTerm2';

test.use({ featureName: 'glossary' });

test.describe('glossary sidebar navigation', () => {
  let glossaryPage: GlossaryPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();
  });

  test('can move a term into its parent term group', async ({ cleanup }) => {
    const termGroup = withRandomSuffix('NavGroup');
    const term = withRandomSuffix('NavTerm');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroup);
    cleanup.track(termGroupUrn);

    await glossaryPage.navigateToGlossary();
    await glossaryPage.clickSidebarNode(termGroupUrn);
    await glossaryPage.openContentsTab();
    const termUrn = await glossaryPage.createTermInContentsTab(term);
    cleanup.track(termUrn);

    await glossaryPage.clickContentsTabItem(termUrn);
    await glossaryPage.moveCurrentEntityTo(termGroup);

    await glossaryPage.clickSidebarNode(termGroupUrn);
    await glossaryPage.openContentsTab();
    await glossaryPage.expectEntityInContentsTab(termUrn);
  });

  test('Properties tab persists when switching between terms', async () => {
    await glossaryPage.navigateToGlossaryTermByUrn(PROPS_TEST_TERM1_URN);
    await glossaryPage.openPropertiesTab();
    await glossaryPage.expectPropertiesTabActive();

    await glossaryPage.clickSidebarTerm(PROPS_TEST_TERM2_URN);
    await glossaryPage.expectPropertiesTabActive();
  });

  test('can move a term group under a parent node', async ({ cleanup }) => {
    const parentNode = withRandomSuffix('NavParent');
    const termGroup = withRandomSuffix('NavGroup');

    const parentNodeUrn = await glossaryPage.createTermGroup(parentNode);
    cleanup.track(parentNodeUrn);

    await glossaryPage.navigateToGlossary();
    const termGroupUrn = await glossaryPage.createTermGroup(termGroup);
    cleanup.track(termGroupUrn);

    await glossaryPage.navigateToGlossary();
    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.moveCurrentEntityTo(parentNode);

    await glossaryPage.clickSidebarNode(parentNodeUrn);
    await glossaryPage.openContentsTab();
    await glossaryPage.expectEntityInContentsTab(termGroupUrn);
  });
});
