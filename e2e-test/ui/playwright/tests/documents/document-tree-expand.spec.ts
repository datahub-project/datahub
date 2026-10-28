/**
 * Documents sidebar tree expand / collapse.
 *
 * Validates single-folder toggle and DataHub section expand-all / collapse-all
 * against a live local frontend (BASE_URL). Creates its own nested docs — no
 * fixture seeding required.
 */

import { test, expect } from '../../fixtures/base-test';
import { withRandomSuffix } from '../../utils/random';
import { DocumentPage } from '../../pages/entity/document.page';
import { TIMEOUTS } from '../../utils/constants';

test.describe('Document tree expand / collapse', () => {
  let documentPage: DocumentPage;

  test.beforeEach(async ({ apiMock, page }) => {
    test.setTimeout(180_000);
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
    documentPage = new DocumentPage(page);
    await documentPage.navigateToDocuments();
  });

  test('should expand and collapse a single document folder in the sidebar', async ({ cleanup }) => {
    const base = withRandomSuffix('doc-expand');
    const parentTitle = `${base}_parent`;
    const childTitle = `${base}_child`;

    const parentUrn = await documentPage.createDocumentWithTitle(parentTitle);
    cleanup.track(parentUrn);
    const childUrn = await documentPage.createChildUnderParentViaPlus(parentUrn, childTitle);
    cleanup.track(childUrn);

    // Prefer sidebar selection over full navigation — large libraries keep network busy.
    await documentPage.getTreeItem(parentUrn).click();
    await expect(documentPage.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await documentPage.expectTreeItemVisibleInSidebar(parentUrn);

    const expandButton = documentPage.getTreeItemExpandButton(parentUrn);
    await expandButton.hover({ force: true });
    if ((await expandButton.getAttribute('aria-expanded')) === 'true') {
      await documentPage.toggleTreeItemExpand(parentUrn);
    }
    await documentPage.expectTreeItemCollapsed(parentUrn);
    await documentPage.expectTreeItemHiddenInSidebar(childUrn);

    await documentPage.toggleTreeItemExpand(parentUrn);
    await documentPage.expectTreeItemExpanded(parentUrn);
    await documentPage.expectTreeItemVisibleInSidebar(childUrn);

    await documentPage.toggleTreeItemExpand(parentUrn);
    await documentPage.expectTreeItemCollapsed(parentUrn);
    await documentPage.expectTreeItemHiddenInSidebar(childUrn);
  });

  test('should expand and collapse all documents in the DataHub section', async ({ cleanup }) => {
    const base = withRandomSuffix('doc-expand-all');
    const parentTitle = `${base}_parent`;
    const childTitle = `${base}_child`;

    const parentUrn = await documentPage.createDocumentWithTitle(parentTitle);
    cleanup.track(parentUrn);
    const childUrn = await documentPage.createChildUnderParentViaPlus(parentUrn, childTitle);
    cleanup.track(childUrn);

    await documentPage.getTreeItem(parentUrn).click();
    await expect(documentPage.titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await documentPage.expectTreeItemVisibleInSidebar(parentUrn);

    // Clear any section-level expansion (other folders / prior navigation).
    await documentPage.ensureDataHubSectionCollapsed();
    await documentPage.expectTreeItemCollapsed(parentUrn);
    await documentPage.expectTreeItemHiddenInSidebar(childUrn);

    await documentPage.clickDataHubSectionExpandAll();
    await documentPage.expectTreeItemExpanded(parentUrn);
    await documentPage.expectTreeItemVisibleInSidebar(childUrn);

    await documentPage.clickDataHubSectionExpandAll();
    await documentPage.expectTreeItemCollapsed(parentUrn);
    await documentPage.expectTreeItemHiddenInSidebar(childUrn);
  });
});
