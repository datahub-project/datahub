/**
 * Glossary V2 tests — migrated from Cypress e2e/glossaryV2/v2_glossary.js
 *
 * Tests the full lifecycle of a Glossary Term Group and Glossary Term:
 *   1. Create a root-level Term Group.
 *   2. Create a Glossary Term inside the group.
 *   3. Assign the term to a dataset via the entity sidebar.
 *   4. Delete the term; verify it is no longer visible.
 *   5. Delete the term group; verify it is no longer visible.
 *
 * Prerequisites:
 *   `SamplePlaywrightGlossaryDataset` dataset must exist (seeded via fixtures/data.json).
 */

import { test } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';
import { DatasetPage } from '../../pages/dataset.page';
import { withRandomSuffix } from '../../utils/random';

test.use({ featureName: 'glossary' });

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightGlossaryDataset,PROD)';
const BATCH_ADD_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightGlossaryBatchAddDataset,PROD)';

test.describe('glossary term group and term lifecycle', () => {
  let glossaryPage: GlossaryPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();
  });

  test('create term group at root level', async ({ cleanup }) => {
    const termGroupName = withRandomSuffix('GlossaryGroup');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroupName);
    cleanup.track(termGroupUrn);

    await glossaryPage.navigateToGlossary();
    await glossaryPage.expectSidebarContainsNode(termGroupUrn);
  });

  test('create glossary term inside term group', async ({ cleanup }) => {
    const termGroupName = withRandomSuffix('GlossaryGroup');
    const termName = withRandomSuffix('GlossaryTerm');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroupName);
    cleanup.track(termGroupUrn);
    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.openContentsTab();
    const termUrn = await glossaryPage.createTermInContentsTab(termName);
    cleanup.track(termUrn);

    await glossaryPage.expectEntityInContentsTab(termUrn);
  });

  test('add glossary term to dataset via sidebar', async ({ page, logger, logDir, cleanup }) => {
    const termGroupName = withRandomSuffix('GlossaryGroup');
    const termName = withRandomSuffix('GlossaryTerm');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroupName);
    cleanup.track(termGroupUrn);
    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.openContentsTab();
    const termUrn = await glossaryPage.createTermInContentsTab(termName);
    cleanup.track(termUrn);

    const datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(DATASET_URN);
    await datasetPage.addGlossaryTerm(termName);
    await datasetPage.expectGlossaryTermVisible(termName);
    await datasetPage.removeGlossaryTerm(termName);
    await datasetPage.expectGlossaryTermNotVisible(termName);
  });

  test('batch add term to dataset via Add to Assets', async ({ page, logger, logDir, cleanup }) => {
    const termGroupName = withRandomSuffix('GlossaryGroup');
    const termName = withRandomSuffix('GlossaryTerm');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroupName);
    cleanup.track(termGroupUrn);
    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.openContentsTab();
    const termUrn = await glossaryPage.createTermInContentsTab(termName);
    cleanup.track(termUrn);

    await glossaryPage.navigateToGlossaryTermByUrn(termUrn);
    await glossaryPage.clickBatchAddButton();
    await glossaryPage.searchInBatchAddModal('PlaywrightGlossaryBatchAddDataset');
    await glossaryPage.selectEntityInBatchAddModal(BATCH_ADD_DATASET_URN);
    await glossaryPage.confirmBatchAdd();

    const datasetPage = new DatasetPage(page, logger, logDir);
    await datasetPage.navigateToDataset(BATCH_ADD_DATASET_URN);
    await datasetPage.expectGlossaryTermVisible(termName);
    await datasetPage.removeGlossaryTerm(termName);
    await datasetPage.expectGlossaryTermNotVisible(termName);
  });

  test('delete glossary term', async ({ cleanup }) => {
    const termGroupName = withRandomSuffix('GlossaryGroup');
    const termName = withRandomSuffix('GlossaryTerm');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroupName);
    cleanup.track(termGroupUrn);
    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.openContentsTab();
    const termUrn = await glossaryPage.createTermInContentsTab(termName);

    await glossaryPage.clickContentsTabItem(termUrn);
    await glossaryPage.deleteCurrentEntity();

    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.openContentsTab();
    await glossaryPage.expectEntityNotInContentsTab(termUrn);
  });

  test('delete term group', async () => {
    const termGroupName = withRandomSuffix('GlossaryGroup');

    const termGroupUrn = await glossaryPage.createTermGroup(termGroupName);

    await glossaryPage.navigateToGlossaryNodeByUrn(termGroupUrn);
    await glossaryPage.deleteCurrentEntity();

    await glossaryPage.navigateToGlossary();
    await glossaryPage.expectSidebarNotContainsNode(termGroupUrn);
  });
});
