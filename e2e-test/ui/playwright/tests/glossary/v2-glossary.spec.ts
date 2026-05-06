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
 * Each test run uses a unique timestamp-based suffix so tests are independent
 * and can run in parallel with other suites without colliding on entity names.
 *
 * Prerequisites: `PlaywrightNode` glossary node and
 *   `SamplePlaywrightGlossaryDataset` dataset must exist (seeded via fixtures/data.json).
 */

import { test, expect } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';

test.use({ featureName: 'glossary' });

// Use serial mode because these tests mutate shared state (the same term group and term).
test.describe.configure({ mode: 'serial' });

const runId = Date.now();
const TERM_GROUP_NAME = `PlaywrightGlossaryGroup${runId}`;
const TERM_NAME = `PlaywrightGlossaryTerm${runId}`;

const DATASET_PATH = 'dataset/urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightGlossaryDataset,PROD)/';
const DATASET_NAME = 'SamplePlaywrightGlossaryDataset';

test.describe('glossary term group and term lifecycle', () => {
  test('create term group at root level', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();

    await glossaryPage.createTermGroup(TERM_GROUP_NAME);

    await glossaryPage.navigateToGlossary();
    await glossaryPage.expectTextVisible(TERM_GROUP_NAME);
  });

  test('create glossary term inside term group', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();

    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.navigateToEntityContentsTab();
    await glossaryPage.createTermInContentsTab(TERM_NAME);
  });

  test('add glossary term to dataset via sidebar', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.addGlossaryTermToDataset(DATASET_PATH, DATASET_NAME, TERM_NAME);
  });

  test('delete glossary term', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();

    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.navigateToGlossaryTerm(TERM_NAME);
    await glossaryPage.deleteCurrentEntity();
    await expect(page.getByText('Deleted Glossary Term!')).toBeVisible({ timeout: 15000 });

    await glossaryPage.navigateToGlossary();
    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.expectTextNotPresent(TERM_NAME);
  });

  test('delete term group', async ({ page, logger, logDir }) => {
    const glossaryPage = new GlossaryPage(page, logger, logDir);
    await glossaryPage.navigateToGlossary();

    await glossaryPage.navigateToGlossaryTerm(TERM_GROUP_NAME);
    await glossaryPage.deleteCurrentEntity();
    await expect(page.getByText('Deleted Term Group!')).toBeVisible({ timeout: 15000 });

    await glossaryPage.navigateToGlossary();
    await glossaryPage.expectTextNotPresent(TERM_GROUP_NAME);
  });
});
