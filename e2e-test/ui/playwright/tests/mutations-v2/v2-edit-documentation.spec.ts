/**
 * V2 Edit documentation and link management tests
 * Migrated from Cypress e2e/mutationsV2/v2_edit_documentation.js
 *
 * Tests the Documentation tab: editing descriptions, adding/updating/removing links,
 * and verifying link visibility in the sidebar, entity header, and documentation tab.
 *
 * Prerequisites: SamplePlaywrightHdfsDataset must exist in the test environment.
 */

import { test, expect } from '../../fixtures/base-test';
import { EntityDocumentationPage } from '../../pages/entity-documentation.page';

test.use({ featureName: 'mutations-v2' });

const SAMPLE_DATASET_NAME = 'SamplePlaywrightHdfsDataset';
const SAMPLE_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsDataset,PROD)';

// Unique test ID generated once per test run to avoid collisions
const testId = `test_v2_edit_documentation_${new Date().getTime()}`;
const SAMPLE_DOCUMENTATION = `This is ${testId} documentation EDITED`;

function getSampleUrl(path?: string): string {
  const url = `https://${testId}.com`;
  if (!path) return url;
  return `${url}/${path}`;
}

// Tests are serial because they mutate the same dataset's documentation/links.
test.describe.configure({ mode: 'serial' });

test.describe('edit documentation and link to dataset', () => {
  let docPage: EntityDocumentationPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    docPage = new EntityDocumentationPage(page, logger, logDir);
    await docPage.navigateToDatasetDocumentationTab(SAMPLE_DATASET_URN, SAMPLE_DATASET_NAME);
  });

  test('should allow to edit the documentation', async ({ page }) => {
    // Documentation tab is already open from beforeEach — navigate directly to editing.
    await docPage.editDocumentation(SAMPLE_DOCUMENTATION);
    await expect(page.getByText(SAMPLE_DOCUMENTATION).first()).toBeVisible();
    await docPage.clearDocumentation();
  });

  test('should validate add link form', async ({ page }) => {
    await docPage.openAddLinkForm();

    // URL validation
    await docPage.urlInput.fill('incorrect_url');
    await expect(page.getByText('This field must be a valid url.')).toBeVisible();

    // URL is required
    await docPage.urlInput.clear();
    await expect(page.getByText('A URL is required.')).toBeVisible();

    // Label is required — type a valid URL, then clear the label
    await docPage.labelInput.fill('label');
    await docPage.labelInput.clear();
    await expect(page.getByText('A label is required.')).toBeVisible();
  });

  test('should successfully add new link', async () => {
    const sample = getSampleUrl('add-new-link');

    await docPage.addLink(sample, sample, false);

    await docPage.expectLinkInDocumentationTab(sample);
    await docPage.expectLinkInSidebar(sample);
    await docPage.expectLinkNotInEntityHeader(sample);

    await docPage.removeLinkByUrl(sample);
  });

  test('should successfully add new link with showing in asset preview', async ({ page }) => {
    const sample = getSampleUrl('add-with-show-in-preview');

    await docPage.addLink(sample, sample, true);

    await docPage.expectLinkInDocumentationTab(sample);
    await docPage.expectLinkInEntityHeader(sample);
    await docPage.expectLinkNotInSidebar(sample);

    // Verify link is visible in entity header from the search page too
    await page.goto(`/search?query=${SAMPLE_DATASET_NAME}`);
    await page.waitForTimeout(3000);
    await expect(page.getByText(SAMPLE_DATASET_NAME).first()).toBeVisible({ timeout: 30000 });
    await docPage.expectLinkInEntityHeader(sample);

    // Return to documentation tab and remove the link
    await docPage.navigateToDatasetDocumentationTab(SAMPLE_DATASET_URN, SAMPLE_DATASET_NAME);
    await docPage.removeLinkByUrl(sample);
  });

  test('should collapse links in the entity header', async () => {
    const sample1 = getSampleUrl('collapse1');
    const sample2 = getSampleUrl('collapse2');
    const sample3 = getSampleUrl('collapse3');

    await docPage.addLink(sample1, sample1, true);
    await docPage.addLink(sample2, sample2, true);
    await docPage.addLink(sample3, sample3, true);

    await docPage.expectLinkInDocumentationTab(sample1);
    await docPage.expectLinkInDocumentationTab(sample2);
    await docPage.expectLinkInDocumentationTab(sample3);
    await docPage.expectLinkNotInSidebar(sample1);
    await docPage.expectLinkNotInSidebar(sample2);
    await docPage.expectLinkNotInSidebar(sample3);
    await docPage.expectLinkInEntityHeader(sample1);
    // sample2 and sample3 overflow into the dropdown — just verify container is visible
    await expect(docPage.platformLinksContainer).toBeVisible();

    await docPage.removeLinkByUrl(sample1);
    await docPage.removeLinkByUrl(sample2);
    await docPage.removeLinkByUrl(sample3);
  });

  test('should successfully update the link', async () => {
    const sampleUrl = getSampleUrl('edit_link');
    const sampleEditedUrl = getSampleUrl('edit_link_edited');

    await docPage.addLink(sampleUrl, sampleUrl, false);

    await docPage.expectLinkInDocumentationTab(sampleUrl);
    await docPage.expectLinkInSidebar(sampleUrl);
    await docPage.expectLinkNotInEntityHeader(sampleUrl);

    await docPage.updateLink(sampleUrl, sampleUrl, sampleEditedUrl, sampleEditedUrl, true);

    await docPage.expectLinkNotInDocumentationTab(sampleUrl);
    await docPage.expectLinkInDocumentationTab(sampleEditedUrl);
    await docPage.expectLinkInEntityHeader(sampleEditedUrl);
    await docPage.expectLinkNotInSidebar(sampleEditedUrl);

    // Update again: move back to not showing in preview
    await docPage.updateLink(sampleEditedUrl, sampleEditedUrl, sampleEditedUrl, sampleEditedUrl, false);

    await docPage.expectLinkInDocumentationTab(sampleEditedUrl);
    await docPage.expectLinkInSidebar(sampleEditedUrl);

    await docPage.removeLinkByUrl(sampleEditedUrl);
  });

  test('should successfully remove the link', async () => {
    const sampleUrl = getSampleUrl('remove_link');

    await docPage.addLink(sampleUrl, sampleUrl, true);

    await docPage.expectLinkInDocumentationTab(sampleUrl);
    await docPage.expectLinkInEntityHeader(sampleUrl);

    await docPage.removeLinkByUrl(sampleUrl);

    await docPage.expectLinkNotInDocumentationTab(sampleUrl);
    await docPage.expectLinkNotInEntityHeader(sampleUrl);
  });
});
