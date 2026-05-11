/**
 * Glossary Term V2 tests — migrated from PlaywrightGlossaryTag e2e/glossaryV2/v2_glossaryTerm.js
 *
 * Tests glossary term operations from the entity profile:
 *   1. Search related entities by query.
 *   2. Apply tag filters on related entities.
 *   3. Use advanced search to filter by tag.
 *
 * Prerequisites (all seeded via tests/glossary/fixtures/data.json):
 *   - PlaywrightGlossaryTermTests node containing three terms:
 *     - PlaywrightSearchRelated → linked to PlaywrightGlossarySearchDataset (tagged PlaywrightGlossarySearchTag)
 *     - PlaywrightFilterRelated → linked to PlaywrightGlossaryFilterDataset (tagged PlaywrightGlossaryFilterTag)
 *     - PlaywrightTaggedRelated → linked to PlaywrightGlossaryTaggedDataset (tagged PlaywrightGlossaryTag)
 */

import { test } from '../../fixtures/base-test';
import { GlossaryPage } from '../../pages/glossary.page';

test.use({ featureName: 'glossary' });

const SEARCH_TERM_URN = 'urn:li:glossaryTerm:PlaywrightGlossaryTermTests.PlaywrightSearchRelated';
const FILTER_TERM_URN = 'urn:li:glossaryTerm:PlaywrightGlossaryTermTests.PlaywrightFilterRelated';
const TAGGED_TERM_URN = 'urn:li:glossaryTerm:PlaywrightGlossaryTermTests.PlaywrightTaggedRelated';

const SEARCH_DATASET = 'PlaywrightGlossarySearchDataset';
const SEARCH_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightGlossarySearchDataset,PROD)';

const FILTER_TAG_URN = 'urn:li:tag:PlaywrightGlossaryFilterTag';
const FILTER_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightGlossaryFilterDataset,PROD)';

const GLOSSARY_TAG = 'PlaywrightGlossaryTag';
const TAGGED_DATASET = 'PlaywrightGlossaryTaggedDataset';
const TAGGED_DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,PlaywrightGlossaryTaggedDataset,PROD)';

test.describe('glossary term entity operations', () => {
  let glossaryPage: GlossaryPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    glossaryPage = new GlossaryPage(page, logger, logDir);
  });

  test('can search related entities by query', async () => {
    await glossaryPage.navigateToGlossaryTermByUrn(SEARCH_TERM_URN);
    await glossaryPage.openRelatedAssetsTab();
    await glossaryPage.searchWithinEntityPage(SEARCH_DATASET);
    await glossaryPage.expectPreviewEntityByUrn(SEARCH_DATASET_URN);
  });

  test('can apply filters on related entities', async () => {
    await glossaryPage.navigateToGlossaryTermByUrn(FILTER_TERM_URN);
    await glossaryPage.openRelatedAssetsTab();
    await glossaryPage.applyFacetTagFilter(FILTER_TAG_URN);
    await glossaryPage.expectPreviewEntityByUrn(FILTER_DATASET_URN);
  });

  test('can search related entities by a specific tag using advanced search', async () => {
    await glossaryPage.navigateToGlossaryTermByUrn(TAGGED_TERM_URN);
    await glossaryPage.openRelatedAssetsTab();
    await glossaryPage.filterRelatedAssetsByTag(GLOSSARY_TAG);
    await glossaryPage.expectPreviewEntityByUrn(TAGGED_DATASET_URN);
    await glossaryPage.searchWithinEntityPage(TAGGED_DATASET);
    await glossaryPage.expectPreviewEntityByUrn(TAGGED_DATASET_URN);
  });
});
