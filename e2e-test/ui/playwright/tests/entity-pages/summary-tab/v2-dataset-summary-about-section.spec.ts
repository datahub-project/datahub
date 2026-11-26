/**
 * Dataset Summary Tab — About Section Tests
 *
 * With datasetSummaryPageV1 on (the default), a dataset's documentation and links are edited
 * from the Summary tab's About section rather than a standalone Documentation tab. This covers
 * that flow for a Dataset; v2-summary-tab-about-section.spec.ts covers the same section for a
 * Domain under assetSummaryPageV1, and mutations-v2/v2-edit-documentation.spec.ts covers the
 * legacy Documentation tab with the flag pinned off.
 *
 * Uses pre-seeded global-data entity: SamplePlaywrightHdfsAssertionDataset (no other spec mutates it).
 * Feature flags required: showNavBarRedesign=true, datasetSummaryPageV1=true
 */

import { test, expect } from '../../../fixtures/base-test';
import { SummaryTabPage } from '../../../pages/entity/summary-tab/summary-tab.page';
import { TIMEOUTS, LOAD_STATES } from '../../../utils/constants';
import {
  LINK_LABELS,
  DESCRIPTIONS,
  LINK_URL_BASE,
  LINK_URL_BASE_UPDATED,
} from '../../../pages/entity/summary-tab/constants';
import { DomainTag, TestTag } from '../../../utils/test-tags';

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hdfs,SamplePlaywrightHdfsAssertionDataset,PROD)';

const FEATURE_FLAGS = {
  showNavBarRedesign: true,
  datasetSummaryPageV1: true,
} as const;

test.describe('Dataset Summary Tab About Section Tests', () => {
  let summaryTabPage: SummaryTabPage;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    await apiMock.setFeatureFlags(FEATURE_FLAGS);
    summaryTabPage = new SummaryTabPage(page, logger, logDir);
    await summaryTabPage.navigate(`/dataset/${encodeURIComponent(DATASET_URN)}`);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(summaryTabPage.summaryTabHeader).toBeVisible({ timeout: TIMEOUTS.LONG });
    await summaryTabPage.summaryTabHeader.click();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  });

  test('dataset about section - description', { tag: [TestTag.P0, DomainTag.Catalog] }, async () => {
    await summaryTabPage.ensureAboutSectionVisible();

    const timestamp = Date.now();
    const description1 = `${DESCRIPTIONS.INITIAL} ${timestamp}`;
    await summaryTabPage.updateDescription(description1, TIMEOUTS.MEDIUM);
    await summaryTabPage.expectDescriptionContains(description1);

    const description2 = `${DESCRIPTIONS.UPDATED} ${timestamp}`;
    await summaryTabPage.updateDescription(description2, TIMEOUTS.MEDIUM);
    await summaryTabPage.expectDescriptionContains(description2);

    await summaryTabPage.updateDescription('', TIMEOUTS.MEDIUM);
  });

  test('dataset about section - links', { tag: [DomainTag.Catalog] }, async () => {
    await summaryTabPage.ensureAboutSectionVisible();

    const timestamp = Date.now();

    const url1 = `${LINK_URL_BASE}/${timestamp}`;
    const label1 = LINK_LABELS.INITIAL;
    await summaryTabPage.addLink(url1, label1, TIMEOUTS.MEDIUM);
    await summaryTabPage.expectLinkExists(url1, label1, TIMEOUTS.MEDIUM);

    const url2 = `${LINK_URL_BASE_UPDATED}/${timestamp}`;
    const label2 = LINK_LABELS.UPDATED;
    await summaryTabPage.updateLink(url1, label1, url2, label2, TIMEOUTS.MEDIUM);
    await summaryTabPage.expectLinkExists(url2, label2, TIMEOUTS.MEDIUM);
    await summaryTabPage.expectLinkNotExists(url1, label1, TIMEOUTS.MEDIUM);

    await summaryTabPage.removeLink(url2, label2, TIMEOUTS.MEDIUM);
    await summaryTabPage.expectLinkNotExists(url2, label2, TIMEOUTS.MEDIUM);
  });
});
