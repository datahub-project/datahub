/**
 * About Section Summary Tab Tests
 *
 * Tests for about section functionality:
 * - Description updates
 * - Adding and removing links
 *
 * Uses pre-seeded global-data entities: domain:testing
 * Feature flags required: showNavBarRedesign=true, assetSummaryPageV1=true
 */

import { test, expect } from '../../../fixtures/base-test';
import { SummaryTabPage } from '../../../pages/entity/summary-tab/summary-tab.page';
import { TIMEOUTS, LOAD_STATES } from '../../../utils/constants';
import {
  ENTITY_URNS,
  LINK_LABELS,
  DESCRIPTIONS,
  LINK_URL_BASE,
  LINK_URL_BASE_UPDATED,
} from '../../../pages/entity/summary-tab/constants';

const FEATURE_FLAGS = {
  showNavBarRedesign: true,
  assetSummaryPageV1: true,
} as const;

test.describe('About Section Tests', () => {
  let summaryTabPage: SummaryTabPage;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    await apiMock.setFeatureFlags(FEATURE_FLAGS);
    summaryTabPage = new SummaryTabPage(page, logger, logDir);
    await summaryTabPage.navigate(`/domain/${encodeURIComponent(ENTITY_URNS.DOMAIN)}`);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(summaryTabPage.summaryTabHeader).toBeVisible({ timeout: TIMEOUTS.LONG });
    await summaryTabPage.summaryTabHeader.click();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  });

  test('about section - description', async () => {
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

  test('about section - links', async () => {
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
