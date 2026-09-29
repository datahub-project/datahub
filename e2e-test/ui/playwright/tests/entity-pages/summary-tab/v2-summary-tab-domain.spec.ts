/**
 * Domain Summary Tab Tests
 *
 * Tests summary tab functionality for domains:
 * - Properties section (Created, Owners)
 * - About section (description and links)
 * - Template section (assets, domains, data products modules)
 *
 * Uses pre-seeded global-data entities: domain:testing
 * Feature flags required: showNavBarRedesign=true, assetSummaryPageV1=true
 */

import { test, expect } from '../../../fixtures/base-test';
import { SummaryTabPage } from '../../../pages/entity/summary-tab/summary-tab.page';
import { TIMEOUTS, LOAD_STATES } from '../../../utils/constants';
import {
  ENTITY_URNS,
  PROPERTY_KEYS,
  EXPECTED_VALUES,
  OWNER_URN,
  MODULE_TYPES,
} from '../../../pages/entity/summary-tab/constants';

const FEATURE_FLAGS = {
  showNavBarRedesign: true,
  assetSummaryPageV1: true,
} as const;

test.describe('Domain Summary Tab', () => {
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

  test('summary tab - properties, about, and modules', async () => {
    await summaryTabPage.ensurePropertiesSectionVisible();
    await summaryTabPage.expectPropertyExists(PROPERTY_KEYS.CREATED);
    await summaryTabPage.expectPropertyExists(PROPERTY_KEYS.OWNERS);
    await summaryTabPage.expectOwnerExists(OWNER_URN, TIMEOUTS.LONG);

    await summaryTabPage.ensureAboutSectionVisible();

    await summaryTabPage.ensureTemplateSectionVisible();
    await expect(summaryTabPage.assetsModule).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
    await summaryTabPage.expectModuleContentContains(
      MODULE_TYPES.ASSETS,
      EXPECTED_VALUES.DOMAIN_ASSETS_MODULE,
      TIMEOUTS.MEDIUM,
    );
    await expect(summaryTabPage.hierarchyModule).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
    await summaryTabPage.expectModuleContentContains(
      MODULE_TYPES.HIERARCHY,
      EXPECTED_VALUES.DOMAIN_HIERARCHY_MODULE,
      TIMEOUTS.MEDIUM,
    );
    await expect(summaryTabPage.getModule(MODULE_TYPES.DATA_PRODUCTS)).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
    await summaryTabPage.expectModuleContentContains(
      MODULE_TYPES.DATA_PRODUCTS,
      EXPECTED_VALUES.DOMAIN_DATA_PRODUCTS_MODULE,
      TIMEOUTS.MEDIUM,
    );
  });
});
