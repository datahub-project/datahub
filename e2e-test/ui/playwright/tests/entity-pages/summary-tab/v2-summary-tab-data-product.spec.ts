/**
 * Data Product Summary Tab Tests
 *
 * Tests summary tab functionality for data products:
 * - Properties section (Created, Owners, Domain, Tags, Glossary Terms)
 * - About section (description and links)
 * - Template section (assets module)
 *
 * Uses pre-seeded global-data entities: dataProduct:testing
 * Feature flags required: showNavBarRedesign=true, assetSummaryPageV1=true
 */

import { test, expect } from '../../../fixtures/base-test';
import { SummaryTabPage } from '../../../pages/entity/summary-tab/summary-tab.page';
import { TIMEOUTS, LOAD_STATES } from '../../../utils/constants';
import { ENTITY_URNS, PROPERTY_KEYS, EXPECTED_VALUES, MODULE_TYPES } from '../../../pages/entity/summary-tab/constants';

const FEATURE_FLAGS = {
  showNavBarRedesign: true,
  assetSummaryPageV1: true,
} as const;

test.describe('Data Product Summary Tab', () => {
  let summaryTabPage: SummaryTabPage;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    await apiMock.setFeatureFlags(FEATURE_FLAGS);
    summaryTabPage = new SummaryTabPage(page, logger, logDir);

    await summaryTabPage.navigate(`/dataProduct/${encodeURIComponent(ENTITY_URNS.DATA_PRODUCT)}`);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await expect(summaryTabPage.summaryTabHeader).toBeVisible({ timeout: TIMEOUTS.LONG });
    await summaryTabPage.summaryTabHeader.click();
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  });

  test('summary tab - properties and modules', async () => {
    await summaryTabPage.ensurePropertiesSectionVisible();

    await summaryTabPage.expectPropertyExists(PROPERTY_KEYS.CREATED);
    await summaryTabPage.expectPropertyExists(PROPERTY_KEYS.OWNERS);
    await summaryTabPage.expectPropertyExistsWithValue(
      PROPERTY_KEYS.DOMAIN,
      EXPECTED_VALUES.DATA_PRODUCT_DOMAIN,
      TIMEOUTS.LONG,
    );
    await summaryTabPage.expectPropertyExistsWithValue(
      PROPERTY_KEYS.TAGS,
      EXPECTED_VALUES.DATA_PRODUCT_TAG,
      TIMEOUTS.LONG,
    );
    await summaryTabPage.expectPropertyExistsWithValue(
      PROPERTY_KEYS.GLOSSARY_TERMS,
      EXPECTED_VALUES.DATA_PRODUCT_GLOSSARY_TERM,
      TIMEOUTS.LONG,
    );

    await summaryTabPage.ensureAboutSectionVisible();

    await summaryTabPage.ensureTemplateSectionVisible();
    await expect(summaryTabPage.assetsModule).toBeVisible({
      timeout: TIMEOUTS.MEDIUM,
    });
    await summaryTabPage.expectModuleContentContains(
      MODULE_TYPES.ASSETS,
      EXPECTED_VALUES.DATA_PRODUCT_ASSETS_MODULE,
      TIMEOUTS.MEDIUM,
    );
  });
});
