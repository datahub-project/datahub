/**
 * Domains V2 Summary Tab Tests
 *
 * Tests domain summary tab content and sections:
 * - Properties section (created timestamp, owners)
 * - About section (description)
 * - Template sections (assets, domains, data products)
 *
 * Prerequisites:
 * - Marketing domain must exist in the system
 *
 * Related files:
 * - v2-domains-core.spec.ts — Domain creation and navigation
 * - v2-domains-advanced.spec.ts — Domain operations (move, edit, docs, links, owners)
 */

import { test, expect } from '../../fixtures/base-test';
import { DomainEntityPage } from '../../pages/domains/domain-entity.page';
import { LOAD_STATES } from '../../utils/constants';

const MARKETING_DOMAIN_URN = 'urn:li:domain:marketing';
const DOMAIN_URL_PATTERN = '/domain/';

test.describe('Domains V2 Summary Tab', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/domains');
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Navigate to Marketing domain for all summary tests
    await page.goto(`${DOMAIN_URL_PATTERN}${MARKETING_DOMAIN_URN}`);
    await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  });

  test('summary tab displays properties section', async ({ page, logger }) => {
    const domainsPage = new DomainEntityPage(page, logger);

    // Verify properties section is visible
    await expect(domainsPage.propertiesSection).toBeVisible();
  });

  test('summary tab displays about section', async ({ page, logger }) => {
    const domainsPage = new DomainEntityPage(page, logger);

    // Verify about section is visible
    await expect(domainsPage.aboutSection).toBeVisible();
  });

  test('summary tab displays template section', async ({ page, logger }) => {
    const domainsPage = new DomainEntityPage(page, logger);

    // Verify template wrapper and all module sections are visible
    await expect(domainsPage.templateSection).toBeVisible();
    await expect(domainsPage.assetsHeading).toBeVisible();
    await expect(domainsPage.domainsHeading).toBeVisible();
    await expect(domainsPage.dataProductsHeading).toBeVisible();
  });
});
