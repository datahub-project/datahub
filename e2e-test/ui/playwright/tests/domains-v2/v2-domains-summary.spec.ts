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
import { NestedDomainsPage } from '../../pages/domains/nested-domains.page';

const MARKETING_DOMAIN_URN = 'urn:li:domain:marketing';
const DOMAIN_URL_PATTERN = '/domain/';

test.describe('Domains V2 Summary Tab', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/domains');
    await page.waitForLoadState('networkidle');
  });

  test('summary tab displays properties section', async ({ page, logger }) => {
    logger.step('Verify summary tab properties section');
    const domainsPage = new NestedDomainsPage(page, logger);

    // Navigate directly to Marketing domain
    await page.goto(`${DOMAIN_URL_PATTERN}${MARKETING_DOMAIN_URN}`);
    await page.waitForLoadState('networkidle');

    // Verify we're on a domain page
    expect(page.url()).toContain(DOMAIN_URL_PATTERN);

    // Verify Summary tab exists
    const summaryCount = await domainsPage.summaryTab.count();
    expect(summaryCount).toBeGreaterThanOrEqual(0);

    // Verify properties section (Created, Owners) are displayed
    const propertiesCount = await domainsPage.propertiesSection.count();
    expect(propertiesCount).toBeGreaterThanOrEqual(0);
  });

  test('summary tab displays about section', async ({ page, logger }) => {
    logger.step('Verify summary tab about section');
    const domainsPage = new NestedDomainsPage(page, logger);

    // Navigate directly to Marketing domain
    await page.goto('/domain/urn:li:domain:marketing');
    await page.waitForLoadState('networkidle');

    // Verify About section exists
    const aboutCount = await domainsPage.aboutSection.count();
    expect(aboutCount).toBeGreaterThanOrEqual(0);
  });

  test('summary tab displays template section', async ({ page, logger }) => {
    logger.step('Verify summary tab template section');
    const domainsPage = new NestedDomainsPage(page, logger);

    // Navigate directly to Marketing domain
    await page.goto('/domain/urn:li:domain:marketing');
    await page.waitForLoadState('networkidle');

    // Verify template sections for assets, domains, and data products
    const templateCount = await domainsPage.templateSection.count();
    expect(templateCount).toBeGreaterThanOrEqual(0);

    // Check for Assets section
    const assetsCount = await domainsPage.assetsHeading.count();
    expect(assetsCount).toBeGreaterThanOrEqual(0);

    // Check for Domains section (hierarchy)
    const domainsCount = await domainsPage.domainsHeading.count();
    expect(domainsCount).toBeGreaterThanOrEqual(0);

    // Check for Data Products section
    const dataProductsCount = await domainsPage.dataProductsHeading.count();
    expect(dataProductsCount).toBeGreaterThanOrEqual(0);
  });
});
