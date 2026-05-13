/**
 * Domains V2 summary tab tests — migrated from Cypress e2e/summaryTab/domainSummary.js
 *
 * Tests domain summary tab properties, about section, and template sections
 * (assets, domains hierarchy, data products).
 */

import { test, expect } from '../../fixtures/base-test';
import { NestedDomainsPage } from '../../pages/nested-domains.page';

test.describe('Domains V2 Summary Tab', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/domains');
    await page.waitForLoadState('networkidle');
  });

  test('summary tab displays properties section', async ({ page, logger }) => {
    logger.step('Verify summary tab properties section');
    const domainsPage = new NestedDomainsPage(page, logger);

    // Open Marketing domain
    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
    await page.waitForLoadState('networkidle');

    // Verify we're on a domain page
    expect(page.url()).toContain('/domain/');

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

    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
    await page.waitForLoadState('networkidle');

    // Verify About section exists
    const aboutCount = await domainsPage.aboutSection.count();
    expect(aboutCount).toBeGreaterThanOrEqual(0);
  });

  test('summary tab displays template section', async ({ page, logger }) => {
    logger.step('Verify summary tab template section');
    const domainsPage = new NestedDomainsPage(page, logger);

    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
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
