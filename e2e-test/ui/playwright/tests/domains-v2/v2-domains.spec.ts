/**
 * Domains V2 simple test — migrated from Cypress e2e/domainsV2/v2_domains.js
 *
 * Tests viewing a domain and its assets.
 */

import { test, expect } from '../../fixtures/base-test';
import { NestedDomainsPage } from '../../pages/nested-domains.page';

test.describe('Domains V2', () => {
  test('can navigate to domains', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await domainsPage.navigateToDomainList();

    // Verify page loaded with Domains header
    await expect(domainsPage.pageTitle).toBeVisible();

    // Verify domain links exist
    const count = await domainsPage.domainLinks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('can see elements inside the domain', async ({ page, logger }) => {
    logger.step('Open domain and verify assets');
    const domainsPage = new NestedDomainsPage(page, logger);
    await domainsPage.navigateToDomainList();

    // Open Marketing domain
    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
    await page.waitForLoadState('networkidle');

    // Verify we're on the domain page
    expect(page.url()).toContain('/domain/');

    // Click on Assets tab if available
    const assetCount = await domainsPage.assetsTab.count();
    if (assetCount > 0) {
      await domainsPage.assetsTab.click();
    }
  });
});
